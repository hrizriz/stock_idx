from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime
import pendulum
import pandas as pd
import logging

from utils.database import get_database_connection, get_latest_stock_date
from utils.telegram import send_telegram_message
from utils.technical_indicators import (
    calculate_all_rsi_indicators,
    calculate_all_macd_indicators,
    calculate_all_bollinger_bands,
    check_and_create_bollinger_bands_table
)
from utils.trading_signals import (
    filter_by_volatility_liquidity,
    calculate_advanced_indicators,
    backtest_trading_signals,
    send_high_probability_signals,
    send_performance_report
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=5)
}

timeframe_params = {
    'DAILY': {
        'lookback_periods': {
            'rsi': 100, 
            'macd': 150, 
            'bb': 50,
            'advanced': 300,
            'macd_fast': 12,
            'macd_slow': 26,
            'macd_signal': 9
        },
        'hold_period': 5,
        'volatility_params': {
            'min': 1.0, 
            'max': 4.0, 
            'volume_min': 1000000,
            'analysis_period': 30
        },
        'min_probability': 0.8,
        'backtest_period': 180,
        'performance_lookback': 60
    },
    'WEEKLY': {
        'lookback_periods': {
            'rsi': 180, 
            'macd': 250, 
            'bb': 150,
            'advanced': 365,
            'macd_fast': 12,
            'macd_slow': 26,
            'macd_signal': 9
        },
        'hold_period': 15,
        'volatility_params': {
            'min': 1.5, 
            'max': 5.0, 
            'volume_min': 5000000,
            'analysis_period': 90
        },
        'min_probability': 0.75,
        'backtest_period': 365,
        'performance_lookback': 90
    },
    'MONTHLY': {
        'lookback_periods': {
            'rsi': 730, 
            'macd': 730, 
            'bb': 300,
            'advanced': 730,
            'macd_fast': 26,
            'macd_slow': 52,
            'macd_signal': 18
        },
        'hold_period': 20,
        'volatility_params': {
            'min': 2.0, 
            'max': 8.0, 
            'volume_min': 10000000,
            'analysis_period': 180
        },
        'min_probability': 0.7,
        'backtest_period': 730,
        'performance_lookback': 365
    }
}

def should_run_weekly():
    """Check if today is Friday (when we run weekly signals)"""
    now = pendulum.now(local_tz)
    is_friday = now.day_of_week == 4  # 0-6 where 0 is Monday, 4 is Friday
    logger.info(f"Today is day of week {now.day_of_week}, run weekly: {is_friday}")
    return is_friday

def should_run_monthly():
    """Check if today is the 1st of the month (when we run monthly signals)"""
    now = pendulum.now(local_tz)
    is_month_start = now.day == 1
    logger.info(f"Today is day {now.day} of month, run monthly: {is_month_start}")
    return is_month_start

def check_signals_table_exists(signal_type='DAILY'):
    """Check if the signals table exists for the given signal type"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        table_name = 'advanced_trading_signals' if signal_type == 'DAILY' else f"advanced_trading_signals_{signal_type.lower()}"
        
        cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = '{table_name}'
        );
        """)
        
        exists = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        logger.info(f"Signals table for {signal_type} exists: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Error checking if signals table exists: {str(e)}")
        return False

def send_unified_trading_report(**context):
    """
    Send a unified report combining signals from all timeframes
    """
    daily_exec = context['ti'].xcom_pull(task_ids='send_daily_high_probability_signals')
    weekly_exec = context['ti'].xcom_pull(task_ids='send_weekly_high_probability_signals')
    monthly_exec = context['ti'].xcom_pull(task_ids='send_monthly_high_probability_signals')
    
    latest_date = get_latest_stock_date()
    if not latest_date:
        return "No data available for reporting"
    
    date_str = latest_date.strftime('%Y-%m-%d')
    
    message = f"🚀 *UNIFIED TRADING SIGNALS REPORT ({date_str})* 🚀\n\n"
    message += "This report combines trading signals across multiple timeframes.\n\n"
    
    def get_signals_for_timeframe(timeframe):
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            table_name = f"public_analytics.advanced_trading_signals_{timeframe.lower()}" if timeframe != 'DAILY' else "public_analytics.advanced_trading_signals"
            
            cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public_analytics' 
                AND table_name = '{'advanced_trading_signals' if timeframe == 'DAILY' else f"advanced_trading_signals_{timeframe.lower()}"}'
            );
            """)
            
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                cursor.close()
                conn.close()
                logger.warning(f"Table {table_name} does not exist")
                return None
            
            query = f"""
            SELECT 
                s.symbol,
                s.date,
                s.buy_score,
                s.winning_probability,
                s.signal_strength,
                m.name,
                m.close
            FROM {table_name} s
            JOIN public.daily_stock_summary m ON s.symbol = m.symbol AND s.date = m.date
            WHERE s.date = '{latest_date}'
            AND s.winning_probability >= 0.7
            AND s.buy_score >= 6
            ORDER BY s.buy_score DESC, s.winning_probability DESC
            LIMIT 5
            """
            
            try:
                signals_df = pd.read_sql(query, conn)
                cursor.close()
                conn.close()
                return signals_df
            except Exception as e:
                logger.error(f"Error getting {timeframe} signals: {str(e)}")
                cursor.close()
                conn.close()
                return None
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            return None
    
    daily_df = get_signals_for_timeframe('DAILY')
    if daily_df is not None and not daily_df.empty:
        message += "📈 *DAILY TRADING SIGNALS (1-5 DAYS)*\n"
        for i, row in enumerate(daily_df.itertuples(), 1):
            if row.buy_score >= 8:
                strength_emoji = "💪"
            elif row.buy_score >= 6:
                strength_emoji = "👍"
            else:
                strength_emoji = "👌"
            
            message += f"{i}. {strength_emoji} *{row.symbol}* ({row.name})\n"
            message += f"   Harga: Rp{row.close:,.0f} | Skor: {row.buy_score}/10\n"
            message += f"   Probabilitas: {row.winning_probability*100:.0f}%\n"
            
            tp1 = row.close * 1.05  # 5% target
            tp2 = row.close * 1.10  # 10% target
            sl = row.close * 0.97   # 3% stop loss
            
            message += f"   🎯 TP1: Rp{tp1:,.0f} (+5%) | TP2: Rp{tp2:,.0f} (+10%)\n"
            message += f"   🛑 SL: Rp{sl:,.0f} (-3%)\n\n"
        
        message += "\n"
    else:
        message += "*No DAILY signals found for today*\n\n"
    
    if weekly_exec:
        weekly_df = get_signals_for_timeframe('WEEKLY')
        if weekly_df is not None and not weekly_df.empty:
            message += "📆 *WEEKLY TRADING SIGNALS (1-3 WEEKS)*\n"
            for i, row in enumerate(weekly_df.itertuples(), 1):
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Harga: Rp{row.close:,.0f} | Skor: {row.buy_score}/10\n"
                message += f"   Probabilitas: {row.winning_probability*100:.0f}%\n"
                
                tp1 = row.close * 1.08  # 8% target
                tp2 = row.close * 1.15  # 15% target
                sl = row.close * 0.95   # 5% stop loss
                
                message += f"   🎯 TP1: Rp{tp1:,.0f} (+8%) | TP2: Rp{tp2:,.0f} (+15%)\n"
                message += f"   🛑 SL: Rp{sl:,.0f} (-5%)\n\n"
            
            message += "\n"
        else:
            message += "*No WEEKLY signals found for today*\n\n"
    
    if monthly_exec:
        monthly_df = get_signals_for_timeframe('MONTHLY')
        if monthly_df is not None and not monthly_df.empty:
            message += "📅 *MONTHLY TRADING SIGNALS (1-3 MONTHS)*\n"
            for i, row in enumerate(monthly_df.itertuples(), 1):
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Harga: Rp{row.close:,.0f} | Skor: {row.buy_score}/10\n"
                message += f"   Probabilitas: {row.winning_probability*100:.0f}%\n"
                
                tp1 = row.close * 1.12  # 12% target
                tp2 = row.close * 1.20  # 20% target
                sl = row.close * 0.92   # 8% stop loss
                
                message += f"   🎯 TP1: Rp{tp1:,.0f} (+12%) | TP2: Rp{tp2:,.0f} (+20%)\n"
                message += f"   🛑 SL: Rp{sl:,.0f} (-8%)\n\n"
            
            message += "\n"
        else:
            message += "*No MONTHLY signals found for today*\n\n"
    
    message += "🔍 *CROSS-TIMEFRAME CONFLUENCE*\n\n"
    
    symbols = set()
    for df in [daily_df, weekly_df if weekly_exec else None, monthly_df if monthly_exec else None]:
        if df is not None and not df.empty:
            symbols.update(df['symbol'].tolist())
    
    confluence_found = False
    for symbol in symbols:
        timeframes = []
        if daily_df is not None and not daily_df.empty and symbol in daily_df['symbol'].values:
            timeframes.append("Daily")
        if weekly_exec and weekly_df is not None and not weekly_df.empty and symbol in weekly_df['symbol'].values:
            timeframes.append("Weekly")
        if monthly_exec and monthly_df is not None and not monthly_df.empty and symbol in monthly_df['symbol'].values:
            timeframes.append("Monthly")
        
        if len(timeframes) > 1:
            symbol_data = None
            for df in [daily_df, weekly_df, monthly_df]:
                if df is not None and not df.empty and symbol in df['symbol'].values:
                    symbol_data = df[df['symbol'] == symbol].iloc[0]
                    break
            
            if symbol_data is not None:
                message += f"⭐ *{symbol}* ({symbol_data['name']}) - Signal in {', '.join(timeframes)}\n"
                message += f"   Harga: Rp{symbol_data['close']:,.0f} | Multi-timeframe confirmation\n\n"
                confluence_found = True
    
    if not confluence_found:
        message += "No stocks with signals across multiple timeframes today.\n\n"
    
    message += "📝 *TRADING STRATEGY TIPS*\n\n"
    message += "• Match your holding period with the signal timeframe\n"
    message += "• Prioritize signals with score 8+ (highest probability)\n"
    message += "• Look for confluence across multiple timeframes\n"
    message += "• Always use proper position sizing and risk management\n\n"
    
    message += "*Disclaimer:* These signals are generated by algorithms and are not financial advice. Always do your own research before trading."
    
    result = send_telegram_message(message)
    if "successfully" in result:
        return f"Unified trading report sent successfully"
    else:
        return result

with DAG(
    dag_id="unified_trading_signals",
    schedule_interval="0 17 * * 1-5",  # Every weekday at 17:00 Jakarta time
    catchup=False,
    default_args=default_args,
    tags=["trading", "technical", "signals", "unified"]
) as dag:
    
    wait_for_transformation = ExternalTaskSensor(
        task_id="wait_for_transformation",
        external_dag_id="data_transformation",
        external_task_id="end_task",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )
    
    check_bb = PythonOperator(
        task_id="check_bollinger_bands_table",
        python_callable=check_and_create_bollinger_bands_table
    )
    
    check_weekly = ShortCircuitOperator(
        task_id="check_weekly_run",
        python_callable=should_run_weekly
    )
    
    check_monthly = ShortCircuitOperator(
        task_id="check_monthly_run",
        python_callable=should_run_monthly
    )
    
    
    calc_daily_rsi = PythonOperator(
        task_id="calculate_daily_rsi",
        python_callable=calculate_all_rsi_indicators,
        op_kwargs={
            'lookback_period': timeframe_params['DAILY']['lookback_periods']['rsi'], 
            'rsi_period': 14, 
            'signal_type': 'DAILY'
        }
    )
    
    calc_daily_macd = PythonOperator(
        task_id="calculate_daily_macd",
        python_callable=calculate_all_macd_indicators,
        op_kwargs={
            'lookback_period': timeframe_params['DAILY']['lookback_periods']['macd'], 
            'fast_period': timeframe_params['DAILY']['lookback_periods'].get('macd_fast', 12),
            'slow_period': timeframe_params['DAILY']['lookback_periods'].get('macd_slow', 26), 
            'signal_period': timeframe_params['DAILY']['lookback_periods'].get('macd_signal', 9),
            'signal_type': 'DAILY'
        }
    )
    
    calc_daily_bb = PythonOperator(
        task_id="calculate_daily_bollinger",
        python_callable=calculate_all_bollinger_bands,
        op_kwargs={
            'lookback_period': timeframe_params['DAILY']['lookback_periods']['bb'], 
            'band_period': 20, 
            'std_dev': 2, 
            'signal_type': 'DAILY'
        }
    )
    
    filter_daily_stocks = PythonOperator(
        task_id="filter_daily_stocks",
        python_callable=filter_by_volatility_liquidity,
        op_kwargs={
            'analysis_period': timeframe_params['DAILY']['volatility_params']['analysis_period'],
            'signal_type': 'DAILY',
            'volatility_min': timeframe_params['DAILY']['volatility_params']['min'],
            'volatility_max': timeframe_params['DAILY']['volatility_params']['max'],
            'volume_min': timeframe_params['DAILY']['volatility_params']['volume_min']
        }
    )
    
    calc_daily_advanced = PythonOperator(
        task_id="calculate_daily_advanced_indicators",
        python_callable=calculate_advanced_indicators,
        op_kwargs={
            'lookback_period': timeframe_params['DAILY']['lookback_periods']['advanced'], 
            'signal_type': 'DAILY'
        }
    )
    
    check_daily_signals_table = ShortCircuitOperator(
        task_id="check_daily_signals_table",
        python_callable=check_signals_table_exists,
        op_kwargs={'signal_type': 'DAILY'}
    )
    
    run_daily_backtest = PythonOperator(
        task_id="run_daily_backtest",
        python_callable=backtest_trading_signals,
        op_kwargs={
            'test_period': timeframe_params['DAILY']['backtest_period'], 
            'hold_period': timeframe_params['DAILY']['hold_period'], 
            'signal_type': 'DAILY',
            'min_win_rate': timeframe_params['DAILY']['min_probability'] - 0.1
        }
    )
    
    send_daily_signals = PythonOperator(
        task_id="send_daily_high_probability_signals",
        python_callable=send_high_probability_signals,
        op_kwargs={
            'signal_type': 'DAILY', 
            'min_probability': timeframe_params['DAILY']['min_probability']
        }
    )
    
    
    calc_weekly_rsi = PythonOperator(
        task_id="calculate_weekly_rsi",
        python_callable=calculate_all_rsi_indicators,
        op_kwargs={
            'lookback_period': timeframe_params['WEEKLY']['lookback_periods']['rsi'], 
            'rsi_period': 14, 
            'signal_type': 'WEEKLY'
        }
    )
    
    calc_weekly_macd = PythonOperator(
        task_id="calculate_weekly_macd",
        python_callable=calculate_all_macd_indicators,
        op_kwargs={
            'lookback_period': timeframe_params['WEEKLY']['lookback_periods']['macd'], 
            'fast_period': timeframe_params['WEEKLY']['lookback_periods'].get('macd_fast', 12),
            'slow_period': timeframe_params['WEEKLY']['lookback_periods'].get('macd_slow', 26), 
            'signal_period': timeframe_params['WEEKLY']['lookback_periods'].get('macd_signal', 9),
            'signal_type': 'WEEKLY'
        }
    )
    
    calc_weekly_bb = PythonOperator(
        task_id="calculate_weekly_bollinger",
        python_callable=calculate_all_bollinger_bands,
        op_kwargs={
            'lookback_period': timeframe_params['WEEKLY']['lookback_periods']['bb'], 
            'band_period': 20, 
            'std_dev': 2, 
            'signal_type': 'WEEKLY'
        }
    )
    
    filter_weekly_stocks = PythonOperator(
        task_id="filter_weekly_stocks",
        python_callable=filter_by_volatility_liquidity,
        op_kwargs={
            'analysis_period': timeframe_params['WEEKLY']['volatility_params']['analysis_period'],
            'signal_type': 'WEEKLY',
            'volatility_min': timeframe_params['WEEKLY']['volatility_params']['min'],
            'volatility_max': timeframe_params['WEEKLY']['volatility_params']['max'],
            'volume_min': timeframe_params['WEEKLY']['volatility_params']['volume_min']
        }
    )
    
    calc_weekly_advanced = PythonOperator(
        task_id="calculate_weekly_advanced_indicators",
        python_callable=calculate_advanced_indicators,
        op_kwargs={
            'lookback_period': timeframe_params['WEEKLY']['lookback_periods']['advanced'], 
            'signal_type': 'WEEKLY'
        }
    )
    
    check_weekly_signals_table = ShortCircuitOperator(
        task_id="check_weekly_signals_table",
        python_callable=check_signals_table_exists,
        op_kwargs={'signal_type': 'WEEKLY'}
    )
    
    run_weekly_backtest = PythonOperator(
        task_id="run_weekly_backtest",
        python_callable=backtest_trading_signals,
        op_kwargs={
            'test_period': timeframe_params['WEEKLY']['backtest_period'], 
            'hold_period': timeframe_params['WEEKLY']['hold_period'], 
            'signal_type': 'WEEKLY',
            'min_win_rate': timeframe_params['WEEKLY']['min_probability'] - 0.1
        }
    )
    
    send_weekly_signals = PythonOperator(
        task_id="send_weekly_high_probability_signals",
        python_callable=send_high_probability_signals,
        op_kwargs={
            'signal_type': 'WEEKLY', 
            'min_probability': timeframe_params['WEEKLY']['min_probability']
        }
    )
    
    
    calc_monthly_rsi = PythonOperator(
        task_id="calculate_monthly_rsi",
        python_callable=calculate_all_rsi_indicators,
        op_kwargs={
            'lookback_period': timeframe_params['MONTHLY']['lookback_periods']['rsi'], 
            'rsi_period': 14, 
            'signal_type': 'MONTHLY'
        }
    )
    
    calc_monthly_macd = PythonOperator(
        task_id="calculate_monthly_macd",
        python_callable=calculate_all_macd_indicators,
        op_kwargs={
            'lookback_period': timeframe_params['MONTHLY']['lookback_periods']['macd'], 
            'fast_period': timeframe_params['MONTHLY']['lookback_periods'].get('macd_fast', 26),
            'slow_period': timeframe_params['MONTHLY']['lookback_periods'].get('macd_slow', 52), 
            'signal_period': timeframe_params['MONTHLY']['lookback_periods'].get('macd_signal', 18),
            'signal_type': 'MONTHLY'
        }
    )
    
    calc_monthly_bb = PythonOperator(
        task_id="calculate_monthly_bollinger",
        python_callable=calculate_all_bollinger_bands,
        op_kwargs={
            'lookback_period': timeframe_params['MONTHLY']['lookback_periods']['bb'], 
            'band_period': 20, 
            'std_dev': 2, 
            'signal_type': 'MONTHLY'
        }
    )
    
    filter_monthly_stocks = PythonOperator(
        task_id="filter_monthly_stocks",
        python_callable=filter_by_volatility_liquidity,
        op_kwargs={
            'analysis_period': timeframe_params['MONTHLY']['volatility_params']['analysis_period'],
            'signal_type': 'MONTHLY',
            'volatility_min': timeframe_params['MONTHLY']['volatility_params']['min'],
            'volatility_max': timeframe_params['MONTHLY']['volatility_params']['max'],
            'volume_min': timeframe_params['MONTHLY']['volatility_params']['volume_min']
        }
    )
    
    calc_monthly_advanced = PythonOperator(
        task_id="calculate_monthly_advanced_indicators",
        python_callable=calculate_advanced_indicators,
        op_kwargs={
            'lookback_period': timeframe_params['MONTHLY']['lookback_periods']['advanced'], 
            'signal_type': 'MONTHLY'
        }
    )
    
    check_monthly_signals_table = ShortCircuitOperator(
        task_id="check_monthly_signals_table",
        python_callable=check_signals_table_exists,
        op_kwargs={'signal_type': 'MONTHLY'}
    )
    
    run_monthly_backtest = PythonOperator(
        task_id="run_monthly_backtest",
        python_callable=backtest_trading_signals,
        op_kwargs={
            'test_period': timeframe_params['MONTHLY']['backtest_period'], 
            'hold_period': timeframe_params['MONTHLY']['hold_period'], 
            'signal_type': 'MONTHLY',
            'min_win_rate': timeframe_params['MONTHLY']['min_probability'] - 0.1
        }
    )
    
    send_monthly_signals = PythonOperator(
        task_id="send_monthly_high_probability_signals",
        python_callable=send_high_probability_signals,
        op_kwargs={
            'signal_type': 'MONTHLY', 
            'min_probability': timeframe_params['MONTHLY']['min_probability']
        }
    )
    
    join_timeframes = DummyOperator(
        task_id="join_timeframes",
        trigger_rule="none_failed"
    )
    
    unified_report = PythonOperator(
        task_id="send_unified_trading_report",
        python_callable=send_unified_trading_report,
        provide_context=True
    )
    
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    
    wait_for_transformation >> check_bb
    
    check_bb >> [calc_daily_rsi, calc_daily_macd, calc_daily_bb]
    [calc_daily_rsi, calc_daily_macd, calc_daily_bb] >> filter_daily_stocks >> calc_daily_advanced >> check_daily_signals_table >> run_daily_backtest >> send_daily_signals
    
    check_bb >> check_weekly
    check_weekly >> [calc_weekly_rsi, calc_weekly_macd, calc_weekly_bb]
    [calc_weekly_rsi, calc_weekly_macd, calc_weekly_bb] >> filter_weekly_stocks >> calc_weekly_advanced >> check_weekly_signals_table >> run_weekly_backtest >> send_weekly_signals
    
    check_bb >> check_monthly
    check_monthly >> [calc_monthly_rsi, calc_monthly_macd, calc_monthly_bb]
    [calc_monthly_rsi, calc_monthly_macd, calc_monthly_bb] >> filter_monthly_stocks >> calc_monthly_advanced >> check_monthly_signals_table >> run_monthly_backtest >> send_monthly_signals
    
    [send_daily_signals, send_weekly_signals, send_monthly_signals] >> join_timeframes
    join_timeframes >> unified_report >> end_task
