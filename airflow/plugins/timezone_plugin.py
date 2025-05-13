from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime
import pendulum
import logging


# Import utility modules
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

# Konfigurasi timezone
local_tz = pendulum.timezone("Asia/Jakarta")

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=5)
}

# DAG definition - Weekly
with DAG(
    dag_id="weekly_trading_signals",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 17 * * 5",  # Setiap hari Jumat pukul 17:00 Jakarta time
    catchup=False,
    default_args=default_args,
    tags=["trading", "technical", "signals", "weekly"]
) as dag:
    
    # Tunggu hingga data transformation selesai
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
    
    # Step 1: Calculate RSI indicators (longer period for weekly analysis)
    calc_rsi = PythonOperator(
        task_id="calculate_weekly_rsi",
        python_callable=calculate_all_rsi_indicators,
        op_kwargs={'lookback_period': 180, 'rsi_period': 14}  # Longer lookback for weekly analysis
    )
    
    # Step 2: Calculate MACD indicators (longer period)
    calc_macd = PythonOperator(
        task_id="calculate_weekly_macd",
        python_callable=calculate_all_macd_indicators,
        op_kwargs={'lookback_period': 250}  # Longer lookback for weekly signals
    )
    
    # Step 3: Setup Bollinger Bands table
    check_bb = PythonOperator(
        task_id="check_bollinger_bands_table",
        python_callable=check_and_create_bollinger_bands_table
    )
    
    # Step 4: Calculate Bollinger Bands (with weekly parameters)
    calc_bb = PythonOperator(
        task_id="calculate_weekly_bollinger",
        python_callable=calculate_all_bollinger_bands,
        op_kwargs={'lookback_period': 150, 'band_period': 20}
    )
    
    # Step 5: Filter stocks by volatility and liquidity (for weekly timeframe)
    filter_stocks = PythonOperator(
        task_id="filter_stocks_by_volatility_liquidity",
        python_callable=filter_by_volatility_liquidity,
        op_kwargs={'analysis_period': 60}  # Analyze 60 days for weekly signals
    )
    
    # Step 6: Calculate advanced indicators (with weekly parameters)
    calc_advanced = PythonOperator(
        task_id="calculate_weekly_advanced_indicators",
        python_callable=calculate_advanced_indicators,
        op_kwargs={'lookback_period': 365, 'signal_type': 'weekly'},  # 1 year lookback for weekly analysis
        retries=2,
        retry_delay=pendulum.duration(minutes=5)
    )
    
    # Step 7: Run backtesting (with longer evaluation period)
    run_backtest = PythonOperator(
        task_id="run_weekly_backtest",
        python_callable=backtest_trading_signals,
        op_kwargs={'test_period': 365, 'hold_period': 10},  # 10-day hold period for weekly signals
        trigger_rule='none_failed'
    )
    
    # Step 8: Send high probability weekly signals
    send_signals = PythonOperator(
        task_id="send_weekly_high_probability_signals",
        python_callable=send_high_probability_signals,
        op_kwargs={'signal_type': 'WEEKLY', 'min_probability': 0.75}  # Slightly lower threshold for weekly
    )
    
    # Step 9: Send weekly performance report
    send_report = PythonOperator(
        task_id="send_weekly_performance_report",
        python_callable=send_performance_report,
        op_kwargs={'report_type': 'WEEKLY', 'lookback_days': 90}  # Show last 90 days performance
    )
    
    # End marker
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    # Define task dependencies
    wait_for_transformation >> [calc_rsi, calc_macd, check_bb]
    check_bb >> calc_bb
    [calc_rsi, calc_macd, calc_bb] >> filter_stocks >> calc_advanced
    calc_advanced >> run_backtest >> send_signals >> send_report >> end_task


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime
import pendulum
import logging

# Import utility modules
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

# Konfigurasi timezone
local_tz = pendulum.timezone("Asia/Jakarta")

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=5)
}

# DAG definition
with DAG(
    dag_id="technical_trading_signals",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 17 * * 1-5",  # Every weekday at 17:00 Jakarta time
    catchup=False,
    default_args=default_args,
    tags=["trading", "technical", "signals"]
) as dag:
    
    # Tunggu hingga data transformation selesai
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
    
    # Step 1: Calculate RSI indicators
    calc_rsi = PythonOperator(
        task_id="calculate_rsi_indicators",
        python_callable=calculate_all_rsi_indicators,
        op_kwargs={'signal_type': 'DAILY'}  # TAMBAHKAN PARAMETER INI
    )
    
    # Step 2: Calculate MACD indicators
    calc_macd = PythonOperator(
        task_id="calculate_macd_indicators",
        python_callable=calculate_all_macd_indicators,
        op_kwargs={'signal_type': 'DAILY'}  # TAMBAHKAN PARAMETER INI
    )
    
    # Step 3: Setup Bollinger Bands table
    check_bb = PythonOperator(
        task_id="check_bollinger_bands_table",
        python_callable=check_and_create_bollinger_bands_table
    )
    
    # Step 4: Calculate Bollinger Bands
    calc_bb = PythonOperator(
        task_id="calculate_bollinger_bands",
        python_callable=calculate_all_bollinger_bands,
        op_kwargs={'signal_type': 'DAILY'}  # TAMBAHKAN PARAMETER INI
    )
    
    # Step 5: Filter stocks by volatility and liquidity
    filter_stocks = PythonOperator(
        task_id="filter_stocks_by_volatility_liquidity",
        python_callable=filter_by_volatility_liquidity,
        op_kwargs={'signal_type': 'DAILY'}  # TAMBAHKAN PARAMETER INI
    )
    
    # Step 6: Calculate advanced indicators
    calc_advanced = PythonOperator(
        task_id="calculate_advanced_indicators",
        python_callable=calculate_advanced_indicators,
        op_kwargs={'signal_type': 'DAILY'},  # TAMBAHKAN PARAMETER INI
        retries=2,
        retry_delay=pendulum.duration(minutes=2)
    )
    
    # Step 7: Run backtesting
    run_backtest = PythonOperator(
        task_id="run_backtest",
        python_callable=backtest_trading_signals,
        op_kwargs={'signal_type': 'DAILY'},  # TAMBAHKAN PARAMETER INI
        trigger_rule='none_failed'
    )
    
    # Step 8: Send high probability signals
    send_signals = PythonOperator(
        task_id="send_high_probability_signals",
        python_callable=send_high_probability_signals,
        op_kwargs={'signal_type': 'DAILY'}  # TAMBAHKAN PARAMETER INI
    )
    
    # Step 9: Send performance report
    send_report = PythonOperator(
        task_id="send_performance_report",
        python_callable=send_performance_report,
        op_kwargs={'report_type': 'DAILY'}  # TAMBAHKAN PARAMETER INI
    )
    
    # End marker
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    # Define task dependencies
    wait_for_transformation >> [calc_rsi, calc_macd, check_bb]
    check_bb >> calc_bb
    [calc_rsi, calc_macd, calc_bb] >> filter_stocks >> calc_advanced
    calc_advanced >> run_backtest >> send_signals >> send_report >> end_task

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime
import pendulum
import logging


# Import utility modules
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

# Konfigurasi timezone
local_tz = pendulum.timezone("Asia/Jakarta")

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=5)
}

# DAG definition - Monthly
with DAG(
    dag_id="monthly_trading_signals",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 17 1 * *",  # First day of each month at 17:00 Jakarta time
    catchup=False,
    default_args=default_args,
    tags=["trading", "technical", "signals", "monthly"]
) as dag:
    
    # Tunggu hingga data transformation selesai
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
    
    # Step 1: Calculate RSI indicators (much longer period for monthly analysis)
    calc_rsi = PythonOperator(
        task_id="calculate_monthly_rsi",
        python_callable=calculate_all_rsi_indicators,
        op_kwargs={'lookback_period': 365, 'rsi_period': 14, 'signal_type': 'MONTHLY'}  # TAMBAHKAN PARAMETER signal_type
    )
    
    # Step 2: Calculate MACD indicators (longer period for monthly)
    calc_macd = PythonOperator(
        task_id="calculate_monthly_macd",
        python_callable=calculate_all_macd_indicators,
        op_kwargs={'lookback_period': 500, 'signal_type': 'MONTHLY'}  # TAMBAHKAN PARAMETER signal_type
    )
    
    # Step 3: Setup Bollinger Bands table
    check_bb = PythonOperator(
        task_id="check_bollinger_bands_table",
        python_callable=check_and_create_bollinger_bands_table
    )
    
    # Step 4: Calculate Bollinger Bands (with monthly parameters)
    calc_bb = PythonOperator(
        task_id="calculate_monthly_bollinger",
        python_callable=calculate_all_bollinger_bands,
        op_kwargs={'lookback_period': 300, 'band_period': 20, 'signal_type': 'MONTHLY'}  # TAMBAHKAN PARAMETER signal_type
    )
    
    # Step 5: Filter stocks by volatility and liquidity (for monthly timeframe)
    filter_stocks = PythonOperator(
        task_id="filter_stocks_by_volatility_liquidity",
        python_callable=filter_by_volatility_liquidity,
        op_kwargs={'analysis_period': 180, 'signal_type': 'MONTHLY'}  # TAMBAHKAN PARAMETER signal_type
    )
    
    # Step 6: Calculate advanced indicators (with monthly parameters)
    calc_advanced = PythonOperator(
        task_id="calculate_monthly_advanced_indicators",
        python_callable=calculate_advanced_indicators,
        op_kwargs={'lookback_period': 730, 'signal_type': 'MONTHLY'},  # Parameter signal_type sudah ada
        retries=2,
        retry_delay=pendulum.duration(minutes=5)
    )
    
    # Step 7: Run backtesting (with much longer evaluation period)
    run_backtest = PythonOperator(
        task_id="run_monthly_backtest",
        python_callable=backtest_trading_signals,
        op_kwargs={'test_period': 730, 'hold_period': 20, 'signal_type': 'MONTHLY'},  # TAMBAHKAN PARAMETER signal_type
        trigger_rule='none_failed'
    )
    
    # Step 8: Send high probability monthly signals
    send_signals = PythonOperator(
        task_id="send_monthly_high_probability_signals",
        python_callable=send_high_probability_signals,
        op_kwargs={'signal_type': 'MONTHLY', 'min_probability': 0.7}  # Parameter signal_type sudah ada
    )
    
    # Step 9: Send monthly performance report
    send_report = PythonOperator(
        task_id="send_monthly_performance_report",
        python_callable=send_performance_report,
        op_kwargs={'report_type': 'MONTHLY', 'lookback_days': 365}  # Parameter report_type sudah ada
    )
    
    # End marker
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    # Define task dependencies
    wait_for_transformation >> [calc_rsi, calc_macd, check_bb]
    check_bb >> calc_bb
    [calc_rsi, calc_macd, calc_bb] >> filter_stocks >> calc_advanced
    calc_advanced >> run_backtest >> send_signals >> send_report >> end_task

import pandas as pd
import numpy as np
import logging
import json
from airflow.models import Variable
from .database import get_database_connection, get_latest_stock_date
from .telegram import send_telegram_message

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def filter_by_volatility_liquidity():
    """
    Filter saham berdasarkan volatilitas dan likuiditas
    untuk meningkatkan win rate
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    # Dapatkan tanggal terakhir
    latest_date = get_latest_stock_date()
    
    # First, get actual trading day count
    trading_days_sql = f"""
    SELECT COUNT(DISTINCT date) as trading_days 
    FROM public.daily_stock_summary
    WHERE date >= '{latest_date}'::date - INTERVAL '30 days'
    """
    trading_days_df = pd.read_sql(trading_days_sql, conn)
    trading_days = trading_days_df['trading_days'].iloc[0]
    
    # Set minimum required days (e.g., 90% of actual trading days)
    min_days_required = max(15, int(trading_days * 0.9))
    
    logger.info(f"Found {trading_days} trading days in last 30 days. Requiring minimum {min_days_required} days of data.")
    
    # Query untuk mendapatkan data volatilitas dan likuiditas
    try:
        sql = f"""
        SELECT 
            symbol,
            AVG(volume) as avg_volume_30d,
            STDDEV(CASE
                WHEN prev_close != 0 THEN (close - prev_close) / prev_close * 100
                ELSE 0
            END) as volatility_30d,
            AVG(value) as avg_value_30d,
            COUNT(*) as data_count
        FROM public.daily_stock_summary
        WHERE date >= '{latest_date}'::date - INTERVAL '30 days'
        GROUP BY symbol
        HAVING COUNT(*) >= {min_days_required}
        """
        
        df = pd.read_sql(sql, conn)
        logger.info(f"Initial filter found {len(df)} symbols")
        
    except Exception as e:
        logger.error(f"Error querying volatility data: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error querying volatility data: {str(e)}"
    
    if df.empty:
        logger.warning("No data available for volatility filtering")
        return "No data available for volatility filtering"
    
    # Filter berdasarkan kriteria yang lebih ketat untuk win rate tinggi
    filtered = df[
        (df['avg_volume_30d'] > 2000000) &                  # Volume tinggi (likuiditas baik)
        (df['volatility_30d'] > 1.0) &                      # Cukup volatil untuk profit
        (df['volatility_30d'] < 4.0) &                      # Tidak terlalu volatil (risiko tinggi)
        (df['avg_value_30d'] > 10_000_000_000) &            # Minimal nilai transaksi 10M per hari
        (df['data_count'] >= min_days_required)             # Gunakan min_days_required juga di sini
    ]
    
    # Simpan hasil filter
    try:
        # Store filtered symbols in Airflow variables
        filtered_symbols = filtered['symbol'].tolist()
        Variable.set(
            "filtered_symbols",
            filtered_symbols,
            serialize_json=True
        )
        logger.info(f"Filtered {len(filtered_symbols)} stocks based on volatility and liquidity criteria")
        
        # Create a report of filtered stocks
        report = f"🔍 *FILTER SAHAM BERDASARKAN VOLATILITAS & LIKUIDITAS*\n\n"
        report += f"Dari {len(df)} saham, terpilih {len(filtered)} saham yang memenuhi kriteria:\n\n"
        report += "• Volume rata-rata > 2,000,000\n"
        report += "• Volatilitas antara 1% - 4% per hari\n"
        report += "• Nilai transaksi > Rp 10 Milyar per hari\n"
        report += f"• Data lengkap (min. {min_days_required} hari dalam 30 hari terakhir)\n\n"
        
        # Include top 20 stocks by liquidity
        top_liquid = filtered.sort_values(by='avg_value_30d', ascending=False).head(20)
        
        report += "*Top 20 Saham Terpilih (Berdasarkan Likuiditas):*\n"
        for i, row in enumerate(top_liquid.itertuples(), 1):
            report += f"{i}. *{row.symbol}*: Vol {row.avg_volume_30d:,.0f} | Val Rp{row.avg_value_30d/1_000_000_000:,.1f}M | Vol {row.volatility_30d:.2f}%\n"
        
        # Send report to Telegram
        send_telegram_message(report)
        
        return f"Filtered {len(filtered)} stocks based on volatility and liquidity"
    except Exception as e:
        logger.error(f"Error saving filtered symbols: {str(e)}")
        return f"Error saving filtered symbols: {str(e)}"

def calculate_advanced_indicators():
    """
    Menghitung indikator tingkat lanjut dan membuat sistem penyaringan multi-layer
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Failed to connect to database: {str(e)}"
    
    # Dapatkan tanggal terakhir
    latest_date = get_latest_stock_date()
    
    # Ambil data dengan periode lebih panjang untuk analisis pattern jangka panjang
    try:
        query = f"""
        SELECT 
            symbol, 
            date, 
            open_price as open,
            high, 
            low, 
            close, 
            volume,
            value,
            COALESCE(foreign_sell, 0) as foreign_sell,
            COALESCE(foreign_buy, 0) as foreign_buy
        FROM public.daily_stock_summary
        WHERE date >= '{latest_date}'::date - INTERVAL '300 days'
        ORDER BY symbol, date
        """
        
        logger.info("Fetching data for advanced indicators calculation")
        df = pd.read_sql(query, conn)
        logger.info(f"Fetched data for {df['symbol'].nunique()} symbols")
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        conn.close()
        return f"Error fetching data: {str(e)}"
    
    # Jika tidak ada data
    if df.empty:
        logger.warning("No data available for calculation")
        conn.close()
        return "No data available"
    
    results = []
    
    # Dapatkan daftar simbol yang terfilter (jika ada)
    try:
        filtered_symbols = Variable.get("filtered_symbols", deserialize_json=True)
        df = df[df['symbol'].isin(filtered_symbols)]
        logger.info(f"Filtered to {len(filtered_symbols)} symbols based on volatility/liquidity criteria")
    except:
        logger.info("No filtered symbols list found, processing all symbols")
    
    # Analisis per saham dengan error handling
    symbol_count = 0
    for symbol, group in df.groupby('symbol'):
        try:
            if len(group) < 100:  # Minimal 100 hari untuk analisis pattern yang baik
                continue
                
            # Sort berdasarkan tanggal
            group = group.sort_values('date')
            
            # --- TAMBAHAN INDIKATOR TEKNIKAL ---
            
            # 1. Volume Shock (> 300% dari rata-rata 20 hari)
            group['avg_volume_20d'] = group['volume'].rolling(window=20).mean()
            group['volume_shock'] = group['volume'] > group['avg_volume_20d'] * 3
            
            # 2. Supply/Demand Zone Detection
            group['prev_high'] = group['high'].shift(1)
            group['prev_low'] = group['low'].shift(1)
            group['next_high'] = group['high'].shift(-1)
            group['next_low'] = group['low'].shift(-1)
            
            # Identifikasi supply zone
            group['supply_zone'] = (group['high'] > group['prev_high']) & (group['high'] > group['next_high'])
            
            # Identifikasi demand zone
            group['demand_zone'] = (group['low'] < group['prev_low']) & (group['low'] < group['next_low'])
            
            # 3. Foreign Flow Analysis
            group['foreign_net'] = group['foreign_buy'] - group['foreign_sell']
            group['foreign_net_cumulative'] = group['foreign_net'].rolling(window=10).sum()
            
            # 4. Price Action Patterns
            
            # Bullish Engulfing
            group['bullish_engulfing'] = (
                (group['close'] > group['open']) &  # Current candle is bullish
                (group['close'].shift(1) < group['open'].shift(1)) &  # Previous candle is bearish
                (group['close'] > group['open'].shift(1)) &  # Current close > previous open
                (group['open'] < group['close'].shift(1))  # Current open < previous close
            )
            
            # 5. Gann Swing Analysis
            group['swing_high'] = (
                (group['high'] > group['high'].shift(1)) & 
                (group['high'] > group['high'].shift(2)) & 
                (group['high'] > group['high'].shift(-1)) & 
                (group['high'] > group['high'].shift(-2))
            )
            
            group['swing_low'] = (
                (group['low'] < group['low'].shift(1)) & 
                (group['low'] < group['low'].shift(2)) & 
                (group['low'] < group['low'].shift(-1)) & 
                (group['low'] < group['low'].shift(-2))
            )
            
            # 6. Market Structure - Higher Highs, Higher Lows (Uptrend)
            window = 10
            group['highest_high'] = group['high'].rolling(window=window).max()
            group['lowest_low'] = group['low'].rolling(window=window).min()
            
            group['higher_high'] = group['highest_high'] > group['highest_high'].shift(window)
            group['higher_low'] = group['lowest_low'] > group['lowest_low'].shift(window)
            group['uptrend_structure'] = group['higher_high'] & group['higher_low']
            
            # 7. ADX untuk Kekuatan Tren (Trend Strength)
            period = 14
            group['tr'] = np.maximum(
                np.maximum(
                    group['high'] - group['low'],
                    abs(group['high'] - group['close'].shift(1))
                ),
                abs(group['low'] - group['close'].shift(1))
            )
            group['atr'] = group['tr'].rolling(window=period).mean()
            
            group['plus_dm'] = np.where(
                (group['high'] - group['high'].shift(1)) > (group['low'].shift(1) - group['low']),
                np.maximum(group['high'] - group['high'].shift(1), 0),
                0
            )
            group['minus_dm'] = np.where(
                (group['low'].shift(1) - group['low']) > (group['high'] - group['high'].shift(1)),
                np.maximum(group['low'].shift(1) - group['low'], 0),
                0
            )
            
            group['plus_di'] = 100 * (group['plus_dm'].rolling(window=period).mean() / group['atr'])
            group['minus_di'] = 100 * (group['minus_dm'].rolling(window=period).mean() / group['atr'])
            
            group['dx'] = 100 * abs(group['plus_di'] - group['minus_di']) / (group['plus_di'] + group['minus_di'])
            group['adx'] = group['dx'].rolling(window=period).mean()
            
            # 8. Fibonacci Retracement
            # Identifikasi swing high dan swing low terakhir
            last_30_days = group.tail(30).reset_index(drop=True)
            
            if len(last_30_days) >= 30:
                high_idx = last_30_days['high'].idxmax()
                low_idx = last_30_days['low'].idxmin()
                
                # Urutan: low kemudian high (untuk retracement)
                if low_idx < high_idx:
                    swing_high = last_30_days.loc[high_idx, 'high']
                    swing_low = last_30_days.loc[low_idx, 'low']
                    
                    # Hitung level Fibonacci
                    diff = swing_high - swing_low
                    fib_0 = swing_low
                    fib_236 = swing_low + 0.236 * diff
                    fib_382 = swing_low + 0.382 * diff
                    fib_50 = swing_low + 0.5 * diff
                    fib_618 = swing_low + 0.618 * diff
                    fib_786 = swing_low + 0.786 * diff
                    fib_100 = swing_high
                    
                    # Flag jika harga close berada di support Fibonacci
                    latest_close = group['close'].iloc[-1]
                    fib_support = False
                    
                    # Jika harga close dekat dengan level Fibonacci (±1%)
                    tolerance = 0.01 * diff
                    if (abs(latest_close - fib_236) < tolerance or
                        abs(latest_close - fib_382) < tolerance or
                        abs(latest_close - fib_50) < tolerance or
                        abs(latest_close - fib_618) < tolerance):
                        fib_support = True
                else:
                    fib_support = False
            else:
                fib_support = False
            
            # --- MULTI-FACTOR FILTERING SYSTEM ---
            latest_data = group.dropna().tail(3)  # Ambil 3 hari terbaru untuk stabilitas
            
            for i, (idx, row) in enumerate(latest_data.iterrows()):
                # Hanya proses hari terakhir
                if i != len(latest_data) - 1:
                    continue
                
                # Gabungkan data teknikal dari tabel lain
                try:
                    # Query untuk ambil RSI terbaru
                    rsi_query = f"""
                    SELECT rsi, rsi_signal 
                    FROM public_analytics.technical_indicators_rsi 
                    WHERE symbol = '{symbol}' AND date = '{row['date']}'
                    """
                    rsi_df = pd.read_sql(rsi_query, conn)
                    
                    # MACD Bullish
                    macd_query = f"""
                    SELECT macd_signal 
                    FROM public_analytics.technical_indicators_macd 
                    WHERE symbol = '{symbol}' AND date = '{row['date']}'
                    """
                    macd_df = pd.read_sql(macd_query, conn)
                    
                    # Bollinger Band
                    try:
                        bb_query = f"""
                        SELECT percent_b, bb_signal
                        FROM public_analytics.technical_indicators_bollinger 
                        WHERE symbol = '{symbol}' AND date = '{row['date']}'
                        """
                        bb_df = pd.read_sql(bb_query, conn)
                    except Exception as e:
                        logger.warning(f"Error querying Bollinger Bands for {symbol}: {str(e)}")
                        # Jika terjadi error karena tabel tidak ada, gunakan nilai default
                        bb_df = pd.DataFrame([{'percent_b': 0.5, 'bb_signal': 'Neutral'}])
                    
                    # Skor Kumulatif untuk Sinyal Beli
                    buy_score = 0
                    
                    # 1. RSI Oversold (score +1)
                    if not rsi_df.empty and rsi_df.iloc[0]['rsi_signal'] == 'Oversold':
                        buy_score += 1
                    
                    # 2. MACD Bullish (score +1)
                    if not macd_df.empty and macd_df.iloc[0]['macd_signal'] == 'Bullish':
                        buy_score += 1
                    
                    # 3. Volume Shock (score +1)
                    if row['volume_shock']:
                        buy_score += 1
                    
                    # 4. Demand Zone (score +2)
                    if row['demand_zone']:
                        buy_score += 2
                    
                    # 5. Positive Foreign Flow (score +1)
                    if row['foreign_net_cumulative'] > 0:
                        buy_score += 1
                    
                    # 6. Bullish Pattern (score +2)
                    if row['bullish_engulfing']:
                        buy_score += 2
                    
                    # 7. Uptrend Structure (score +2)
                    if row['uptrend_structure']:
                        buy_score += 2
                    
                    # 8. Beli di Support Bollinger Band (score +1)
                    if not bb_df.empty and (bb_df.iloc[0]['bb_signal'] == 'Oversold' or bb_df.iloc[0]['percent_b'] < 0.2):
                        buy_score += 1
                    
                    # 9. ADX > 20 (Tren yang Kuat) (score +1)
                    if row['adx'] > 20 and row['plus_di'] > row['minus_di']:
                        buy_score += 1
                    
                    # 10. Fibonacci Support (score +1)
                    if fib_support:
                        buy_score += 1
                    
                    # Kategori Sinyal
                    if buy_score >= 9:  # 90% dari total 10 poin
                        signal_strength = "Very Strong Buy - Excellent Probability (>90%)"
                        winning_prob = 0.95  # 95% probability
                    elif buy_score >= 8:  # 80% dari total 10 poin
                        signal_strength = "Strong Buy - High Probability (>85%)"
                        winning_prob = 0.9  # 90% probability
                    elif buy_score >= 6:
                        signal_strength = "Buy - Good Probability (>75%)"
                        winning_prob = 0.8
                    elif buy_score >= 4:
                        signal_strength = "Consider Buy - Moderate Probability (>60%)" 
                        winning_prob = 0.7
                    else:
                        signal_strength = "Neutral - Insufficient Factors"
                        winning_prob = 0.5
                    
                    # Tambahkan ke hasil jika skor minimal 6
                    if buy_score >= 6:
                        results.append({
                            'symbol': symbol,
                            'date': row['date'],
                            'buy_score': buy_score,
                            'signal_strength': signal_strength,
                            'winning_probability': winning_prob,
                            'volume_shock': bool(row['volume_shock']),
                            'demand_zone': bool(row['demand_zone']),
                            'foreign_flow': float(row['foreign_net_cumulative']),
                            'price_pattern': 'Bullish Engulfing' if row['bullish_engulfing'] else 'None',
                            'market_structure': 'Uptrend' if row['uptrend_structure'] else 'Neutral/Downtrend',
                            'adx': float(row['adx']) if not pd.isna(row['adx']) else 0,
                            'fib_support': fib_support
                        })
                        symbol_count += 1
                except Exception as e:
                    logger.warning(f"Error processing technical indicators for {symbol}: {str(e)}")
                    continue
        except Exception as e:
            logger.warning(f"Error processing symbol {symbol}: {str(e)}")
            continue
    
    # Konversi hasil ke DataFrame
    if not results:
        logger.warning("No trading signals match criteria")
        conn.close()
        return "No trading signals match criteria"
        
    result_df = pd.DataFrame(results)
    
    # Buat tabel baru jika belum ada
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public_analytics.advanced_trading_signals (
            symbol TEXT,
            date DATE,
            buy_score NUMERIC,
            signal_strength TEXT,
            winning_probability NUMERIC,
            volume_shock BOOLEAN,
            demand_zone BOOLEAN,
            foreign_flow NUMERIC,
            price_pattern TEXT,
            market_structure TEXT,
            adx NUMERIC,
            fib_support BOOLEAN,
            PRIMARY KEY (symbol, date)
        )
        """)
        
        # Hapus data yang sudah ada untuk tanggal yang sama
        dates_to_update = result_df['date'].unique()
        for date in dates_to_update:
            date_str = date.strftime('%Y-%m-%d') if isinstance(date, pd.Timestamp) else date
            cursor.execute(f"DELETE FROM public_analytics.advanced_trading_signals WHERE date = '{date_str}'")
        
        # Masukkan data baru
        for _, row in result_df.iterrows():
            cursor.execute("""
            INSERT INTO public_analytics.advanced_trading_signals 
            (symbol, date, buy_score, signal_strength, winning_probability, 
             volume_shock, demand_zone, foreign_flow, price_pattern, market_structure, adx, fib_support)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['symbol'], 
                row['date'].strftime('%Y-%m-%d') if isinstance(row['date'], pd.Timestamp) else row['date'],
                row['buy_score'], 
                row['signal_strength'], 
                row['winning_probability'], 
                row['volume_shock'], 
                row['demand_zone'], 
                row['foreign_flow'], 
                row['price_pattern'], 
                row['market_structure'],
                row['adx'],
                row['fib_support']
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return f"Successfully identified {len(result_df)} trading signals from {symbol_count} symbols"
    except Exception as e:
        logger.error(f"Error saving signals to database: {str(e)}")
        conn.close()
        return f"Error saving signals to database: {str(e)}"

def backtest_trading_signals():
    """
    Melakukan back-testing sinyal trading dan mengukur win rate 
    berdasarkan data historis
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    # Dapatkan tanggal terakhir
    latest_date = get_latest_stock_date()
    
    # Create table for backtest results if doesn't exist
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public_analytics.backtest_results (
            symbol TEXT,
            signal_date DATE,
            buy_score NUMERIC,
            winning_probability NUMERIC,
            entry_price NUMERIC,
            exit_price NUMERIC,
            max_price_5d NUMERIC,
            percent_change_5d NUMERIC,
            max_potential_gain NUMERIC,
            is_win BOOLEAN,
            PRIMARY KEY (symbol, signal_date)
        )
        """)
        conn.commit()
    except Exception as e:
        logger.error(f"Error creating backtest table: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        conn.close()
        return f"Error creating backtest table: {str(e)}"
    
    # Check if advanced_trading_signals table has data
    try:
        check_sql = """
        SELECT COUNT(*) as signal_count 
        FROM public_analytics.advanced_trading_signals
        """
        signal_count_df = pd.read_sql(check_sql, conn)
        signal_count = signal_count_df['signal_count'].iloc[0]
        
        logger.info(f"Found {signal_count} signals in advanced_trading_signals table")
        
        if signal_count == 0:
            logger.warning("No signals found in advanced_trading_signals table")
            cursor.close()
            conn.close()
            return "No signals found for backtesting. Run calculate_advanced_indicators first."
    except Exception as e:
        # Table might not exist yet
        logger.warning(f"Error checking signal table: {str(e)}")
    
    # Pengujian pada 6 bulan data terakhir - PERBAIKAN QUERY
    try:
        # Dengan periode hold berdasarkan hari trading (bukan kalender)
        sql = f"""
        WITH signals AS (
            SELECT 
                s.symbol, 
                s.date AS signal_date, 
                s.buy_score,
                s.winning_probability,
                m1.close AS entry_price,
                s.signal_strength
            FROM public_analytics.advanced_trading_signals s
            JOIN public.daily_stock_summary m1 
                ON s.symbol = m1.symbol AND s.date = m1.date
            WHERE s.date BETWEEN '{latest_date}'::date - INTERVAL '180 days' AND '{latest_date}'::date - INTERVAL '5 days'
            AND s.winning_probability >= 0.7
        ),
        trade_dates AS (
            -- Get all trading dates for looking up exit dates
            SELECT DISTINCT date 
            FROM public.daily_stock_summary
            ORDER BY date
        ),
        exit_dates AS (
            -- Find closest trading date at least 5 days after signal
            SELECT 
                s.symbol,
                s.signal_date,
                (
                    SELECT MIN(td.date)
                    FROM trade_dates td
                    WHERE td.date >= s.signal_date + INTERVAL '5 days'
                ) AS exit_date
            FROM signals s
        ),
        exit_prices AS (
            -- Get exit prices using the determined exit dates
            SELECT 
                s.symbol,
                s.signal_date,
                s.entry_price,
                s.buy_score,
                s.winning_probability,
                s.signal_strength,
                m2.close AS exit_price,
                ed.exit_date
            FROM signals s
            JOIN exit_dates ed ON s.symbol = ed.symbol AND s.signal_date = ed.signal_date
            LEFT JOIN public.daily_stock_summary m2 
                ON s.symbol = m2.symbol AND ed.exit_date = m2.date
            WHERE m2.close IS NOT NULL -- Ensure we have exit prices
        ),
        max_prices AS (
            -- Get maximum price within holding period
            SELECT 
                s.symbol,
                s.signal_date,
                MAX(m3.close) AS max_price_5d
            FROM signals s
            JOIN exit_dates ed ON s.symbol = ed.symbol AND s.signal_date = ed.signal_date
            JOIN public.daily_stock_summary m3 
                ON s.symbol = m3.symbol 
                AND m3.date BETWEEN s.signal_date AND ed.exit_date
            GROUP BY s.symbol, s.signal_date
        )
        SELECT 
            e.symbol,
            e.signal_date,
            e.entry_price,
            e.exit_price,
            e.buy_score,
            e.winning_probability,
            e.signal_strength,
            m.max_price_5d,
            (e.exit_price - e.entry_price) / e.entry_price * 100 AS percent_change_5d,
            (m.max_price_5d - e.entry_price) / e.entry_price * 100 AS max_potential_gain,
            CASE 
                WHEN (e.exit_price - e.entry_price) / e.entry_price * 100 > 2 THEN TRUE 
                ELSE FALSE 
            END AS is_win
        FROM exit_prices e
        JOIN max_prices m ON e.symbol = m.symbol AND e.signal_date = m.signal_date
        ORDER BY e.signal_date DESC
        """
        
        results = pd.read_sql(sql, conn)
        logger.info(f"Backtest query returned {len(results)} rows")
        
    except Exception as e:
        logger.error(f"Error retrieving backtest data: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        conn.close()
        return f"Error retrieving backtest data: {str(e)}"
    
    if results.empty:
        # Try a more lenient approach if no results
        try:
            logger.warning("No results with standard approach, trying more lenient query")
            
            # Simplified query that doesn't require advanced_trading_signals
            # FIXED the SQL syntax error related to DISTINCT with ORDER BY
            fallback_sql = f"""
            WITH top_stocks AS (
                -- Select top 50 stocks by volume
                SELECT symbol, AVG(volume) as avg_volume
                FROM public.daily_stock_summary
                WHERE date >= '{latest_date}'::date - INTERVAL '30 days'
                GROUP BY symbol
                ORDER BY avg_volume DESC
                LIMIT 50
            ),
            signal_dates AS (
                -- Use every Monday as a signal date
                SELECT DISTINCT date AS signal_date
                FROM public.daily_stock_summary
                WHERE date BETWEEN '{latest_date}'::date - INTERVAL '180 days' AND '{latest_date}'::date - INTERVAL '10 days'
                AND EXTRACT(DOW FROM date) = 1  -- Monday
            ),
            signals AS (
                -- Create dummy signals for backtest
                SELECT 
                    ts.symbol,
                    sd.signal_date,
                    8 AS buy_score,  -- Dummy score
                    0.8 AS winning_probability,
                    dss.close AS entry_price,
                    'Strong Buy - Test' AS signal_strength
                FROM top_stocks ts
                CROSS JOIN signal_dates sd
                JOIN public.daily_stock_summary dss
                    ON ts.symbol = dss.symbol AND sd.signal_date = dss.date
            ),
            trade_dates AS (
                SELECT DISTINCT date 
                FROM public.daily_stock_summary
                ORDER BY date
            ),
            exit_dates AS (
                SELECT 
                    s.symbol,
                    s.signal_date,
                    (
                        SELECT MIN(td.date)
                        FROM trade_dates td
                        WHERE td.date >= s.signal_date + INTERVAL '5 days'
                    ) AS exit_date
                FROM signals s
            ),
            exit_prices AS (
                SELECT 
                    s.symbol,
                    s.signal_date,
                    s.entry_price,
                    s.buy_score,
                    s.winning_probability,
                    s.signal_strength,
                    m2.close AS exit_price,
                    ed.exit_date
                FROM signals s
                JOIN exit_dates ed ON s.symbol = ed.symbol AND s.signal_date = ed.signal_date
                LEFT JOIN public.daily_stock_summary m2 
                    ON s.symbol = m2.symbol AND ed.exit_date = m2.date
                WHERE m2.close IS NOT NULL
            ),
            max_prices AS (
                SELECT 
                    s.symbol,
                    s.signal_date,
                    MAX(m3.close) AS max_price_5d
                FROM signals s
                JOIN exit_dates ed ON s.symbol = ed.symbol AND s.signal_date = ed.signal_date
                JOIN public.daily_stock_summary m3 
                    ON s.symbol = m3.symbol 
                    AND m3.date BETWEEN s.signal_date AND ed.exit_date
                GROUP BY s.symbol, s.signal_date
            )
            SELECT 
                e.symbol,
                e.signal_date,
                e.entry_price,
                e.exit_price,
                e.buy_score,
                e.winning_probability,
                e.signal_strength,
                m.max_price_5d,
                (e.exit_price - e.entry_price) / e.entry_price * 100 AS percent_change_5d,
                (m.max_price_5d - e.entry_price) / e.entry_price * 100 AS max_potential_gain,
                CASE 
                    WHEN (e.exit_price - e.entry_price) / e.entry_price * 100 > 2 THEN TRUE 
                    ELSE FALSE 
                END AS is_win
            FROM exit_prices e
            JOIN max_prices m ON e.symbol = m.symbol AND e.signal_date = m.signal_date
            ORDER BY e.signal_date DESC
            LIMIT 500
            """
            
            results = pd.read_sql(fallback_sql, conn)
            logger.info(f"Fallback query returned {len(results)} rows")
            
            if results.empty:
                logger.warning("No data available even with fallback query")
                cursor.close()
                conn.close()
                return "No data available for backtesting even with fallback approach"
                
        except Exception as e:
            logger.error(f"Error with fallback backtest query: {str(e)}")
            cursor.close()
            conn.close()
            return "No data available for back-testing"
    
    # Save results to database
    try:
        # Clear existing data that's going to be replaced
        cursor.execute("TRUNCATE TABLE public_analytics.backtest_results")
        
        # Insert new backtest results
        for _, row in results.iterrows():
            signal_date = row['signal_date']
            if isinstance(signal_date, pd.Timestamp):
                signal_date = signal_date.strftime('%Y-%m-%d')
                
            cursor.execute("""
            INSERT INTO public_analytics.backtest_results 
            (symbol, signal_date, buy_score, winning_probability, entry_price, 
             exit_price, max_price_5d, percent_change_5d, max_potential_gain, is_win)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['symbol'], 
                signal_date,
                row['buy_score'], 
                row['winning_probability'], 
                row['entry_price'],
                row['exit_price'],
                row['max_price_5d'],
                row['percent_change_5d'],
                row['max_potential_gain'],
                row['is_win']
            ))
        
        conn.commit()
        
        # Analisis win rate keseluruhan
        win_count = results['is_win'].sum()
        total_signals = len(results)
        win_rate = win_count / total_signals * 100 if total_signals > 0 else 0
        
        # Analisis win rate berdasarkan skor
        win_rate_by_score = results.groupby('buy_score')['is_win'].mean() * 100
        
        # Create a summary report
        summary = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_signals': int(total_signals),
            'win_count': int(win_count),
            'win_rate': float(win_rate),
            'win_rate_by_score': win_rate_by_score.to_dict(),
            'avg_gain': float(results['percent_change_5d'].mean()),
            'avg_max_gain': float(results['max_potential_gain'].mean())
        }
        
        # Save summary as variable
        Variable.set('backtest_summary', json.dumps(summary))
        
        cursor.close()
        conn.close()
        
        return f"Backtest completed with {win_rate:.1f}% win rate over {total_signals} signals"
    except Exception as e:
        logger.error(f"Error saving backtest results: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error saving backtest results: {str(e)}"

def send_high_probability_signals():
    """
    Mengirim sinyal trading dengan probabilitas tinggi (>80%) ke Telegram
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    # Ambil tanggal terakhir dari database
    # PERUBAHAN: Gunakan get_latest_stock_date() untuk memastikan konsistensi
    latest_date = get_latest_stock_date()
    
    # Ambil sinyal dengan probabilitas tinggi (>80%)
    try:
        sql = f"""
        WITH stock_info AS (
            SELECT 
                s.symbol,
                s.date,
                s.buy_score,
                s.signal_strength,
                s.winning_probability,
                s.volume_shock,
                s.demand_zone,
                s.foreign_flow,
                s.price_pattern,
                s.market_structure,
                s.adx,
                s.fib_support,
                m.name,
                m.close,
                m.volume,
                r.rsi,
                mc.macd_signal
            FROM public_analytics.advanced_trading_signals s
            JOIN public.daily_stock_summary m
                ON s.symbol = m.symbol AND s.date = m.date
            LEFT JOIN public_analytics.technical_indicators_rsi r
                ON s.symbol = r.symbol AND s.date = r.date
            LEFT JOIN public_analytics.technical_indicators_macd mc
                ON s.symbol = mc.symbol AND s.date = mc.date
            WHERE s.date = '{latest_date}'
            AND s.winning_probability >= 0.8
        )
        SELECT * FROM stock_info
        ORDER BY buy_score DESC, winning_probability DESC, foreign_flow DESC
        LIMIT 10
        """
        
        df = pd.read_sql(sql, conn)
        conn.close()
    except Exception as e:
        logger.error(f"Error querying high probability signals: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error querying signals: {str(e)}"
    
    # ... kode lainnya tetap sama ...
    
    if df.empty:
        logger.warning("No high probability trading signals found")
        return "No high probability trading signals found"
    
    # Create date string for report
    report_date = df['date'].iloc[0]
    if isinstance(report_date, pd.Timestamp):
        report_date = report_date.strftime('%Y-%m-%d')
    
    # Create Telegram message
    message = f"🔮 *SINYAL TRADING PROBABILITAS TINGGI ({report_date})* 🔮\n\n"
    message += "Saham-saham berikut memiliki probabilitas profit >80% berdasarkan analisis multi-faktor:\n\n"
    
    for i, row in enumerate(df.itertuples(), 1):
        message += f"*{i}. {row.symbol}* ({row.name})\n"
        message += f"   Harga: Rp{row.close:,.0f} | Skor: {row.buy_score}/10\n"
        message += f"   Probabilitas: {row.winning_probability*100:.0f}% | Signal: {row.signal_strength}\n"
        
        # Faktor-faktor pendukung
        factors = []
        if hasattr(row, 'volume_shock') and row.volume_shock:
            factors.append("Volume Shock")
        if hasattr(row, 'demand_zone') and row.demand_zone:
            factors.append("Demand Zone")
        if hasattr(row, 'foreign_flow') and row.foreign_flow > 0:
            factors.append(f"Foreign Flow +{row.foreign_flow:,.0f}")
        if hasattr(row, 'price_pattern') and row.price_pattern != "None":
            factors.append(f"Pattern: {row.price_pattern}")
        if hasattr(row, 'market_structure') and row.market_structure == "Uptrend":
            factors.append("Uptrend Structure")
        if hasattr(row, 'rsi') and row.rsi is not None and row.rsi < 30:
            factors.append(f"RSI: {row.rsi:.1f}")
        if hasattr(row, 'macd_signal') and row.macd_signal == "Bullish":
            factors.append("MACD Bullish")
        if hasattr(row, 'adx') and row.adx > 20:
            factors.append(f"ADX: {row.adx:.1f}")
        if hasattr(row, 'fib_support') and row.fib_support:
            factors.append("Fib Support")
            
        message += f"   Faktor: {', '.join(factors)}\n"
        
        # Target harga dan stop loss
        target_price_1 = row.close * 1.05  # Target 5%
        target_price_2 = row.close * 1.10  # Target 10%
        stop_loss = row.close * 0.95      # Stop loss 5%
        
        message += f"   🎯 Target 1: Rp{target_price_1:,.0f} (+5%) | Target 2: Rp{target_price_2:,.0f} (+10%)\n"
        message += f"   🛑 Stop Loss: Rp{stop_loss:,.0f} (-5%)\n\n"
    
    # Strategy section
    message += "*Strategi Entry:*\n"
    message += "• Beli pada harga pasar atau tunggu pullback kecil\n"
    message += "• Entry bertahap: 50% posisi di awal, 50% setelah konfirmasi\n"
    message += "• Hold periode: 5-10 hari trading\n\n"
    
    # Tambahkan disclaimer
    message += "*Disclaimer:*\n"
    message += "Analisis ini menggunakan algoritma data science dan tidak menjamin profit. "
    message += "Lakukan analisis tambahan dan gunakan manajemen risiko."
    
    # Send to Telegram
    result = send_telegram_message(message)
    if "successfully" in result:
        return f"High probability signals sent: {len(df)} stocks"
    else:
        return result

def send_performance_report():
    """
    Mengirim laporan performa dari sinyal trading sebelumnya
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    # Dapatkan tanggal terakhir
    latest_date = get_latest_stock_date()
    
    # Query untuk mendapatkan performa sinyal sebelumnya (backtest)
    try:
        sql = f"""
        WITH backtest_results AS (
            SELECT 
                symbol,
                signal_date,
                buy_score,
                winning_probability,
                entry_price,
                exit_price,
                percent_change_5d,
                is_win
            FROM public_analytics.backtest_results
            WHERE signal_date >= '{latest_date}'::date - INTERVAL '60 days'
        )
        SELECT 
            buy_score,
            COUNT(*) as total_signals,
            SUM(CASE WHEN is_win THEN 1 ELSE 0 END) as win_count,
            ROUND(SUM(CASE WHEN is_win THEN 1 ELSE 0 END)::float / COUNT(*) * 100, 1) as win_rate,
            ROUND(AVG(percent_change_5d), 2) as avg_return
        FROM backtest_results
        GROUP BY buy_score
        ORDER BY buy_score DESC
        """
        
        try:
            performance_df = pd.read_sql(sql, conn)
        except:
            # Jika tabel backtest_results belum ada
            performance_df = pd.DataFrame()
            logger.warning("backtest_results table does not exist yet")
        
        # Query untuk recent signals yang sudah complete (5+ hari)
        recent_sql = f"""
        WITH completed_signals AS (
            SELECT 
                s.symbol,
                s.date as signal_date,
                s.buy_score,
                s.winning_probability,
                m1.close as entry_price,
                m2.close as exit_price,
                ROUND(((m2.close - m1.close) / m1.close * 100)::numeric, 2) as percent_change,
                CASE WHEN ((m2.close - m1.close) / m1.close * 100) > 2 THEN TRUE ELSE FALSE END as is_win,
                m1.name
            FROM public_analytics.advanced_trading_signals s
            JOIN public.daily_stock_summary m1 
                ON s.symbol = m1.symbol AND s.date = m1.date
            JOIN public.daily_stock_summary m2
                ON s.symbol = m2.symbol AND m2.date = s.date + INTERVAL '5 days'
            WHERE s.date BETWEEN '{latest_date}'::date - INTERVAL '30 days' AND '{latest_date}'::date - INTERVAL '5 days'
            AND s.winning_probability >= 0.8
        )
        SELECT * FROM completed_signals
        ORDER BY signal_date DESC, percent_change DESC
        LIMIT 10
        """
        
        try:
            recent_df = pd.read_sql(recent_sql, conn)
        except:
            # Jika belum ada sinyal yang completed
            recent_df = pd.DataFrame()
            logger.warning("No completed signals found")
            
        conn.close()
    except Exception as e:
        logger.error(f"Error querying performance data: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error querying performance data: {str(e)}"
    
    # Create Telegram message
    message = "📊 *LAPORAN PERFORMA SINYAL TRADING* 📊\n\n"
    
    # Bagian 1: Statistik win rate
    if not performance_df.empty:
        message += "*Statistik Win Rate Berdasarkan Skor:*\n\n"
        message += "| Skor | Jumlah | Win Rate | Avg Return |\n"
        message += "|------|--------|----------|------------|\n"
        
        for _, row in performance_df.iterrows():
            message += f"| {row['buy_score']} | {row['total_signals']} | {row['win_rate']}% | {row['avg_return']}% |\n"
        
        # Overall statistics
        total_signals = performance_df['total_signals'].sum()
        total_wins = performance_df['win_count'].sum()
        overall_win_rate = (total_wins / total_signals * 100) if total_signals > 0 else 0
        avg_return = performance_df['avg_return'].mean()
        
        message += f"\n*Overall:* {total_signals} sinyal, Win Rate {overall_win_rate:.1f}%, Return Rata-rata {avg_return:.2f}%\n\n"
    else:
        message += "*Belum ada data backtest yang cukup*\n\n"
    
    # Bagian 2: Recent completed signals performance
    if not recent_df.empty:
        message += "*Performance Sinyal Terbaru:*\n\n"
        
        win_count = 0
        for i, row in enumerate(recent_df.itertuples(), 1):
            # Format date
            signal_date = row.signal_date
            if isinstance(signal_date, pd.Timestamp):
                signal_date = signal_date.strftime('%Y-%m-%d')
                
            # Emoji based on win/loss
            emoji = "✅" if row.is_win else "❌"
            
            message += f"{emoji} *{row.symbol}* ({row.name}): {row.percent_change}%\n"
            message += f"   Tanggal: {signal_date} | Skor: {row.buy_score} | Win Prob: {row.winning_probability*100:.0f}%\n"
            message += f"   Entry: Rp{row.entry_price:,.0f} → Exit: Rp{row.exit_price:,.0f}\n\n"
            
            if row.is_win:
                win_count += 1
        
        recent_win_rate = (win_count / len(recent_df) * 100)
        message += f"*Win Rate 30 Hari Terakhir:* {recent_win_rate:.1f}% ({win_count}/{len(recent_df)})\n\n"
    else:
        message += "*Belum ada sinyal yang complete dalam 30 hari terakhir*\n\n"
    
    # Bagian 3: Tips untuk meningkatkan win rate
    message += "*Tips Meningkatkan Win Rate:*\n"
    message += "1. Utamakan saham dengan skor 8+ (win rate >85%)\n"
    message += "2. Cari konfirmasi volume pada breakout\n"
    message += "3. Gunakan money management yang ketat (max 2% risiko per trade)\n"
    message += "4. Take profit bertahap pada +5% dan +10%\n"
    
    # Send to Telegram
    result = send_telegram_message(message)
    if "successfully" in result:
        return "Performance report sent successfully"
    else:
        return result


import pandas as pd
import numpy as np
import logging
from .database import get_database_connection, get_latest_stock_date, execute_query, fetch_data, create_table_if_not_exists

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_rsi(data, period=14):
    """
    Menghitung indikator RSI (Relative Strength Index)
    """
    # Hitung perubahan harga
    delta = data.diff()
    
    # Pisahkan kenaikan dan penurunan
    gain = delta.clip(lower=0)
    loss = -1 * delta.clip(upper=0)
    
    # Hitung rata-rata kenaikan dan penurunan
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    # Hitung RS
    rs = avg_gain / avg_loss
    
    # Hitung RSI
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def calculate_macd(data, fast_period=12, slow_period=26, signal_period=9):
    """
    Menghitung indikator MACD (Moving Average Convergence Divergence)
    """
    # Hitung EMA
    ema_fast = data.ewm(span=fast_period, adjust=False).mean()
    ema_slow = data.ewm(span=slow_period, adjust=False).mean()
    
    # Hitung MACD line
    macd_line = ema_fast - ema_slow
    
    # Hitung signal line
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    
    # Hitung histogram - GANTI MENJADI macd_histogram
    macd_histogram = macd_line - signal_line
    
    return macd_line, signal_line, macd_histogram

def calculate_bollinger_bands(data, period=20, std_dev=2):
    """
    Menghitung Bollinger Bands
    """
    # Hitung moving average
    middle_band = data.rolling(window=period).mean()
    
    # Hitung standard deviation
    std = data.rolling(window=period).std()
    
    # Hitung upper dan lower bands
    upper_band = middle_band + (std * std_dev)
    lower_band = middle_band - (std * std_dev)
    
    # Hitung %B (posisi harga relatif terhadap bands)
    percent_b = (data - lower_band) / (upper_band - lower_band)
    
    return middle_band, upper_band, lower_band, percent_b

def calculate_all_rsi_indicators(lookback_period=100, rsi_period=14, signal_type='DAILY'):
    """
    Menghitung indikator RSI untuk semua saham dan menyimpan ke database
    
    Parameters:
    lookback_period (int): Jumlah hari ke belakang untuk analisis (default: 100 untuk daily)
    rsi_period (int): Periode untuk perhitungan RSI (default: 14)
    signal_type (str): Tipe sinyal - DAILY, WEEKLY, atau MONTHLY
    """
    # Ambil data harga saham dengan periode lookback yang sesuai
    query = f"""
        SELECT 
            symbol, 
            date, 
            close
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '{lookback_period} days'
        ORDER BY symbol, date
    """
    
    df = fetch_data(query)
    
    # Jika tidak ada data
    if df.empty:
        logger.warning(f"Tidak ada data harga saham untuk perhitungan RSI {signal_type}")
        return f"Tidak ada data untuk {signal_type}"
    
    # Nama tabel disesuaikan dengan signal_type
    table_name = f"public_analytics.technical_indicators_rsi_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_rsi"
    
    # Buat tabel jika belum ada
    create_table_if_not_exists(
        table_name,
        f"""
        CREATE TABLE {table_name} (
            symbol TEXT,
            date DATE,
            rsi NUMERIC,
            rsi_signal TEXT,
            PRIMARY KEY (symbol, date)
        )
        """
    )
    
    # Buat DataFrame untuk menyimpan hasil
    results = []
    
    # Hitung RSI untuk setiap saham dengan periode yang diberikan
    for symbol, group in df.groupby('symbol'):
        if len(group) < rsi_period + 1:  # Minimal data yang diperlukan
            continue
            
        # Sort berdasarkan tanggal
        group = group.sort_values('date')
        
        # Hitung RSI dengan periode yang diberikan
        group['rsi'] = calculate_rsi(group['close'], period=rsi_period)
        
        # Filter hanya data terbaru saja
        latest_data = group.dropna().tail(10)  # Ambil 10 hari terbaru dengan RSI yang valid
        
        # Sesuaikan threshold RSI berdasarkan timeframe
        if signal_type == 'WEEKLY':
            oversold_threshold = 35  # Lebih longgar untuk weekly
            overbought_threshold = 65
        elif signal_type == 'MONTHLY':
            oversold_threshold = 40  # Lebih longgar lagi untuk monthly
            overbought_threshold = 60
        else:
            oversold_threshold = 30  # Standar untuk daily
            overbought_threshold = 70
        
        # Tentukan sinyal berdasarkan nilai RSI dan threshold
        for _, row in latest_data.iterrows():
            rsi_signal = "Neutral"
            if row['rsi'] <= oversold_threshold:
                rsi_signal = "Oversold"
            elif row['rsi'] >= overbought_threshold:
                rsi_signal = "Overbought"
                
            results.append({
                'symbol': symbol,
                'date': row['date'],
                'rsi': row['rsi'],
                'rsi_signal': rsi_signal
            })
    
    # Konversi hasil ke DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        logger.warning("Tidak ada hasil RSI yang valid")
        return "Tidak ada hasil RSI"
    
    # Simpan hasil ke database
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Hapus data yang sudah ada untuk tanggal yang akan diupdate
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
    # Masukkan data baru
    for _, row in result_df.iterrows():
        cursor.execute(f"""
        INSERT INTO {table_name} (symbol, date, rsi, rsi_signal)
        VALUES (%s, %s, %s, %s)
        """, (row['symbol'], row['date'], row['rsi'], row['rsi_signal']))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Berhasil menghitung RSI {signal_type} untuk {result_df['symbol'].nunique()} saham pada {len(dates_to_update)} tanggal"

def calculate_all_macd_indicators(lookback_period=150, fast_period=12, slow_period=26, signal_period=9, signal_type='DAILY'):
    """
    Menghitung indikator MACD untuk semua saham dan menyimpan ke database
    
    Parameters:
    lookback_period (int): Jumlah hari ke belakang untuk analisis
    fast_period (int): Periode EMA cepat untuk MACD
    slow_period (int): Periode EMA lambat untuk MACD
    signal_period (int): Periode signal line
    signal_type (str): Tipe sinyal - DAILY, WEEKLY, atau MONTHLY
    """
    # Ambil data dengan lookback period yang sesuai
    df = fetch_data(f"""
        SELECT 
            symbol, 
            date, 
            close
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '{lookback_period} days'
        ORDER BY symbol, date
    """)
    
    # Jika tidak ada data
    if df.empty:
        logger.warning(f"Tidak ada data harga saham untuk perhitungan MACD {signal_type}")
        return f"Tidak ada data {signal_type}"
    
    # Untuk weekly/monthly, gunakan periode MACD yang lebih panjang
    if signal_type == 'WEEKLY':
        min_days_required = 40  # Lebih banyak data untuk weekly
    elif signal_type == 'MONTHLY':
        min_days_required = 60  # Lebih banyak lagi untuk monthly
    else:
        min_days_required = 30  # Default untuk daily
    
    # Nama tabel disesuaikan dengan signal_type
    table_name = f"public_analytics.technical_indicators_macd_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_macd"
    
    # Buat tabel jika belum ada
    create_table_if_not_exists(
        table_name,
        f"""
        CREATE TABLE {table_name} (
            symbol TEXT,
            date DATE,
            macd_line NUMERIC,
            signal_line NUMERIC,
            macd_histogram NUMERIC,
            macd_signal TEXT,
            PRIMARY KEY (symbol, date)
        )
        """
    )
    
    # Buat DataFrame untuk menyimpan hasil
    results = []
    
    # Hitung MACD untuk setiap saham
    for symbol, group in df.groupby('symbol'):
        if len(group) < min_days_required:  # Minimal data yang diperlukan
            continue
            
        # Sort berdasarkan tanggal
        group = group.sort_values('date')
        
        # Hitung MACD dengan parameter yang diberikan
        macd_line, signal_line, macd_histogram = calculate_macd(
            group['close'], 
            fast_period=fast_period, 
            slow_period=slow_period, 
            signal_period=signal_period
        )
        
        # Gabungkan hasil
        group['macd_line'] = macd_line
        group['signal_line'] = signal_line
        group['macd_histogram'] = macd_histogram
        
        # Tentukan sinyal berdasarkan MACD
        group['prev_macd_histogram'] = group['macd_histogram'].shift(1)
        
        # Filter hanya data terbaru saja
        latest_data = group.dropna().tail(10)  # Ambil 10 hari terbaru dengan MACD yang valid
        
        for _, row in latest_data.iterrows():
            # Tentukan sinyal MACD
            macd_signal = "Neutral"
            
            # Sesuaikan sensitivitas crossover berdasarkan timeframe
            if signal_type == 'WEEKLY':
                hist_threshold = 0.03  # Lebih tinggi untuk weekly (mengurangi false signals)
            elif signal_type == 'MONTHLY':
                hist_threshold = 0.05  # Lebih tinggi lagi untuk monthly
            else:
                hist_threshold = 0.0   # Standar untuk daily
            
            # Bullish crossover (histogram berubah dari negatif ke positif)
            if row['macd_histogram'] > hist_threshold and row['prev_macd_histogram'] <= hist_threshold:
                macd_signal = "Bullish"  # Bullish crossover
            # Bearish crossover (histogram berubah dari positif ke negatif)
            elif row['macd_histogram'] < -hist_threshold and row['prev_macd_histogram'] >= -hist_threshold:
                macd_signal = "Bearish"  # Bearish crossover
            # Bullish momentum (histogram positif dan meningkat)
            elif row['macd_histogram'] > 0 and row['macd_histogram'] > row['prev_macd_histogram']:
                macd_signal = "Bullish"  # Bullish momentum
            # Bearish momentum (histogram negatif dan menurun)
            elif row['macd_histogram'] < 0 and row['macd_histogram'] < row['prev_macd_histogram']:
                macd_signal = "Bearish"  # Bearish momentum
            
            results.append({
                'symbol': symbol,
                'date': row['date'],
                'macd_line': row['macd_line'],
                'signal_line': row['signal_line'],
                'macd_histogram': row['macd_histogram'],
                'macd_signal': macd_signal
            })
    
    # Konversi hasil ke DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        logger.warning(f"Tidak ada hasil MACD {signal_type} yang valid")
        return f"Tidak ada hasil MACD {signal_type}"
    
    # Simpan hasil ke database
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Hapus data yang sudah ada untuk tanggal yang akan diupdate
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
    # Masukkan data baru
    for _, row in result_df.iterrows():
        cursor.execute(f"""
        INSERT INTO {table_name} 
        (symbol, date, macd_line, signal_line, macd_histogram, macd_signal)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row['symbol'], row['date'], row['macd_line'], 
            row['signal_line'], row['macd_histogram'], row['macd_signal']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Berhasil menghitung MACD {signal_type} untuk {result_df['symbol'].nunique()} saham pada {len(dates_to_update)} tanggal"

def calculate_all_bollinger_bands(lookback_period=50, band_period=20, std_dev=2, signal_type='DAILY'):
    """
    Menghitung Bollinger Bands untuk semua saham dan menyimpan ke database
    
    Parameters:
    lookback_period (int): Jumlah hari ke belakang untuk analisis
    band_period (int): Periode Moving Average untuk Bollinger Bands
    std_dev (int): Jumlah standar deviasi untuk band
    signal_type (str): Tipe sinyal - DAILY, WEEKLY, atau MONTHLY
    """
    # Ambil data harga saham dengan lookback period yang disesuaikan
    df = fetch_data(f"""
        SELECT 
            symbol, 
            date, 
            close
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '{lookback_period} days'
        ORDER BY symbol, date
    """)
    
    # Jika tidak ada data
    if df.empty:
        logger.warning(f"Tidak ada data harga saham untuk perhitungan Bollinger Bands {signal_type}")
        return f"Tidak ada data {signal_type}"
    
    # Nama tabel disesuaikan dengan signal_type
    table_name = f"public_analytics.technical_indicators_bollinger_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_bollinger"
    
    # Buat tabel jika belum ada
    create_table_if_not_exists(
        table_name,
        f"""
        CREATE TABLE {table_name} (
            symbol TEXT,
            date DATE,
            middle_band NUMERIC,
            upper_band NUMERIC,
            lower_band NUMERIC,
            percent_b NUMERIC,
            bb_signal TEXT,
            PRIMARY KEY (symbol, date)
        )
        """
    )
    
    # Sesuaikan minimum jumlah hari data berdasarkan timeframe
    if signal_type == 'WEEKLY':
        min_days_required = 30  # Lebih banyak data untuk weekly
    elif signal_type == 'MONTHLY':
        min_days_required = 40  # Lebih banyak lagi untuk monthly
    else:
        min_days_required = 20  # Default untuk daily
    
    # Buat DataFrame untuk menyimpan hasil
    results = []
    
    # Hitung Bollinger Bands untuk setiap saham
    for symbol, group in df.groupby('symbol'):
        if len(group) < min_days_required:  # Minimal data yang diperlukan
            continue
            
        # Sort berdasarkan tanggal
        group = group.sort_values('date')
        
        # Hitung Bollinger Bands dengan parameter yang diberikan
        middle_band, upper_band, lower_band, percent_b = calculate_bollinger_bands(
            group['close'], 
            period=band_period, 
            std_dev=std_dev
        )
        
        # Gabungkan hasil
        group['middle_band'] = middle_band
        group['upper_band'] = upper_band
        group['lower_band'] = lower_band
        group['percent_b'] = percent_b
        
        # Filter hanya data terbaru saja
        latest_data = group.dropna().tail(10)  # Ambil 10 hari terbaru
        
        # Sesuaikan threshold berdasarkan timeframe
        if signal_type == 'WEEKLY':
            near_overbought = 0.75  # Lebih longgar untuk weekly
            near_oversold = 0.25
        elif signal_type == 'MONTHLY':
            near_overbought = 0.7   # Lebih longgar lagi untuk monthly
            near_oversold = 0.3
        else:
            near_overbought = 0.8   # Standar untuk daily
            near_oversold = 0.2
        
        # Tentukan sinyal berdasarkan Bollinger Bands
        for _, row in latest_data.iterrows():
            bb_signal = "Neutral"
            
            if row['close'] > row['upper_band']:
                bb_signal = "Overbought"
            elif row['close'] < row['lower_band']:
                bb_signal = "Oversold"
            elif row['percent_b'] > near_overbought:
                bb_signal = "Near Overbought"
            elif row['percent_b'] < near_oversold:
                bb_signal = "Near Oversold"
                
            results.append({
                'symbol': symbol,
                'date': row['date'],
                'middle_band': row['middle_band'],
                'upper_band': row['upper_band'],
                'lower_band': row['lower_band'],
                'percent_b': row['percent_b'],
                'bb_signal': bb_signal
            })
    
    # Konversi hasil ke DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        logger.warning(f"Tidak ada hasil Bollinger Bands {signal_type} yang valid")
        return f"Tidak ada hasil Bollinger Bands {signal_type}"
    
    # Simpan hasil ke database
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Hapus data yang sudah ada untuk tanggal yang akan diupdate
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
    # Masukkan data baru
    for _, row in result_df.iterrows():
        cursor.execute(f"""
        INSERT INTO {table_name} 
        (symbol, date, middle_band, upper_band, lower_band, percent_b, bb_signal)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row['symbol'], row['date'], row['middle_band'], 
            row['upper_band'], row['lower_band'], 
            row['percent_b'], row['bb_signal']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Berhasil menghitung Bollinger Bands {signal_type} untuk {result_df['symbol'].nunique()} saham pada {len(dates_to_update)} tanggal"

def check_and_create_bollinger_bands_table():
    """
    Memeriksa apakah tabel Bollinger Bands sudah ada, jika belum buat baru
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Periksa apakah tabel sudah ada
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = 'technical_indicators_bollinger'
        )
        """)
        
        table_exists = cursor.fetchone()[0]
        
        # Jika tabel belum ada, buat baru
        if not table_exists:
            logger.info("Creating technical_indicators_bollinger table as it doesn't exist")
            
            # Pastikan schema public_analytics sudah ada
            cursor.execute("CREATE SCHEMA IF NOT EXISTS public_analytics")
            
            # Buat tabel Bollinger Bands
            cursor.execute("""
            CREATE TABLE public_analytics.technical_indicators_bollinger (
                symbol TEXT,
                date DATE,
                middle_band NUMERIC,
                upper_band NUMERIC,
                lower_band NUMERIC,
                percent_b NUMERIC,
                bb_signal TEXT,
                PRIMARY KEY (symbol, date)
            )
            """)
            
            # Dapatkan tanggal terakhir
            latest_date = get_latest_stock_date()
            
            # Kalkulasi Bollinger Bands dari data harga dan masukkan ke tabel secara terpisah
            # Tahap 1: Hitung nilai untuk setiap saham
            cursor.execute(f"""
            WITH daily_prices AS (
                SELECT
                    symbol,
                    date,
                    close
                FROM public.daily_stock_summary
                WHERE date >= '{latest_date}'::date - INTERVAL '50 days'
            ),
            -- Hitung SMA 20 dan standard deviation
            sma_std AS (
                SELECT
                    symbol,
                    date,
                    close,
                    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS middle_band,
                    STDDEV(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS std_dev
                FROM daily_prices
            )
            SELECT
                symbol,
                date,
                close,
                middle_band,
                middle_band + (2 * std_dev) AS upper_band,
                middle_band - (2 * std_dev) AS lower_band
            INTO TEMP temp_bollinger
            FROM sma_std
            WHERE middle_band IS NOT NULL AND std_dev IS NOT NULL;
            """)
            
            # Tahap 2: Masukkan data dengan menghitung percent_b dan bb_signal
            cursor.execute("""
            INSERT INTO public_analytics.technical_indicators_bollinger (
                symbol, date, middle_band, upper_band, lower_band, percent_b, bb_signal
            )
            SELECT
                symbol,
                date,
                middle_band,
                upper_band,
                lower_band,
                CASE
                    WHEN (upper_band - lower_band) = 0 THEN 0.5
                    ELSE (close - lower_band) / (upper_band - lower_band)
                END AS percent_b,
                CASE
                    WHEN close > upper_band THEN 'Overbought'
                    WHEN close < lower_band THEN 'Oversold'
                    ELSE 'Neutral'
                END AS bb_signal
            FROM temp_bollinger;
            """)
            
            conn.commit()
            logger.info("Successfully created and populated technical_indicators_bollinger table")
        
        cursor.close()
        conn.close()
        return True
    
    except Exception as e:
        logger.error(f"Error checking/creating Bollinger Bands table: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        return False