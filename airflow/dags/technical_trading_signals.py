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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure timezone
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

def create_trading_signals_dag(
    dag_id,
    schedule_interval,
    lookback_periods,
    hold_period,
    signal_type,
    volatility_params,
    min_probability=0.7,
    backtest_period=180,
    performance_lookback=60
):
    """
    Factory function to create trading signal DAGs with different timeframe settings
    
    Parameters:
    dag_id (str): ID for the DAG
    schedule_interval (str): Cron expression for scheduling
    lookback_periods (dict): Dictionary with lookback periods for different indicators
    hold_period (int): Number of days to hold position in backtest
    signal_type (str): Signal type - DAILY, WEEKLY, or MONTHLY
    volatility_params (dict): Parameters for filtering stocks by volatility
    min_probability (float): Minimum probability for signals
    backtest_period (int): Period for backtesting in days
    performance_lookback (int): Days to look back for performance report
    
    Returns:
    DAG: Configured Airflow DAG
    """
    # Derive tags based on signal type
    tags = ["trading", "technical", "signals"]
    if signal_type != 'DAILY':
        tags.append(signal_type.lower())
    
    # Create DAG
    with DAG(
        dag_id=dag_id,
        start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
        schedule_interval=schedule_interval,
        catchup=False,
        default_args=default_args,
        tags=tags
    ) as dag:
        
        # Task 1: Wait for data transformation to complete
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
        
        # Task 2: Calculate RSI indicators
        calc_rsi = PythonOperator(
            task_id=f"calculate_{signal_type.lower()}_rsi",
            python_callable=calculate_all_rsi_indicators,
            op_kwargs={
                'lookback_period': lookback_periods['rsi'], 
                'rsi_period': 14, 
                'signal_type': signal_type
            }
        )
        
        # Task 3: Calculate MACD indicators
        calc_macd = PythonOperator(
            task_id=f"calculate_{signal_type.lower()}_macd",
            python_callable=calculate_all_macd_indicators,
            op_kwargs={
                'lookback_period': lookback_periods['macd'], 
                'fast_period': lookback_periods.get('macd_fast', 12),
                'slow_period': lookback_periods.get('macd_slow', 26), 
                'signal_period': lookback_periods.get('macd_signal', 9),
                'signal_type': signal_type
            }
        )
        
        # Task 4: Setup Bollinger Bands table
        check_bb = PythonOperator(
            task_id="check_bollinger_bands_table",
            python_callable=check_and_create_bollinger_bands_table
        )
        
        # Task 5: Calculate Bollinger Bands
        calc_bb = PythonOperator(
            task_id=f"calculate_{signal_type.lower()}_bollinger",
            python_callable=calculate_all_bollinger_bands,
            op_kwargs={
                'lookback_period': lookback_periods['bb'], 
                'band_period': 20, 
                'std_dev': 2, 
                'signal_type': signal_type
            }
        )
        
        # Task 6: Filter stocks by volatility and liquidity
        filter_stocks = PythonOperator(
            task_id="filter_stocks_by_volatility_liquidity",
            python_callable=filter_by_volatility_liquidity,
            op_kwargs={
                'analysis_period': volatility_params.get('analysis_period', 30),
                'signal_type': signal_type,
                'volatility_min': volatility_params.get('min', 1.0),
                'volatility_max': volatility_params.get('max', 4.0),
                'volume_min': volatility_params.get('volume_min', 1000000)
            }
        )
        
        # Task 7: Calculate advanced indicators
        calc_advanced = PythonOperator(
            task_id=f"calculate_{signal_type.lower()}_advanced_indicators",
            python_callable=calculate_advanced_indicators,
            op_kwargs={
                'lookback_period': lookback_periods.get('advanced', 300), 
                'signal_type': signal_type
            },
            retries=2,
            retry_delay=pendulum.duration(minutes=5)
        )
        
        # Optional task: Confirm signals with multiple indicators
        confirm_signals = PythonOperator(
            task_id=f"confirm_{signal_type.lower()}_signals",
            python_callable=lambda: logger.info(f"Confirming {signal_type} signals with multiple indicators")
        )
        
        # Task 8: Run backtesting
        run_backtest = PythonOperator(
            task_id=f"run_{signal_type.lower()}_backtest",
            python_callable=backtest_trading_signals,
            op_kwargs={
                'test_period': backtest_period, 
                'hold_period': hold_period, 
                'signal_type': signal_type,
                'min_win_rate': min_probability - 0.1  # Slightly lower threshold for backtest
            },
            trigger_rule='none_failed'
        )
        
        # Task 9: Send high probability signals
        send_signals = PythonOperator(
            task_id=f"send_{signal_type.lower()}_high_probability_signals",
            python_callable=send_high_probability_signals,
            op_kwargs={
                'signal_type': signal_type, 
                'min_probability': min_probability
            }
        )
        
        # Task 10: Send performance report
        send_report = PythonOperator(
            task_id=f"send_{signal_type.lower()}_performance_report",
            python_callable=send_performance_report,
            op_kwargs={
                'report_type': signal_type, 
                'lookback_days': performance_lookback
            }
        )
        
        # Task 11: End marker
        end_task = DummyOperator(
            task_id="end_task"
        )
        
        # Define task dependencies (identical across all timeframes)
        wait_for_transformation >> [calc_rsi, calc_macd, check_bb]
        check_bb >> calc_bb
        [calc_rsi, calc_macd, calc_bb] >> filter_stocks >> calc_advanced
        calc_advanced >> [run_backtest, confirm_signals]
        [run_backtest, confirm_signals] >> send_signals >> send_report >> end_task
        
        return dag

# Create Daily Trading Signals DAG
daily_signals = create_trading_signals_dag(
    dag_id="technical_trading_signals",
    schedule_interval="0 17 * * 1-5",  # Every weekday at 17:00 Jakarta time
    lookback_periods={
        'rsi': 100, 
        'macd': 150, 
        'bb': 50,
        'advanced': 300,
        'macd_fast': 12,
        'macd_slow': 26,
        'macd_signal': 9
    },
    hold_period=5,
    signal_type='DAILY',
    volatility_params={
        'min': 1.0, 
        'max': 4.0, 
        'volume_min': 1000000,
        'analysis_period': 30
    },
    min_probability=0.8,
    backtest_period=180,
    performance_lookback=60
)

# Create Weekly Trading Signals DAG
weekly_signals = create_trading_signals_dag(
    dag_id="weekly_trading_signals",
    schedule_interval="30 16 * * 5",  # Every Friday at 16:30 Jakarta time
    lookback_periods={
        'rsi': 180, 
        'macd': 250, 
        'bb': 150,
        'advanced': 365,
        'macd_fast': 12,
        'macd_slow': 26,
        'macd_signal': 9
    },
    hold_period=15,
    signal_type='WEEKLY',
    volatility_params={
        'min': 1.5, 
        'max': 5.0, 
        'volume_min': 5000000,
        'analysis_period': 90
    },
    min_probability=0.75,
    backtest_period=365,
    performance_lookback=90
)

# Create Monthly Trading Signals DAG
monthly_signals = create_trading_signals_dag(
    dag_id="monthly_trading_signals",
    schedule_interval="0 17 1 * *",  # First day of each month at 17:00 Jakarta time
    lookback_periods={
        'rsi': 730, 
        'macd': 730, 
        'bb': 300,
        'advanced': 730,
        'macd_fast': 26,
        'macd_slow': 52,
        'macd_signal': 18
    },
    hold_period=20,
    signal_type='MONTHLY',
    volatility_params={
        'min': 2.0, 
        'max': 8.0, 
        'volume_min': 10000000,
        'analysis_period': 180
    },
    min_probability=0.7,
    backtest_period=730,
    performance_lookback=365
)