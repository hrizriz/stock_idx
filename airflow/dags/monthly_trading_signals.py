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

# DAG definition - Monthly
with DAG(
    dag_id="monthly_trading_signals",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 17 1 * *",  # First day of each month at 17:00 Jakarta time
    catchup=False,
    default_args=default_args,
    tags=["trading", "technical", "signals", "monthly"]
) as dag:
    
    # Wait until data transformation is complete
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
        op_kwargs={'lookback_period': 365, 'rsi_period': 14, 'signal_type': 'MONTHLY'}
    )
    
    # Step 2: Calculate MACD indicators (longer period for monthly)
    calc_macd = PythonOperator(
        task_id="calculate_monthly_macd",
        python_callable=calculate_all_macd_indicators,
        op_kwargs={'lookback_period': 500, 'fast_period': 12, 'slow_period': 26, 'signal_period': 9, 'signal_type': 'MONTHLY'}
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
        op_kwargs={'lookback_period': 300, 'band_period': 20, 'std_dev': 2, 'signal_type': 'MONTHLY'}
    )
    
    # Step 5: Filter stocks by volatility and liquidity (for monthly timeframe)
    filter_stocks = PythonOperator(
        task_id="filter_stocks_by_volatility_liquidity",
        python_callable=filter_by_volatility_liquidity,
        op_kwargs={'analysis_period': 180, 'signal_type': 'MONTHLY'}
    )
    
    # Step 6: Calculate advanced indicators (with monthly parameters)
    calc_advanced = PythonOperator(
        task_id="calculate_monthly_advanced_indicators",
        python_callable=calculate_advanced_indicators,
        op_kwargs={'lookback_period': 730, 'signal_type': 'MONTHLY'},
        retries=2,
        retry_delay=pendulum.duration(minutes=5)
    )
    
    # Step 7: Run backtesting (with much longer evaluation period)
    run_backtest = PythonOperator(
        task_id="run_monthly_backtest",
        python_callable=backtest_trading_signals,
        op_kwargs={'test_period': 730, 'hold_period': 20, 'signal_type': 'MONTHLY'},
        trigger_rule='none_failed'
    )
    
    # Step 8: Send high probability monthly signals
    send_signals = PythonOperator(
        task_id="send_monthly_high_probability_signals",
        python_callable=send_high_probability_signals,
        op_kwargs={'signal_type': 'MONTHLY', 'min_probability': 0.7}
    )
    
    # Step 9: Send monthly performance report
    send_report = PythonOperator(
        task_id="send_monthly_performance_report",
        python_callable=send_performance_report,
        op_kwargs={'report_type': 'MONTHLY', 'lookback_days': 365}
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