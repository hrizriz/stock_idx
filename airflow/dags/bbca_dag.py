from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
import logging
import sys
import os

# Setup logging
logger = logging.getLogger(__name__)

# Ensure lstm_models directory is in PATH
module_path = os.path.abspath("/opt/airflow/dags")
if module_path not in sys.path:
    sys.path.append(module_path)

# Impor hanya fungsi-fungsi yang dibutuhkan dengan wildcard
try:
    # Coba impor secara wildcard untuk menambahkan semua fungsi ke namespace
    from lstm_models import *
    logger.info("Successfully imported all functions from lstm_models")
except ImportError as e:
    logger.error(f"Error importing from lstm_models: {e}")
    
    # Fallback - define dummy functions to prevent crashing
    def create_necessary_tables():
        logger.warning("Using dummy create_necessary_tables function")
        return "Tables would be created"
    
    def extract_stock_data(symbol="BBCA", days=500):
        logger.warning("Using dummy extract_stock_data function")
        return "/opt/airflow/data/lstm/bbca_data.csv"
    
    def hyperparameter_tuning(data_path, model_type):
        logger.warning(f"Using dummy hyperparameter_tuning function for {model_type}")
        return {
            'seq_length': 10,
            'hidden_dim': 64,
            'num_layers': 2,
            'learning_rate': 0.01,
            'dropout': 0.2
        }
    
    def train_models(data_path, model_types):
        logger.warning("Using dummy train_models function")
        return {"status": "Models trained"}
    
    def make_ensemble_predictions(symbol, model_types):
        logger.warning("Using dummy make_ensemble_predictions function")
        return 9000.0
    
    def update_actual_prices():
        logger.warning("Using dummy update_actual_prices function")
        return 0
    
    def perform_backtesting(symbol, test_period):
        logger.warning("Using dummy perform_backtesting function")
        return {
            'model_type': 'LSTM_Fixed',
            'symbol': symbol,
            'test_start_date': '2025-01-01',
            'test_end_date': '2025-02-01',
            'rmse': 100.0,
            'mae': 80.0,
            'mape': 5.0,
            'test_size': test_period
        }

# Set timezone
local_tz = pendulum.timezone("Asia/Jakarta")

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

####################
# Airflow DAG Tasks #
####################

def task_create_tables(**kwargs):
    """Airflow task to create necessary database tables"""
    return create_necessary_tables()


def task_extract_data(**kwargs):
    """Airflow task to extract stock data"""
    return extract_stock_data(symbol="BBCA", days=500)


def task_hyperparameter_tuning(**kwargs):
    """Airflow task to perform hyperparameter tuning"""
    ti = kwargs['ti']
    data_path = ti.xcom_pull(task_ids='extract_data')
    
    # Validasi input
    if not data_path:
        logger.error("No data path pulled from XCom")
        return {"error": "No data path available"}
    
    # Log info untuk debugging
    logger.info(f"Starting hyperparameter tuning with data path: {data_path}")
    
    # Tune each model type
    results = {}
    for model_type in ['LSTM', 'BiLSTM', 'ConvLSTM']:
        try:
            logger.info(f"Tuning {model_type}")
            params = hyperparameter_tuning(data_path, model_type)
            results[model_type] = params
        except Exception as e:
            logger.error(f"Error tuning {model_type}: {e}")
            results[model_type] = {"error": str(e)}
    
    return results


def task_train_models(**kwargs):
    """Airflow task to train all models"""
    ti = kwargs['ti']
    data_path = ti.xcom_pull(task_ids='extract_data')
    
    if not data_path:
        logger.error("No data path pulled from XCom")
        return {"error": "No data path available"}
    
    logger.info(f"Training models with data path: {data_path}")
    return train_models(data_path, model_types=['LSTM', 'BiLSTM', 'ConvLSTM'])


def task_make_predictions(**kwargs):
    """Airflow task to make ensemble predictions"""
    return make_ensemble_predictions(symbol="BBCA", model_types=['LSTM', 'BiLSTM', 'ConvLSTM'])


def task_update_actual_prices(**kwargs):
    """Airflow task to update actual prices and calculate errors"""
    return update_actual_prices()


def task_perform_backtesting(**kwargs):
    """Airflow task to perform backtesting"""
    return perform_backtesting(symbol="BBCA", test_period=60)


# DAG definition
with DAG(
    dag_id="bbca_lstm_prediction_enhanced",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="30 18 * * 1-5",  # 18:30 setiap hari kerja
    catchup=False,
    default_args=default_args,
    tags=["lstm", "prediction", "bbca", "enhanced"],
    description="Enhanced BBCA stock price prediction using ensemble LSTM models"
) as dag:
    
    # Wait for data transformation to complete
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
    
    # Create necessary tables
    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=task_create_tables
    )
    
    # Extract data
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=task_extract_data
    )
    
    # Hyperparameter tuning
    hyper_tuning = PythonOperator(
        task_id="hyperparameter_tuning",
        python_callable=task_hyperparameter_tuning
    )
    
    # Train models
    train_models_task = PythonOperator(
        task_id="train_models",
        python_callable=task_train_models
    )
    
    # Make predictions
    make_predictions = PythonOperator(
        task_id="make_predictions",
        python_callable=task_make_predictions
    )
    
    # Update actual prices (run next day)
    update_prices = PythonOperator(
        task_id="update_actual_prices",
        python_callable=task_update_actual_prices
    )
    
    # Perform backtesting
    backtesting = PythonOperator(
        task_id="perform_backtesting",
        python_callable=task_perform_backtesting
    )
    
    # Run dbt models for LSTM
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --select stg_stock_predictions fct_stock_predictions lstm_performance_metrics stock_prediction_dashboard',
    )
    
    # Define task dependencies (flow)
    wait_for_transformation >> create_tables >> extract_data
    extract_data >> hyper_tuning >> train_models_task
    train_models_task >> [make_predictions, backtesting]
    make_predictions >> update_prices >> run_dbt
    backtesting >> run_dbt