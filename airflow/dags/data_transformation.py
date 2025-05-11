from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import psycopg2
import pendulum

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

def verify_source_tables():
    """
    Memverifikasi bahwa semua tabel sumber yang dibutuhkan oleh model DBT telah ada
    """
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    
    # Daftar tabel yang dibutuhkan untuk model DBT
    required_tables = [
        "daily_stock_summary",
        "dim_companies",
        "detik_ticker_sentiment",
        "detik_news"
    ]
    
    # Periksa apakah tabel ada
    missing_tables = []
    for table in required_tables:
        cur.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{table}'
        )
        """)
        exists = cur.fetchone()[0]
        if not exists:
            missing_tables.append(table)
    
    # Jika ada tabel yang hilang, buat tabel kosong
    if missing_tables:
        print(f"⚠️ Tabel yang hilang: {missing_tables}")
        
        # Buat tabel yang hilang
        for table in missing_tables:
            if table == "daily_stock_summary":
                cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_stock_summary (
                    symbol TEXT,
                    name TEXT,
                    date DATE,
                    prev_close NUMERIC,
                    open_price NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    change NUMERIC,
                    volume BIGINT,
                    value NUMERIC,
                    frequency INTEGER,
                    index_individual NUMERIC,
                    weight_for_index NUMERIC,
                    foreign_buy BIGINT,
                    foreign_sell BIGINT,
                    offer NUMERIC,
                    offer_volume BIGINT,
                    bid NUMERIC,
                    bid_volume BIGINT,
                    listed_shares BIGINT,
                    tradable_shares BIGINT,
                    non_regular_volume BIGINT,
                    non_regular_value NUMERIC,
                    non_regular_frequency INTEGER,
                    upload_file TEXT,
                    PRIMARY KEY (symbol, date)
                )
                """)
                print(f"✅ Tabel {table} telah dibuat")
            
            elif table == "dim_companies":
                cur.execute("""
                CREATE TABLE IF NOT EXISTS dim_companies (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    sector TEXT,
                    industry TEXT,
                    listing_date DATE,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
                print(f"✅ Tabel {table} telah dibuat")
            
            elif table == "detik_ticker_sentiment":
                cur.execute("""
                CREATE TABLE IF NOT EXISTS detik_ticker_sentiment (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT,
                    date DATE,
                    news_count INTEGER,
                    avg_sentiment NUMERIC,
                    positive_count INTEGER,
                    negative_count INTEGER,
                    neutral_count INTEGER,
                    UNIQUE (ticker, date)
                )
                """)
                print(f"✅ Tabel {table} telah dibuat")
            
            elif table == "detik_news":
                cur.execute("""
                CREATE TABLE IF NOT EXISTS detik_news (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT,
                    title TEXT,
                    snippet TEXT,
                    url TEXT,
                    published_at TIMESTAMP,
                    sentiment TEXT,
                    sentiment_score NUMERIC,
                    positive_count INTEGER,
                    negative_count INTEGER,
                    scrape_date DATE,
                    UNIQUE(url)
                )
                """)
                print(f"✅ Tabel {table} telah dibuat")
    else:
        print("✅ Semua tabel sumber tersedia")
    
    conn.commit()
    cur.close()
    conn.close()
    
    return len(missing_tables)

def check_parameters(**context):
    dag_run = context["dag_run"]
    if dag_run and dag_run.conf and dag_run.conf.get("bulk_load_complete"):
        print("Bulk load selesai, lanjutkan dengan transformasi")
        return True
    else:
        print("Bulk load belum selesai, berhenti")
        return False

# DAG definition
with DAG(
    dag_id="data_transformation",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["dbt", "transformation", "analytics"]
) as dag:

    # Tunggu hingga DAG ingestion selesai
    wait_for_stock_data = ExternalTaskSensor(
        task_id="wait_for_stock_data",
        external_dag_id="stock_data_ingestion",
        external_task_id="end_task",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )
    
    wait_for_news_data = ExternalTaskSensor(
        task_id="wait_for_news_data",
        external_dag_id="news_data_ingestion",
        external_task_id="end_task",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    wait_for_bulk_load = BranchPythonOperator(
    task_id="check_bulk_load",
    python_callable=check_parameters,
    provide_context=True
    )
    
    # Verify tabel
    verify_tables = PythonOperator(
        task_id="verify_source_tables",
        python_callable=verify_source_tables
    )

    run_dbt_staging = BashOperator(
    task_id='run_dbt_staging',
    bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --select staging --exclude stg_stock_predictions',
    )

    # Jalankan model core tanpa LSTM
    run_dbt_core = BashOperator(
        task_id='run_dbt_core',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --select marts.core --exclude fct_stock_predictions',
    )

    # Jalankan model analytics tanpa LSTM
    run_dbt_analytics = BashOperator(
        task_id='run_dbt_analytics',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --select marts.analytics --exclude lstm_performance_metrics stock_prediction_dashboard',
    )

    # Jalankan test
    test_dbt = BashOperator(
        task_id='test_dbt_models',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt --exclude stg_stock_predictions fct_stock_predictions lstm_performance_metrics stock_prediction_dashboard',
    )
    
    # Marker task
    end_task = DummyOperator(
        task_id="end_task"
    )
    # Task dependencies
    [wait_for_stock_data, wait_for_news_data, wait_for_bulk_load] >> verify_tables >> run_dbt_staging >> run_dbt_core >> run_dbt_analytics >> test_dbt >> end_task