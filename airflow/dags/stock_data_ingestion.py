from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowSkipException
from datetime import datetime, date
import pandas as pd
from pandas.tseries.offsets import BDay
import psycopg2
from pathlib import Path
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

def sensing_data():
    """Memeriksa apakah ada file data saham di folder"""
    folder = Path("/opt/airflow/data")
    
    files = list(folder.glob("Ringkasan Saham-*.xlsx"))
    
    if not files:
        print("📊 Tidak ada file ditemukan, akan coba lagi nanti")
        return False
    
    for file in files:
        try:
            pd.read_excel(file, nrows=5)
        except Exception as e:
            print(f"⚠️ File {file.name} tidak bisa dibaca: {str(e)}")
            return False
    
    print(f"📊 Ditemukan {len(files)} file data saham yang valid")
    return True

def create_stock_tables_if_not_exists():
    """Membuat tabel-tabel untuk data saham jika belum ada"""
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    
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
    );
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_companies (
        symbol TEXT PRIMARY KEY,
        name TEXT,
        sector TEXT,
        industry TEXT,
        listing_date DATE,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("✅ Tabel stock telah dibuat atau sudah ada sebelumnya")

def import_excel_files():
    """Mengimpor data saham dari file Excel terbaru ke database"""
    folder = Path("/opt/airflow/data")
    
    files_with_dates = []
    for file in folder.glob("Ringkasan Saham-*.xlsx"):
        try:
            tanggal_str = file.stem.split("-")[-1]
            file_date = datetime.strptime(tanggal_str, "%Y%m%d").date()
            files_with_dates.append((file, file_date))
        except Exception as e:
            print(f"⚠️ Error mengekstrak tanggal dari file {file.name}: {str(e)}")
            continue
    
    if not files_with_dates:
        print("🚫 Tidak ada file data saham yang valid ditemukan")
        return
    
    files_with_dates.sort(key=lambda x: x[1], reverse=True)
    latest_file, latest_date = files_with_dates[0]
    
    print(f"📊 File terbaru: {latest_file.name} (tanggal: {latest_date})")
    
    print(f"📥 Memproses file: {latest_file.name}")
    df = pd.read_excel(latest_file)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    column_map = {
        "kode_saham": "symbol",
        "nama_perusahaan": "name",
        "sebelumnya": "prev_close",
        "open_price": "open_price",
        "tertinggi": "high",
        "terendah": "low",
        "penutupan": "close",
        "selisih": "change",
        "volume": "volume",
        "nilai": "value",
        "frekuensi": "frequency",
        "index_individual": "index_individual",
        "weight_for_index": "weight_for_index",
        "foreign_buy": "foreign_buy",
        "foreign_sell": "foreign_sell",
        "offer": "offer",
        "offer_volume": "offer_volume",
        "bid": "bid",
        "bid_volume": "bid_volume",
        "listed_shares": "listed_shares",
        "tradeble_shares": "tradable_shares",
        "non_regular_volume": "non_regular_volume",
        "non_regular_value": "non_regular_value",
        "non_regular_frequency": "non_regular_frequency"
    }

    df = df.rename(columns=column_map)
    df = df[list(column_map.values())]
    df["date"] = latest_date
    df["upload_file"] = latest_file.name

    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    try:
        for _, row in df.iterrows():
            cur.execute(""" 
                INSERT INTO daily_stock_summary (
                    symbol, name, date,
                    prev_close, open_price, high, low, close, change,
                    volume, value, frequency,
                    index_individual, weight_for_index,
                    foreign_buy, foreign_sell,
                    offer, offer_volume, bid, bid_volume,
                    listed_shares, tradable_shares,
                    non_regular_volume, non_regular_value, non_regular_frequency,
                    upload_file
                ) VALUES (
                    %(symbol)s, %(name)s, %(date)s,
                    %(prev_close)s, %(open_price)s, %(high)s, %(low)s, %(close)s, %(change)s,
                    %(volume)s, %(value)s, %(frequency)s,
                    %(index_individual)s, %(weight_for_index)s,
                    %(foreign_buy)s, %(foreign_sell)s,
                    %(offer)s, %(offer_volume)s, %(bid)s, %(bid_volume)s,
                    %(listed_shares)s, %(tradable_shares)s,
                    %(non_regular_volume)s, %(non_regular_value)s, %(non_regular_frequency)s,
                    %(upload_file)s
                )
                ON CONFLICT (symbol, date) DO UPDATE SET
                    name = EXCLUDED.name,
                    prev_close = EXCLUDED.prev_close,
                    open_price = EXCLUDED.open_price,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    change = EXCLUDED.change,
                    volume = EXCLUDED.volume,
                    value = EXCLUDED.value,
                    frequency = EXCLUDED.frequency,
                    index_individual = EXCLUDED.index_individual,
                    weight_for_index = EXCLUDED.weight_for_index,
                    foreign_buy = EXCLUDED.foreign_buy,
                    foreign_sell = EXCLUDED.foreign_sell,
                    offer = EXCLUDED.offer,
                    offer_volume = EXCLUDED.offer_volume,
                    bid = EXCLUDED.bid,
                    bid_volume = EXCLUDED.bid_volume,
                    listed_shares = EXCLUDED.listed_shares,
                    tradable_shares = EXCLUDED.tradable_shares,
                    non_regular_volume = EXCLUDED.non_regular_volume,
                    non_regular_value = EXCLUDED.non_regular_value,
                    non_regular_frequency = EXCLUDED.non_regular_frequency,
                    upload_file = EXCLUDED.upload_file;
            """, row.to_dict())
            
            cur.execute(""" 
                INSERT INTO dim_companies (symbol, name)
                VALUES (%(symbol)s, %(name)s)
                ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name,
                    last_updated = CURRENT_TIMESTAMP;
            """, {"symbol": row["symbol"], "name": row["name"]})

        conn.commit()
        print(f"✅ File {latest_file.name} berhasil diproses ({len(df)} baris data)")
        
        cur.execute("""
            INSERT INTO processing_log (
                process_name, file_name, process_date, records_processed, status
            ) VALUES (
                'stock_data_ingestion', %s, CURRENT_TIMESTAMP, %s, 'SUCCESS'
            )
        """, (latest_file.name, len(df)))
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error memproses file {latest_file.name}: {str(e)}")
        
        try:
            cur.execute("""
                INSERT INTO processing_log (
                    process_name, file_name, process_date, status, error_message
                ) VALUES (
                    'stock_data_ingestion', %s, CURRENT_TIMESTAMP, 'ERROR', %s
                )
            """, (latest_file.name, str(e)))
            conn.commit()
        except:
            pass
            
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id="stock_data_ingestion",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval='0 18 * * 1-5',
    catchup=False,
    default_args=default_args,
    sla_miss_callback=lambda: print("SLA missed for stock_data_ingestion"),
    tags=["idx", "stock", "excel", "ingestion"]
) as dag:

    data_sensing = PythonSensor(
        task_id="sensing_data",
        python_callable=sensing_data,
        poke_interval=300,
        timeout=7200,
        mode='reschedule'
    )

    create_tables = PythonOperator(
        task_id="create_stock_tables",
        python_callable=create_stock_tables_if_not_exists
    )

    import_data = PythonOperator(
        task_id="import_excel_files",
        python_callable=import_excel_files
    )

    end_task = DummyOperator(
        task_id="end_task"
    )

    data_sensing >> create_tables >> import_data >> end_task



"""
ambil data business date
"""







    

    
    
    
    


    






            

        






