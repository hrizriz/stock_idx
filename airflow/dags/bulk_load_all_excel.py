from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from pathlib import Path
import pendulum
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

def create_tables_if_not_exist():
    """
    Membuat tabel-tabel yang diperlukan jika belum ada
    Ini penting untuk memastikan tabel ada sebelum bulk import
    """
    try:
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
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS processing_log (
            id SERIAL PRIMARY KEY,
            process_name TEXT,
            file_name TEXT,
            process_date TIMESTAMP,
            records_processed INTEGER,
            status TEXT,
            error_message TEXT
        );
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info("✅ Tabel untuk data saham telah dibuat atau sudah ada sebelumnya")
    except Exception as e:
        logger.error(f"❌ Error creating tables: {str(e)}")
        raise

def verify_data_folder():
    """
    Memverifikasi keberadaan folder data dan jumlah file Excel
    """
    folder = Path("/opt/airflow/data")
    if not folder.exists():
        folder.mkdir(parents=True, exist_ok=True)
        logger.info("📁 Folder data dibuat.")
    
    excel_files = list(folder.glob("Ringkasan Saham-*.xlsx"))
    logger.info(f"📊 Ditemukan {len(excel_files)} file Excel di folder data.")
    
    return len(excel_files)

def bulk_import_excel_files():
    """
    Mengimpor semua data Excel historis ke database dengan metode batch processing
    """
    start_time = datetime.now()
    folder = Path("/opt/airflow/data")
    all_files = list(folder.glob("Ringkasan Saham-*.xlsx"))
    
    if not all_files:
        logger.warning("🚫 Tidak ada file Excel ditemukan di folder data.")
        return {"total_files": 0, "imported_files": 0, "total_records": 0}
    
    total_files = len(all_files)
    imported_files = 0
    total_records = 0
    
    logger.info(f"🔍 Menemukan {total_files} file Excel untuk diimpor.")
    
    BATCH_SIZE = 500
    
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
    
    stock_sql = """
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
    """
    
    company_sql = """
        INSERT INTO dim_companies (symbol, name)
        VALUES (%(symbol)s, %(name)s)
        ON CONFLICT (symbol) DO UPDATE SET
            name = EXCLUDED.name,
            last_updated = CURRENT_TIMESTAMP;
    """
    
    for file in all_files:
        logger.info(f"📥 Import file: {file.name}")
        try:
            tanggal_str = file.stem.split("-")[-1]
            file_date = datetime.strptime(tanggal_str, "%Y%m%d").date()
            
            df = pd.read_excel(file)
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            
            df = df.rename(columns=column_map)
            
            df = df[list(column_map.values())]
            
            df["date"] = file_date
            df["upload_file"] = file.name
            
            conn = psycopg2.connect(
                host="postgres",
                dbname="airflow",
                user="airflow",
                password="airflow"
            )
            conn.autocommit = False  # Explicitly manage transactions
            cur = conn.cursor()
            
            try:
                stock_rows = df.to_dict('records')
                
                company_rows = []
                for symbol, name in df[['symbol', 'name']].drop_duplicates().itertuples(index=False):
                    company_rows.append({'symbol': symbol, 'name': name})
                
                rows_imported = 0
                for i in range(0, len(stock_rows), BATCH_SIZE):
                    batch = stock_rows[i:i + BATCH_SIZE]
                    execute_batch(cur, stock_sql, batch)
                    rows_imported += len(batch)
                    
                    if len(stock_rows) > 1000 and (i + BATCH_SIZE) % 1000 == 0:
                        logger.info(f"   Progress: {i + BATCH_SIZE}/{len(stock_rows)} rows")
                
                execute_batch(cur, company_sql, company_rows)
                
                conn.commit()
                
                imported_files += 1
                total_records += rows_imported
                
                logger.info(f"✅ File {file.name}: {rows_imported} baris berhasil diimpor")
                
                cur.execute("""
                    INSERT INTO processing_log 
                    (process_name, file_name, process_date, records_processed, status)
                    VALUES (%s, %s, %s, %s, %s)
                """, ("bulk_stock_loader", file.name, datetime.now(), rows_imported, "SUCCESS"))
                conn.commit()
                
            except Exception as e:
                conn.rollback()
                logger.error(f"❌ Database error processing {file.name}: {str(e)}")
                
                try:
                    cur.execute("""
                        INSERT INTO processing_log 
                        (process_name, file_name, process_date, status, error_message)
                        VALUES (%s, %s, %s, %s, %s)
                    """, ("bulk_stock_loader", file.name, datetime.now(), "ERROR", str(e)))
                    conn.commit()
                except:
                    pass  # Ignore errors in error logging
            finally:
                cur.close()
                conn.close()
            
        except Exception as e:
            logger.error(f"❌ Gagal import file {file.name}: {str(e)}")
            continue
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"📊 Ringkasan Bulk Import:")
    logger.info(f"   - Total file: {total_files}")
    logger.info(f"   - Berhasil diimpor: {imported_files}")
    logger.info(f"   - Total record: {total_records}")
    logger.info(f"   - Waktu proses: {duration:.2f} detik")
    logger.info(f"   - Kecepatan: {total_records/duration:.2f} baris/detik" if duration > 0 else "")
    
    return {
        "total_files": total_files,
        "imported_files": imported_files,
        "total_records": total_records,
        "duration_seconds": duration
    }

def log_import_summary(**context):
    """
    Mencatat ringkasan hasil import ke log
    """
    ti = context['ti']
    result = ti.xcom_pull(task_ids='run_bulk_import')
    
    if not result:
        logger.error("❌ Tidak ada data hasil import yang tersedia.")
        return
    
    total_files = result.get('total_files', 0)
    imported_files = result.get('imported_files', 0)
    total_records = result.get('total_records', 0)
    duration = result.get('duration_seconds', 0)
    
    success_rate = (imported_files / total_files * 100) if total_files > 0 else 0
    records_per_second = (total_records / duration) if duration > 0 else 0
    
    logger.info(f"📋 RINGKASAN BULK IMPORT SELESAI:")
    logger.info(f"   - Total file diproses: {total_files}")
    logger.info(f"   - File berhasil diimpor: {imported_files} ({success_rate:.1f}%)")
    logger.info(f"   - Total record diimpor: {total_records}")
    logger.info(f"   - Waktu proses: {duration:.2f} detik")
    logger.info(f"   - Kecepatan: {records_per_second:.2f} baris/detik" if duration > 0 else "")
    logger.info(f"   - Waktu selesai: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow"
        )
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO processing_log 
            (process_name, file_name, process_date, records_processed, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            "bulk_stock_loader_summary", 
            f"batch_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}", 
            datetime.now(), 
            total_records, 
            "COMPLETE", 
            f"Files: {imported_files}/{total_files}, Speed: {records_per_second:.2f} rec/sec"
        ))
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Could not record summary: {str(e)}")
    
    return f"Bulk import selesai: {imported_files}/{total_files} file, {total_records} record"

with DAG(
    dag_id="bulk_stock_loader",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval=None,  # Trigger manual
    catchup=False,
    default_args=default_args,
    tags=["idx", "bulk", "excel", "historical"]
) as dag:

    verify_folder = PythonOperator(
        task_id="verify_data_folder",
        python_callable=verify_data_folder
    )
    
    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables_if_not_exist
    )
    
    run_bulk_import = PythonOperator(
        task_id="run_bulk_import",
        python_callable=bulk_import_excel_files
    )
    
    log_summary = PythonOperator(
        task_id="log_import_summary",
        python_callable=log_import_summary,
        provide_context=True
    )
    
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    verify_folder >> create_tables >> run_bulk_import >> log_summary >> end_task
