from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pandas as pd
import psycopg2
from pathlib import Path
import pendulum

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
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    
    # Buat tabel daily_stock_summary jika belum ada
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
    
    # Buat tabel dim_companies untuk menyimpan informasi perusahaan
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
    
    print("✅ Tabel untuk data saham telah dibuat atau sudah ada sebelumnya")

def verify_data_folder():
    """
    Memverifikasi keberadaan folder data dan jumlah file Excel
    """
    folder = Path("/opt/airflow/data")
    if not folder.exists():
        folder.mkdir(parents=True, exist_ok=True)
        print("📁 Folder data dibuat.")
    
    excel_files = list(folder.glob("Ringkasan Saham-*.xlsx"))
    print(f"📊 Ditemukan {len(excel_files)} file Excel di folder data.")
    
    return len(excel_files)

def bulk_import_excel_files():
    """
    Mengimpor semua data Excel historis ke database
    """
    folder = Path("/opt/airflow/data")
    all_files = list(folder.glob("Ringkasan Saham-*.xlsx"))
    
    if not all_files:
        print("🚫 Tidak ada file Excel ditemukan di folder data.")
        return
    
    total_files = len(all_files)
    imported_files = 0
    total_records = 0
    
    print(f"🔍 Menemukan {total_files} file Excel untuk diimpor.")
    
    for file in all_files:
        print(f"📥 Import file: {file.name}")
        try:
            # Ekstrak tanggal dari nama file
            tanggal_str = file.stem.split("-")[-1]
            file_date = datetime.strptime(tanggal_str, "%Y%m%d").date()
            
            # Baca file Excel
            df = pd.read_excel(file)
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            
            # Pemetaan kolom dari bahasa Indonesia ke bahasa Inggris
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
            
            # Rename kolom dan pilih yang sesuai
            df = df.rename(columns=column_map)
            df = df[list(column_map.values())]
            
            # Tambah kolom tanggal dan nama file
            df["date"] = file_date
            df["upload_file"] = file.name
            
            # Koneksi ke database
            conn = psycopg2.connect(
                host="postgres",
                dbname="airflow",
                user="airflow",
                password="airflow"
            )
            cur = conn.cursor()
            
            # Import data ke database
            rows_imported = 0
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
                
                # Update juga tabel dim_companies
                cur.execute("""
                    INSERT INTO dim_companies (symbol, name)
                    VALUES (%(symbol)s, %(name)s)
                    ON CONFLICT (symbol) DO UPDATE SET
                        name = EXCLUDED.name,
                        last_updated = CURRENT_TIMESTAMP;
                """, {"symbol": row["symbol"], "name": row["name"]})
                
                rows_imported += 1
            
            # Commit perubahan
            conn.commit()
            cur.close()
            conn.close()
            
            # Update counter
            imported_files += 1
            total_records += rows_imported
            
            print(f"✅ File {file.name}: {rows_imported} baris berhasil diimpor")
            
        except Exception as e:
            print(f"❌ Gagal import file {file.name}: {e}")
            continue
    
    print(f"📊 Ringkasan Bulk Import:")
    print(f"   - Total file: {total_files}")
    print(f"   - Berhasil diimpor: {imported_files}")
    print(f"   - Total record: {total_records}")
    
    return {
        "total_files": total_files,
        "imported_files": imported_files,
        "total_records": total_records
    }

def log_import_summary(**context):
    """
    Mencatat ringkasan hasil import ke log
    """
    ti = context['ti']
    result = ti.xcom_pull(task_ids='run_bulk_import')
    
    if not result:
        print("❌ Tidak ada data hasil import yang tersedia.")
        return
    
    total_files = result.get('total_files', 0)
    imported_files = result.get('imported_files', 0)
    total_records = result.get('total_records', 0)
    
    success_rate = (imported_files / total_files * 100) if total_files > 0 else 0
    
    print(f"📋 RINGKASAN BULK IMPORT SELESAI:")
    print(f"   - Total file diproses: {total_files}")
    print(f"   - File berhasil diimpor: {imported_files} ({success_rate:.1f}%)")
    print(f"   - Total record diimpor: {total_records}")
    print(f"   - Waktu selesai: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return f"Bulk import selesai: {imported_files}/{total_files} file, {total_records} record"

# DAG definition
with DAG(
    dag_id="bulk_stock_loader",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval=None,  # Trigger manual
    catchup=False,
    default_args=default_args,
    tags=["idx", "bulk", "excel", "historical"]
) as dag:

    # Task untuk verifikasi folder
    verify_folder = PythonOperator(
        task_id="verify_data_folder",
        python_callable=verify_data_folder
    )
    
    # Task untuk membuat tabel
    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables_if_not_exist
    )
    
    # Task untuk bulk import
    run_bulk_import = PythonOperator(
        task_id="run_bulk_import",
        python_callable=bulk_import_excel_files
    )
    
    # Task untuk log summary
    log_summary = PythonOperator(
        task_id="log_import_summary",
        python_callable=log_import_summary,
        provide_context=True
    )
    
    # Task marker untuk end
    end_task = DummyOperator(
        task_id="end_task"
    )

    # trigger_transformation = TriggerDagRunOperator(
    # task_id="trigger_transformation",
    # trigger_dag_id="data_transformation",
    # conf={"bulk_load_complete": True},  # Parameter yang diteruskan
    # wait_for_completion=False
    # )
    
    # Task dependencies
    verify_folder >> create_tables >> run_bulk_import >> log_summary >> end_task