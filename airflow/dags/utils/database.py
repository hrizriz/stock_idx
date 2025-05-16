import os
import psycopg2
import pandas as pd
import logging
import time

# Impor modul untuk connection pooling
from psycopg2 import pool

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inisialisasi pool sebagai variabel global dengan nilai default None
connection_pool = None

def initialize_connection_pool():
    """
    Inisialisasi PostgreSQL connection pool
    """
    global connection_pool
    
    try:
        # Dapatkan detail koneksi dari environment variable atau gunakan default
        db_host = os.environ.get('POSTGRES_HOST', 'postgres')
        db_port = os.environ.get('POSTGRES_PORT', '5432')
        db_name = os.environ.get('POSTGRES_DB', 'postgres')
        db_user = os.environ.get('POSTGRES_USER', 'postgres')
        db_password = os.environ.get('POSTGRES_PASSWORD', 'postgres')
        
        # Buat connection pool
        connection_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        
        logger.info("Connection pool initialized successfully")
        return connection_pool
    except Exception as e:
        logger.error(f"Error initializing connection pool: {str(e)}")
        connection_pool = None
        return None

def get_database_connection():
    """
    Menghubungkan ke database PostgreSQL menggunakan connection pool
    """
    global connection_pool
    
    # Jika pool belum diinisialisasi, inisialisasi terlebih dahulu
    if connection_pool is None:
        connection_pool = initialize_connection_pool()
    
    start_time = time.time()
    max_attempts = 3
    attempt = 0
    
    while attempt < max_attempts:
        try:
            # Coba mendapatkan koneksi dari pool
            if connection_pool:
                conn = connection_pool.getconn()
                logger.info(f"Database connection obtained in {time.time() - start_time:.2f} seconds")
                return conn
            else:
                # Jika pool masih None, coba inisialisasi ulang
                logger.warning("Connection pool is None. Attempting to reinitialize.")
                connection_pool = initialize_connection_pool()
                attempt += 1
                time.sleep(1)  # Tunggu sebentar sebelum mencoba lagi
        except Exception as e:
            logger.warning(f"Connection pool error: {str(e)}")
            attempt += 1
            time.sleep(1)  # Tunggu sebentar sebelum mencoba lagi
    
    # Jika semua upaya pool gagal, coba koneksi langsung sebagai fallback
    logger.warning("All connection pool attempts failed. Trying direct connection.")
    try:
        db_host = os.environ.get('POSTGRES_HOST', 'postgres')
        db_port = os.environ.get('POSTGRES_PORT', '5432')
        db_name = os.environ.get('POSTGRES_DB', 'postgres')
        db_user = os.environ.get('POSTGRES_USER', 'postgres')
        db_password = os.environ.get('POSTGRES_PASSWORD', 'postgres')
        
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        logger.info(f"Direct database connection obtained in {time.time() - start_time:.2f} seconds")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise

def return_connection(conn):
    """
    Mengembalikan koneksi ke pool
    """
    global connection_pool
    
    if connection_pool and conn:
        try:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool")
        except Exception as e:
            logger.warning(f"Error returning connection to pool: {str(e)}")
            try:
                conn.close()
            except:
                pass

def fetch_data(query, params=None):
    """
    Mengeksekusi query dan mengembalikan hasil sebagai DataFrame
    """
    conn = None
    try:
        conn = get_database_connection()
        df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise
    finally:
        if conn:
            return_connection(conn)

def execute_query(query, params=None):
    """
    Mengeksekusi query tanpa mengembalikan hasil
    """
    conn = None
    cursor = None
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            return_connection(conn)

def get_latest_stock_date():
    """
    Mendapatkan tanggal terbaru dari data saham
    """
    query = """
    SELECT MAX(date) as latest_date
    FROM daily_stock_summary
    """
    try:
        df = fetch_data(query)
        if not df.empty and pd.notna(df['latest_date'].iloc[0]):
            return df['latest_date'].iloc[0]
        else:
            # Jika tidak ada data, gunakan tanggal kemarin
            return pd.Timestamp.now().date() - pd.Timedelta(days=1)
    except Exception as e:
        logger.error(f"Error getting latest stock date: {str(e)}")
        # Jika error, gunakan tanggal kemarin sebagai fallback
        return pd.Timestamp.now().date() - pd.Timedelta(days=1)

def create_table_if_not_exists(table_name, create_statement):
    """
    Membuat tabel jika belum ada
    """
    conn = None
    cursor = None
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Check if table exists
        check_query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = '{table_name.split('.')[0]}' 
            AND table_name = '{table_name.split('.')[1]}'
        )
        """
        
        cursor.execute(check_query)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.info(f"Creating table {table_name}")
            # Ensure schema exists
            schema = table_name.split('.')[0]
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            # Create table
            cursor.execute(create_statement)
            conn.commit()
            logger.info(f"Table {table_name} created successfully")
        
        return True
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            return_connection(conn)