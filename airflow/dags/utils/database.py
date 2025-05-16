import psycopg2
import logging
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_database_connection():
    """
    Create database connection with better error handling
    """
    try:
        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow"
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

def get_latest_stock_date():
    """
    Get the latest date available in the daily_stock_summary table
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date) FROM public.daily_stock_summary")
        latest_date = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        logger.info(f"Latest stock date: {latest_date}")
        return latest_date
    except Exception as e:
        logger.error(f"Error getting latest stock date: {str(e)}")
        raise

def execute_query(query, params=None):
    """
    Execute SQL query with optional parameters
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise

def fetch_data(query, params=None):
    """
    Fetch data from database using SQL query
    """
    try:
        conn = get_database_connection()
        if params:
            df = pd.read_sql(query, conn, params=params)
        else:
            df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        raise

def create_table_if_not_exists(table_name, schema):
    """
    Create table if it doesn't exist
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Check if table already exists
        schema_name, table = table_name.split('.')
        cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = '{schema_name}' 
            AND table_name = '{table}'
        )
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            cursor.execute(schema)
            logger.info(f"Table {table_name} created successfully")
        
        cursor.close()
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        raise