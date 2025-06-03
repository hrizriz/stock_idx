import streamlit as st
import psycopg2
import pandas as pd
import os
import warnings

# Suppress pandas warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'database': os.getenv('DB_NAME', 'airflow'),
    'user': os.getenv('DB_USER', 'airflow'),
    'password': os.getenv('DB_PASSWORD', 'airflow'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

def create_fresh_connection():
    """Create a fresh database connection"""
    try:
        conn = psycopg2.connect(
            **DB_CONFIG,
            connect_timeout=10,
            application_name="streamlit_dashboard"
        )
        # Test connection immediately
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        return conn
    except Exception as e:
        st.error(f"❌ Failed to create connection: {str(e)}")
        return None

def execute_query_safe(query, description="Query"):
    """Execute query with proper connection management"""
    conn = None
    try:
        conn = create_fresh_connection()
        if not conn:
            return pd.DataFrame()
        
        df = pd.read_sql(query, conn)
        return df
        
    except psycopg2.OperationalError as e:
        if "closed" in str(e).lower():
            st.error(f"❌ Connection closed during {description}. Retrying...")
            try:
                conn_retry = create_fresh_connection()
                if conn_retry:
                    df = pd.read_sql(query, conn_retry)
                    conn_retry.close()
                    return df
            except Exception as retry_e:
                st.error(f"❌ Retry failed: {str(retry_e)}")
        else:
            st.error(f"❌ Database error in {description}: {str(e)}")
        return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ {description} failed: {str(e)}")
        with st.expander("🔍 View Query"):
            st.code(query, language='sql')
        return pd.DataFrame()
        
    finally:
        if conn and not conn.closed:
            try:
                conn.close()
            except:
                pass

@st.cache_data(ttl=180, show_spinner=True)
def fetch_data_cached(query, description="Data"):
    """Cached wrapper for database queries"""
    return execute_query_safe(query, description)

def test_database_connection():
    """Test database connection and show status"""
    conn = create_fresh_connection()
    
    if not conn:
        st.sidebar.markdown("""
        <div style="padding: 10px; border-radius: 5px; margin: 10px 0; font-weight: bold; background-color: #f8d7da; color: #721c24;">
            ❌ Connection Failed
        </div>
        """, unsafe_allow_html=True)
        return False
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema IN ('public', 'public_analytics')
            """)
            total_tables = cursor.fetchone()[0]
            
            cursor.execute("SELECT MAX(date) FROM daily_stock_summary")
            latest_date = cursor.fetchone()[0]
            
        st.sidebar.markdown(f"""
        <div style="padding: 10px; border-radius: 5px; margin: 10px 0; font-weight: bold; background-color: #d4edda; color: #155724;">
            🔗 Database Connected<br>
            Tables: {total_tables}<br>
            Latest: {latest_date}
        </div>
        """, unsafe_allow_html=True)
        
        return True
        
    except Exception as e:
        st.sidebar.markdown(f"""
        <div style="padding: 10px; border-radius: 5px; margin: 10px 0; font-weight: bold; background-color: #f8d7da; color: #721c24;">
            ❌ Database Error<br>
            {str(e)[:50]}...
        </div>
        """, unsafe_allow_html=True)
        return False
    finally:
        if conn and not conn.closed:
            conn.close()

def clear_all_caches():
    """Clear all caches"""
    st.cache_data.clear()
    if hasattr(st, 'cache_resource'):
        st.cache_resource.clear()