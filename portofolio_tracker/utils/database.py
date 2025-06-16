"""
Portofolio Database Utilities
"""
import psycopg2
import pandas as pd
import os
import streamlit as st

class PortofolioDatabase:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'postgres'),
            'database': os.getenv('DB_NAME', 'airflow'), 
            'user': os.getenv('DB_USER', 'airflow'),
            'password': os.getenv('DB_PASSWORD', 'airflow'),
            'port': int(os.getenv('DB_PORT', '5432'))
        }
    
    def get_connection(self):
        try:
            return psycopg2.connect(**self.db_config, connect_timeout=10)
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return None
    
    def check_connection(self):
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'portofolio')")
                schema_exists = cursor.fetchone()[0]
                
                if not schema_exists:
                    st.warning("⚠️ Portofolio schema not found. Please run database setup.")
                    return False
                
            return True
        except Exception as e:
            st.error(f"Database check error: {e}")
            return False
        finally:
            conn.close()
