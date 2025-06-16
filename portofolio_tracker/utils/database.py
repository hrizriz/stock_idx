"""
Portfolio Database - Simplified Version
"""
import psycopg2
import pandas as pd
import os
import streamlit as st

class PortfolioDatabase:
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
                cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'portfolio')")
                schema_exists = cursor.fetchone()[0]
                
                if not schema_exists:
                    st.warning("⚠️ Portfolio schema not found. Please run database setup.")
                    return False
                
            return True
        except Exception as e:
            st.error(f"Database check error: {e}")
            return False
        finally:
            conn.close()
    
    def add_transaction(self, user_id, transaction_data):
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                insert_query = """
                INSERT INTO portfolio.transactions 
                (user_id, trade_date, symbol, company_name, transaction_type, quantity, price, total_value, fees, ref_number, source_file)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(insert_query, (
                    user_id,
                    transaction_data.get('trade_date'),
                    transaction_data.get('symbol'),
                    transaction_data.get('company_name'),
                    transaction_data.get('transaction_type'),
                    transaction_data.get('quantity'),
                    transaction_data.get('price'),
                    transaction_data.get('total_value'),
                    transaction_data.get('fees', 0),
                    transaction_data.get('ref_number'),
                    transaction_data.get('source_file')
                ))
            
            conn.commit()
            return True
        except Exception as e:
            st.error(f"Error adding transaction: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def get_current_holdings(self, user_id):
        conn = self.get_connection()
        if not conn:
            return pd.DataFrame()
        
        try:
            query = """
            SELECT * FROM portfolio.holdings 
            WHERE user_id = %s AND total_quantity > 0
            ORDER BY current_value DESC NULLS LAST
            """
            df = pd.read_sql(query, conn, params=[user_id])
            return df
        except Exception as e:
            st.error(f"Error getting holdings: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def get_all_transactions(self, user_id):
        conn = self.get_connection()
        if not conn:
            return pd.DataFrame()
        
        try:
            query = """
            SELECT * FROM portfolio.transactions 
            WHERE user_id = %s 
            ORDER BY trade_date DESC, created_at DESC
            """
            df = pd.read_sql(query, conn, params=[user_id])
            return df
        except Exception as e:
            st.error(f"Error getting transactions: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
