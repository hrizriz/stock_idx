# ============================================================================
# PORTFOLIO DATABASE UTILITIES
# File: portofolio_tracker/utils/database.py
# ============================================================================

import psycopg2
import pandas as pd
import os
from datetime import datetime
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
        """Get database connection"""
        try:
            return psycopg2.connect(**self.db_config, connect_timeout=10)
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return None
    
    def check_connection(self):
        """Check if database connection works and schema exists"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                # Check if portfolio schema exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.schemata 
                        WHERE schema_name = 'portfolio'
                    )
                """)
                schema_exists = cursor.fetchone()[0]
                
                if not schema_exists:
                    st.warning("⚠️ Portfolio schema not found. Please run the database schema setup first.")
                    return False
                
                # Check if tables exist
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'portfolio'
                """)
                tables = [row[0] for row in cursor.fetchall()]
                
                required_tables = ['transactions', 'holdings', 'portfolio_summary', 'price_history']
                missing_tables = [table for table in required_tables if table not in tables]
                
                if missing_tables:
                    st.warning(f"⚠️ Missing portfolio tables: {', '.join(missing_tables)}")
                    return False
                
            return True
        
        except Exception as e:
            st.error(f"Database check error: {e}")
            return False
        finally:
            conn.close()
    
    def add_transaction(self, user_id, transaction_data):
        """Add new transaction to database"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                # Check for duplicate transactions
                check_query = """
                SELECT COUNT(*) FROM portfolio.transactions 
                WHERE user_id = %s AND symbol = %s AND trade_date = %s 
                AND transaction_type = %s AND quantity = %s AND price = %s
                """
                cursor.execute(check_query, (
                    user_id,
                    transaction_data.get('symbol'),
                    transaction_data.get('trade_date'),
                    transaction_data.get('transaction_type'),
                    transaction_data.get('quantity'),
                    transaction_data.get('price')
                ))
                
                if cursor.fetchone()[0] > 0:
                    st.warning(f"⚠️ Duplicate transaction detected for {transaction_data.get('symbol')} - skipping")
                    return False
                
                insert_query = """
                INSERT INTO portfolio.transactions 
                (user_id, trade_date, settlement_date, symbol, company_name, 
                 transaction_type, quantity, price, total_value, fees, ref_number, source_file)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(insert_query, (
                    user_id,
                    transaction_data.get('trade_date'),
                    transaction_data.get('settlement_date'),
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
        """Get current portfolio holdings"""
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
    
    def get_portfolio_summary(self, user_id):
        """Get portfolio summary history"""
        conn = self.get_connection()
        if not conn:
            return pd.DataFrame()
        
        try:
            query = """
            SELECT * FROM portfolio.portfolio_summary 
            WHERE user_id = %s 
            ORDER BY summary_date ASC
            """
            
            df = pd.read_sql(query, conn, params=[user_id])
            return df
        
        except Exception as e:
            st.error(f"Error getting portfolio summary: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def get_all_transactions(self, user_id):
        """Get all transactions for user"""
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
    
    def get_transactions_for_symbol(self, user_id, symbol):
        """Get transactions for specific symbol"""
        conn = self.get_connection()
        if not conn:
            return pd.DataFrame()
        
        try:
            query = """
            SELECT * FROM portfolio.transactions 
            WHERE user_id = %s AND symbol = %s 
            ORDER BY trade_date DESC
            """
            
            df = pd.read_sql(query, conn, params=[user_id, symbol])
            return df
        
        except Exception as e:
            st.error(f"Error getting transactions for symbol: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def update_current_price(self, user_id, symbol, current_price):
        """Update current price for a holding"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                # Update current price and calculate new metrics
                update_query = """
                UPDATE portfolio.holdings 
                SET current_price = %s,
                    current_value = total_quantity * %s,
                    unrealized_pnl = (total_quantity * %s) - total_cost,
                    unrealized_pnl_pct = CASE 
                        WHEN total_cost > 0 
                        THEN ((total_quantity * %s) - total_cost) / total_cost * 100 
                        ELSE 0 
                    END,
                    last_updated = CURRENT_TIMESTAMP
                WHERE user_id = %s AND symbol = %s
                """
                
                cursor.execute(update_query, (
                    current_price, current_price, current_price, current_price, user_id, symbol
                ))
            
            conn.commit()
            return True
        
        except Exception as e:
            st.error(f"Error updating current price: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def get_portfolio_stats(self, user_id):
        """Get quick portfolio statistics"""
        conn = self.get_connection()
        if not conn:
            return {}
        
        try:
            with conn.cursor() as cursor:
                # Get latest summary
                cursor.execute("""
                    SELECT * FROM portfolio.portfolio_summary 
                    WHERE user_id = %s 
                    ORDER BY summary_date DESC 
                    LIMIT 1
                """, (user_id,))
                
                latest_summary = cursor.fetchone()
                
                if latest_summary:
                    columns = [desc[0] for desc in cursor.description]
                    summary_dict = dict(zip(columns, latest_summary))
                    return summary_dict
                
                return {}
        
        except Exception as e:
            st.error(f"Error getting portfolio stats: {e}")
            return {}
        finally:
            conn.close()
    
    def delete_transaction(self, transaction_id):
        """Delete a transaction"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM portfolio.transactions 
                    WHERE id = %s
                """, (transaction_id,))
            
            conn.commit()
            return True
        
        except Exception as e:
            st.error(f"Error deleting transaction: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def update_all_holdings_prices(self, price_data):
        """Update prices for multiple holdings at once"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                for symbol, price in price_data.items():
                    cursor.execute("""
                        UPDATE portfolio.holdings 
                        SET current_price = %s,
                            current_value = total_quantity * %s,
                            unrealized_pnl = (total_quantity * %s) - total_cost,
                            unrealized_pnl_pct = CASE 
                                WHEN total_cost > 0 
                                THEN ((total_quantity * %s) - total_cost) / total_cost * 100 
                                ELSE 0 
                            END,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE symbol = %s
                    """, (price, price, price, price, symbol))
            
            conn.commit()
            return True
        
        except Exception as e:
            st.error(f"Error updating holdings prices: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()