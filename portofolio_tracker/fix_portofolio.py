#!/usr/bin/env python3
"""
Portfolio Tracker Fix Script
Simpan sebagai: fix_portfolio.py
Jalankan: python fix_portfolio.py
"""

import os
import shutil

def fix_portfolio_tracker():
    """Fix semua masalah portfolio tracker"""
    
    print("🔧 Fixing Portfolio Tracker...")
    
    # 1. Buat direktori yang diperlukan
    directories = [
        "portofolio_tracker",
        "portofolio_tracker/utils", 
        "portofolio_tracker/pages",
        "portofolio_tracker/database"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✅ Created directory: {directory}")
    
    # 2. Buat __init__.py files
    init_files = [
        "portofolio_tracker/__init__.py",
        "portofolio_tracker/utils/__init__.py",
        "portofolio_tracker/pages/__init__.py"
    ]
    
    for init_file in init_files:
        with open(init_file, 'w', encoding='utf-8') as f:
            f.write("# Portfolio Tracker Module\n")
        print(f"✅ Created: {init_file}")
    
    # 3. Buat database.py yang sederhana
    database_content = '''"""
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
'''
    
    with open("portofolio_tracker/utils/database.py", 'w', encoding='utf-8') as f:
        f.write(database_content)
    print("✅ Created: portofolio_tracker/utils/database.py")
    
    # 4. Buat portfolio_calculator.py sederhana
    calculator_content = '''"""
Portfolio Calculator - Simplified Version
"""
from datetime import date
import streamlit as st

try:
    from .database import PortfolioDatabase
except ImportError:
    from database import PortfolioDatabase

class PortfolioCalculator:
    def __init__(self):
        self.db = PortfolioDatabase()
    
    def calculate_portfolio_metrics(self, user_id):
        holdings = self.db.get_current_holdings(user_id)
        
        if holdings.empty:
            return {}
        
        metrics = {
            'total_value': holdings['current_value'].sum() if 'current_value' in holdings.columns else holdings['total_cost'].sum(),
            'total_cost': holdings['total_cost'].sum(),
            'total_pnl': holdings['unrealized_pnl'].sum() if 'unrealized_pnl' in holdings.columns else 0,
            'total_stocks': len(holdings),
            'winners': len(holdings[holdings['unrealized_pnl'] > 0]) if 'unrealized_pnl' in holdings.columns else 0,
            'losers': len(holdings[holdings['unrealized_pnl'] < 0]) if 'unrealized_pnl' in holdings.columns else 0
        }
        
        metrics['total_pnl_pct'] = (metrics['total_pnl'] / metrics['total_cost'] * 100) if metrics['total_cost'] > 0 else 0
        metrics['win_rate'] = (metrics['winners'] / metrics['total_stocks'] * 100) if metrics['total_stocks'] > 0 else 0
        
        return metrics
    
    def recalculate_portfolio(self, user_id):
        try:
            # Update current values berdasarkan average price
            conn = self.db.get_connection()
            if conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE portfolio.holdings 
                        SET current_value = total_quantity * COALESCE(current_price, average_price),
                            unrealized_pnl = (total_quantity * COALESCE(current_price, average_price)) - total_cost,
                            unrealized_pnl_pct = CASE 
                                WHEN total_cost > 0 
                                THEN ((total_quantity * COALESCE(current_price, average_price)) - total_cost) / total_cost * 100 
                                ELSE 0 
                            END
                        WHERE user_id = %s
                    """, (user_id,))
                conn.commit()
                conn.close()
            return True
        except Exception as e:
            st.error(f"Error recalculating portfolio: {e}")
            return False
'''
    
    with open("portofolio_tracker/utils/portfolio_calculator.py", 'w', encoding='utf-8') as f:
        f.write(calculator_content)
    print("✅ Created: portofolio_tracker/utils/portfolio_calculator.py")
    
    # 5. Buat portfolio page yang bekerja
    page_content = '''"""
Portfolio Tracker Page - Working Version
"""
import streamlit as st
import pandas as pd
from datetime import datetime

def show():
    """Main portfolio page function"""
    st.markdown("# 💼 Portfolio Tracker")
    st.markdown("**Real-time Indonesian stock portfolio tracking**")
    
    # Import modules
    try:
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
        
        from database import PortfolioDatabase
        from portfolio_calculator import PortfolioCalculator
        
        db = PortfolioDatabase()
        calculator = PortfolioCalculator()
        
        if not db.check_connection():
            show_setup_instructions()
            return
        
        # Main interface
        show_portfolio_interface(db, calculator)
        
    except ImportError as e:
        st.error(f"❌ Module import error: {e}")
        show_dependency_help()
    except Exception as e:
        st.error(f"❌ Application error: {e}")
        show_troubleshooting()

def show_portfolio_interface(db, calculator):
    """Show main portfolio interface"""
    user_id = "default_user"
    
    # Tab layout
    tab1, tab2, tab3 = st.tabs(["📊 Overview", "➕ Add Transaction", "📋 History"])
    
    with tab1:
        show_portfolio_overview(db, calculator, user_id)
    
    with tab2:
        show_add_transaction_form(db, user_id)
    
    with tab3:
        show_transaction_history(db, user_id)

def show_portfolio_overview(db, calculator, user_id):
    """Portfolio overview tab"""
    st.subheader("📊 Portfolio Overview")
    
    # Get data
    holdings = db.get_current_holdings(user_id)
    
    if holdings.empty:
        st.info("📭 No holdings found. Add your first transaction to get started!")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🎯 Add Sample Data", help="Load sample transactions for testing"):
                add_sample_data(db, user_id)
                st.rerun()
        
        with col2:
            st.info("👆 Click 'Add Transaction' tab to input manually")
        return
    
    # Calculate metrics
    metrics = calculator.calculate_portfolio_metrics(user_id)
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "💰 Total Investment", 
            f"Rp {metrics.get('total_cost', 0):,.0f}"
        )
    
    with col2:
        st.metric(
            "💎 Current Value", 
            f"Rp {metrics.get('total_value', 0):,.0f}"
        )
    
    with col3:
        pnl = metrics.get('total_pnl', 0)
        pnl_pct = metrics.get('total_pnl_pct', 0)
        st.metric(
            "📈 Total P&L", 
            f"Rp {pnl:,.0f}",
            f"{pnl_pct:+.2f}%"
        )
    
    with col4:
        st.metric(
            "📦 Holdings", 
            f"{metrics.get('total_stocks', 0)} stocks"
        )
    
    # Holdings table
    st.markdown("---")
    st.subheader("🏪 Current Holdings")
    
    if not holdings.empty:
        # Format display
        display_holdings = holdings.copy()
        
        # Format currency columns
        currency_cols = ['total_cost', 'current_value', 'unrealized_pnl']
        for col in currency_cols:
            if col in display_holdings.columns:
                display_holdings[f"{col}_formatted"] = display_holdings[col].apply(
                    lambda x: f"Rp {x:,.0f}" if pd.notna(x) else "Rp 0"
                )
        
        # Format percentage
        if 'unrealized_pnl_pct' in display_holdings.columns:
            display_holdings['pnl_pct_formatted'] = display_holdings['unrealized_pnl_pct'].apply(
                lambda x: f"{x:.2f}%" if pd.notna(x) else "0.00%"
            )
        
        # Select columns to display
        display_cols = []
        rename_map = {}
        
        if 'symbol' in display_holdings.columns:
            display_cols.append('symbol')
            rename_map['symbol'] = 'Symbol'
        
        if 'company_name' in display_holdings.columns:
            display_cols.append('company_name')  
            rename_map['company_name'] = 'Company'
        
        if 'total_quantity' in display_holdings.columns:
            display_cols.append('total_quantity')
            rename_map['total_quantity'] = 'Qty'
        
        if 'total_cost_formatted' in display_holdings.columns:
            display_cols.append('total_cost_formatted')
            rename_map['total_cost_formatted'] = 'Cost Basis'
        
        if 'current_value_formatted' in display_holdings.columns:
            display_cols.append('current_value_formatted')
            rename_map['current_value_formatted'] = 'Market Value'
        
        if 'unrealized_pnl_formatted' in display_holdings.columns:
            display_cols.append('unrealized_pnl_formatted')
            rename_map['unrealized_pnl_formatted'] = 'P&L'
        
        if 'pnl_pct_formatted' in display_holdings.columns:
            display_cols.append('pnl_pct_formatted')
            rename_map['pnl_pct_formatted'] = 'P&L %'
        
        if display_cols:
            st.dataframe(
                display_holdings[display_cols].rename(columns=rename_map),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.dataframe(holdings, use_container_width=True, hide_index=True)
    
    # Refresh button
    st.markdown("---")
    if st.button("🔄 Refresh Portfolio", help="Recalculate portfolio metrics"):
        calculator.recalculate_portfolio(user_id)
        st.success("✅ Portfolio refreshed!")
        st.rerun()

def show_add_transaction_form(db, user_id):
    """Add transaction form"""
    st.subheader("➕ Add New Transaction")
    
    with st.form("add_transaction_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            trade_date = st.date_input("📅 Trade Date", value=datetime.now().date())
            symbol = st.text_input("🏷️ Stock Symbol", placeholder="e.g., BBCA", max_chars=4).upper()
            company_name = st.text_input("🏢 Company Name", placeholder="e.g., Bank Central Asia Tbk.")
        
        with col2:
            transaction_type = st.selectbox("📊 Transaction Type", ["BUY", "SELL"])
            quantity = st.number_input("📦 Quantity", min_value=1, value=100, step=1)
            price = st.number_input("💰 Price per Share", min_value=0.0, value=1000.0, step=50.0)
        
        fees = st.number_input("💸 Fees", min_value=0.0, value=0.0, step=100.0)
        ref_number = st.text_input("🔖 Reference Number", placeholder="Optional")
        
        submitted = st.form_submit_button("✅ Add Transaction", type="primary")
        
        if submitted:
            if symbol and company_name and quantity > 0 and price > 0:
                transaction = {
                    'trade_date': trade_date,
                    'symbol': symbol,
                    'company_name': company_name,
                    'transaction_type': transaction_type,
                    'quantity': quantity,
                    'price': price,
                    'total_value': quantity * price,
                    'fees': fees,
                    'ref_number': ref_number,
                    'source_file': 'MANUAL_ENTRY'
                }
                
                if db.add_transaction(user_id, transaction):
                    st.success(f"✅ {transaction_type} transaction for {quantity} {symbol} added successfully!")
                    st.rerun()
                else:
                    st.error("❌ Failed to add transaction")
            else:
                st.error("❌ Please fill in all required fields")

def show_transaction_history(db, user_id):
    """Transaction history tab"""
    st.subheader("📋 Transaction History")
    
    transactions = db.get_all_transactions(user_id)
    
    if transactions.empty:
        st.info("📭 No transactions found.")
        return
    
    # Summary stats
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Transactions", len(transactions))
    
    with col2:
        buy_count = len(transactions[transactions['transaction_type'] == 'BUY'])
        st.metric("Buy Orders", buy_count)
    
    with col3:
        sell_count = len(transactions[transactions['transaction_type'] == 'SELL'])
        st.metric("Sell Orders", sell_count)
    
    # Transactions table
    st.markdown("---")
    
    # Format for display
    display_transactions = transactions.copy()
    
    if 'total_value' in display_transactions.columns:
        display_transactions['Total Value'] = display_transactions['total_value'].apply(lambda x: f"Rp {x:,.0f}")
    
    if 'price' in display_transactions.columns:
        display_transactions['Price'] = display_transactions['price'].apply(lambda x: f"Rp {x:,.0f}")
    
    # Select display columns
    display_cols = []
    rename_map = {}
    
    for col, display_name in [
        ('trade_date', 'Date'),
        ('symbol', 'Symbol'), 
        ('company_name', 'Company'),
        ('transaction_type', 'Type'),
        ('quantity', 'Quantity'),
        ('Price', 'Price'),
        ('Total Value', 'Total Value')
    ]:
        if col in display_transactions.columns:
            display_cols.append(col)
            rename_map[col] = display_name
    
    if display_cols:
        st.dataframe(
            display_transactions[display_cols].rename(columns=rename_map).sort_values('Date', ascending=False),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.dataframe(transactions, use_container_width=True, hide_index=True)

def add_sample_data(db, user_id):
    """Add sample transaction data"""
    sample_transactions = [
        {
            'trade_date': datetime(2025, 6, 13).date(),
            'symbol': 'BBCA',
            'company_name': 'Bank Central Asia Tbk.',
            'transaction_type': 'BUY',
            'quantity': 1000,
            'price': 10000.00,
            'total_value': 10000000.00,
            'fees': 25000,
            'ref_number': 'SAMPLE001',
            'source_file': 'SAMPLE_DATA'
        },
        {
            'trade_date': datetime(2025, 6, 13).date(),
            'symbol': 'BMRI',
            'company_name': 'Bank Mandiri Tbk.',
            'transaction_type': 'BUY',
            'quantity': 2000,
            'price': 5000.00,
            'total_value': 10000000.00,
            'fees': 25000,
            'ref_number': 'SAMPLE002',
            'source_file': 'SAMPLE_DATA'
        }
    ]
    
    for transaction in sample_transactions:
        db.add_transaction(user_id, transaction)

def show_setup_instructions():
    """Show database setup instructions"""
    st.warning("⚠️ Portfolio database not ready")
    
    st.markdown("""
    ### 🔧 Database Setup Required
    
    1. **Connect to PostgreSQL:**
    ```bash
    docker-compose exec postgres psql -U airflow -d airflow
    ```
    
    2. **Run the Portfolio Schema:**
    Execute the SQL schema from Step 2 above.
    
    3. **Restart the application:**
    ```bash
    docker-compose restart streamlit-dashboard
    ```
    """)

def show_dependency_help():
    """Show dependency installation help"""
    st.error("❌ Missing dependencies")
    
    st.markdown("""
    ### 📦 Install Required Packages:
    ```bash
    pip install pandas plotly psycopg2-binary
    ```
    """)

def show_troubleshooting():
    """Show troubleshooting guide"""
    st.error("❌ Application error")
    
    st.markdown("""
    ### 🔍 Troubleshooting:
    
    1. **Check database connection**
    2. **Verify schema exists** 
    3. **Restart the application**
    4. **Check logs** for detailed errors
    """)
'''
    
    with open("portofolio_tracker/pages/portofolio_tracker.py", 'w', encoding='utf-8') as f:
        f.write(page_content)
    print("✅ Created: portofolio_tracker/pages/portofolio_tracker.py")
    
    print("\n🎉 Portfolio Tracker fixed!")
    print("\nNext steps:")
    print("1. Run the SQL schema (Step 2) in PostgreSQL")
    print("2. Update app.py with the fixed import (Step 1)")
    print("3. Restart Streamlit: streamlit run dashboard/app.py")

if __name__ == "__main__":
    fix_portfolio_tracker()