#!/usr/bin/env python3
"""
Portfolio Tracker Fix Script - Clean Version (No Emojis)
Fixes import issues and file structure problems
"""

import os
import shutil

def fix_portfolio_structure():
    """Fix portfolio tracker file structure and imports"""
    
    print("Fixing Portfolio Tracker Structure...")
    
    # 1. Move pdf_parser.py to utils directory if it's in wrong location
    pdf_parser_wrong = "portofolio_tracker/pdf_parser.py"
    pdf_parser_correct = "portofolio_tracker/utils/pdf_parser.py"
    
    if os.path.exists(pdf_parser_wrong) and not os.path.exists(pdf_parser_correct):
        print(f"Moving {pdf_parser_wrong} to {pdf_parser_correct}")
        os.makedirs(os.path.dirname(pdf_parser_correct), exist_ok=True)
        shutil.move(pdf_parser_wrong, pdf_parser_correct)
    
    # 2. Create missing __init__.py files
    init_files = [
        "portofolio_tracker/__init__.py",
        "portofolio_tracker/utils/__init__.py", 
        "portofolio_tracker/pages/__init__.py"
    ]
    
    for init_file in init_files:
        if not os.path.exists(init_file):
            print(f"Creating {init_file}")
            os.makedirs(os.path.dirname(init_file), exist_ok=True)
            with open(init_file, 'w', encoding='utf-8') as f:
                f.write("# Portfolio Tracker Module\n")
    
    # 3. Fix portfolio_calculator.py
    calculator_content = '''# ============================================================================
# PORTFOLIO CALCULATOR
# File: portofolio_tracker/utils/portfolio_calculator.py
# ============================================================================

from datetime import datetime, date
import pandas as pd
import streamlit as st
import sys
import os

# Import database utilities
try:
    from .database import PortfolioDatabase
except ImportError:
    try:
        from portofolio_tracker.utils.database import PortfolioDatabase
    except ImportError:
        from database import PortfolioDatabase

# Import main dashboard database utilities for price fetching
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'dashboard', 'utils'))
try:
    from database import fetch_data_cached
    MAIN_DB_AVAILABLE = True
except ImportError:
    MAIN_DB_AVAILABLE = False

class PortfolioCalculator:
    def __init__(self):
        self.db = PortfolioDatabase()
    
    def recalculate_portfolio(self, user_id):
        """Recalculate entire portfolio metrics"""
        try:
            self._update_holdings_metrics(user_id)
            self._update_portfolio_summary(user_id)
            return True
        except Exception as e:
            st.error(f"Error recalculating portfolio: {e}")
            return False
    
    def _update_holdings_metrics(self, user_id):
        """Update current value and P&L for all holdings"""
        conn = self.db.get_connection()
        if not conn:
            return
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE portfolio.holdings 
                    SET current_value = total_quantity * COALESCE(current_price, average_price),
                        unrealized_pnl = (total_quantity * COALESCE(current_price, average_price)) - total_cost,
                        unrealized_pnl_pct = CASE 
                            WHEN total_cost > 0 
                            THEN ((total_quantity * COALESCE(current_price, average_price)) - total_cost) / total_cost * 100 
                            ELSE 0 
                        END,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE user_id = %s
                """, (user_id,))
            
            conn.commit()
        except Exception as e:
            st.error(f"Error updating holdings metrics: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def _update_portfolio_summary(self, user_id):
        """Update portfolio summary for today"""
        conn = self.db.get_connection()
        if not conn:
            return
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COALESCE(SUM(total_cost), 0) as total_invested,
                        COALESCE(SUM(current_value), 0) as current_value,
                        COALESCE(SUM(unrealized_pnl), 0) as unrealized_pnl,
                        COUNT(*) as total_stocks
                    FROM portfolio.holdings 
                    WHERE user_id = %s AND total_quantity > 0
                """, (user_id,))
                
                result = cursor.fetchone()
                if result:
                    total_invested, current_value, unrealized_pnl, total_stocks = result
                    total_pnl = unrealized_pnl
                    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0
                    today = date.today()
                    
                    cursor.execute("""
                        INSERT INTO portfolio.portfolio_summary 
                        (user_id, summary_date, total_invested, current_value, 
                         total_pnl, total_pnl_pct, realized_pnl, unrealized_pnl, total_stocks)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, summary_date)
                        DO UPDATE SET
                            total_invested = EXCLUDED.total_invested,
                            current_value = EXCLUDED.current_value,
                            total_pnl = EXCLUDED.total_pnl,
                            total_pnl_pct = EXCLUDED.total_pnl_pct,
                            realized_pnl = EXCLUDED.realized_pnl,
                            unrealized_pnl = EXCLUDED.unrealized_pnl,
                            total_stocks = EXCLUDED.total_stocks
                    """, (user_id, today, total_invested, current_value, 
                         total_pnl, total_pnl_pct, 0, unrealized_pnl, total_stocks))
            
            conn.commit()
        except Exception as e:
            st.error(f"Error updating portfolio summary: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def calculate_portfolio_metrics(self, user_id):
        """Calculate comprehensive portfolio metrics"""
        holdings = self.db.get_current_holdings(user_id)
        
        if holdings.empty:
            return {}
        
        metrics = {
            'total_value': holdings['current_value'].sum(),
            'total_cost': holdings['total_cost'].sum(),
            'total_pnl': holdings['unrealized_pnl'].sum(),
            'total_stocks': len(holdings),
            'winners': len(holdings[holdings['unrealized_pnl'] > 0]),
            'losers': len(holdings[holdings['unrealized_pnl'] < 0])
        }
        
        metrics['total_pnl_pct'] = (metrics['total_pnl'] / metrics['total_cost'] * 100) if metrics['total_cost'] > 0 else 0
        metrics['win_rate'] = (metrics['winners'] / metrics['total_stocks'] * 100) if metrics['total_stocks'] > 0 else 0
        
        return metrics
'''
    
    # Write the fixed calculator
    os.makedirs("portofolio_tracker/utils", exist_ok=True)
    with open("portofolio_tracker/utils/portfolio_calculator.py", "w", encoding='utf-8') as f:
        f.write(calculator_content)
    
    print("Portfolio structure fixed!")

def create_simple_portfolio_page():
    """Create a simplified portfolio page that works"""
    
    portfolio_page_content = '''# ============================================================================
# SIMPLIFIED PORTFOLIO TRACKER  
# File: portofolio_tracker/pages/portofolio_tracker.py
# ============================================================================

import streamlit as st
import pandas as pd
import sys
import os

def show():
    """Main portfolio tracker interface"""
    st.markdown("# Portfolio Tracker - Indonesian Stocks")
    st.markdown("**Real-time portfolio tracking with trade confirmation upload**")
    
    if not check_dependencies():
        show_setup_guide()
        return
    
    try:
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
        from database import PortfolioDatabase
        from portfolio_calculator import PortfolioCalculator
        
        db = PortfolioDatabase()
        calculator = PortfolioCalculator()
        
        if not db.check_connection():
            show_database_setup()
            return
        
        show_portfolio_interface(db, calculator)
        
    except ImportError as e:
        st.error(f"Failed to import portfolio modules: {e}")
        show_module_setup_guide()

def check_dependencies():
    """Check if required dependencies are installed"""
    missing_deps = []
    
    for module in ['pandas', 'plotly', 'psycopg2']:
        try:
            __import__(module)
        except ImportError:
            missing_deps.append(module if module != 'psycopg2' else 'psycopg2-binary')
    
    if missing_deps:
        st.error(f"Missing dependencies: {', '.join(missing_deps)}")
        st.markdown(f"Install with: `pip install {' '.join(missing_deps)}`")
        return False
    
    return True

def show_portfolio_interface(db, calculator):
    """Show main portfolio interface"""
    st.sidebar.markdown("### Portfolio Controls")
    user_id = st.sidebar.selectbox("Portfolio Owner:", ["default_user"])
    
    tab1, tab2, tab3 = st.tabs(["Overview", "Add Transaction", "History"])
    
    with tab1:
        show_portfolio_overview(db, user_id)
    
    with tab2:
        show_add_transaction(db, user_id)
    
    with tab3:
        show_transaction_history(db, user_id)

def show_portfolio_overview(db, user_id):
    """Show portfolio overview"""
    st.markdown("### Portfolio Overview")
    
    try:
        holdings = db.get_current_holdings(user_id)
        
        if holdings.empty:
            st.info("No holdings found. Add your first transaction to get started!")
            
            if st.button("Load Sample Data"):
                load_sample_data(db, user_id)
                st.rerun()
            return
        
        # Summary metrics
        total_value = holdings['current_value'].sum()
        total_cost = holdings['total_cost'].sum() 
        total_pnl = holdings['unrealized_pnl'].sum()
        total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Investment", f"Rp{total_cost:,.0f}")
        
        with col2:
            st.metric("Current Value", f"Rp{total_value:,.0f}")
        
        with col3:
            st.metric("Total P&L", f"Rp{total_pnl:,.0f}", f"{total_pnl_pct:.2f}%")
        
        with col4:
            st.metric("Holdings", f"{len(holdings)} stocks")
        
        # Holdings table
        st.markdown("---")
        st.markdown("### Current Holdings")
        
        display_holdings = holdings.copy()
        display_holdings['Market Value'] = display_holdings['current_value'].apply(lambda x: f"Rp{x:,.0f}")
        display_holdings['Cost Basis'] = display_holdings['total_cost'].apply(lambda x: f"Rp{x:,.0f}")
        display_holdings['P&L'] = display_holdings['unrealized_pnl'].apply(lambda x: f"Rp{x:,.0f}")
        display_holdings['P&L %'] = display_holdings['unrealized_pnl_pct'].apply(lambda x: f"{x:.2f}%")
        
        st.dataframe(
            display_holdings[['symbol', 'company_name', 'total_quantity', 'Cost Basis', 'Market Value', 'P&L', 'P&L %']].rename(columns={
                'symbol': 'Symbol',
                'company_name': 'Company', 
                'total_quantity': 'Quantity'
            }),
            use_container_width=True,
            hide_index=True
        )
        
    except Exception as e:
        st.error(f"Error loading portfolio: {e}")

def show_add_transaction(db, user_id):
    """Show add transaction form"""
    st.markdown("### Add Transaction")
    
    with st.form("add_transaction"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            trade_date = st.date_input("Trade Date")
            transaction_type = st.selectbox("Type", ["BUY", "SELL"])
            symbol = st.text_input("Stock Symbol", placeholder="e.g., BBCA").upper()
        
        with col2:
            company_name = st.text_input("Company Name", placeholder="e.g., Bank Central Asia Tbk.")
            quantity = st.number_input("Quantity", min_value=1, value=100)
            price = st.number_input("Price per Share", min_value=0.0, value=1000.0)
        
        with col3:
            fees = st.number_input("Fees", min_value=0.0, value=0.0)
            ref_number = st.text_input("Reference #", placeholder="Optional")
            st.write("")
            submitted = st.form_submit_button("Add Transaction", type="primary")
        
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
                    st.success("Transaction added successfully!")
                    st.rerun()
                else:
                    st.error("Failed to add transaction")
            else:
                st.error("Please fill in all required fields")

def show_transaction_history(db, user_id):
    """Show transaction history"""
    st.markdown("### Transaction History")
    
    try:
        transactions = db.get_all_transactions(user_id)
        
        if transactions.empty:
            st.info("No transactions found.")
            return
        
        # Summary
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
        display_transactions = transactions.copy()
        display_transactions['Total Value'] = display_transactions['total_value'].apply(lambda x: f"Rp{x:,.0f}")
        display_transactions['Price'] = display_transactions['price'].apply(lambda x: f"Rp{x:,.0f}")
        
        st.dataframe(
            display_transactions[['trade_date', 'symbol', 'company_name', 'transaction_type', 'quantity', 'Price', 'Total Value']].rename(columns={
                'trade_date': 'Date',
                'symbol': 'Symbol',
                'company_name': 'Company',
                'transaction_type': 'Type',
                'quantity': 'Quantity'
            }).sort_values('Date', ascending=False),
            use_container_width=True,
            hide_index=True
        )
        
    except Exception as e:
        st.error(f"Error loading transactions: {e}")

def load_sample_data(db, user_id):
    """Load sample transaction data"""
    from datetime import datetime
    
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
    
    st.success("Sample data loaded!")

def show_setup_guide():
    """Show setup guide"""
    st.error("Portfolio Tracker Setup Required")
    
    st.markdown("""
    ### Setup Steps:
    
    1. **Install Dependencies:**
    ```bash
    pip install pandas plotly psycopg2-binary PyPDF2 openpyxl
    ```
    
    2. **Run Database Setup:**
    Execute the portfolio schema SQL in your PostgreSQL database.
    
    3. **Restart Application:**
    Restart your Streamlit app after setup.
    """)

def show_database_setup():
    """Show database setup guide"""
    st.warning("Portfolio database not connected")
    
    st.markdown("""
    ### Database Setup Required
    
    Run this SQL in your PostgreSQL database:
    
    ```sql
    CREATE SCHEMA IF NOT EXISTS portfolio;
    
    CREATE TABLE IF NOT EXISTS portfolio.transactions (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(50) DEFAULT 'default_user',
        trade_date DATE NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        company_name VARCHAR(255),
        transaction_type VARCHAR(10) NOT NULL,
        quantity INTEGER NOT NULL,
        price DECIMAL(15,2) NOT NULL,
        total_value DECIMAL(20,2) NOT NULL,
        fees DECIMAL(10,2) DEFAULT 0,
        ref_number VARCHAR(50),
        source_file VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS portfolio.holdings (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(50) DEFAULT 'default_user',
        symbol VARCHAR(10) NOT NULL,
        company_name VARCHAR(255),
        total_quantity INTEGER NOT NULL DEFAULT 0,
        average_price DECIMAL(15,2) NOT NULL DEFAULT 0,
        total_cost DECIMAL(20,2) NOT NULL DEFAULT 0,
        current_price DECIMAL(15,2),
        current_value DECIMAL(20,2),
        unrealized_pnl DECIMAL(20,2),
        unrealized_pnl_pct DECIMAL(8,4),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, symbol)
    );
    ```
    """)

def show_module_setup_guide():
    """Show module setup guide"""
    st.error("Portfolio modules not found")
    
    st.markdown("""
    ### Required Files:
    - portofolio_tracker/utils/database.py
    - portofolio_tracker/utils/portfolio_calculator.py
    - portofolio_tracker/utils/pdf_parser.py
    """)
'''
    
    # Write the simplified portfolio page
    os.makedirs("portofolio_tracker/pages", exist_ok=True)
    with open("portofolio_tracker/pages/portofolio_tracker.py", "w", encoding='utf-8') as f:
        f.write(portfolio_page_content)
    
    print("Simplified portfolio page created!")

def install_dependencies():
    """Install required dependencies"""
    print("Installing dependencies...")
    
    try:
        import subprocess
        import sys
        
        dependencies = ["PyPDF2", "openpyxl", "xlsxwriter"]
        
        for dep in dependencies:
            print(f"Installing {dep}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", dep])
        
        print("Dependencies installed successfully!")
        
    except Exception as e:
        print(f"Failed to install dependencies: {e}")
        print("Please install manually: pip install PyPDF2 openpyxl xlsxwriter")

if __name__ == "__main__":
    fix_portfolio_structure()
    create_simple_portfolio_page()
    install_dependencies()
    
    print("\nPortfolio Tracker setup complete!")
    print("\nNext steps:")
    print("1. Execute the database schema SQL in PostgreSQL")
    print("2. Restart your Streamlit app: streamlit run dashboard/app.py")
    print("3. Navigate to 'Portfolio Tracker' in the sidebar")