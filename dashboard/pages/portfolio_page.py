"""
Portfolio Tracker Bridge Page
Bridge antara dashboard utama dan portfolio tracker
"""

import streamlit as st
import sys
import os

# Add portfolio tracker to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'portofolio_tracker'))

def show():
    """Main portfolio bridge function"""
    
    # Check if portfolio modules are available
    try:
        from pages.portofolio_tracker import show as show_portfolio
        from utils.database import PortfolioDatabase
        
        # Test database connection
        db = PortfolioDatabase()
        
        if not db.check_connection():
            show_portfolio_setup()
            return
        
        # Show portfolio tracker
        show_portfolio()
        
    except ImportError as e:
        show_module_error(e)
    except Exception as e:
        show_general_error(e)

def show_portfolio_setup():
    """Show portfolio setup instructions"""
    st.markdown("# 💼 Portfolio Tracker Setup")
    st.warning("⚠️ Portfolio database schema not found")
    
    st.markdown("""
    ### 🔧 Database Setup Required
    
    Portfolio Tracker needs its own database schema. Follow these steps:
    
    #### Step 1: Connect to PostgreSQL
    ```bash
    docker-compose exec postgres psql -U airflow -d airflow
    ```
    
    #### Step 2: Create Portfolio Schema
    ```sql
    -- Create portfolio schema
    CREATE SCHEMA IF NOT EXISTS portfolio;
    
    -- Create transactions table
    CREATE TABLE IF NOT EXISTS portfolio.transactions (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(50) DEFAULT 'default_user',
        trade_date DATE NOT NULL,
        settlement_date DATE,
        symbol VARCHAR(10) NOT NULL,
        company_name VARCHAR(255),
        transaction_type VARCHAR(10) NOT NULL CHECK (transaction_type IN ('BUY', 'SELL')),
        quantity INTEGER NOT NULL,
        price DECIMAL(15,2) NOT NULL,
        total_value DECIMAL(20,2) NOT NULL,
        fees DECIMAL(10,2) DEFAULT 0,
        ref_number VARCHAR(50),
        source_file VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create holdings table
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
    
    -- Create portfolio summary table
    CREATE TABLE IF NOT EXISTS portfolio.portfolio_summary (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(50) DEFAULT 'default_user',
        summary_date DATE NOT NULL,
        total_invested DECIMAL(20,2) NOT NULL DEFAULT 0,
        current_value DECIMAL(20,2) NOT NULL DEFAULT 0,
        total_pnl DECIMAL(20,2) NOT NULL DEFAULT 0,
        total_pnl_pct DECIMAL(8,4) NOT NULL DEFAULT 0,
        realized_pnl DECIMAL(20,2) NOT NULL DEFAULT 0,
        unrealized_pnl DECIMAL(20,2) NOT NULL DEFAULT 0,
        total_stocks INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, summary_date)
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_transactions_user_symbol ON portfolio.transactions(user_id, symbol);
    CREATE INDEX IF NOT EXISTS idx_transactions_date ON portfolio.transactions(trade_date);
    CREATE INDEX IF NOT EXISTS idx_holdings_user ON portfolio.holdings(user_id);
    
    -- Create update trigger function
    CREATE OR REPLACE FUNCTION portfolio.update_holdings_after_transaction()
    RETURNS TRIGGER AS $$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            INSERT INTO portfolio.holdings (user_id, symbol, company_name, total_quantity, average_price, total_cost)
            VALUES (NEW.user_id, NEW.symbol, NEW.company_name, 
                    CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.quantity ELSE -NEW.quantity END,
                    CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.price ELSE 0 END,
                    CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.total_value ELSE -NEW.total_value END)
            ON CONFLICT (user_id, symbol)
            DO UPDATE SET
                total_quantity = holdings.total_quantity + 
                    CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.quantity ELSE -NEW.quantity END,
                total_cost = holdings.total_cost + 
                    CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.total_value ELSE -NEW.total_value END,
                average_price = CASE 
                    WHEN (holdings.total_quantity + CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.quantity ELSE -NEW.quantity END) > 0
                    THEN (holdings.total_cost + CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.total_value ELSE -NEW.total_value END) / 
                         (holdings.total_quantity + CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.quantity ELSE -NEW.quantity END)
                    ELSE 0
                END,
                company_name = COALESCE(holdings.company_name, NEW.company_name),
                last_updated = CURRENT_TIMESTAMP;
            
            DELETE FROM portfolio.holdings 
            WHERE user_id = NEW.user_id AND symbol = NEW.symbol AND total_quantity <= 0;
            
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    
    -- Create trigger
    DROP TRIGGER IF EXISTS trigger_update_holdings ON portfolio.transactions;
    CREATE TRIGGER trigger_update_holdings
        AFTER INSERT ON portfolio.transactions
        FOR EACH ROW
        EXECUTE FUNCTION portfolio.update_holdings_after_transaction();
    ```
    
    #### Step 3: Restart Application
    ```bash
    docker-compose restart streamlit-dashboard
    ```
    """)
    
    if st.button("🔄 Retry Connection", type="primary"):
        st.rerun()

def show_module_error(error):
    """Show module import error"""
    st.error("❌ Portfolio Tracker modules not found")
    
    st.markdown(f"""
    ### 🔧 Module Error: {str(error)}
    
    #### Possible Solutions:
    
    1. **Check file structure:**
    ```
    portofolio_tracker/
    ├── __init__.py
    ├── pages/
    │   ├── __init__.py
    │   └── portofolio_tracker.py
    └── utils/
        ├── __init__.py
        ├── database.py
        └── portfolio_calculator.py
    ```
    
    2. **Run the integration fix:**
    ```bash
    python portfolio_integration_fix.py
    ```
    
    3. **Install dependencies:**
    ```bash
    pip install psycopg2-binary pandas plotly
    ```
    """)

def show_general_error(error):
    """Show general error"""
    st.error(f"❌ Portfolio Tracker Error: {str(error)}")
    
    st.markdown("""
    ### 🔍 Troubleshooting Steps:
    
    1. **Check database connection**
    2. **Verify portfolio schema exists**
    3. **Check file permissions**
    4. **Restart the application**
    
    ### 🆘 Get Help:
    If the problem persists, check the application logs:
    ```bash
    docker-compose logs streamlit-dashboard
    ```
    """)
