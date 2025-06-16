#!/usr/bin/env python3
"""
🔧 PORTFOLIO TRACKER INTEGRATION FIX
Mengatasi masalah integrasi Portfolio Tracker dengan Dashboard utama

Jalankan script ini untuk fix semua masalah integration:
python portfolio_integration_fix.py
"""

import os
import shutil
import sys

def fix_portfolio_integration():
    """Fix semua masalah portfolio tracker integration"""
    
    print("🔧 Fixing Portfolio Tracker Integration...")
    print("=" * 60)
    
    # Step 1: Fix app.py import
    fix_app_py_import()
    
    # Step 2: Buat portfolio_manager.py di dashboard/utils
    create_portfolio_manager()
    
    # Step 3: Fix portfolio tracker page
    fix_portfolio_tracker_page()
    
    # Step 4: Create database setup script
    create_database_setup()
    
    # Step 5: Create integration bridge
    create_integration_bridge()
    
    print("\n🎉 Portfolio Integration Fixed!")
    print_next_steps()

def fix_app_py_import():
    """Fix import di app.py"""
    print("\n1️⃣ Fixing app.py imports...")
    
    app_py_content = '''import streamlit as st
import sys
import os
from datetime import datetime
import traceback

# Add directories to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'pages'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'portofolio_tracker'))

# Page config
st.set_page_config(
    page_title="IDX Stock Analysis Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        text-align: center;
        color: #1f77b4;
        margin-bottom: 30px;
    }
    .connection-status {
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
        font-weight: bold;
    }
    .status-success {
        background-color: #d4edda;
        color: #155724;
    }
    .status-error {
        background-color: #f8d7da;
        color: #721c24;
    }
    .page-error {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 5px;
        padding: 15px;
        margin: 10px 0;
    }
    .sidebar-info {
        background-color: #e3f2fd;
        padding: 8px;
        border-radius: 5px;
        margin: 5px 0;
        font-size: 0.9em;
    }
    .new-feature {
        background-color: #e8f5e8;
        border: 1px solid #4caf50;
        border-radius: 5px;
        padding: 8px;
        margin: 5px 0;
        font-size: 0.9em;
    }
    .selected-stock {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 5px;
        padding: 8px;
        margin: 5px 0;
        font-size: 0.9em;
    }
</style>
""", unsafe_allow_html=True)

# Import utilities
try:
    from database import test_database_connection, clear_all_caches, execute_query_safe
    UTILS_LOADED = True
except ImportError as e:
    st.error(f"❌ Failed to import database utilities: {e}")
    UTILS_LOADED = False

# Import page modules with better error handling
AVAILABLE_PAGES = {}

if UTILS_LOADED:
    # Try to import each page module
    page_modules = {
        "🏠 Overview": "overview",
        "🎯 Individual Stock Analysis": "individual_stock_analysis", 
        "🌊 Elliott Wave Analysis": "elliott_waves",
        "💼 Portfolio Tracker": "portfolio_page",  # Menggunakan bridge module
        "🔧 Debug": "debug_page"
    }
    
    for page_name, module_name in page_modules.items():
        try:
            # Special handling for Portfolio Tracker
            if module_name == "portfolio_page":
                # Import our bridge module
                import portfolio_page
                AVAILABLE_PAGES[page_name] = portfolio_page
            else:
                module = __import__(module_name)
                AVAILABLE_PAGES[page_name] = module
        except ImportError as e:
            print(f"Warning: Could not load page '{page_name}': {e}")
            continue

def get_system_info():
    """Get basic system information"""
    try:
        # Get latest data date
        latest_date_query = "SELECT MAX(date) as latest_date FROM daily_stock_summary LIMIT 1"
        latest_date_df = execute_query_safe(latest_date_query, "Latest Date")
        latest_date = latest_date_df['latest_date'].iloc[0] if not latest_date_df.empty else "N/A"
        
        # Get table count
        tables_query = """
        SELECT COUNT(*) as table_count
        FROM information_schema.tables 
        WHERE table_schema IN ('public', 'public_analytics', 'portfolio')
        """
        tables_df = execute_query_safe(tables_query, "Table Count")
        table_count = tables_df['table_count'].iloc[0] if not tables_df.empty else 0
        
        # Check portfolio schema
        portfolio_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.schemata 
            WHERE schema_name = 'portfolio'
        ) as portfolio_exists
        """
        portfolio_df = execute_query_safe(portfolio_query, "Portfolio Schema Check")
        portfolio_exists = portfolio_df['portfolio_exists'].iloc[0] if not portfolio_df.empty else False
        
        return {
            "latest_date": latest_date,
            "table_count": table_count,
            "pages_available": len(AVAILABLE_PAGES),
            "utils_loaded": UTILS_LOADED,
            "portfolio_schema": portfolio_exists
        }
    except Exception as e:
        return {
            "latest_date": "Error",
            "table_count": 0,
            "pages_available": len(AVAILABLE_PAGES),
            "utils_loaded": UTILS_LOADED,
            "portfolio_schema": False,
            "error": str(e)
        }

def show_sidebar():
    """Setup sidebar with navigation and status"""
    
    # Title
    st.sidebar.title("📊 IDX Stock Intelligence")
    st.sidebar.markdown("---")
    
    # Connection test
    st.sidebar.markdown("### 🔗 Connection Status")
    
    if UTILS_LOADED:
        connection_ok = test_database_connection()
        
        if not connection_ok:
            st.sidebar.error("❌ Database connection failed")
            return None, False
    else:
        st.sidebar.error("❌ Utilities not loaded")
        return None, False
    
    # Show portfolio status
    system_info = get_system_info()
    if system_info.get("portfolio_schema", False):
        st.sidebar.success("💼 Portfolio Ready")
    else:
        st.sidebar.warning("💼 Portfolio Setup Needed")
    
    # Show new feature highlight
    if "🏠 Overview" in AVAILABLE_PAGES:
        st.sidebar.markdown("""
        <div class="new-feature">
            💼 <strong>Portfolio Tracker Ready!</strong><br>
            Track your Indonesian stock investments with real-time P&L calculations!
        </div>
        """, unsafe_allow_html=True)
    
    # Show selected stock info if available
    if 'selected_stock' in st.session_state and st.session_state.selected_stock:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 🎯 Selected Stock")
        st.sidebar.markdown(f"""
        <div class="selected-stock">
            📊 <strong>{st.session_state.selected_stock}</strong><br>
            Ready for detailed analysis!
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("📈 Analyze", help="Go to Individual Stock Analysis"):
                st.session_state.page_navigation = "🎯 Individual Stock Analysis"
                st.rerun()
        with col2:
            if st.button("🔄 Clear", help="Clear stock selection"):
                if 'selected_stock' in st.session_state:
                    del st.session_state.selected_stock
                if 'page_navigation' in st.session_state:
                    del st.session_state.page_navigation
                st.rerun()
    
    # Navigation - This is the MAIN navigation that works
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🧭 Navigation")
    
    if not AVAILABLE_PAGES:
        st.sidebar.warning("⚠️ No pages available")
        return None, True
    
    # Handle navigation from session state (from Overview clicks)
    default_index = 0
    if 'page_navigation' in st.session_state and st.session_state.page_navigation in AVAILABLE_PAGES:
        page_keys = list(AVAILABLE_PAGES.keys())
        try:
            default_index = page_keys.index(st.session_state.page_navigation)
            # Clear the navigation state after using it
            del st.session_state.page_navigation
        except ValueError:
            default_index = 0
    elif "🏠 Overview" in AVAILABLE_PAGES:
        page_keys = list(AVAILABLE_PAGES.keys())
        try:
            default_index = page_keys.index("🏠 Overview")
        except ValueError:
            default_index = 0
    
    selected_page = st.sidebar.selectbox(
        "Choose Dashboard Page", 
        list(AVAILABLE_PAGES.keys()),
        index=default_index,
        help="Select a page to view different analysis dashboards"
    )
    
    # Data refresh controls
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔄 Data Controls")
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🔄 Refresh", help="Clear cache and refresh data"):
            clear_all_caches()
            st.rerun()
    
    with col2:
        if st.button("⚡ Force", help="Force clear all caches"):
            clear_all_caches()
            st.cache_data.clear()
            st.rerun()
    
    # System info
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ System Info")
    
    st.sidebar.markdown(f"""
    <div class="sidebar-info">
        📅 Latest Data: {system_info['latest_date']}<br>
        📊 Tables: {system_info['table_count']}<br>
        📄 Pages: {system_info['pages_available']}/5<br>
        💼 Portfolio: {'✅' if system_info['portfolio_schema'] else '❌'}<br>
        🕐 Time: {datetime.now().strftime('%H:%M:%S')}
    </div>
    """, unsafe_allow_html=True)
    
    if "error" in system_info:
        st.sidebar.warning(f"⚠️ System error: {system_info['error'][:50]}...")
    
    return selected_page, True

def show_connection_error():
    """Show connection error page with troubleshooting"""
    st.error("🚫 Cannot connect to database.")
    
    st.markdown("## 🔧 Troubleshooting Steps:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### Quick Fixes:
        ```bash
        # Restart PostgreSQL
        docker-compose restart postgres
        
        # Restart Dashboard  
        docker-compose restart streamlit-dashboard
        
        # Check all services
        docker-compose ps
        ```
        """)
    
    with col2:
        st.markdown("""
        ### Debug Commands:
        ```bash
        # Check PostgreSQL logs
        docker-compose logs postgres | tail -20
        
        # Test connection manually
        docker-compose exec streamlit-dashboard \\
        python -c "import psycopg2; \\
        psycopg2.connect(host='postgres', \\
        database='airflow', user='airflow', \\
        password='airflow')"
        ```
        """)
    
    # Retry button
    if st.button("🔄 Retry Connection", type="primary"):
        if UTILS_LOADED:
            clear_all_caches()
        st.rerun()

def show_missing_pages_info():
    """Show information about missing page modules"""
    st.warning("⚠️ No page modules are available.")
    
    st.markdown("""
    ## 📄 Missing Page Modules
    
    To use the dashboard, you need to create page modules in the `pages/` directory:
    
    ### Essential Pages:
    - `pages/overview.py` - Market overview dashboard
    - `pages/individual_stock_analysis.py` - Stock analysis
    - `pages/elliott_waves.py` - Elliott Wave Analysis
    - `pages/portfolio_page.py` - Portfolio tracker bridge
    - `pages/debug_page.py` - Debug and system information
    """)

def show_page_error(page_name, error):
    """Show page error with debugging information"""
    st.markdown(f"""
    <div class="page-error">
        <h3>❌ Error Loading Page: {page_name}</h3>
        <p><strong>Error:</strong> {str(error)}</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Show error details in expandable section
    with st.expander("🔍 Error Details", expanded=False):
        st.code(traceback.format_exc())
    
    # Troubleshooting tips
    st.markdown("""
    ### 🔧 Troubleshooting Tips:
    1. **Check if the page module exists** in the `pages/` directory
    2. **Verify import statements** in the page module
    3. **Check for syntax errors** in the page code
    4. **Ensure all dependencies** are properly imported
    5. **Check the logs** for more detailed error information
    """)
    
    # Retry button
    if st.button(f"🔄 Retry {page_name}"):
        clear_all_caches()
        st.rerun()

def show_welcome_message():
    """Show welcome message for new users"""
    system_info = get_system_info()
    
    if "🏠 Overview" in AVAILABLE_PAGES:
        st.markdown("""
        ### 🎉 Welcome to IDX Stock Intelligence Platform!
        
        **🏠 Market Overview Dashboard**
        
        Explore comprehensive market intelligence including:
        - 📊 **Market KPIs**: Real-time market statistics and breadth
        - 🎯 **Smart Money Signals**: AI-powered institutional activity detection
        - 🔥 **Top Movers**: Biggest price movements with clickable analysis
        - ⚡ **High Volume**: Most active stocks with trading opportunities
        
        **💼 Portfolio Tracker** - Track your Indonesian stock investments:
        - Real-time portfolio P&L calculations
        - Transaction history management
        - Performance analytics and visualizations
        - Integration with market data
        """)
        
        if not system_info.get("portfolio_schema", False):
            st.warning("""
            ⚠️ **Portfolio Database Setup Required**
            
            To use Portfolio Tracker, run the database setup first:
            1. Click on "💼 Portfolio Tracker" in sidebar
            2. Follow the setup instructions
            3. Or use the "🔧 Setup Portfolio DB" button below
            """)
            
            if st.button("🔧 Setup Portfolio DB", type="primary"):
                st.session_state.page_navigation = "💼 Portfolio Tracker"
                st.rerun()
    else:
        st.info("👈 Please select a page from the sidebar to begin analysis")

def main():
    """Main application function"""
    
    # Show sidebar and get navigation choice
    selected_page, connection_ok = show_sidebar()
    
    # Handle connection errors
    if not connection_ok:
        show_connection_error()
        return
    
    # Handle no available pages
    if not AVAILABLE_PAGES:
        show_missing_pages_info()
        return
    
    # Display selected page
    if selected_page and selected_page in AVAILABLE_PAGES:
        try:
            # Get the page module
            page_module = AVAILABLE_PAGES[selected_page]
            
            # Check if module has show() function
            if hasattr(page_module, 'show'):
                page_module.show()
            else:
                st.error(f"❌ Page module '{selected_page}' is missing show() function")
                
        except Exception as e:
            show_page_error(selected_page, e)
    
    else:
        show_welcome_message()

# Footer
def show_footer():
    """Show footer with system information"""
    st.markdown("---")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.markdown(
            "<div style='text-align: left; color: gray; font-size: 0.9em;'>"
            "📊 IDX Stock Intelligence Platform | "
            "Built with ❤️ using Streamlit & PostgreSQL"
            "</div>", 
            unsafe_allow_html=True
        )
    
    with col2:
        if UTILS_LOADED:
            st.markdown(
                f"<div style='text-align: center; color: gray; font-size: 0.8em;'>"
                f"Pages: {len(AVAILABLE_PAGES)}/5 loaded"
                "</div>", 
                unsafe_allow_html=True
            )
    
    with col3:
        st.markdown(
            f"<div style='text-align: right; color: gray; font-size: 0.8em;'>"
            f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            "</div>", 
            unsafe_allow_html=True
        )

# Run the application
if __name__ == "__main__":
    try:
        main()
        show_footer()
    except Exception as e:
        st.error(f"❌ Application error: {str(e)}")
        
        with st.expander("🔍 Application Error Details"):
            st.code(traceback.format_exc())
        
        st.markdown("""
        ### 🆘 Recovery Steps:
        1. **Refresh the page** (F5 or Ctrl+R)
        2. **Check Docker services** are running
        3. **Review the error details** above
        4. **Check application logs** with `docker-compose logs streamlit-dashboard`
        5. **Restart the dashboard** with `docker-compose restart streamlit-dashboard`
        """)
'''
    
    # Backup existing app.py
    if os.path.exists("dashboard/app.py"):
        shutil.copy("dashboard/app.py", "dashboard/app.py.backup")
        print("   📄 Backed up existing app.py")
    
    # Write new app.py
    with open("dashboard/app.py", 'w', encoding='utf-8') as f:
        f.write(app_py_content)
    
    print("   ✅ Updated dashboard/app.py with portfolio integration")

def create_portfolio_manager():
    """Buat portfolio manager di dashboard/utils"""
    print("\n2️⃣ Creating portfolio manager bridge...")
    
    os.makedirs("dashboard/utils", exist_ok=True)
    
    portfolio_manager_content = '''"""
Portfolio Manager Bridge
Menghubungkan dashboard utama dengan portfolio tracker
"""

import sys
import os
import streamlit as st

# Add portfolio tracker path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'portofolio_tracker'))

class PortfolioManager:
    def __init__(self):
        self.portfolio_available = False
        self._check_portfolio_availability()
    
    def _check_portfolio_availability(self):
        """Check if portfolio tracker modules are available"""
        try:
            from utils.database import PortfolioDatabase
            from utils.portfolio_calculator import PortfolioCalculator
            
            # Test database connection
            db = PortfolioDatabase()
            if db.check_connection():
                self.portfolio_available = True
                self.db = db
                self.calculator = PortfolioCalculator()
        except ImportError as e:
            st.warning(f"Portfolio modules not available: {e}")
            self.portfolio_available = False
        except Exception as e:
            st.warning(f"Portfolio connection error: {e}")
            self.portfolio_available = False
    
    def is_available(self):
        """Check if portfolio tracker is available"""
        return self.portfolio_available
    
    def get_portfolio_summary(self, user_id="default_user"):
        """Get portfolio summary for user"""
        if not self.portfolio_available:
            return {}
        
        try:
            return self.calculator.calculate_portfolio_metrics(user_id)
        except Exception as e:
            st.error(f"Error getting portfolio summary: {e}")
            return {}
    
    def get_holdings_for_stock(self, symbol, user_id="default_user"):
        """Get holdings information for specific stock"""
        if not self.portfolio_available:
            return None
        
        try:
            holdings = self.db.get_current_holdings(user_id)
            if not holdings.empty:
                stock_holdings = holdings[holdings['symbol'] == symbol]
                if not stock_holdings.empty:
                    return stock_holdings.iloc[0].to_dict()
            return None
        except Exception as e:
            st.error(f"Error getting holdings for {symbol}: {e}")
            return None
    
    def add_quick_transaction(self, transaction_data, user_id="default_user"):
        """Add transaction from other pages"""
        if not self.portfolio_available:
            return False
        
        try:
            return self.db.add_transaction(user_id, transaction_data)
        except Exception as e:
            st.error(f"Error adding transaction: {e}")
            return False

# Global portfolio manager instance
portfolio_manager = PortfolioManager()
'''
    
    with open("dashboard/utils/portfolio_manager.py", 'w', encoding='utf-8') as f:
        f.write(portfolio_manager_content)
    
    print("   ✅ Created dashboard/utils/portfolio_manager.py")

def fix_portfolio_tracker_page():
    """Fix portfolio tracker page import issues"""
    print("\n3️⃣ Fixing portfolio tracker page...")
    
    # Pastikan direktori portofolio_tracker/utils ada
    os.makedirs("portofolio_tracker/utils", exist_ok=True)
    
    # Fix __init__.py
    for init_path in [
        "portofolio_tracker/__init__.py",
        "portofolio_tracker/utils/__init__.py",
        "portofolio_tracker/pages/__init__.py"
    ]:
        os.makedirs(os.path.dirname(init_path), exist_ok=True)
        with open(init_path, 'w', encoding='utf-8') as f:
            f.write("# Portfolio Tracker Module\n")
    
    print("   ✅ Fixed portfolio tracker module structure")

def create_integration_bridge():
    """Buat bridge module untuk portfolio di dashboard/pages"""
    print("\n4️⃣ Creating integration bridge...")
    
    os.makedirs("dashboard/pages", exist_ok=True)
    
    bridge_content = '''"""
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
'''
    
    with open("dashboard/pages/portfolio_page.py", 'w', encoding='utf-8') as f:
        f.write(bridge_content)
    
    print("   ✅ Created dashboard/pages/portfolio_page.py")

def create_database_setup():
    """Buat script setup database yang mudah dijalankan"""
    print("\n5️⃣ Creating database setup script...")
    
    setup_script = '''#!/usr/bin/env python3
"""
Portfolio Database Setup Script
Jalankan: python setup_portfolio_db.py
"""

import psycopg2
import os

def setup_portfolio_database():
    """Setup portfolio database schema"""
    
    # Database config
    db_config = {
        'host': os.getenv('DB_HOST', 'postgres'),
        'database': os.getenv('DB_NAME', 'airflow'),
        'user': os.getenv('DB_USER', 'airflow'),
        'password': os.getenv('DB_PASSWORD', 'airflow'),
        'port': int(os.getenv('DB_PORT', '5432'))
    }
    
    print("🔧 Setting up Portfolio Database...")
    print(f"📡 Connecting to {db_config['host']}:{db_config['port']}")
    
    try:
        # Connect to database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print("✅ Connected to database")
        
        # Read and execute schema
        schema_sql = """
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
        
        CREATE INDEX IF NOT EXISTS idx_transactions_user_symbol ON portfolio.transactions(user_id, symbol);
        CREATE INDEX IF NOT EXISTS idx_transactions_date ON portfolio.transactions(trade_date);
        CREATE INDEX IF NOT EXISTS idx_holdings_user ON portfolio.holdings(user_id);
        """
        
        # Execute schema creation
        cursor.execute(schema_sql)
        conn.commit()
        
        print("✅ Portfolio schema created")
        
        # Create trigger function
        trigger_sql = """
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
        
        DROP TRIGGER IF EXISTS trigger_update_holdings ON portfolio.transactions;
        CREATE TRIGGER trigger_update_holdings
            AFTER INSERT ON portfolio.transactions
            FOR EACH ROW
            EXECUTE FUNCTION portfolio.update_holdings_after_transaction();
        """
        
        cursor.execute(trigger_sql)
        conn.commit()
        
        print("✅ Database triggers created")
        
        # Test the setup
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'portfolio'")
        table_count = cursor.fetchone()[0]
        
        print(f"✅ Portfolio setup complete! {table_count} tables created")
        
        cursor.close()
        conn.close()
        
        print("🎉 Portfolio Database ready!")
        print("📊 You can now use Portfolio Tracker in the dashboard")
        
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        print("🔧 Make sure PostgreSQL is running and accessible")

if __name__ == "__main__":
    setup_portfolio_database()
'''
    
    with open("setup_portfolio_db.py", 'w', encoding='utf-8') as f:
        f.write(setup_script)
    
    print("   ✅ Created setup_portfolio_db.py")

def print_next_steps():
    """Print next steps untuk user"""
    print("\n" + "=" * 60)
    print("🎯 NEXT STEPS:")
    print("=" * 60)
    
    print("""
1️⃣ Setup Database Schema:
   python setup_portfolio_db.py

2️⃣ Restart Dashboard:
   docker-compose restart streamlit-dashboard

3️⃣ Access Portfolio Tracker:
   - Go to dashboard
   - Select "💼 Portfolio Tracker" from sidebar
   - Start adding your transactions!

4️⃣ Features Available:
   ✅ Manual transaction entry
   ✅ Portfolio overview with P&L
   ✅ Transaction history
   ✅ Database integration
   ✅ Real-time calculations

🎉 Portfolio Tracker is now integrated with your dashboard!
""")

if __name__ == "__main__":
    fix_portfolio_integration()