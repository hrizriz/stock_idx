import streamlit as st
import sys
import os
from datetime import datetime
import traceback
import pandas as pd
import yfinance as yf
from plotly.subplots import make_subplots
import numpy as np
from .utils import format_currency, format_percentage, calculate_metrics
from .stock_analyzer import StockAnalyzer
from .market_overview import MarketOverview
from .news_analyzer import NewsAnalyzer
from .screener import Screener
from .backtest import Backtest
from .alerts import Alerts
from .settings import Settings

# Add directories to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'pages'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

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

# Import page modules
AVAILABLE_PAGES = {}

if UTILS_LOADED:
    # Try to import each page module
    page_modules = {
        "🏠 Overview": "overview",
        "🎯 Individual Stock Analysis": "individual_stock_analysis",
        "🌊 Elliott Wave Analysis": "elliott_waves",
        "🔧 Debug": "debug_page"
    }
    
    for page_name, module_name in page_modules.items():
        try:
            import importlib
            module = importlib.import_module(module_name)
            AVAILABLE_PAGES[page_name] = module
        except ImportError as e:
            st.error(f"❌ Gagal memuat halaman '{page_name}': {e}")
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
        WHERE table_schema IN ('public', 'public_analytics')
        """
        tables_df = execute_query_safe(tables_query, "Table Count")
        table_count = tables_df['table_count'].iloc[0] if not tables_df.empty else 0
        
        return {
            "latest_date": latest_date,
            "table_count": table_count,
            "pages_available": len(AVAILABLE_PAGES),
            "utils_loaded": UTILS_LOADED
        }
    except Exception as e:
        return {
            "latest_date": "Error",
            "table_count": 0,
            "pages_available": len(AVAILABLE_PAGES),
            "utils_loaded": UTILS_LOADED,
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
    
    system_info = get_system_info()
    st.sidebar.markdown(f"""
    <div class="sidebar-info">
        📅 Latest Data: {system_info['latest_date']}<br>
        📊 Tables: {system_info['table_count']}<br>
        📄 Pages: {system_info['pages_available']}/4<br>
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
        docker-compose exec streamlit-dashboard \
        python -c "import psycopg2; \
        psycopg2.connect(host='postgres', \
        database='airflow', user='airflow', \
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
        """)
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
                f"Pages: {len(AVAILABLE_PAGES)}/4 loaded"
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
