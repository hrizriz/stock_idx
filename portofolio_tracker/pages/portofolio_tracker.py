# ============================================================================
# PORTFOLIO TRACKER MAIN PAGE
# File: portofolio_tracker/pages/portofolio_tracker.py
# ============================================================================

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import sys
import os
from datetime import datetime, timedelta
import io
import base64

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'dashboard', 'utils'))

# Import portfolio utilities
try:
    from portofolio_tracker.utils.database import PortfolioDatabase
    from portofolio_tracker.utils.pdf_parser import TradeConfirmationParser
    from portofolio_tracker.utils.portfolio_calculator import PortfolioCalculator
    PORTFOLIO_MODULES_AVAILABLE = True
except ImportError:
    try:
        # Fallback import paths
        from utils.database import PortfolioDatabase
        from utils.pdf_parser import TradeConfirmationParser
        from utils.portfolio_calculator import PortfolioCalculator
        PORTFOLIO_MODULES_AVAILABLE = True
    except ImportError:
        st.error("❌ Portfolio tracker modules not found. Please check installation.")
        PORTFOLIO_MODULES_AVAILABLE = False

# Import main dashboard utilities for price data
try:
    from database import fetch_data_cached
    MAIN_DB_AVAILABLE = True
except ImportError:
    MAIN_DB_AVAILABLE = False

class PortfolioTracker:
    def __init__(self):
        if PORTFOLIO_MODULES_AVAILABLE:
            self.db = PortfolioDatabase()
            self.parser = TradeConfirmationParser()
            self.calculator = PortfolioCalculator()
        else:
            st.error("❌ Portfolio tracker not properly configured.")
    
    def show(self):
        """Main portfolio tracker interface"""
        st.markdown("# 💼 Portfolio Tracker - Indonesian Stocks")
        st.markdown("**📊 Real-time portfolio tracking with trade confirmation upload**")
        st.markdown("*Track your Indonesian stock investments with automatic P&L calculation and performance analysis*")
        
        if not PORTFOLIO_MODULES_AVAILABLE:
            self.show_setup_instructions()
            return
        
        # Check if portfolio database is ready
        if not self.db.check_connection():
            st.error("❌ Portfolio database not connected. Please run database setup first.")
            self.show_database_setup_guide()
            return
        
        # Sidebar for user controls
        self.show_sidebar()
        
        # Main content tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Portfolio Overview", 
            "📤 Upload Trade Confirmation", 
            "📈 Performance Analysis",
            "💹 Holdings Detail",
            "📋 Transaction History"
        ])
        
        with tab1:
            self.show_portfolio_overview()
        
        with tab2:
            self.show_upload_interface()
        
        with tab3:
            self.show_performance_analysis()
        
        with tab4:
            self.show_holdings_detail()
        
        with tab5:
            self.show_transaction_history()
    
    def show_sidebar(self):
        """Portfolio tracker sidebar"""
        st.sidebar.markdown("### 💼 Portfolio Controls")
        
        # User selection
        user_id = st.sidebar.selectbox(
            "👤 Portfolio Owner:",
            ["default_user"],
            help="Select portfolio owner (multi-user support available)"
        )
        st.session_state.portfolio_user = user_id
        
        # Quick stats
        try:
            portfolio_stats = self.db.get_portfolio_stats(user_id)
            if portfolio_stats:
                st.sidebar.markdown("#### 📊 Quick Stats")
                st.sidebar.metric("💰 Total Invested", f"Rp{portfolio_stats.get('total_invested', 0):,.0f}")
                st.sidebar.metric("📈 Current Value", f"Rp{portfolio_stats.get('current_value', 0):,.0f}")
                st.sidebar.metric("💹 Total P&L", 
                                f"Rp{portfolio_stats.get('total_pnl', 0):,.0f}", 
                                f"{portfolio_stats.get('total_pnl_pct', 0):.2f}%")
                st.sidebar.metric("🏢 Total Stocks", f"{portfolio_stats.get('total_stocks', 0)}")
        except Exception as e:
            st.sidebar.warning("⚠️ Could not load portfolio stats")
        
        # Portfolio actions
        st.sidebar.markdown("---")
        st.sidebar.markdown("#### ⚡ Quick Actions")
        
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("🔄 Refresh", help="Update current stock prices", use_container_width=True):
                with st.spinner("Updating prices..."):
                    if self.update_current_prices():
                        st.success("✅ Prices updated!")
                        st.rerun()
                    else:
                        st.error("❌ Price update failed")
        
        with col2:
            if st.button("📊 Recalc", help="Recalculate portfolio metrics", use_container_width=True):
                with st.spinner("Recalculating..."):
                    if self.calculator.recalculate_portfolio(user_id):
                        st.success("✅ Portfolio recalculated!")
                        st.rerun()
                    else:
                        st.error("❌ Recalculation failed")
        
        # Export options
        st.sidebar.markdown("---")
        st.sidebar.markdown("#### 📤 Export Options")
        
        if st.sidebar.button("📥 Export Portfolio", use_container_width=True):
            self.export_portfolio_data()
    
    def show_portfolio_overview(self):
        """Portfolio overview dashboard"""
        user_id = st.session_state.get('portfolio_user', 'default_user')
        
        # Get portfolio data
        holdings = self.db.get_current_holdings(user_id)
        summary = self.db.get_portfolio_summary(user_id)
        
        if holdings.empty:
            st.info("📭 No holdings found. Upload your first trade confirmation to get started!")
            
            # Show getting started guide
            with st.expander("🚀 Getting Started Guide", expanded=True):
                st.markdown("""
                ### 📋 How to Start Tracking Your Portfolio:
                
                **1. Upload Trade Confirmation PDF:**
                - Go to "📤 Upload Trade Confirmation" tab
                - Upload your Stockbit trade confirmation PDF
                - System will automatically parse transactions
                
                **2. Manual Entry (Alternative):**
                - Use manual transaction entry for other brokers
                - Enter stock symbol, quantity, price, and type (BUY/SELL)
                
                **3. Automatic Tracking:**
                - Portfolio updates automatically with each transaction
                - P&L calculated in real-time
                - Performance analytics generated
                
                **4. Features Available:**
                - ✅ Real-time P&L tracking
                - ✅ Portfolio allocation charts  
                - ✅ Performance analysis
                - ✅ Transaction history
                - ✅ Data export capabilities
                """)
            return
        
        # Portfolio summary metrics
        latest_summary = summary.iloc[-1] if not summary.empty else None
        
        if latest_summary is not None:
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric(
                    "💰 Total Invested",
                    f"Rp{latest_summary['total_invested']:,.0f}",
                    help="Total amount invested (cost basis)"
                )
            
            with col2:
                st.metric(
                    "📈 Current Value", 
                    f"Rp{latest_summary['current_value']:,.0f}",
                    help="Current market value of all holdings"
                )
            
            with col3:
                pnl_color = "🟢" if latest_summary['total_pnl'] >= 0 else "🔴"
                st.metric(
                    "💹 Total P&L",
                    f"Rp{latest_summary['total_pnl']:,.0f}",
                    f"{pnl_color} {latest_summary['total_pnl_pct']:.2f}%"
                )
            
            with col4:
                st.metric(
                    "📊 Realized P&L",
                    f"Rp{latest_summary['realized_pnl']:,.0f}",
                    help="Profit/Loss from completed trades"
                )
            
            with col5:
                st.metric(
                    "🏢 Holdings",
                    f"{latest_summary['total_stocks']} stocks",
                    help="Number of different stocks in portfolio"
                )
        
        st.markdown("---")
        
        # Portfolio charts
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("### 🥧 Portfolio Allocation")
            
            if not holdings.empty:
                # Create pie chart
                fig_pie = px.pie(
                    holdings, 
                    values='current_value', 
                    names='symbol',
                    title="Portfolio Allocation by Market Value",
                    hover_data={'company_name': True, 'total_quantity': True},
                    labels={'current_value': 'Market Value (Rp)'}
                )
                fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                fig_pie.update_layout(height=500)
                st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            st.markdown("### 🏆 Top Holdings")
            
            # Show top 5 holdings by value
            top_holdings = holdings.nlargest(5, 'current_value')
            
            for i, (_, holding) in enumerate(top_holdings.iterrows()):
                pnl_color = "🟢" if holding['unrealized_pnl'] >= 0 else "🔴"
                pnl_pct = holding['unrealized_pnl_pct'] if pd.notna(holding['unrealized_pnl_pct']) else 0
                
                # Create styled metric card
                st.markdown(f"""
                <div style="
                    background-color: {'#e8f5e8' if holding['unrealized_pnl'] >= 0 else '#ffe6e6'};
                    border: 1px solid {'#4caf50' if holding['unrealized_pnl'] >= 0 else '#f44336'};
                    border-radius: 8px;
                    padding: 10px;
                    margin: 5px 0;
                ">
                    <strong>#{i+1} {holding['symbol']}</strong> {pnl_color}<br>
                    <small>{holding['company_name'][:30]}{'...' if len(holding['company_name']) > 30 else ''}</small><br>
                    💰 Value: Rp{holding['current_value']:,.0f}<br>
                    📊 P&L: {pnl_pct:.2f}%<br>
                    📦 Qty: {holding['total_quantity']:,} shares
                </div>
                """, unsafe_allow_html=True)
        
        # Performance trend chart
        st.markdown("---")
        st.markdown("### 📈 Portfolio Performance Trend")
        
        if not summary.empty and len(summary) > 1:
            fig_trend = go.Figure()
            
            # Portfolio value line
            fig_trend.add_trace(go.Scatter(
                x=summary['summary_date'],
                y=summary['current_value'],
                mode='lines+markers',
                name='Portfolio Value',
                line=dict(color='#1f77b4', width=3),
                hovertemplate='Date: %{x}<br>Value: Rp%{y:,.0f}<extra></extra>'
            ))
            
            # Cost basis line
            fig_trend.add_trace(go.Scatter(
                x=summary['summary_date'],
                y=summary['total_invested'],
                mode='lines',
                name='Total Invested',
                line=dict(color='gray', width=2, dash='dash'),
                hovertemplate='Date: %{x}<br>Invested: Rp%{y:,.0f}<extra></extra>'
            ))
            
            # Add P&L area
            pnl_values = summary['current_value'] - summary['total_invested']
            colors = ['rgba(76, 175, 80, 0.3)' if pnl >= 0 else 'rgba(244, 67, 54, 0.3)' for pnl in pnl_values]
            
            fig_trend.add_trace(go.Scatter(
                x=summary['summary_date'],
                y=summary['total_invested'],
                fill=None,
                mode='lines',
                line_color='rgba(0,0,0,0)',
                showlegend=False
            ))
            
            fig_trend.add_trace(go.Scatter(
                x=summary['summary_date'],
                y=summary['current_value'],
                fill='tonexty',
                mode='lines',
                line_color='rgba(0,0,0,0)',
                name='P&L Area',
                fillcolor='rgba(76, 175, 80, 0.3)' if summary['total_pnl'].iloc[-1] >= 0 else 'rgba(244, 67, 54, 0.3)'
            ))
            
            fig_trend.update_layout(
                title="Portfolio Value vs Investment Over Time",
                xaxis_title="Date",
                yaxis_title="Value (Rp)",
                height=400,
                hovermode='x unified'
            )
            
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("📊 Portfolio trend will appear after multiple trading sessions are recorded.")
    
    def show_upload_interface(self):
        """Trade confirmation upload interface"""
        st.markdown("### 📤 Upload Trade Confirmation")
        st.markdown("*Upload your Stockbit trade confirmation PDF to automatically update your portfolio*")
        
        # File upload section
        col1, col2 = st.columns([2, 1])
        
        with col1:
            uploaded_file = st.file_uploader(
                "Choose PDF file",
                type=['pdf'],
                help="Upload your trade confirmation PDF from Stockbit",
                accept_multiple_files=False
            )
        
        with col2:
            st.markdown("#### 📋 Supported Formats:")
            st.markdown("✅ Stockbit Trade Confirmation")
            st.markdown("✅ PDF format only")
            st.markdown("⏳ More brokers coming soon")
        
        if uploaded_file is not None:
            # Show file details
            st.success(f"✅ File uploaded: {uploaded_file.name}")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.info(f"📋 Size: {uploaded_file.size:,} bytes")
            with col2:
                st.info(f"📅 Type: {uploaded_file.type}")
            with col3:
                if st.button("🔍 Parse PDF", type="primary"):
                    self.process_uploaded_pdf(uploaded_file)
        
        # Sample data section
        st.markdown("---")
        st.markdown("### 📋 Sample Data for Testing")
        st.markdown("*Use the sample trade confirmation data from your existing document*")
        
        if st.button("📥 Load Sample Data"):
            self.load_sample_data()
        
        # Manual transaction entry
        st.markdown("---")
        st.markdown("### ✏️ Manual Transaction Entry")
        st.markdown("*Alternatively, you can manually enter transaction details*")
        
        with st.expander("📝 Add Manual Transaction", expanded=False):
            self.show_manual_entry_form()
    
    def process_uploaded_pdf(self, uploaded_file):
        """Process uploaded PDF file"""
        with st.spinner("🔄 Parsing trade confirmation..."):
            try:
                # Create uploads directory
                uploads_dir = os.path.join("portofolio_tracker", "uploads")
                os.makedirs(uploads_dir, exist_ok=True)
                
                # Save uploaded file temporarily
                file_path = os.path.join(uploads_dir, uploaded_file.name)
                
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                # Parse the PDF
                transactions = self.parser.parse_stockbit_pdf(file_path)
                
                if transactions:
                    st.success(f"✅ Found {len(transactions)} transactions!")
                    
                    # Show parsed transactions
                    st.markdown("#### 📋 Parsed Transactions:")
                    transactions_df = pd.DataFrame(transactions)
                    
                    # Format for display
                    display_df = transactions_df.copy()
                    display_df['Total Value'] = display_df['total_value'].apply(lambda x: f"Rp{x:,.0f}")
                    display_df['Price'] = display_df['price'].apply(lambda x: f"Rp{x:,.0f}")
                    
                    st.dataframe(
                        display_df[['trade_date', 'symbol', 'company_name', 'transaction_type', 
                                  'quantity', 'Price', 'Total Value']].rename(columns={
                            'trade_date': 'Date',
                            'symbol': 'Symbol', 
                            'company_name': 'Company',
                            'transaction_type': 'Type',
                            'quantity': 'Quantity'
                        }), 
                        use_container_width=True, 
                        hide_index=True
                    )
                    
                    # Import confirmation
                    col1, col2 = st.columns([1, 1])
                    with col1:
                        if st.button("💾 Import All Transactions", type="primary", use_container_width=True):
                            self.import_transactions(transactions)
                    
                    with col2:
                        if st.button("❌ Cancel", use_container_width=True):
                            st.info("Import cancelled.")
                            # Clean up temp file
                            try:
                                os.remove(file_path)
                            except:
                                pass
                else:
                    st.error("❌ No transactions found in PDF. Please check the file format.")
                    st.markdown("""
                    #### 🔧 Troubleshooting:
                    - Ensure the PDF is a Stockbit trade confirmation
                    - Check that the PDF is not corrupted
                    - Try using manual entry for other broker formats
                    """)
            
            except Exception as e:
                st.error(f"❌ Error parsing PDF: {str(e)}")
                st.markdown("💡 Try using manual transaction entry as an alternative.")
    
    def import_transactions(self, transactions):
        """Import parsed transactions to database"""
        user_id = st.session_state.get('portfolio_user', 'default_user')
        
        success_count = 0
        error_count = 0
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, transaction in enumerate(transactions):
            status_text.text(f"Importing transaction {i+1}/{len(transactions)}: {transaction['symbol']}")
            
            if self.db.add_transaction(user_id, transaction):
                success_count += 1
            else:
                error_count += 1
            
            progress_bar.progress((i + 1) / len(transactions))
        
        # Update portfolio calculations
        if success_count > 0:
            status_text.text("Recalculating portfolio...")
            self.calculator.recalculate_portfolio(user_id)
        
        # Show results
        if success_count > 0:
            st.success(f"✅ Successfully imported {success_count} transactions!")
            if error_count > 0:
                st.warning(f"⚠️ {error_count} transactions were skipped (likely duplicates)")
            
            st.balloons()
            
            # Auto-refresh portfolio
            st.rerun()
        else:
            st.error("❌ No transactions were imported. They may already exist in your portfolio.")
    
    def load_sample_data(self):
        """Load sample data from the uploaded trade confirmation"""
        sample_transactions = [
            {
                'trade_date': datetime(2025, 6, 13).date(),
                'symbol': 'AGRS',
                'company_name': 'Bank IBK Indonesia Tbk.',
                'transaction_type': 'BUY',
                'quantity': 46600,
                'price': 67.00,
                'total_value': 3122200.00,
                'fees': 0,
                'ref_number': '384417'
            },
            {
                'trade_date': datetime(2025, 6, 13).date(),
                'symbol': 'SMKL',
                'company_name': 'Satyamitra Kemas Lestari Tbk.',
                'transaction_type': 'BUY',
                'quantity': 4800,
                'price': 153.00,
                'total_value': 734400.00,
                'fees': 0,
                'ref_number': '384429'
            },
            {
                'trade_date': datetime(2025, 6, 13).date(),
                'symbol': 'SMKL',
                'company_name': 'Satyamitra Kemas Lestari Tbk.',
                'transaction_type': 'SELL',
                'quantity': 4800,
                'price': 155.00,
                'total_value': 744000.00,
                'fees': 0,
                'ref_number': '510514'
            }
        ]
        
        st.info("📥 Loading sample data...")
        self.import_transactions(sample_transactions)
    
    def show_manual_entry_form(self):
        """Show manual transaction entry form"""
        with st.form("manual_transaction"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                trade_date = st.date_input("Trade Date", value=datetime.now().date())
                transaction_type = st.selectbox("Type", ["BUY", "SELL"])
                symbol = st.text_input("Stock Symbol", placeholder="e.g., BBCA").upper()
            
            with col2:
                company_name = st.text_input("Company Name", placeholder="e.g., Bank Central Asia Tbk.")
                quantity = st.number_input("Quantity", min_value=1, value=100, step=100)
                price = st.number_input("Price per Share", min_value=0.0, value=1000.0, step=50.0)
            
            with col3:
                fees = st.number_input("Total Fees", min_value=0.0, value=0.0, step=1000.0)
                ref_number = st.text_input("Reference Number", placeholder="Optional")
                st.write("")  # Spacing
                submitted = st.form_submit_button("💾 Add Transaction", type="primary", use_container_width=True)
            
            if submitted:
                if symbol and company_name and quantity > 0 and price > 0:
                    user_id = st.session_state.get('portfolio_user', 'default_user')
                    
                    transaction = self.parser.parse_manual_input({
                        'trade_date': trade_date,
                        'symbol': symbol,
                        'company_name': company_name,
                        'transaction_type': transaction_type,
                        'quantity': quantity,
                        'price': price,
                        'fees': fees,
                        'ref_number': ref_number
                    })
                    
                    if transaction and self.db.add_transaction(user_id, transaction):
                        st.success("✅ Transaction added successfully!")
                        self.calculator.recalculate_portfolio(user_id)
                        st.rerun()
                    else:
                        st.error("❌ Failed to add transaction")
                else:
                    st.error("❌ Please fill in all required fields")
    
    def show_performance_analysis(self):
        """Performance analysis dashboard"""
        user_id = st.session_state.get('portfolio_user', 'default_user')
        
        st.markdown("### 📈 Performance Analysis")
        
        holdings = self.db.get_current_holdings(user_id)
        summary_history = self.db.get_portfolio_summary(user_id)
        
        if holdings.empty:
            st.info("📭 No performance data available. Add some transactions first!")
            return
        
        # Calculate comprehensive metrics
        try:
            metrics = self.calculator.calculate_portfolio_metrics(user_id)
            
            # Performance metrics row
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                roi = metrics.get('total_pnl_pct', 0)
                st.metric("📊 Total ROI", f"{roi:.2f}%", "return on investment")
            
            with col2:
                win_rate = metrics.get('win_rate', 0)
                st.metric("🎯 Win Rate", f"{win_rate:.1f}%", f"{metrics.get('winners', 0)}/{metrics.get('total_stocks', 0)} stocks")
            
            with col3:
                if 'daily_change_pct' in metrics:
                    daily_change = metrics['daily_change_pct']
                    st.metric("📅 Daily Change", f"{daily_change:+.2f}%", "from yesterday")
                else:
                    st.metric("📅 Daily Change", "N/A", "need more data")
            
            with col4:
                max_dd = metrics.get('max_drawdown', 0)
                st.metric("📉 Max Drawdown", f"{max_dd:.2f}%", "worst decline")
            
            # Best/worst performers
            col1, col2 = st.columns(2)
            
            with col1:
                if 'best_performer' in metrics:
                    best = metrics['best_performer']
                    st.success(f"🏆 **Best Performer:** {best['symbol']} (+{best['pnl_pct']:.2f}%)")
            
            with col2:
                if 'worst_performer' in metrics:
                    worst = metrics['worst_performer']
                    st.error(f"📉 **Worst Performer:** {worst['symbol']} ({worst['pnl_pct']:.2f}%)")
            
        except Exception as e:
            st.warning(f"Could not calculate advanced metrics: {e}")
        
        # Performance chart
        st.markdown("---")
        st.markdown("### 🏆 Individual Stock Performance")
        
        if not holdings.empty:
            # Sort by P&L percentage for chart
            holdings_sorted = holdings.sort_values('unrealized_pnl_pct', ascending=True)
            
            # Create horizontal bar chart
            fig_performance = go.Figure()
            
            colors = ['#ff4444' if x < 0 else '#44ff44' for x in holdings_sorted['unrealized_pnl_pct']]
            
            fig_performance.add_trace(go.Bar(
                x=holdings_sorted['unrealized_pnl_pct'],
                y=holdings_sorted['symbol'],
                orientation='h',
                marker_color=colors,
                text=[f"Rp{x:,.0f}" for x in holdings_sorted['unrealized_pnl']],
                textposition='auto',
                hovertemplate='<b>%{y}</b><br>P&L: %{x:.2f}%<br>Amount: %{text}<br><extra></extra>'
            ))
            
            fig_performance.update_layout(
                title="Stock Performance (P&L Percentage)",
                xaxis_title="P&L Percentage (%)",
                yaxis_title="Stock Symbol",
                height=max(400, len(holdings) * 30),
                showlegend=False
            )
            
            st.plotly_chart(fig_performance, use_container_width=True)
    
    def show_holdings_detail(self):
        """Detailed holdings view"""
        user_id = st.session_state.get('portfolio_user', 'default_user')
        
        st.markdown("### 💹 Holdings Detail")
        
        holdings = self.db.get_current_holdings(user_id)
        
        if holdings.empty:
            st.info("📭 No holdings found.")
            return
        
        # Holdings summary
        col1, col2, col3 = st.columns(3)
        
        total_value = holdings['current_value'].sum()
        total_cost = holdings['total_cost'].sum()
        total_pnl = holdings['unrealized_pnl'].sum()
        
        with col1:
            st.metric("💰 Total Portfolio Value", f"Rp{total_value:,.0f}")
        
        with col2:
            st.metric("💸 Total Cost Basis", f"Rp{total_cost:,.0f}")
        
        with col3:
            total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0
            st.metric("📈 Total Unrealized P&L", f"Rp{total_pnl:,.0f}", f"{total_pnl_pct:.2f}%")
        
        # Individual holdings
        st.markdown("---")
        st.markdown("### 🏢 Individual Holdings")
        
        # Create detailed holdings table
        display_holdings = holdings.copy()
        display_holdings['Weight %'] = (display_holdings['current_value'] / total_value * 100).round(2)
        display_holdings['Market Value'] = display_holdings['current_value'].apply(lambda x: f"Rp{x:,.0f}")
        display_holdings['Cost Basis'] = display_holdings['total_cost'].apply(lambda x: f"Rp{x:,.0f}")
        display_holdings['P&L Amount'] = display_holdings['unrealized_pnl'].apply(lambda x: f"Rp{x:,.0f}")
        display_holdings['P&L %'] = display_holdings['unrealized_pnl_pct'].apply(lambda x: f"{x:.2f}%")
        display_holdings['Current Price'] = display_holdings['current_price'].apply(
            lambda x: f"Rp{x:,.0f}" if pd.notna(x) else "N/A"
        )
        display_holdings['Avg Price'] = display_holdings['average_price'].apply(lambda x: f"Rp{x:,.0f}")
        
        # Select and rename columns for display
        table_columns = ['symbol', 'company_name', 'total_quantity', 'Avg Price', 'Current Price', 
                        'Cost Basis', 'Market Value', 'P&L Amount', 'P&L %', 'Weight %']
        
        column_config = {
            'symbol': st.column_config.TextColumn('Symbol', width="small"),
            'company_name': st.column_config.TextColumn('Company', width="large"),
            'total_quantity': st.column_config.NumberColumn('Quantity', format="%d"),
            'Weight %': st.column_config.NumberColumn('Weight %', format="%.1f%%")
        }
        
        st.dataframe(
            display_holdings[table_columns].rename(columns={
                'symbol': 'Symbol',
                'company_name': 'Company',
                'total_quantity': 'Quantity'
            }),
            use_container_width=True,
            hide_index=True,
            column_config=column_config
        )
        
        # Expandable detail for each holding
        st.markdown("---")
        st.markdown("### 📊 Detailed Analysis by Stock")
        
        for _, holding in holdings.iterrows():
            with st.expander(f"📈 {holding['symbol']} - {holding['company_name']}", expanded=False):
                self.show_individual_holding_detail(user_id, holding)
    
    def show_individual_holding_detail(self, user_id, holding):
        """Show detailed analysis for individual holding"""
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📦 Quantity", f"{holding['total_quantity']:,}")
            st.metric("💵 Avg Price", f"Rp{holding['average_price']:,.2f}")
        
        with col2:
            current_price = holding['current_price'] if pd.notna(holding['current_price']) else 0
            st.metric("💰 Current Price", f"Rp{current_price:,.2f}")
            st.metric("💸 Total Cost", f"Rp{holding['total_cost']:,.0f}")
        
        with col3:
            st.metric("📈 Market Value", f"Rp{holding['current_value']:,.0f}")
            unrealized_pnl = holding['unrealized_pnl'] if pd.notna(holding['unrealized_pnl']) else 0
            st.metric("💹 Unrealized P&L", f"Rp{unrealized_pnl:,.0f}")
        
        with col4:
            pnl_pct = holding['unrealized_pnl_pct'] if pd.notna(holding['unrealized_pnl_pct']) else 0
            st.metric("📊 P&L %", f"{pnl_pct:.2f}%")
            
            # Portfolio weight
            total_value = self.db.get_current_holdings(user_id)['current_value'].sum()
            weight = (holding['current_value'] / total_value * 100) if total_value > 0 else 0
            st.metric("⚖️ Portfolio Weight", f"{weight:.1f}%")
        
        # Transaction history for this stock
        transactions = self.db.get_transactions_for_symbol(user_id, holding['symbol'])
        if not transactions.empty:
            st.markdown("**📋 Transaction History:**")
            
            display_transactions = transactions.copy()
            display_transactions['Total Value'] = display_transactions['total_value'].apply(lambda x: f"Rp{x:,.0f}")
            display_transactions['Price'] = display_transactions['price'].apply(lambda x: f"Rp{x:,.0f}")
            
            st.dataframe(
                display_transactions[['trade_date', 'transaction_type', 'quantity', 'Price', 'Total Value']].rename(columns={
                    'trade_date': 'Date',
                    'transaction_type': 'Type',
                    'quantity': 'Qty'
                }),
                use_container_width=True,
                hide_index=True
            )
    
    def show_transaction_history(self):
        """Transaction history view"""
        user_id = st.session_state.get('portfolio_user', 'default_user')
        
        st.markdown("### 📋 Transaction History")
        
        transactions = self.db.get_all_transactions(user_id)
        
        if transactions.empty:
            st.info("📭 No transactions found.")
            return
        
        # Transaction summary
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_transactions = len(transactions)
            st.metric("📝 Total Transactions", total_transactions)
        
        with col2:
            buy_count = len(transactions[transactions['transaction_type'] == 'BUY'])
            st.metric("🟢 Buy Orders", buy_count)
        
        with col3:
            sell_count = len(transactions[transactions['transaction_type'] == 'SELL'])
            st.metric("🔴 Sell Orders", sell_count)
        
        with col4:
            unique_stocks = transactions['symbol'].nunique()
            st.metric("🏢 Unique Stocks", unique_stocks)
        
        # Filters
        st.markdown("---")
        st.markdown("#### 🔍 Filter Transactions")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            symbol_filter = st.selectbox(
                "Stock Symbol:",
                ["All"] + sorted(transactions['symbol'].unique().tolist())
            )
        
        with col2:
            type_filter = st.selectbox(
                "Transaction Type:",
                ["All", "BUY", "SELL"]
            )
        
        with col3:
            date_filter = st.date_input(
                "From Date:",
                value=transactions['trade_date'].min() if not transactions.empty else datetime.now().date()
            )
        
        # Apply filters
        filtered_transactions = transactions.copy()
        
        if symbol_filter != "All":
            filtered_transactions = filtered_transactions[filtered_transactions['symbol'] == symbol_filter]
        
        if type_filter != "All":
            filtered_transactions = filtered_transactions[filtered_transactions['transaction_type'] == type_filter]
        
        filtered_transactions = filtered_transactions[filtered_transactions['trade_date'] >= date_filter]
        
        # Show filtered results
        st.markdown("---")
        st.markdown(f"#### 📊 Filtered Results ({len(filtered_transactions)} transactions)")
        
        if not filtered_transactions.empty:
            # Format for display
            display_transactions = filtered_transactions.copy()
            display_transactions['Total Value'] = display_transactions['total_value'].apply(lambda x: f"Rp{x:,.0f}")
            display_transactions['Price'] = display_transactions['price'].apply(lambda x: f"Rp{x:,.0f}")
            display_transactions['Fees'] = display_transactions['fees'].apply(lambda x: f"Rp{x:,.0f}")
            
            # Sort by date (newest first)
            display_transactions = display_transactions.sort_values('trade_date', ascending=False)
            
            st.dataframe(
                display_transactions[['trade_date', 'symbol', 'company_name', 'transaction_type', 
                                   'quantity', 'Price', 'Total Value', 'Fees', 'ref_number']].rename(columns={
                    'trade_date': 'Date',
                    'symbol': 'Symbol',
                    'company_name': 'Company',
                    'transaction_type': 'Type',
                    'quantity': 'Quantity',
                    'ref_number': 'Ref #'
                }),
                use_container_width=True,
                hide_index=True
            )
            
            # Filter summary
            st.markdown("#### 📈 Filter Summary")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_buy_value = filtered_transactions[filtered_transactions['transaction_type'] == 'BUY']['total_value'].sum()
                st.metric("💰 Total Buy Value", f"Rp{total_buy_value:,.0f}")
            
            with col2:
                total_sell_value = filtered_transactions[filtered_transactions['transaction_type'] == 'SELL']['total_value'].sum()
                st.metric("💸 Total Sell Value", f"Rp{total_sell_value:,.0f}")
            
            with col3:
                total_fees = filtered_transactions['fees'].sum()
                st.metric("💳 Total Fees", f"Rp{total_fees:,.0f}")
        else:
            st.info("📭 No transactions match the selected filters.")
    
    def update_current_prices(self):
        """Update current stock prices from market data"""
        if not MAIN_DB_AVAILABLE:
            st.warning("⚠️ Market data connection not available")
            return False
        
        user_id = st.session_state.get('portfolio_user', 'default_user')
        holdings = self.db.get_current_holdings(user_id)
        
        if holdings.empty:
            return True
        
        try:
            # Get latest prices from market data
            symbols = holdings['symbol'].tolist()
            symbols_str = "', '".join(symbols)
            
            price_query = f"""
            SELECT symbol, close as current_price, date
            FROM daily_stock_summary
            WHERE symbol IN ('{symbols_str}')
            AND date = (SELECT MAX(date) FROM daily_stock_summary)
            """
            
            current_prices = fetch_data_cached(price_query, "Current Prices")
            
            if not current_prices.empty:
                # Update prices in database
                for _, price_row in current_prices.iterrows():
                    self.db.update_current_price(
                        user_id, 
                        price_row['symbol'], 
                        price_row['current_price']
                    )
                
                # Recalculate portfolio metrics
                self.calculator.recalculate_portfolio(user_id)
                return True
            else:
                st.warning("⚠️ No current price data available")
                return False
        
        except Exception as e:
            st.error(f"❌ Failed to update prices: {str(e)}")
            return False
    
    def export_portfolio_data(self):
        """Export portfolio data to Excel"""
        user_id = st.session_state.get('portfolio_user', 'default_user')
        
        try:
            # Get all data
            holdings = self.db.get_current_holdings(user_id)
            transactions = self.db.get_all_transactions(user_id)
            summary = self.db.get_portfolio_summary(user_id)
            
            # Create Excel file in memory
            output = io.BytesIO()
            
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                # Export holdings
                if not holdings.empty:
                    holdings.to_excel(writer, sheet_name='Current Holdings', index=False)
                
                # Export transactions
                if not transactions.empty:
                    transactions.to_excel(writer, sheet_name='Transaction History', index=False)
                
                # Export summary
                if not summary.empty:
                    summary.to_excel(writer, sheet_name='Portfolio Summary', index=False)
                
                # Create summary sheet
                summary_data = {
                    'Export Date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                    'User ID': [user_id],
                    'Total Holdings': [len(holdings)],
                    'Total Transactions': [len(transactions)],
                    'Portfolio Value': [holdings['current_value'].sum() if not holdings.empty else 0],
                    'Total Invested': [holdings['total_cost'].sum() if not holdings.empty else 0]
                }
                
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Export Info', index=False)
            
            output.seek(0)
            
            # Download button
            st.download_button(
                label="📥 Download Portfolio Export",
                data=output.getvalue(),
                file_name=f"portfolio_export_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
            st.success("✅ Portfolio data exported successfully!")
        
        except Exception as e:
            st.error(f"❌ Export failed: {str(e)}")
    
    def show_setup_instructions(self):
        """Show setup instructions when modules are not available"""
        st.error("❌ Portfolio Tracker modules not found")
        
        st.markdown("""
        ### 🔧 Setup Required
        
        The Portfolio Tracker requires additional setup. Please follow these steps:
        
        #### 1. Install Required Packages:
        ```bash
        pip install PyPDF2 openpyxl xlsxwriter
        ```
        
        #### 2. Create Required Files:
        - `portofolio_tracker/utils/database.py`
        - `portofolio_tracker/utils/pdf_parser.py`
        - `portofolio_tracker/utils/portfolio_calculator.py`
        - `portofolio_tracker/database/schema.sql`
        
        #### 3. Run Database Setup:
        Execute the schema.sql file in your PostgreSQL database to create portfolio tables.
        
        #### 4. Restart Application:
        Restart your Streamlit application after creating all files.
        """)
    
    def show_database_setup_guide(self):
        """Show database setup guide"""
        st.markdown("""
        ### 🗄️ Database Setup Required
        
        #### Run this SQL in your PostgreSQL database:
        ```sql
        -- Create portfolio schema
        CREATE SCHEMA IF NOT EXISTS portfolio;
        
        -- Create transactions table
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create holdings table  
        CREATE TABLE IF NOT EXISTS portfolio.holdings (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(50) DEFAULT 'default_user',
            symbol VARCHAR(10) NOT NULL,
            total_quantity INTEGER NOT NULL DEFAULT 0,
            average_price DECIMAL(15,2) NOT NULL DEFAULT 0,
            total_cost DECIMAL(20,2) NOT NULL DEFAULT 0,
            current_price DECIMAL(15,2),
            current_value DECIMAL(20,2),
            unrealized_pnl DECIMAL(20,2),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, symbol)
        );
        ```
        
        #### After running the SQL:
        1. Refresh this page
        2. The portfolio tracker will be ready to use
        """)

def show():
    """Main entry point for portfolio tracker page"""
    tracker = PortfolioTracker()
    tracker.show()