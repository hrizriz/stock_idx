"""
🚀 COMPLETE PORTFOLIO TRACKER INDONESIA
📊 Upload PDF Trade Confirmation → Auto Parse → Portfolio Dashboard & Analytics

Features:
- PDF Upload & Auto-parsing dari sekuritas Indonesia  
- Portfolio tracking dengan P&L calculation
- Real-time dashboard dengan metrics
- Advanced analytics & visualizations
- Export data to CSV
- Transaction history management

Author: Data Engineer Portfolio Tracker
Version: 2.0 Complete
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re
from datetime import datetime, date, timedelta
import io
import numpy as np
import hashlib

# Try to import PyPDF2
try:
    import PyPDF2
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

# Configure Streamlit page
st.set_page_config(
    page_title="Portfolio Tracker Indonesia", 
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    """Main Portfolio Tracker Application"""
    
    st.markdown("# 💼 Portfolio Tracker Indonesia")
    st.markdown("**📊 Upload PDF Trade Confirmation untuk tracking portfolio otomatis**")
    
    # Initialize session state
    initialize_session_state()
    
    # Sidebar navigation
    with st.sidebar:
        show_sidebar_info()
    
    # Main navigation tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Portfolio Dashboard", 
        "📤 Upload PDF Trade Confirmation",
        "📋 Transaction History",
        "📈 Analytics & Visualization",
        "⚙️ Settings & Tools"
    ])
    
    with tab1:
        show_portfolio_dashboard()
    
    with tab2:
        show_pdf_upload_interface()
    
    with tab3:
        show_transaction_history()
    
    with tab4:
        show_analytics_visualization()
        
    with tab5:
        show_settings_tools()

def initialize_session_state():
    """Initialize session state variables"""
    if 'portfolio_transactions' not in st.session_state:
        st.session_state.portfolio_transactions = []
    
    if 'portfolio_initialized' not in st.session_state:
        st.session_state.portfolio_initialized = True
        show_welcome_info()
    
    if 'mock_data_loaded' not in st.session_state:
        st.session_state.mock_data_loaded = False

def show_sidebar_info():
    """Sidebar with portfolio summary and quick actions"""
    st.markdown("### 🎯 Quick Stats")
    
    transactions = st.session_state.get('portfolio_transactions', [])
    
    if transactions:
        holdings_df, portfolio_metrics = calculate_portfolio_metrics(transactions)
        
        # Quick metrics
        st.metric("💰 Total Invested", f"Rp {portfolio_metrics['total_invested']:,.0f}")
        st.metric("📈 Total P&L", f"Rp {portfolio_metrics['total_pnl']:,.0f}")
        st.metric("🏢 Holdings", f"{portfolio_metrics['total_stocks']} stocks")
        
        # Quick actions
        st.markdown("---")
        st.markdown("### ⚡ Quick Actions")
        
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.rerun()
            
        if st.button("📥 Export Portfolio", use_container_width=True):
            portfolio_csv = create_portfolio_export(transactions, holdings_df)
            st.download_button(
                label="💾 Download CSV",
                data=portfolio_csv,
                file_name=f"portfolio_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                use_container_width=True
            )
    else:
        st.info("Upload PDF untuk melihat stats")
        
        # Demo data option
        st.markdown("---")
        st.markdown("### 🎮 Demo Mode")
        if st.button("📊 Load Demo Data", use_container_width=True):
            load_demo_data()
            st.success("✅ Demo data loaded!")
            st.rerun()

def show_welcome_info():
    """Welcome information for new users"""
    st.info("""
    🎉 **Selamat datang di Portfolio Tracker Indonesia!**
    
    **Cara menggunakan:**
    1. 📤 Upload PDF Trade Confirmation dari sekuritas Anda (Stockbit, dll)
    2. 🔄 Sistem akan auto-parse semua transaksi BELI/JUAL
    3. 📊 Lihat dashboard portfolio dengan P&L otomatis
    4. 📈 Monitor performance dengan visualisasi lengkap
    
    **💡 Tips:** Gunakan demo data untuk test drive fitur!
    """)

def show_portfolio_dashboard():
    """Main portfolio dashboard with metrics and holdings"""
    st.markdown("### 📊 Portfolio Dashboard")
    
    transactions = st.session_state.get('portfolio_transactions', [])
    
    if not transactions:
        show_empty_portfolio_state()
        return
    
    # Calculate portfolio metrics
    holdings_df, portfolio_metrics = calculate_portfolio_metrics(transactions)
    
    # Display main metrics
    show_portfolio_metrics(portfolio_metrics)
    
    # Display holdings table
    if not holdings_df.empty:
        show_holdings_table(holdings_df)
    
    # Portfolio performance chart
    show_portfolio_performance_chart(holdings_df)
    
    # Quick actions
    show_dashboard_quick_actions(transactions, holdings_df)

def show_empty_portfolio_state():
    """Show empty state with call-to-action"""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 2rem;">
            <h3>📭 Portfolio masih kosong</h3>
            <p>Mulai tracking portfolio Anda sekarang!</p>
        </div>
        """, unsafe_allow_html=True)
        
        col_a, col_b = st.columns(2)
        
        with col_a:
            if st.button("📤 Upload PDF", type="primary", use_container_width=True):
                st.switch_page("Upload PDF Trade Confirmation")
        
        with col_b:
            if st.button("🎮 Load Demo", use_container_width=True):
                load_demo_data()
                st.success("✅ Demo data loaded!")
                st.rerun()
        
        # Benefits showcase
        st.markdown("---")
        st.markdown("""
        **🚀 Fitur yang akan Anda dapatkan:**
        
        📊 **Real-time Portfolio Tracking**
        • Total invested vs current value
        • Unrealized P&L per saham & portfolio  
        • Performance metrics & win rate
        
        📈 **Advanced Analytics**
        • Portfolio composition & allocation
        • Top/worst performers analysis
        • Transaction timeline & volume
        
        📱 **User-Friendly Interface**
        • Auto-parse PDF trade confirmation
        • Interactive charts & visualizations
        • Export data untuk analysis lebih lanjut
        """)

def show_portfolio_metrics(metrics):
    """Display portfolio key metrics with enhanced styling"""
    st.markdown("#### 💎 Portfolio Overview")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "💰 Total Invested",
            f"Rp {metrics['total_invested']:,.0f}",
            help="Total modal yang diinvestasikan"
        )
    
    with col2:
        st.metric(
            "💎 Current Value", 
            f"Rp {metrics['current_value']:,.0f}",
            help="Nilai portfolio saat ini (simulasi)"
        )
    
    with col3:
        pnl = metrics['total_pnl']
        pnl_pct = metrics['total_pnl_pct']
        delta_color = "normal" if pnl >= 0 else "inverse"
        
        st.metric(
            "📈 Total P&L",
            f"Rp {pnl:,.0f}",
            f"{pnl_pct:+.2f}%",
            delta_color=delta_color,
            help="Total profit/loss (unrealized)"
        )
    
    with col4:
        st.metric(
            "🏢 Holdings",
            f"{metrics['total_stocks']} stocks",
            help="Jumlah saham yang dimiliki"
        )
    
    with col5:
        win_rate = metrics['win_rate']
        winners = metrics['winners']
        total = metrics['total_stocks']
        
        st.metric(
            "🎯 Win Rate",
            f"{win_rate:.1f}%",
            f"{winners}/{total}",
            help="Persentase saham yang profit"
        )

def show_holdings_table(holdings_df):
    """Display current holdings table with enhanced formatting"""
    st.markdown("---")
    st.markdown("#### 🏪 Current Holdings")
    
    # Format holdings for display
    display_df = holdings_df.copy()
    
    # Calculate portfolio weights
    total_value = display_df['current_value'].sum()
    display_df['weight'] = (display_df['current_value'] / total_value * 100) if total_value > 0 else 0
    
    # Format columns
    display_df['Avg Price'] = display_df['avg_price'].apply(lambda x: f"Rp {x:,.0f}")
    display_df['Current Price'] = display_df['current_price'].apply(lambda x: f"Rp {x:,.0f}")
    display_df['Total Cost'] = display_df['total_cost'].apply(lambda x: f"Rp {x:,.0f}")
    display_df['Market Value'] = display_df['current_value'].apply(lambda x: f"Rp {x:,.0f}")
    
    # Format P&L with colors and emojis
    display_df['P&L Amount'] = display_df.apply(lambda row: 
        f"🟢 Rp {row['unrealized_pnl']:,.0f}" if row['unrealized_pnl'] > 0 
        else f"🔴 Rp {row['unrealized_pnl']:,.0f}" if row['unrealized_pnl'] < 0 
        else "⚪ Rp 0", axis=1)
    
    display_df['P&L %'] = display_df.apply(lambda row: 
        f"🟢 {row['unrealized_pnl_pct']:+.2f}%" if row['unrealized_pnl'] > 0 
        else f"🔴 {row['unrealized_pnl_pct']:+.2f}%" if row['unrealized_pnl'] < 0 
        else "⚪ 0.00%", axis=1)
    
    display_df['Weight'] = display_df['weight'].apply(lambda x: f"{x:.1f}%")
    
    # Select columns for display
    columns_to_show = [
        'symbol', 'company_name', 'quantity', 'Avg Price', 'Current Price', 
        'Total Cost', 'Market Value', 'P&L Amount', 'P&L %', 'Weight'
    ]
    
    column_config = {
        'symbol': st.column_config.TextColumn('Symbol', width="small"),
        'company_name': st.column_config.TextColumn('Company', width="large"),
        'quantity': st.column_config.NumberColumn('Qty', format="%d")
    }
    
    st.dataframe(
        display_df[columns_to_show].rename(columns={'symbol': 'Symbol', 'company_name': 'Company', 'quantity': 'Qty'}),
        use_container_width=True,
        hide_index=True,
        column_config=column_config
    )

def show_portfolio_performance_chart(holdings_df):
    """Show portfolio performance visualization"""
    if holdings_df.empty:
        return
        
    st.markdown("---")
    st.markdown("#### 📊 Performance Overview")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # P&L waterfall chart
        fig_waterfall = go.Figure(go.Waterfall(
            name="Portfolio P&L",
            orientation="v",
            measure=["absolute"] + ["relative"] * len(holdings_df),
            x=["Starting Value"] + holdings_df['symbol'].tolist(),
            textposition="outside",
            text=[f"Rp {holdings_df['total_cost'].sum():,.0f}"] + [f"Rp {pnl:,.0f}" for pnl in holdings_df['unrealized_pnl']],
            y=[holdings_df['total_cost'].sum()] + holdings_df['unrealized_pnl'].tolist(),
            connector={"line": {"color": "rgb(63, 63, 63)"}},
        ))
        fig_waterfall.update_layout(title="Portfolio P&L Breakdown", height=400)
        st.plotly_chart(fig_waterfall, use_container_width=True)
    
    with col2:
        # Portfolio allocation donut chart
        fig_donut = px.pie(
            holdings_df, 
            values='current_value', 
            names='symbol',
            title="Portfolio Allocation",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig_donut.update_traces(textposition='inside', textinfo='percent+label')
        fig_donut.update_layout(height=400)
        st.plotly_chart(fig_donut, use_container_width=True)

def show_dashboard_quick_actions(transactions, holdings_df):
    """Show quick action buttons on dashboard"""
    st.markdown("---")
    st.markdown("#### ⚡ Quick Actions")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🔄 Refresh Portfolio", help="Recalculate portfolio metrics", use_container_width=True):
            st.success("✅ Portfolio refreshed!")
            st.rerun()
    
    with col2:
        if st.button("📤 Upload More PDFs", help="Add more transactions", use_container_width=True):
            st.info("👆 Klik tab 'Upload PDF Trade Confirmation'")
    
    with col3:
        if transactions:
            portfolio_csv = create_portfolio_export(transactions, holdings_df)
            st.download_button(
                label="📥 Export CSV",
                data=portfolio_csv,
                file_name=f"portfolio_export_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                help="Download portfolio data",
                use_container_width=True
            )
    
    with col4:
        if st.button("📊 View Analytics", help="Go to analytics tab", use_container_width=True):
            st.info("👆 Klik tab 'Analytics & Visualization'")

def show_pdf_upload_interface():
    """PDF upload and parsing interface"""
    st.markdown("### 📤 Upload PDF Trade Confirmation")
    st.markdown("*Upload file PDF trade confirmation dari sekuritas untuk auto-parsing transaksi*")
    
    if not PDF_AVAILABLE:
        show_pdf_library_warning()
        return
    
    # Upload instructions
    with st.expander("📖 Panduan Upload PDF", expanded=False):
        show_upload_guide()
    
    # File uploader
    uploaded_file = st.file_uploader(
        "📄 Pilih file PDF Trade Confirmation",
        type=['pdf'],
        help="Upload PDF trade confirmation dari Stockbit atau sekuritas lainnya"
    )
    
    if uploaded_file is not None:
        handle_pdf_upload(uploaded_file)
    else:
        show_upload_demo_options()

def show_pdf_library_warning():
    """Show warning when PyPDF2 is not available"""
    st.error("""
    ❌ **PyPDF2 Library tidak tersedia**
    
    Untuk menggunakan fitur upload PDF, install terlebih dahulu:
    ```bash
    pip install PyPDF2
    ```
    
    **Alternative:** Gunakan demo data untuk test drive aplikasi!
    """)
    
    if st.button("🎮 Load Demo Data", type="primary"):
        load_demo_data()
        st.success("✅ Demo data loaded! Check tab 'Portfolio Dashboard'")
        st.rerun()

def handle_pdf_upload(uploaded_file):
    """Handle PDF file upload and processing"""
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.success(f"✅ **File uploaded:** {uploaded_file.name}")
        
        # File info
        file_size = len(uploaded_file.getvalue()) / 1024
        st.info(f"📊 **File size:** {file_size:.1f} KB")
        
        # File preview
        if st.checkbox("👁️ Preview PDF content"):
            show_pdf_preview(uploaded_file)
    
    with col2:
        if st.button("🔄 Parse PDF", type="primary", use_container_width=True):
            parse_pdf_and_update_portfolio(uploaded_file)
        
        if st.button("📊 Use Demo Instead", use_container_width=True):
            load_demo_data()
            st.success("✅ Demo data loaded!")
            st.rerun()

def show_pdf_preview(uploaded_file):
    """Show preview of PDF content"""
    try:
        pdf_reader = PyPDF2.PdfReader(uploaded_file)
        
        st.markdown("**📄 PDF Preview:**")
        st.write(f"Pages: {len(pdf_reader.pages)}")
        
        # Show first page text preview
        if len(pdf_reader.pages) > 0:
            first_page = pdf_reader.pages[0]
            text_preview = first_page.extract_text()[:500]
            st.text_area("Text preview (first 500 chars):", text_preview, height=150)
        
        # Reset file pointer
        uploaded_file.seek(0)
        
    except Exception as e:
        st.error(f"Error previewing PDF: {str(e)}")

def show_upload_demo_options():
    """Show demo options when no file uploaded"""
    st.markdown("---")
    st.markdown("### 🎮 Demo Mode")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **🚀 Ingin test drive aplikasi?**
        
        Load demo data untuk melihat:
        • Portfolio dashboard lengkap
        • Analytics & visualizations  
        • Export functionality
        • Transaction history
        """)
    
    with col2:
        if st.button("📊 Load Demo Portfolio", type="primary", use_container_width=True):
            load_demo_data()
            st.success("✅ Demo data loaded! Check 'Portfolio Dashboard' tab")
            st.balloons()
            st.rerun()

def parse_pdf_and_update_portfolio(uploaded_file):
    """Parse PDF and update portfolio with extracted transactions"""
    with st.spinner("🔍 Parsing PDF trade confirmation..."):
        success, result = parse_stockbit_pdf(uploaded_file)
        
        if success:
            process_parsed_transactions(result)
        else:
            st.error(f"❌ **Error parsing PDF:** {result}")
            show_parsing_tips()

def process_parsed_transactions(new_transactions):
    """Process and add new transactions to portfolio"""
    existing_transactions = st.session_state.get('portfolio_transactions', [])
    
    # Check for duplicates and add new transactions
    added_count = 0
    skipped_count = 0
    
    for transaction in new_transactions:
        # Check if transaction already exists
        is_duplicate = any(
            existing['ref_number'] == transaction['ref_number'] and 
            existing['symbol'] == transaction['symbol'] and
            existing['trade_date'] == transaction['trade_date']
            for existing in existing_transactions
        )
        
        if not is_duplicate:
            existing_transactions.append(transaction)
            added_count += 1
        else:
            skipped_count += 1
    
    # Update session state
    st.session_state.portfolio_transactions = existing_transactions
    
    # Show results
    if added_count > 0:
        st.success(f"✅ **Berhasil import {added_count} transaksi baru!**")
        st.balloons()
        
        # Show summary
        show_import_summary(new_transactions)
        
        if skipped_count > 0:
            st.info(f"ℹ️ {skipped_count} transaksi duplicate (dilewati)")
        
        st.rerun()
    else:
        st.warning("⚠️ Semua transaksi sudah ada di portfolio (duplicate)")

def parse_stockbit_pdf(uploaded_file):
    """Parse Stockbit PDF and extract transactions"""
    try:
        # Read PDF
        pdf_reader = PyPDF2.PdfReader(uploaded_file)
        text = ""
        
        for page in pdf_reader.pages:
            text += page.extract_text()
        
        # Extract transaction date
        trade_date = extract_trade_date(text)
        settlement_date = extract_settlement_date(text)
        
        # Extract transactions
        transactions = extract_transactions_from_text(text, trade_date, settlement_date)
        
        if not transactions:
            return False, "Tidak ada transaksi yang berhasil di-parse dari PDF"
        
        return True, transactions
        
    except Exception as e:
        return False, f"Error reading PDF: {str(e)}"

def extract_trade_date(text):
    """Extract trade date from PDF text"""
    patterns = [
        r'Transaction Date\s+(\d{2}/\d{2}/\d{4})',
        r'Trade Date\s+(\d{2}/\d{2}/\d{4})',
        r'Tanggal Transaksi\s+(\d{2}/\d{2}/\d{4})'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                return datetime.strptime(match.group(1), '%d/%m/%Y').date()
            except ValueError:
                continue
    
    return date.today()

def extract_settlement_date(text):
    """Extract settlement date from PDF text"""
    patterns = [
        r'Settlement Date\s+(\d{2}/\d{2}/\d{4})',
        r'Tanggal Settlement\s+(\d{2}/\d{2}/\d{4})'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                return datetime.strptime(match.group(1), '%d/%m/%Y').date()
            except ValueError:
                continue
    
    return None

def extract_transactions_from_text(text, trade_date, settlement_date):
    """Extract individual transactions from PDF text"""
    transactions = []
    lines = text.split('\n')
    
    for line in lines:
        transaction = parse_transaction_line(line, trade_date, settlement_date)
        if transaction:
            transactions.append(transaction)
    
    return transactions

def parse_transaction_line(line, trade_date, settlement_date):
    """Parse individual transaction line with improved regex"""
    try:
        line = line.strip()
        if len(line) < 20:
            return None
        
        # Split and clean
        parts = [p.strip() for p in line.split() if p.strip()]
        
        if len(parts) < 8:
            return None
        
        # Check for reference number (6 digits)
        if not (parts[0].isdigit() and len(parts[0]) == 6):
            return None
        
        ref_number = parts[0]
        
        # Skip board type (RG, NG, etc.)
        start_idx = 2 if parts[1] in ['RG', 'NG', 'TN', 'UH'] else 1
        
        # Find stock symbol (4 letters)
        symbol = None
        symbol_idx = start_idx
        
        for i in range(start_idx, min(start_idx + 3, len(parts))):
            if len(parts[i]) == 4 and parts[i].isalpha():
                symbol = parts[i].upper()
                symbol_idx = i
                break
        
        if not symbol:
            return None
        
        # Extract company name
        company_parts = []
        name_start = symbol_idx + 1
        
        for i in range(name_start, len(parts)):
            part = parts[i]
            # Stop at first number
            if re.match(r'^\d+$', part) or re.match(r'^\d+,', part):
                break
            company_parts.append(part)
        
        company_name = ' '.join(company_parts).replace('Tbk.', 'Tbk').strip()
        
        # Extract numeric values (lot, quantity, price, buy_value, sell_value)
        numbers = []
        for part in parts:
            cleaned = part.replace(',', '')
            try:
                numbers.append(float(cleaned))
            except ValueError:
                continue
        
        if len(numbers) < 5:
            return None
        
        # Parse values
        lot = int(numbers[0])
        quantity = int(numbers[1])
        price = numbers[2]
        buy_value = numbers[3]
        sell_value = numbers[4]
        
        # Determine transaction type
        if buy_value > 0:
            transaction_type = 'BUY'
            total_value = buy_value
        elif sell_value > 0:
            transaction_type = 'SELL'
            total_value = sell_value
        else:
            return None
        
        # Validate
        if quantity <= 0 or price <= 0 or total_value <= 0:
            return None
        
        return {
            'trade_date': trade_date,
            'settlement_date': settlement_date,
            'ref_number': ref_number,
            'symbol': symbol,
            'company_name': company_name,
            'transaction_type': transaction_type,
            'lot': lot,
            'quantity': quantity,
            'price': price,
            'total_value': total_value,
            'source': 'PDF_UPLOAD',
            'created_at': datetime.now()
        }
        
    except Exception:
        return None

def show_import_summary(transactions):
    """Show summary of imported transactions"""
    if not transactions:
        return
        
    df = pd.DataFrame(transactions)
    
    st.markdown("#### 📊 Import Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        buy_count = len(df[df['transaction_type'] == 'BUY'])
        st.metric("🟢 Buy Orders", buy_count)
    
    with col2:
        sell_count = len(df[df['transaction_type'] == 'SELL'])
        st.metric("🔴 Sell Orders", sell_count)
    
    with col3:
        unique_stocks = df['symbol'].nunique()
        st.metric("🏢 Unique Stocks", unique_stocks)
    
    with col4:
        total_value = df['total_value'].sum()
        st.metric("💰 Total Value", f"Rp {total_value:,.0f}")
    
    # Show sample transactions
    st.markdown("**📋 Sample Transactions:**")
    sample_df = df.head(3)[['symbol', 'company_name', 'transaction_type', 'quantity', 'price', 'total_value']]
    st.dataframe(sample_df, use_container_width=True, hide_index=True)

def show_upload_guide():
    """Show comprehensive upload guide"""
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **📄 Format yang Didukung:**
        • PDF Trade Confirmation Stockbit
        • PDF yang bisa dibaca (bukan scan)
        • File tidak di-password protect
        • Format standar sekuritas Indonesia
        
        **📊 Data yang Akan Di-extract:**
        • REF # (Nomor referensi)
        • Simbol saham (AGRS, AHAP, dll)
        • Nama perusahaan lengkap
        • Jenis transaksi (BELI/JUAL)
        • Jumlah lot dan saham
        • Harga per saham
        • Total nilai transaksi
        """)
    
    with col2:
        st.markdown("""
        **🔍 Tips untuk Hasil Terbaik:**
        • Pastikan PDF memiliki text yang bisa dicopy
        • Format tabel transaksi yang jelas
        • Tidak ada password protection
        • File size tidak terlalu besar (< 10MB)
        
        **⚡ Jika Parsing Gagal:**
        • Coba PDF dari sumber berbeda
        • Pastikan format sesuai contoh
        • Gunakan demo data untuk testing
        • Check apakah ada data transaksi
        """)

def show_parsing_tips():
    """Show tips when parsing fails"""
    st.markdown("""
    ### 💡 Tips Jika Parsing Gagal:
    
    **📋 Pastikan PDF Anda:**
    • Format PDF yang bisa dibaca (bukan scan)
    • Mengandung tabel transaksi yang jelas  
    • Tidak di-password protect
    • Format standar dari sekuritas Indonesia
    
    **🔧 Alternative Solutions:**
    • Coba upload PDF yang lebih baru/clear
    • Gunakan demo data untuk test drive
    • Manual input data (fitur coming soon)
    • Export dari platform trading ke CSV
    """)

def show_transaction_history():
    """Show comprehensive transaction history with filters"""
    st.markdown("### 📋 Transaction History")
    
    transactions = st.session_state.get('portfolio_transactions', [])
    
    if not transactions:
        show_empty_transaction_state()
        return
    
    df = pd.DataFrame(transactions)
    
    # Summary metrics
    show_transaction_summary_metrics(df)
    
    # Filters and search
    filtered_df = show_transaction_filters(df)
    
    # Transaction table
    show_transaction_table(filtered_df, len(df))
    
    # Transaction management
    show_transaction_management()

def show_empty_transaction_state():
    """Show empty state for transactions"""
    st.info("📭 Belum ada transaksi. Upload PDF atau load demo data untuk memulai!")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📤 Upload PDF", type="primary", use_container_width=True):
            st.info("👆 Klik tab 'Upload PDF Trade Confirmation'")
    
    with col2:
        if st.button("🎮 Load Demo", use_container_width=True):
            load_demo_data()
            st.success("✅ Demo data loaded!")
            st.rerun()

def show_transaction_summary_metrics(df):
    """Show transaction summary metrics"""
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("📝 Total Transactions", len(df))
    
    with col2:
        buy_count = len(df[df['transaction_type'] == 'BUY'])
        st.metric("🟢 Buy Orders", buy_count)
    
    with col3:
        sell_count = len(df[df['transaction_type'] == 'SELL'])
        st.metric("🔴 Sell Orders", sell_count)
    
    with col4:
        unique_stocks = df['symbol'].nunique()
        st.metric("🏢 Unique Stocks", unique_stocks)
    
    with col5:
        total_value = df['total_value'].sum()
        st.metric("💰 Total Volume", f"Rp {total_value:,.0f}")

def show_transaction_filters(df):
    """Show transaction filters and return filtered dataframe"""
    st.markdown("---")
    st.markdown("#### 🔍 Filters & Search")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        filter_type = st.selectbox("Transaction Type:", ["ALL", "BUY", "SELL"])
    
    with col2:
        filter_symbol = st.selectbox("Stock Symbol:", ["ALL"] + sorted(df['symbol'].unique()))
    
    with col3:
        # Date range filter
        date_range = st.selectbox("Date Range:", ["ALL", "Last 7 days", "Last 30 days", "Last 90 days"])
    
    with col4:
        sort_by = st.selectbox("Sort by:", ["Date (Newest)", "Date (Oldest)", "Value (Highest)", "Symbol"])
    
    # Search box
    search_term = st.text_input("🔍 Search (REF#, Symbol, Company):", placeholder="Type to search...")
    
    # Apply filters
    filtered_df = df.copy()
    
    if filter_type != "ALL":
        filtered_df = filtered_df[filtered_df['transaction_type'] == filter_type]
    
    if filter_symbol != "ALL":
        filtered_df = filtered_df[filtered_df['symbol'] == filter_symbol]
    
    # Date range filter
    if date_range != "ALL":
        days_map = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90}
        cutoff_date = date.today() - timedelta(days=days_map[date_range])
        filtered_df = filtered_df[pd.to_datetime(filtered_df['trade_date']).dt.date >= cutoff_date]
    
    # Search filter
    if search_term:
        search_mask = (
            filtered_df['ref_number'].astype(str).str.contains(search_term, case=False) |
            filtered_df['symbol'].str.contains(search_term, case=False) |
            filtered_df['company_name'].str.contains(search_term, case=False)
        )
        filtered_df = filtered_df[search_mask]
    
    # Apply sorting
    if sort_by == "Date (Newest)":
        filtered_df = filtered_df.sort_values('trade_date', ascending=False)
    elif sort_by == "Date (Oldest)":
        filtered_df = filtered_df.sort_values('trade_date', ascending=True)
    elif sort_by == "Value (Highest)":
        filtered_df = filtered_df.sort_values('total_value', ascending=False)
    elif sort_by == "Symbol":
        filtered_df = filtered_df.sort_values('symbol')
    
    return filtered_df

def show_transaction_table(filtered_df, total_count):
    """Show transaction table with formatting"""
    if not filtered_df.empty:
        display_df = filtered_df.copy()
        
        # Format for display
        display_df['Type'] = display_df['transaction_type'].apply(
            lambda x: f"🟢 {x}" if x == 'BUY' else f"🔴 {x}"
        )
        display_df['Price'] = display_df['price'].apply(lambda x: f"Rp {x:,.0f}")
        display_df['Total Value'] = display_df['total_value'].apply(lambda x: f"Rp {x:,.0f}")
        display_df['Date'] = pd.to_datetime(display_df['trade_date']).dt.strftime('%d/%m/%Y')
        
        # Column configuration
        column_config = {
            'Date': st.column_config.TextColumn('Date', width="small"),
            'REF #': st.column_config.TextColumn('REF #', width="small"),
            'Symbol': st.column_config.TextColumn('Symbol', width="small"),
            'Company': st.column_config.TextColumn('Company', width="large"),
            'Type': st.column_config.TextColumn('Type', width="small"),
            'Quantity': st.column_config.NumberColumn('Quantity', format="%d"),
            'Price': st.column_config.TextColumn('Price', width="small"),
            'Total Value': st.column_config.TextColumn('Total Value', width="medium")
        }
        
        st.dataframe(
            display_df[['Date', 'ref_number', 'symbol', 'company_name', 'Type', 'quantity', 'Price', 'Total Value']].rename(columns={
                'ref_number': 'REF #',
                'symbol': 'Symbol', 
                'company_name': 'Company',
                'quantity': 'Quantity'
            }),
            use_container_width=True,
            hide_index=True,
            column_config=column_config
        )
        
        st.caption(f"Showing {len(filtered_df)} of {total_count} transactions")
    else:
        st.warning("⚠️ No transactions match the selected filters")

def show_transaction_management():
    """Show transaction management options"""
    st.markdown("---")
    st.markdown("#### ⚙️ Transaction Management")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📥 Export Transactions", use_container_width=True):
            transactions = st.session_state.get('portfolio_transactions', [])
            if transactions:
                df = pd.DataFrame(transactions)
                csv = df.to_csv(index=False)
                st.download_button(
                    label="💾 Download CSV",
                    data=csv,
                    file_name=f"transactions_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv"
                )
    
    with col2:
        if st.button("🔄 Reload Demo Data", use_container_width=True):
            load_demo_data()
            st.success("✅ Demo data reloaded!")
            st.rerun()
    
    with col3:
        # Clear data with confirmation
        if st.button("🗑️ Clear All Data", use_container_width=True):
            st.warning("⚠️ This will delete all transaction data!")
            if st.button("⚠️ Confirm Delete", type="secondary"):
                st.session_state.portfolio_transactions = []
                st.success("✅ All data cleared!")
                st.rerun()

def show_analytics_visualization():
    """Show comprehensive portfolio analytics and visualizations"""
    st.markdown("### 📈 Portfolio Analytics & Visualization")
    
    transactions = st.session_state.get('portfolio_transactions', [])
    
    if not transactions:
        show_empty_analytics_state()
        return
    
    holdings_df, portfolio_metrics = calculate_portfolio_metrics(transactions)
    
    if holdings_df.empty:
        st.info("📭 Tidak ada holdings untuk dianalisis.")
        return
    
    # Analytics overview
    show_analytics_overview(portfolio_metrics)
    
    # Portfolio composition
    show_portfolio_composition_charts(holdings_df)
    
    # Performance analysis
    show_performance_analysis(holdings_df)
    
    # Transaction analytics
    show_transaction_analytics(transactions)
    
    # Advanced metrics
    show_advanced_metrics(holdings_df, portfolio_metrics)

def show_empty_analytics_state():
    """Show empty state for analytics"""
    st.info("📭 Belum ada data untuk analytics. Upload PDF atau load demo data!")
    
    if st.button("🎮 Load Demo Data for Analytics", type="primary"):
        load_demo_data()
        st.success("✅ Demo data loaded!")
        st.rerun()

def show_analytics_overview(portfolio_metrics):
    """Show analytics overview metrics"""
    st.markdown("#### 📊 Portfolio Analytics Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        roi = portfolio_metrics['total_pnl_pct']
        st.metric("📈 Portfolio ROI", f"{roi:+.2f}%")
    
    with col2:
        win_rate = portfolio_metrics['win_rate']
        st.metric("🎯 Win Rate", f"{win_rate:.1f}%")
    
    with col3:
        avg_holding_value = portfolio_metrics['current_value'] / portfolio_metrics['total_stocks'] if portfolio_metrics['total_stocks'] > 0 else 0
        st.metric("💎 Avg Holding Value", f"Rp {avg_holding_value:,.0f}")
    
    with col4:
        portfolio_diversity = min(portfolio_metrics['total_stocks'] / 10 * 100, 100)  # Simple diversity score
        st.metric("🌈 Diversity Score", f"{portfolio_diversity:.0f}/100")

def show_portfolio_composition_charts(holdings_df):
    """Show portfolio composition visualizations"""
    st.markdown("---")
    st.markdown("#### 🍰 Portfolio Composition")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Portfolio allocation pie chart
        fig_pie = px.pie(
            holdings_df, 
            values='current_value', 
            names='symbol',
            title="Portfolio Allocation by Market Value",
            hover_data=['company_name'],
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Holdings value bar chart
        fig_bar = px.bar(
            holdings_df.sort_values('current_value', ascending=True),
            x='current_value',
            y='symbol',
            orientation='h',
            title="Holdings by Market Value",
            color='current_value',
            color_continuous_scale='viridis',
            hover_data=['company_name']
        )
        fig_bar.update_layout(height=400)
        st.plotly_chart(fig_bar, use_container_width=True)

def show_performance_analysis(holdings_df):
    """Show performance analysis charts"""
    st.markdown("---")
    st.markdown("#### 📊 Performance Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # P&L analysis chart
        fig_pnl = px.bar(
            holdings_df.sort_values('unrealized_pnl', ascending=True),
            x='unrealized_pnl',
            y='symbol',
            orientation='h',
            title="Unrealized P&L by Stock",
            color='unrealized_pnl',
            color_continuous_scale=['red', 'yellow', 'green'],
            hover_data=['company_name', 'unrealized_pnl_pct']
        )
        fig_pnl.update_layout(height=400)
        st.plotly_chart(fig_pnl, use_container_width=True)
    
    with col2:
        # Performance percentage chart
        fig_pct = px.scatter(
            holdings_df,
            x='total_cost',
            y='unrealized_pnl_pct',
            size='current_value',
            color='unrealized_pnl_pct',
            hover_name='symbol',
            title="Performance vs Investment Size",
            labels={'total_cost': 'Investment Amount (Rp)', 'unrealized_pnl_pct': 'P&L Percentage (%)'},
            color_continuous_scale=['red', 'yellow', 'green']
        )
        fig_pct.add_hline(y=0, line_dash="dash", line_color="gray")
        fig_pct.update_layout(height=400)
        st.plotly_chart(fig_pct, use_container_width=True)

def show_transaction_analytics(transactions):
    """Show transaction analytics and patterns"""
    st.markdown("---")
    st.markdown("#### 📅 Transaction Analytics")
    
    df = pd.DataFrame(transactions)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Daily transaction volume
        daily_volume = df.groupby(['trade_date', 'transaction_type'])['total_value'].sum().reset_index()
        
        fig_timeline = px.bar(
            daily_volume,
            x='trade_date',
            y='total_value',
            color='transaction_type',
            title="Daily Transaction Volume",
            color_discrete_map={'BUY': 'green', 'SELL': 'red'},
            labels={'total_value': 'Transaction Value (Rp)', 'trade_date': 'Date'}
        )
        fig_timeline.update_layout(height=400)
        st.plotly_chart(fig_timeline, use_container_width=True)
    
    with col2:
        # Transaction count by stock
        stock_activity = df.groupby('symbol').size().reset_index(name='transaction_count')
        stock_activity = stock_activity.sort_values('transaction_count', ascending=False).head(10)
        
        fig_activity = px.bar(
            stock_activity,
            x='transaction_count',
            y='symbol',
            orientation='h',
            title="Most Active Stocks (by transaction count)",
            color='transaction_count',
            color_continuous_scale='blues'
        )
        fig_activity.update_layout(height=400)
        st.plotly_chart(fig_activity, use_container_width=True)

def show_advanced_metrics(holdings_df, portfolio_metrics):
    """Show advanced portfolio metrics and rankings"""
    st.markdown("---")
    st.markdown("#### 🏆 Advanced Metrics & Rankings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🚀 Top Performers (by % Return)**")
        top_performers = holdings_df.nlargest(5, 'unrealized_pnl_pct')[['symbol', 'company_name', 'unrealized_pnl_pct', 'unrealized_pnl']]
        
        for i, (_, row) in enumerate(top_performers.iterrows()):
            medal = ["🥇", "🥈", "🥉", "🏅", "🏅"][i]
            st.markdown(f"{medal} **{row['symbol']}**: {row['unrealized_pnl_pct']:+.2f}% (Rp {row['unrealized_pnl']:,.0f})")
    
    with col2:
        st.markdown("**📉 Worst Performers (by % Return)**")
        worst_performers = holdings_df.nsmallest(5, 'unrealized_pnl_pct')[['symbol', 'company_name', 'unrealized_pnl_pct', 'unrealized_pnl']]
        
        for _, row in worst_performers.iterrows():
            st.markdown(f"🔴 **{row['symbol']}**: {row['unrealized_pnl_pct']:+.2f}% (Rp {row['unrealized_pnl']:,.0f})")
    
    # Portfolio health metrics
    st.markdown("---")
    st.markdown("#### 🏥 Portfolio Health Check")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Concentration risk
        max_weight = (holdings_df['current_value'].max() / holdings_df['current_value'].sum() * 100) if not holdings_df.empty else 0
        concentration_risk = "🟢 Low" if max_weight < 20 else "🟡 Medium" if max_weight < 40 else "🔴 High"
        st.metric("🎯 Concentration Risk", concentration_risk, f"Max: {max_weight:.1f}%")
    
    with col2:
        # Portfolio balance
        winners = len(holdings_df[holdings_df['unrealized_pnl'] > 0])
        losers = len(holdings_df[holdings_df['unrealized_pnl'] < 0])
        balance_score = "🟢 Good" if winners >= losers else "🟡 Fair" if winners > 0 else "🔴 Poor"
        st.metric("⚖️ Portfolio Balance", balance_score, f"{winners}W / {losers}L")
    
    with col3:
        # Average return
        avg_return = holdings_df['unrealized_pnl_pct'].mean() if not holdings_df.empty else 0
        return_health = "🟢 Excellent" if avg_return > 10 else "🟡 Good" if avg_return > 0 else "🔴 Poor"
        st.metric("📈 Avg Return Health", return_health, f"{avg_return:.1f}%")
    
    with col4:
        # Volatility (simplified)
        volatility = holdings_df['unrealized_pnl_pct'].std() if not holdings_df.empty else 0
        vol_level = "🟢 Low" if volatility < 10 else "🟡 Medium" if volatility < 25 else "🔴 High"
        st.metric("📊 Volatility Level", vol_level, f"σ: {volatility:.1f}%")

def show_settings_tools():
    """Show settings and tools page"""
    st.markdown("### ⚙️ Settings & Tools")
    
    # Data management
    show_data_management()
    
    # Export options
    show_export_options()
    
    # Demo and testing
    show_demo_testing_tools()
    
    # App information
    show_app_information()

def show_data_management():
    """Show data management options"""
    st.markdown("#### 🗄️ Data Management")
    
    transactions = st.session_state.get('portfolio_transactions', [])
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**📊 Current Data Status**")
        st.write(f"Total Transactions: {len(transactions)}")
        
        if transactions:
            df = pd.DataFrame(transactions)
            st.write(f"Unique Stocks: {df['symbol'].nunique()}")
            st.write(f"Date Range: {df['trade_date'].min()} to {df['trade_date'].max()}")
            st.write(f"Total Volume: Rp {df['total_value'].sum():,.0f}")
    
    with col2:
        st.markdown("**🔄 Data Actions**")
        
        if st.button("🔄 Refresh Portfolio", use_container_width=True):
            st.success("✅ Portfolio data refreshed!")
            st.rerun()
        
        if st.button("🎮 Reload Demo Data", use_container_width=True):
            load_demo_data()
            st.success("✅ Demo data loaded!")
            st.rerun()
    
    with col3:
        st.markdown("**⚠️ Danger Zone**")
        
        if st.button("🗑️ Clear All Data", use_container_width=True, type="secondary"):
            if st.button("⚠️ Confirm Delete All", type="secondary"):
                st.session_state.portfolio_transactions = []
                st.session_state.mock_data_loaded = False
                st.success("✅ All data cleared!")
                st.rerun()

def show_export_options():
    """Show export options"""
    st.markdown("---")
    st.markdown("#### 📥 Export Options")
    
    transactions = st.session_state.get('portfolio_transactions', [])
    
    if not transactions:
        st.info("📭 No data to export. Upload PDF or load demo data first!")
        return
    
    holdings_df, portfolio_metrics = calculate_portfolio_metrics(transactions)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**📋 Transactions Export**")
        trans_df = pd.DataFrame(transactions)
        trans_csv = trans_df.to_csv(index=False)
        
        st.download_button(
            label="📥 Download Transactions CSV",
            data=trans_csv,
            file_name=f"transactions_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col2:
        st.markdown("**🏪 Holdings Export**")
        if not holdings_df.empty:
            holdings_csv = holdings_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Holdings CSV",
                data=holdings_csv,
                file_name=f"holdings_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                use_container_width=True
            )
    
    with col3:
        st.markdown("**📊 Complete Portfolio**")
        complete_csv = create_portfolio_export(transactions, holdings_df)
        st.download_button(
            label="📥 Download Complete Portfolio",
            data=complete_csv,
            file_name=f"portfolio_complete_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            use_container_width=True
        )

def show_demo_testing_tools():
    """Show demo and testing tools"""
    st.markdown("---")
    st.markdown("#### 🎮 Demo & Testing Tools")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🚀 Quick Demo Options**")
        
        if st.button("📊 Load Demo Portfolio", use_container_width=True):
            load_demo_data()
            st.success("✅ Demo portfolio loaded!")
            st.rerun()
        
        if st.button("🔄 Regenerate Demo Data", use_container_width=True):
            st.session_state.mock_data_loaded = False
            load_demo_data()
            st.success("✅ Fresh demo data generated!")
            st.rerun()
    
    with col2:
        st.markdown("**🧪 Testing Features**")
        
        if st.button("📈 Test Price Simulation", use_container_width=True):
            st.info("💡 Prices are simulated using hash-based algorithm for demo purposes")
        
        if st.button("📋 Show Sample PDF Format", use_container_width=True):
            show_sample_pdf_format()

def show_sample_pdf_format():
    """Show sample PDF format"""
    st.markdown("**📄 Sample PDF Trade Confirmation Format:**")
    st.code("""
REF #    Board  Share  Company                           Lot    Quantity   Price    Buy        Sell
384417   RG     AGRS   Bank IBK Indonesia Tbk.           466    46,600.00  67.00    3,122,200  0.00
384418   RG     AHAP   Asuransi Harta Aman Pratama Tbk.  105    10,500.00  73.00    766,500    0.00
384419   NG     BRIS   Bank Syariah Indonesia Tbk.       200    20,000.00  85.00    1,700,000  0.00
384420   RG     CTRA   Ciputra Development Tbk.          150    15,000.00  92.00    1,380,000  0.00
384421   TN     DMAS   Puradelta Lestari Tbk.            80     8,000.00   125.00   1,000,000  0.00
    """)

def show_app_information():
    """Show application information"""
    st.markdown("---")
    st.markdown("#### ℹ️ Application Information")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **🚀 Portfolio Tracker Indonesia v2.0**
        
        **📊 Features:**
        • PDF Upload & Auto-parsing
        • Real-time Portfolio Tracking  
        • P&L Calculation (FIFO method)
        • Advanced Analytics & Visualizations
        • Export to CSV
        • Demo Mode for Testing
        
        **🎯 Supported Formats:**
        • Stockbit PDF Trade Confirmations
        • Standard Indonesian Securities Format
        """)
    
    with col2:
        st.markdown("""
        **💡 Technical Details:**
        
        **📚 Libraries Used:**
        • Streamlit (Web Interface)
        • Pandas (Data Processing)  
        • Plotly (Interactive Charts)
        • PyPDF2 (PDF Parsing)
        
        **🔧 Features:**
        • FIFO Cost Basis Calculation
        • Real-time Metrics Updates
        • Responsive Design
        • Data Persistence (Session)
        """)
    
    # System status
    st.markdown("**🔍 System Status:**")
    
    status_col1, status_col2, status_col3 = st.columns(3)
    
    with status_col1:
        pdf_status = "✅ Available" if PDF_AVAILABLE else "❌ Not Available"
        st.write(f"PDF Library: {pdf_status}")
    
    with status_col2:
        transactions = st.session_state.get('portfolio_transactions', [])
        data_status = "✅ Loaded" if transactions else "📭 Empty"
        st.write(f"Portfolio Data: {data_status}")
    
    with status_col3:
        demo_status = "✅ Loaded" if st.session_state.get('mock_data_loaded', False) else "📭 Not Loaded"
        st.write(f"Demo Data: {demo_status}")

def calculate_portfolio_metrics(transactions):
    """Calculate comprehensive portfolio metrics using FIFO method"""
    if not transactions:
        return pd.DataFrame(), get_empty_portfolio_metrics()
    
    df = pd.DataFrame(transactions)
    holdings = []
    
    # Calculate holdings for each symbol using FIFO
    for symbol in df['symbol'].unique():
        symbol_trans = df[df['symbol'] == symbol].sort_values('trade_date')
        
        total_buy_qty = 0
        total_buy_value = 0
        total_sell_qty = 0
        company_name = symbol_trans['company_name'].iloc[0]
        
        # Calculate using FIFO method
        for _, trans in symbol_trans.iterrows():
            if trans['transaction_type'] == 'BUY':
                total_buy_qty += trans['quantity']
                total_buy_value += trans['total_value']
            else:  # SELL
                total_sell_qty += trans['quantity']
        
        current_qty = total_buy_qty - total_sell_qty
        
        if current_qty > 0:
            avg_price = total_buy_value / total_buy_qty if total_buy_qty > 0 else 0
            
            # Simulate current price (in real app, fetch from market API)
            current_price = simulate_current_price(symbol, avg_price)
            
            total_cost = current_qty * avg_price
            current_value = current_qty * current_price
            unrealized_pnl = current_value - total_cost
            unrealized_pnl_pct = (unrealized_pnl / total_cost * 100) if total_cost > 0 else 0
            
            holdings.append({
                'symbol': symbol,
                'company_name': company_name,
                'quantity': int(current_qty),
                'avg_price': avg_price,
                'current_price': current_price,
                'total_cost': total_cost,
                'current_value': current_value,
                'unrealized_pnl': unrealized_pnl,
                'unrealized_pnl_pct': unrealized_pnl_pct
            })
    
    holdings_df = pd.DataFrame(holdings)
    portfolio_metrics = calculate_portfolio_summary(holdings_df)
    
    return holdings_df, portfolio_metrics

def simulate_current_price(symbol, avg_price):
    """Simulate current market price for demo purposes"""
    # Use hash of symbol for consistent "random" price movements
    hash_int = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16)
    
    # Create price variation between -30% to +50%
    price_change_pct = ((hash_int % 8000) / 100) - 30  # Range: -30 to +50
    
    # Apply price change
    current_price = avg_price * (1 + price_change_pct / 100)
    
    # Ensure minimum price
    return max(current_price, avg_price * 0.5)

def calculate_portfolio_summary(holdings_df):
    """Calculate portfolio summary metrics"""
    if holdings_df.empty:
        return get_empty_portfolio_metrics()
    
    total_invested = holdings_df['total_cost'].sum()
    current_value = holdings_df['current_value'].sum()
    total_pnl = current_value - total_invested
    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0
    
    winners = len(holdings_df[holdings_df['unrealized_pnl'] > 0])
    total_stocks = len(holdings_df)
    win_rate = (winners / total_stocks * 100) if total_stocks > 0 else 0
    
    return {
        'total_invested': total_invested,
        'current_value': current_value,
        'total_pnl': total_pnl,
        'total_pnl_pct': total_pnl_pct,
        'total_stocks': total_stocks,
        'winners': winners,
        'win_rate': win_rate
    }

def get_empty_portfolio_metrics():
    """Return empty portfolio metrics"""
    return {
        'total_invested': 0,
        'current_value': 0,
        'total_pnl': 0,
        'total_pnl_pct': 0,
        'total_stocks': 0,
        'winners': 0,
        'win_rate': 0
    }

def create_portfolio_export(transactions, holdings_df):
    """Create comprehensive CSV export of portfolio data"""
    # Create comprehensive export data
    export_sections = []
    
    # Summary section
    if not holdings_df.empty:
        summary_data = {
            'Metric': ['Export Date', 'Total Transactions', 'Current Holdings', 'Total Invested', 'Current Value', 'Total P&L', 'Portfolio Return %'],
            'Value': [
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                len(transactions),
                len(holdings_df),
                f"Rp {holdings_df['total_cost'].sum():,.0f}",
                f"Rp {holdings_df['current_value'].sum():,.0f}",
                f"Rp {holdings_df['unrealized_pnl'].sum():,.0f}",
                f"{(holdings_df['unrealized_pnl'].sum() / holdings_df['total_cost'].sum() * 100):.2f}%" if holdings_df['total_cost'].sum() > 0 else "0.00%"
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        export_sections.append("=== PORTFOLIO SUMMARY ===")
        export_sections.append(summary_df.to_csv(index=False))
    
    # Transactions section
    if transactions:
        trans_df = pd.DataFrame(transactions)
        export_sections.append("\n=== ALL TRANSACTIONS ===")
        export_sections.append(trans_df.to_csv(index=False))
    
    # Holdings section
    if not holdings_df.empty:
        export_sections.append("\n=== CURRENT HOLDINGS ===")
        export_sections.append(holdings_df.to_csv(index=False))
    
    return '\n'.join(export_sections)

def load_demo_data():
    """Load comprehensive demo data for testing"""
    if st.session_state.get('mock_data_loaded', False):
        return
    
    # Demo stocks data
    demo_stocks = [
        {"symbol": "AGRS", "company": "Bank IBK Indonesia Tbk.", "base_price": 67},
        {"symbol": "AHAP", "company": "Asuransi Harta Aman Pratama Tbk.", "base_price": 73},
        {"symbol": "BRIS", "company": "Bank Syariah Indonesia Tbk.", "base_price": 85},
        {"symbol": "CTRA", "company": "Ciputra Development Tbk.", "base_price": 92},
        {"symbol": "DMAS", "company": "Puradelta Lestari Tbk.", "base_price": 125},
        {"symbol": "EMTK", "company": "Elang Mahkota Teknologi Tbk.", "base_price": 156},
        {"symbol": "FREN", "company": "Smartfren Telecom Tbk.", "base_price": 44},
        {"symbol": "GOTO", "company": "GoTo Gojek Tokopedia Tbk.", "base_price": 78},
        {"symbol": "HMSP", "company": "HM Sampoerna Tbk.", "base_price": 890},
        {"symbol": "INKP", "company": "Indah Kiat Pulp & Paper Tbk.", "base_price": 245}
    ]
    
    demo_transactions = []
    base_date = date(2024, 1, 15)
    ref_counter = 384400
    
    # Generate realistic demo transactions
    for i, stock in enumerate(demo_stocks):
        # Initial buy transaction
        trade_date = base_date + timedelta(days=i*7)
        quantity = np.random.randint(100, 1000) * 100  # Lot-based quantities
        price_variation = np.random.uniform(0.95, 1.05)
        buy_price = stock["base_price"] * price_variation
        
        demo_transactions.append({
            'trade_date': trade_date,
            'settlement_date': trade_date + timedelta(days=2),
            'ref_number': str(ref_counter + i),
            'symbol': stock["symbol"],
            'company_name': stock["company"],
            'transaction_type': 'BUY',
            'lot': quantity // 100,
            'quantity': quantity,
            'price': buy_price,
            'total_value': quantity * buy_price,
            'source': 'DEMO_DATA',
            'created_at': datetime.now()
        })
        
        # Some additional transactions for variety
        if i % 3 == 0:  # Add more buy for 1/3 of stocks
            trade_date2 = trade_date + timedelta(days=30)
            quantity2 = np.random.randint(50, 300) * 100
            buy_price2 = buy_price * np.random.uniform(0.90, 1.10)
            
            demo_transactions.append({
                'trade_date': trade_date2,
                'settlement_date': trade_date2 + timedelta(days=2),
                'ref_number': str(ref_counter + len(demo_stocks) + i),
                'symbol': stock["symbol"],
                'company_name': stock["company"],
                'transaction_type': 'BUY',
                'lot': quantity2 // 100,
                'quantity': quantity2,
                'price': buy_price2,
                'total_value': quantity2 * buy_price2,
                'source': 'DEMO_DATA',
                'created_at': datetime.now()
            })
        
        if i % 4 == 0:  # Add sell for 1/4 of stocks
            trade_date3 = trade_date + timedelta(days=45)
            sell_quantity = quantity // 2  # Sell half
            sell_price = buy_price * np.random.uniform(1.05, 1.25)  # Sell higher
            
            demo_transactions.append({
                'trade_date': trade_date3,
                'settlement_date': trade_date3 + timedelta(days=2),
                'ref_number': str(ref_counter + len(demo_stocks) * 2 + i),
                'symbol': stock["symbol"],
                'company_name': stock["company"],
                'transaction_type': 'SELL',
                'lot': sell_quantity // 100,
                'quantity': sell_quantity,
                'price': sell_price,
                'total_value': sell_quantity * sell_price,
                'source': 'DEMO_DATA',
                'created_at': datetime.now()
            })
    
    # Update session state
    st.session_state.portfolio_transactions = demo_transactions
    st.session_state.mock_data_loaded = True

# Run the application
if __name__ == "__main__":
    main()