import streamlit as st
import pandas as pd
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database import fetch_data_cached
from queries import OverviewQueries
from charts import ChartFactory

def get_bandarmology_signals():
    """Get smart money signals using bandarmology analysis"""
    query = """
    WITH volume_analysis AS (
        -- Analisis volume selama 20 hari terakhir
        SELECT 
            symbol,
            date,
            volume,
            ROUND(AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING), 0) AS avg_volume_20d,
            close,
            prev_close,
            ((close - prev_close) / NULLIF(prev_close, 0) * 100) AS percent_change,
            foreign_buy,
            foreign_sell,
            (foreign_buy - foreign_sell) AS foreign_net,
            bid,
            offer,
            bid_volume,
            offer_volume
        FROM public.daily_stock_summary
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
    ),
    accumulation_signals AS (
        -- Identifikasi pola akumulasi
        SELECT 
            v.symbol,
            v.date,
            v.close,
            v.volume,
            v.avg_volume_20d,
            v.percent_change,
            v.foreign_net,
            -- Indikator volume spike (>2x rata-rata)
            CASE WHEN v.volume > 2 * v.avg_volume_20d THEN 1 ELSE 0 END AS volume_spike,
            -- Indikator akumulasi asing
            CASE WHEN v.foreign_net > 0 THEN 1 ELSE 0 END AS foreign_buying,
            -- Indikator kekuatan bid (demand lebih besar dari supply)
            CASE WHEN v.bid_volume > 1.5 * v.offer_volume THEN 1 ELSE 0 END AS strong_bid,
            -- Indikator hidden buying (volume tinggi dengan pergerakan harga terbatas)
            CASE WHEN v.volume > 1.5 * v.avg_volume_20d AND ABS(v.percent_change) < 1.0 THEN 1 ELSE 0 END AS hidden_buying,
            -- Cek apakah ada closing price positif
            CASE WHEN v.close > v.prev_close THEN 1 ELSE 0 END AS positive_close
        FROM volume_analysis v
    ),
    final_scores AS (
        -- Kalkulasi skor final dan tambahkan data historis untuk analisis
        SELECT 
            a.symbol,
            a.date,
            a.close,
            a.percent_change,
            a.volume,
            a.avg_volume_20d,
            a.volume/NULLIF(a.avg_volume_20d, 0) AS volume_ratio,
            a.foreign_net,
            -- Kalkulasi skor bandarmology (semakin tinggi semakin kuat)
            (a.volume_spike + a.foreign_buying + a.strong_bid + a.hidden_buying + a.positive_close) AS bandar_score,
            -- Cek performa harga 1 hari setelah tanggal
            LEAD(a.percent_change, 1) OVER (PARTITION BY a.symbol ORDER BY a.date) AS next_day_change,
            -- Tambahkan data untuk analisis tren jangka pendek
            SUM(a.positive_close) OVER (PARTITION BY a.symbol ORDER BY a.date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS positive_days_last5
        FROM accumulation_signals a
    ),
    bandar_ranking AS (
        -- Final selection dari saham teratas dengan potensi kenaikan
        SELECT 
            symbol,
            date,
            close,
            ROUND(volume_ratio, 2) AS volume_ratio,
            bandar_score,
            positive_days_last5,
            foreign_net,
            CASE
                WHEN bandar_score >= 4 THEN 'Sangat Kuat'
                WHEN bandar_score = 3 THEN 'Kuat'
                WHEN bandar_score = 2 THEN 'Sedang'
                ELSE 'Lemah'
            END AS signal_strength
        FROM final_scores
        WHERE date = (SELECT MAX(date) FROM public.daily_stock_summary) -- Hanya ambil data hari terakhir
        AND bandar_score >= 2 -- Filter hanya sinyal sedang ke atas
        AND volume_ratio > 1.5 -- Harus ada kenaikan volume signifikan
    )
    -- Join dengan tabel nama untuk hasil lengkap
    SELECT 
        b.symbol,
        d.name,
        b.close,
        b.volume_ratio,
        b.bandar_score,
        b.signal_strength,
        b.positive_days_last5,
        b.foreign_net
    FROM bandar_ranking b
    LEFT JOIN public.daily_stock_summary d 
        ON b.symbol = d.symbol 
        AND d.date = (SELECT MAX(date) FROM public.daily_stock_summary)
    ORDER BY b.bandar_score DESC, b.volume_ratio DESC
    LIMIT 15
    """
    return fetch_data_cached(query, "Bandarmology Signals")

def show():
    """Overview dashboard page"""
    st.markdown("<h1 style='text-align: center; color: #1f77b4; margin-bottom: 30px;'>🚀 Indonesian Stock Market Intelligence</h1>", 
                unsafe_allow_html=True)
    
    # Quick KPI Metrics
    st.markdown("### 📊 Market Overview")
    
    # Check analytics schema
    check_analytics_query = """
    SELECT EXISTS (
        SELECT FROM information_schema.schemata 
        WHERE schema_name = 'public_analytics'
    )
    """
    
    analytics_df = fetch_data_cached(check_analytics_query, "Analytics Schema Check")
    use_analytics = analytics_df.iloc[0, 0] if not analytics_df.empty else False
    
    metrics_table = "public_analytics.daily_stock_metrics" if use_analytics else "daily_stock_summary"
    
    if use_analytics:
        st.info("✅ Using analytics tables")
    else:
        st.info("ℹ️ Using raw data tables (analytics schema not found)")
    
    # KPI Metrics
    try:
        kpi_data = fetch_data_cached(
            OverviewQueries.get_market_kpis(metrics_table), 
            "KPI Data"
        )
        
        if not kpi_data.empty:
            kpi = kpi_data.iloc[0]
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_stocks = int(kpi['total_stocks']) if pd.notna(kpi['total_stocks']) else 0
                avg_change = kpi['avg_change'] if pd.notna(kpi['avg_change']) else 0
                st.metric("📈 Total Stocks", f"{total_stocks}", f"Avg: {avg_change:.2f}%")
            
            with col2:
                total_volume = kpi['total_volume'] if pd.notna(kpi['total_volume']) else 0
                volume_b = total_volume / 1e9
                st.metric("🔊 Daily Volume", f"{volume_b:.1f}B lots", "↗️ Active")
            
            with col3:
                gainers = int(kpi['gainers']) if pd.notna(kpi['gainers']) else 0
                losers = int(kpi['losers']) if pd.notna(kpi['losers']) else 0
                total_movers = gainers + losers
                gainers_pct = (gainers / total_movers * 100) if total_movers > 0 else 0
                st.metric("⚖️ Market Breadth", f"{gainers_pct:.1f}% Positive", f"🟢 {gainers} vs 🔴 {losers}")
            
            with col4:
                total_value = kpi['total_value'] if pd.notna(kpi['total_value']) else 0
                value_t = total_value / 1e12
                st.metric("💰 Trading Value", f"Rp{value_t:.2f}T", "💹 Today")
        else:
            st.warning("⚠️ No KPI data available")
    
    except Exception as e:
        st.error(f"❌ Failed to load KPIs: {str(e)}")

    st.markdown("---")

    # ADD SMART MONEY SIGNALS SECTION (NEW)
    st.markdown("### 🎯 Smart Money Signals (Bandarmology)")
    st.markdown("*🧠 AI-powered detection of institutional investor activity*")
    
    try:
        bandarmology_df = get_bandarmology_signals()
        
        if not bandarmology_df.empty:
            # Show summary stats
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                very_strong = len(bandarmology_df[bandarmology_df['signal_strength'] == 'Sangat Kuat'])
                st.metric("🔥 Very Strong", very_strong, "signals")
            
            with col2:
                strong = len(bandarmology_df[bandarmology_df['signal_strength'] == 'Kuat'])
                st.metric("💪 Strong", strong, "signals")
            
            with col3:
                avg_score = bandarmology_df['bandar_score'].mean()
                st.metric("📊 Avg Score", f"{avg_score:.1f}/5", "quality")
            
            with col4:
                avg_volume_ratio = bandarmology_df['volume_ratio'].mean()
                st.metric("⚡ Avg Volume", f"{avg_volume_ratio:.1f}x", "normal")
            
            st.markdown("#### 🎯 Top Smart Money Picks:")
            st.markdown("*Click any stock below for detailed analysis*")
            
            # Create clickable smart money signals
            cols = st.columns(3)
            
            for i, (_, row) in enumerate(bandarmology_df.head(12).iterrows()):
                col_idx = i % 3
                with cols[col_idx]:
                    # Determine signal color and emoji
                    if row['signal_strength'] == 'Sangat Kuat':
                        signal_color = "🔥"
                        bg_color = "#e8f5e8"
                        border_color = "#4caf50"
                    elif row['signal_strength'] == 'Kuat':
                        signal_color = "💪"
                        bg_color = "#fff3cd"
                        border_color = "#ffc107"
                    else:
                        signal_color = "📊"
                        bg_color = "#e3f2fd"
                        border_color = "#2196f3"
                    
                    # Foreign flow indicator
                    foreign_emoji = "🟢" if row['foreign_net'] > 0 else "🔴" if row['foreign_net'] < 0 else "⚪"
                    
                    # Create styled button
                    button_html = f"""
                    <div style="
                        background-color: {bg_color}; 
                        border: 2px solid {border_color}; 
                        border-radius: 10px; 
                        padding: 10px; 
                        margin: 5px 0; 
                        text-align: center;
                        cursor: pointer;
                        transition: all 0.3s ease;
                    ">
                        <strong style="font-size: 16px;">{signal_color} {row['symbol']}</strong><br>
                        <span style="font-size: 12px; color: #666;">{row['name'][:20]}{'...' if len(str(row['name'])) > 20 else ''}</span><br>
                        <strong>Score: {row['bandar_score']}/5 ({row['signal_strength']})</strong><br>
                        Vol: {row['volume_ratio']:.1f}x | {foreign_emoji} Foreign<br>
                        <span style="font-size: 11px;">Rp{row['close']:,.0f} | +{row['positive_days_last5']}/5 days</span>
                    </div>
                    """
                    
                    if st.button(
                        f"{signal_color} {row['symbol']}\nScore: {row['bandar_score']}/5\nVol: {row['volume_ratio']:.1f}x\n{row['signal_strength']}",
                        key=f"bandar_{row['symbol']}",
                        help=f"Smart Money Signal: {row['signal_strength']} - Click to analyze {row['symbol']}",
                        use_container_width=True
                    ):
                        st.session_state.selected_stock = row['symbol']
                        st.session_state.page_navigation = "🎯 Individual Stock Analysis"
                        st.rerun()
            
            # Show detailed table for top signals
            with st.expander("📋 Detailed Smart Money Analysis", expanded=False):
                # Format the dataframe for display
                display_df = bandarmology_df.copy()
                display_df['Price'] = display_df['close'].apply(lambda x: f"Rp{x:,.0f}")
                display_df['Volume Ratio'] = display_df['volume_ratio'].apply(lambda x: f"{x:.1f}x")
                display_df['Foreign Flow'] = display_df['foreign_net'].apply(
                    lambda x: f"🟢 +{x:,.0f}" if x > 0 else f"🔴 {x:,.0f}" if x < 0 else "⚪ Neutral"
                )
                display_df['Momentum'] = display_df['positive_days_last5'].apply(lambda x: f"{x}/5 days")
                
                # Select columns for display
                table_df = display_df[['symbol', 'name', 'Price', 'bandar_score', 'signal_strength', 
                                     'Volume Ratio', 'Foreign Flow', 'Momentum']].head(10)
                table_df.columns = ['Symbol', 'Company', 'Price', 'Score', 'Strength', 'Volume', 'Foreign', 'Momentum']
                
                st.dataframe(table_df, use_container_width=True, hide_index=True)
        
        else:
            st.info("📭 No strong smart money signals detected today. Market may be consolidating.")
            
    except Exception as e:
        st.error(f"❌ Failed to load smart money signals: {str(e)}")

    st.markdown("---")
    
    # Charts section (existing code)
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 🔥 Top Movers")
        st.markdown("*💡 Click any stock below to jump to detailed analysis*")
        
        try:
            movers_df = fetch_data_cached(
                OverviewQueries.get_top_movers(metrics_table, 15),
                "Top Movers"
            )
            
            if not movers_df.empty:
                fig = ChartFactory.create_top_movers_chart(
                    movers_df, 
                    "Top 15 Stock Movers (% Change)"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Create clickable buttons for top movers
                st.markdown("#### 🎯 Quick Analysis:")
                cols = st.columns(5)
                for i, (_, row) in enumerate(movers_df.head(10).iterrows()):
                    col_idx = i % 5
                    with cols[col_idx]:
                        change_emoji = "🟢" if row['percent_change'] > 0 else "🔴" if row['percent_change'] < 0 else "⚪"
                        if st.button(
                            f"{change_emoji} {row['symbol']}\n{row['percent_change']:+.1f}%", 
                            key=f"mover_{row['symbol']}",
                            help=f"Analyze {row['symbol']} - {row['name']}"
                        ):
                            # Set session state and switch to individual analysis
                            st.session_state.selected_stock = row['symbol']
                            st.session_state.page_navigation = "🎯 Individual Stock Analysis"
                            st.rerun()
                
            else:
                st.info("📭 No stock movement data available")
                
        except Exception as e:
            st.error(f"❌ Failed to load top movers: {str(e)}")
    
    with col2:
        st.markdown("### ⚡ Top Volume")
        st.markdown("*💡 Click any stock for detailed analysis*")
        
        try:
            volume_query = f"""
            WITH latest_date AS (
                SELECT MAX(date) as max_date FROM {metrics_table}
            )
            SELECT 
                symbol,
                name,
                volume,
                close,
                CASE
                    WHEN prev_close IS NOT NULL AND prev_close > 0 
                        THEN (close - prev_close) / prev_close * 100
                    ELSE 0
                END as percent_change
            FROM {metrics_table}, latest_date
            WHERE date = latest_date.max_date
            AND volume > 0
            ORDER BY volume DESC
            LIMIT 8
            """
            
            volume_df = fetch_data_cached(volume_query, "Top Volume")
            
            if not volume_df.empty:
                for _, row in volume_df.iterrows():
                    pct_change = row['percent_change'] if pd.notna(row['percent_change']) else 0
                    change_color = "🟢" if pct_change > 0 else "🔴" if pct_change < 0 else "⚪"
                    
                    # CREATE CLICKABLE STOCK CARDS
                    if st.button(
                        f"**{row['symbol']}** {change_color}\nVol: {row['volume']:,.0f}\n{pct_change:+.2f}% | Rp{row['close']:,.0f}",
                        key=f"volume_{row['symbol']}",
                        help=f"Click to analyze {row['symbol']} - {row['name']}",
                        use_container_width=True
                    ):
                        # Set session state and navigate
                        st.session_state.selected_stock = row['symbol']
                        st.session_state.page_navigation = "🎯 Individual Stock Analysis"
                        st.rerun()
            else:
                st.info("📭 No volume data available")
                
        except Exception as e:
            st.error(f"❌ Failed to load volume data: {str(e)}")

    # ADD QUICK SEARCH SECTION
    st.markdown("---")
    st.markdown("### 🔍 Quick Stock Search")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        # Quick search input
        search_symbol = st.text_input(
            "Enter Stock Symbol:", 
            placeholder="e.g., BBCA, TLKM, BMRI...",
            help="Type a stock symbol and press Enter or click Search"
        )
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)  # Add spacing
        if st.button("🔍 Search & Analyze", type="primary"):
            if search_symbol:
                st.session_state.selected_stock = search_symbol.upper()
                st.session_state.page_navigation = "🎯 Individual Stock Analysis"
                st.rerun()
            else:
                st.warning("Please enter a stock symbol")

    # Show navigation hint
    if 'selected_stock' in st.session_state and st.session_state.selected_stock:
        st.success(f"✨ Ready to analyze **{st.session_state.selected_stock}**! Navigate to Individual Stock Analysis or the page will auto-switch.")

    # ADD MARKET HIGHLIGHTS SECTION
    st.markdown("---")
    st.markdown("### 📈 Market Highlights")
    
    try:
        highlights_query = f"""
        WITH latest_date AS (
            SELECT MAX(date) as max_date FROM {metrics_table}
        ),
        market_highlights AS (
            SELECT 
                symbol,
                name,
                close,
                volume,
                CASE
                    WHEN prev_close IS NOT NULL AND prev_close > 0 
                        THEN (close - prev_close) / prev_close * 100
                    ELSE 0
                END as percent_change
            FROM {metrics_table}, latest_date
            WHERE date = latest_date.max_date
            AND volume > 0
            AND prev_close IS NOT NULL AND prev_close > 0
        ),
        top_gainers AS (
            SELECT 'Top Gainer' as category, symbol, name, percent_change, volume
            FROM market_highlights
            WHERE percent_change > 0
            ORDER BY percent_change DESC
            LIMIT 3
        ),
        top_losers AS (
            SELECT 'Top Loser' as category, symbol, name, percent_change, volume
            FROM market_highlights
            WHERE percent_change < 0
            ORDER BY percent_change ASC
            LIMIT 3
        ),
        most_active AS (
            SELECT 'Most Active' as category, symbol, name, percent_change, volume
            FROM market_highlights
            ORDER BY volume DESC
            LIMIT 3
        )
        SELECT * FROM top_gainers
        UNION ALL
        SELECT * FROM top_losers
        UNION ALL
        SELECT * FROM most_active
        ORDER BY category, percent_change DESC
        """
        
        highlights_df = fetch_data_cached(highlights_query, "Market Highlights")
        
        if not highlights_df.empty:
            col1, col2, col3 = st.columns(3)
            
            categories = ['Top Gainer', 'Top Loser', 'Most Active']
            columns = [col1, col2, col3]
            
            for category, col in zip(categories, columns):
                with col:
                    if category == 'Top Gainer':
                        st.markdown("#### 🚀 Top Gainers")
                        emoji = "🟢"
                    elif category == 'Top Loser':
                        st.markdown("#### 📉 Top Losers")
                        emoji = "🔴"
                    else:
                        st.markdown("#### ⚡ Most Active")
                        emoji = "💹"
                    
                    category_data = highlights_df[highlights_df['category'] == category]
                    
                    for _, row in category_data.iterrows():
                        if st.button(
                            f"{emoji} **{row['symbol']}**\n{row['percent_change']:+.2f}%\nVol: {row['volume']:,.0f}",
                            key=f"highlight_{category}_{row['symbol']}",
                            help=f"Analyze {row['symbol']} - {row['name']}",
                            use_container_width=True
                        ):
                            st.session_state.selected_stock = row['symbol']
                            st.session_state.page_navigation = "🎯 Individual Stock Analysis"
                            st.rerun()
    
    except Exception as e:
        st.error(f"❌ Failed to load market highlights: {str(e)}")

    # ADD QUICK STATS
    st.markdown("---")
    st.markdown("### 📊 Market Intelligence Summary")
    
    try:
        quick_stats_query = f"""
        WITH latest_date AS (
            SELECT MAX(date) as max_date FROM {metrics_table}
        )
        SELECT 
            COUNT(CASE WHEN (close - prev_close) / prev_close * 100 > 5 THEN 1 END) as big_gainers,
            COUNT(CASE WHEN (close - prev_close) / prev_close * 100 < -5 THEN 1 END) as big_losers,
            COUNT(CASE WHEN volume > 10000000 THEN 1 END) as high_volume_stocks,
            COUNT(*) as total_active_stocks,
            AVG(volume) as avg_volume,
            MAX(volume) as max_volume
        FROM {metrics_table}, latest_date
        WHERE date = latest_date.max_date
        AND volume > 0
        AND prev_close > 0
        """
        
        stats_df = fetch_data_cached(quick_stats_query, "Quick Stats")
        
        if not stats_df.empty:
            stats = stats_df.iloc[0]
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("🚀 Big Gainers", f"{int(stats['big_gainers'])}", ">5% gain")
            
            with col2:
                st.metric("📉 Big Losers", f"{int(stats['big_losers'])}", ">5% loss")
            
            with col3:
                st.metric("⚡ High Volume", f"{int(stats['high_volume_stocks'])}", ">10M lots")
            
            with col4:
                avg_vol_m = stats['avg_volume'] / 1e6
                st.metric("📊 Avg Volume", f"{avg_vol_m:.1f}M", "lots")
    
    except Exception as e:
        st.error(f"❌ Failed to load quick stats: {str(e)}")

    # ADD FOOTER MESSAGE
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray; font-size: 0.9em; padding: 20px;'>
        🧠 <strong>Smart Money Detection</strong> powered by advanced institutional activity algorithms<br>
        💡 Click any stock above to dive deep into technical, sentiment, and bandarmology analysis
    </div>
    """, unsafe_allow_html=True)