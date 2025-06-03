
import streamlit as st
import pandas as pd
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database import fetch_data_cached
from queries import OverviewQueries
from charts import ChartFactory

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
    
    # Charts section
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 🔥 Top Movers")
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
            else:
                st.info("📭 No stock movement data available")
                
        except Exception as e:
            st.error(f"❌ Failed to load top movers: {str(e)}")
    
    with col2:
        st.markdown("### ⚡ Top Volume")
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
                    
                    st.markdown(f"""
                    <div style="padding: 10px; border-radius: 5px; margin: 10px 0; background-color: #f0f2f6;">
                        <strong>{row['symbol']}</strong> {change_color}<br>
                        Volume: {row['volume']:,.0f}<br>
                        Change: {pct_change:+.2f}% | Rp{row['close']:,.0f}
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("📭 No volume data available")
                
        except Exception as e:
            st.error(f"❌ Failed to load volume data: {str(e)}")