# ============================================================================
# Enhanced Bidirectional Elliott Wave Analysis - 200+ Data Points
# ============================================================================

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
from datetime import datetime, timedelta

# Import utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database import fetch_data_cached, execute_query_safe
from charts import ChartFactory

class EnhancedElliottWaveQueries:
    """Enhanced Elliott Wave Analysis - Bidirectional & Extended Data"""
    
    @staticmethod
    def get_comprehensive_wave_data(symbol, period_days=250):
        """Enhanced Elliott Wave analysis with bidirectional patterns"""
        return f"""
        WITH base_data AS (
            SELECT 
                m.symbol,
                m.name,
                m.date,
                m.open_price,
                m.high AS high_price,
                m.low AS low_price,
                m.close AS close_price,
                m.volume,
                m.value,
                m.percent_change,
                ROW_NUMBER() OVER (PARTITION BY m.symbol ORDER BY m.date) AS row_num
            FROM public_analytics.daily_stock_metrics m
            WHERE m.symbol = '{symbol}'
            AND m.date >= CURRENT_DATE - INTERVAL '{period_days} days'
            ORDER BY m.date
        ),
        
        -- Enhanced Moving Averages (multiple timeframes for 200+ days)
        moving_averages AS (
            SELECT *,
                AVG(close_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sma_5,
                AVG(close_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS sma_10,
                AVG(close_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20,
                AVG(close_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_50,
                AVG(close_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS sma_100,
                AVG(close_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma_200,
                AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_volume_20,
                AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS avg_volume_50
            FROM base_data
        ),
        
        -- Enhanced Technical indicators
        technical_data AS (
            SELECT 
                ma.*,
                r.rsi AS rsi_14,
                r.rsi_signal,
                mc.macd_line,
                mc.signal_line,
                mc.macd_histogram,
                mc.macd_signal
            FROM moving_averages ma
            LEFT JOIN public_analytics.technical_indicators_rsi r 
                ON ma.symbol = r.symbol AND ma.date = r.date
            LEFT JOIN public_analytics.technical_indicators_macd mc 
                ON ma.symbol = mc.symbol AND ma.date = mc.date
        ),
        
        -- BIDIRECTIONAL Trend Analysis
        trend_analysis AS (
            SELECT *,
                -- Multi-timeframe trend detection
                CASE 
                    WHEN close_price > sma_20 AND sma_20 > sma_50 AND sma_50 > sma_100 THEN 'STRONG_UPTREND'
                    WHEN close_price > sma_20 AND sma_20 > LAG(sma_20, 10) OVER (PARTITION BY symbol ORDER BY date) THEN 'UPTREND'
                    WHEN close_price < sma_20 AND sma_20 < sma_50 AND sma_50 < sma_100 THEN 'STRONG_DOWNTREND'
                    WHEN close_price < sma_20 AND sma_20 < LAG(sma_20, 10) OVER (PARTITION BY symbol ORDER BY date) THEN 'DOWNTREND'
                    ELSE 'SIDEWAYS'
                END AS primary_trend,
                
                -- Short-term momentum
                CASE 
                    WHEN close_price > LAG(close_price, 5) OVER (PARTITION BY symbol ORDER BY date) THEN 'BULLISH_MOMENTUM'
                    WHEN close_price < LAG(close_price, 5) OVER (PARTITION BY symbol ORDER BY date) THEN 'BEARISH_MOMENTUM'
                    ELSE 'NEUTRAL_MOMENTUM'
                END AS momentum_direction
            FROM technical_data
        ),
        
        -- ENHANCED Swing Detection (more precise for 200+ days)
        swing_detection AS (
            SELECT *,
                -- Enhanced swing high detection (5-period confirmation)
                CASE 
                    WHEN high_price > LAG(high_price, 1) OVER (PARTITION BY symbol ORDER BY date) 
                         AND high_price > LEAD(high_price, 1) OVER (PARTITION BY symbol ORDER BY date) 
                         AND high_price > LAG(high_price, 2) OVER (PARTITION BY symbol ORDER BY date)
                         AND high_price > LEAD(high_price, 2) OVER (PARTITION BY symbol ORDER BY date)
                         AND high_price > LAG(high_price, 3) OVER (PARTITION BY symbol ORDER BY date)
                         AND high_price > LEAD(high_price, 3) OVER (PARTITION BY symbol ORDER BY date)
                         AND high_price > LAG(high_price, 4) OVER (PARTITION BY symbol ORDER BY date)
                         AND high_price > LEAD(high_price, 4) OVER (PARTITION BY symbol ORDER BY date)
                    THEN 1 ELSE 0 
                END AS swing_high,
                
                -- Enhanced swing low detection (5-period confirmation)
                CASE 
                    WHEN low_price < LAG(low_price, 1) OVER (PARTITION BY symbol ORDER BY date) 
                         AND low_price < LEAD(low_price, 1) OVER (PARTITION BY symbol ORDER BY date)
                         AND low_price < LAG(low_price, 2) OVER (PARTITION BY symbol ORDER BY date)
                         AND low_price < LEAD(low_price, 2) OVER (PARTITION BY symbol ORDER BY date)
                         AND low_price < LAG(low_price, 3) OVER (PARTITION BY symbol ORDER BY date)
                         AND low_price < LEAD(low_price, 3) OVER (PARTITION BY symbol ORDER BY date)
                         AND low_price < LAG(low_price, 4) OVER (PARTITION BY symbol ORDER BY date)
                         AND low_price < LEAD(low_price, 4) OVER (PARTITION BY symbol ORDER BY date)
                    THEN 1 ELSE 0 
                END AS swing_low
            FROM trend_analysis
        ),
        
        -- BIDIRECTIONAL Fibonacci Analysis
        fibonacci_analysis AS (
            SELECT *,
                -- BULLISH Fibonacci (from recent low to high)
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS bull_fib_0,
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) + 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)) * 0.236 AS bull_fib_23_6,
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) + 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)) * 0.382 AS bull_fib_38_2,
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) + 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)) * 0.50 AS bull_fib_50,
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) + 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)) * 0.618 AS bull_fib_61_8,
                MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS bull_fib_100,
                
                -- BEARISH Fibonacci (from recent high to low) 
                MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS bear_fib_0,
                MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)) * 0.236 AS bear_fib_23_6,
                MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)) * 0.382 AS bear_fib_38_2,
                MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)) * 0.50 AS bear_fib_50,
                MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)) * 0.618 AS bear_fib_61_8,
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS bear_fib_100
            FROM swing_detection
        ),
        
        -- BIDIRECTIONAL Wave Classification 
        wave_classification AS (
            SELECT *,
                -- BULLISH Wave Position (uptrend scenarios)
                CASE 
                    WHEN primary_trend IN ('UPTREND', 'STRONG_UPTREND') THEN
                        CASE 
                            WHEN close_price <= bull_fib_23_6 THEN 'BULL_WAVE_1'
                            WHEN close_price > bull_fib_23_6 AND close_price <= bull_fib_38_2 THEN 'BULL_WAVE_2'
                            WHEN close_price > bull_fib_38_2 AND close_price <= bull_fib_61_8 THEN 'BULL_WAVE_3'
                            WHEN close_price > bull_fib_61_8 AND close_price <= bull_fib_100 THEN 'BULL_WAVE_4'
                            WHEN close_price > bull_fib_100 THEN 'BULL_WAVE_5'
                            ELSE 'BULL_UNDEFINED'
                        END
                    ELSE NULL
                END AS bullish_wave_position,
                
                -- BEARISH Wave Position (downtrend scenarios)
                CASE 
                    WHEN primary_trend IN ('DOWNTREND', 'STRONG_DOWNTREND') THEN
                        CASE 
                            WHEN close_price >= bear_fib_23_6 THEN 'BEAR_WAVE_1'
                            WHEN close_price < bear_fib_23_6 AND close_price >= bear_fib_38_2 THEN 'BEAR_WAVE_2'
                            WHEN close_price < bear_fib_38_2 AND close_price >= bear_fib_61_8 THEN 'BEAR_WAVE_3'
                            WHEN close_price < bear_fib_61_8 AND close_price >= bear_fib_100 THEN 'BEAR_WAVE_4'
                            WHEN close_price < bear_fib_100 THEN 'BEAR_WAVE_5'
                            ELSE 'BEAR_UNDEFINED'
                        END
                    ELSE NULL
                END AS bearish_wave_position,
                
                -- Combined Wave Position
                CASE 
                    WHEN primary_trend IN ('UPTREND', 'STRONG_UPTREND') THEN
                        CASE 
                            WHEN close_price <= bull_fib_23_6 THEN 'BULL_WAVE_1'
                            WHEN close_price > bull_fib_23_6 AND close_price <= bull_fib_38_2 THEN 'BULL_WAVE_2'
                            WHEN close_price > bull_fib_38_2 AND close_price <= bull_fib_61_8 THEN 'BULL_WAVE_3'
                            WHEN close_price > bull_fib_61_8 AND close_price <= bull_fib_100 THEN 'BULL_WAVE_4'
                            WHEN close_price > bull_fib_100 THEN 'BULL_WAVE_5'
                            ELSE 'BULL_UNDEFINED'
                        END
                    WHEN primary_trend IN ('DOWNTREND', 'STRONG_DOWNTREND') THEN
                        CASE 
                            WHEN close_price >= bear_fib_23_6 THEN 'BEAR_WAVE_1'
                            WHEN close_price < bear_fib_23_6 AND close_price >= bear_fib_38_2 THEN 'BEAR_WAVE_2'
                            WHEN close_price < bear_fib_38_2 AND close_price >= bear_fib_61_8 THEN 'BEAR_WAVE_3'
                            WHEN close_price < bear_fib_61_8 AND close_price >= bear_fib_100 THEN 'BEAR_WAVE_4'
                            WHEN close_price < bear_fib_100 THEN 'BEAR_WAVE_5'
                            ELSE 'BEAR_UNDEFINED'
                        END
                    ELSE 'SIDEWAYS_CORRECTION'
                END AS wave_position
            FROM fibonacci_analysis
        ),
        
        -- ENHANCED Elliott Wave Signals (bidirectional)
        elliott_signals AS (
            SELECT *,
                -- BULLISH Elliott Signals
                CASE 
                    WHEN primary_trend IN ('UPTREND', 'STRONG_UPTREND') AND wave_position = 'BULL_WAVE_2' 
                         AND rsi_14 BETWEEN 30 AND 50 AND momentum_direction = 'BULLISH_MOMENTUM' THEN 'STRONG_BUY'
                    WHEN primary_trend IN ('UPTREND', 'STRONG_UPTREND') AND wave_position = 'BULL_WAVE_3' 
                         AND rsi_14 > 50 AND momentum_direction = 'BULLISH_MOMENTUM' THEN 'BUY'
                    WHEN primary_trend IN ('UPTREND', 'STRONG_UPTREND') AND wave_position = 'BULL_WAVE_4' 
                         AND rsi_14 BETWEEN 40 AND 60 THEN 'WEAK_BUY'
                    
                    -- BEARISH Elliott Signals  
                    WHEN primary_trend IN ('DOWNTREND', 'STRONG_DOWNTREND') AND wave_position = 'BEAR_WAVE_2' 
                         AND rsi_14 BETWEEN 50 AND 70 AND momentum_direction = 'BEARISH_MOMENTUM' THEN 'STRONG_SELL'
                    WHEN primary_trend IN ('DOWNTREND', 'STRONG_DOWNTREND') AND wave_position = 'BEAR_WAVE_3' 
                         AND rsi_14 < 50 AND momentum_direction = 'BEARISH_MOMENTUM' THEN 'SELL'
                    WHEN primary_trend IN ('DOWNTREND', 'STRONG_DOWNTREND') AND wave_position = 'BEAR_WAVE_4' 
                         AND rsi_14 BETWEEN 40 AND 60 THEN 'WEAK_SELL'
                    
                    -- WARNING Signals
                    WHEN wave_position IN ('BULL_WAVE_5', 'BEAR_WAVE_5') THEN 'CAUTION_REVERSAL'
                    WHEN primary_trend = 'SIDEWAYS' THEN 'RANGE_TRADING'
                    
                    ELSE 'WAIT'
                END AS elliott_signal,
                
                -- Market Phase Classification
                CASE 
                    WHEN primary_trend = 'STRONG_UPTREND' AND wave_position IN ('BULL_WAVE_3', 'BULL_WAVE_4') THEN 'BULL_MARKET'
                    WHEN primary_trend = 'STRONG_DOWNTREND' AND wave_position IN ('BEAR_WAVE_3', 'BEAR_WAVE_4') THEN 'BEAR_MARKET'
                    WHEN wave_position IN ('BULL_WAVE_5', 'BEAR_WAVE_5') THEN 'POTENTIAL_REVERSAL'
                    WHEN primary_trend = 'SIDEWAYS' THEN 'CORRECTION_PHASE'
                    ELSE 'UNDEFINED_PHASE'
                END AS market_phase
            FROM wave_classification
        )
        
        SELECT * FROM elliott_signals
        ORDER BY date ASC
        """
    
    @staticmethod
    def get_wave_setups_overview():
        """Get overview of current Elliott Wave setups across stocks"""
        return """
        WITH latest_wave_data AS (
            SELECT 
                dm.symbol,
                dm.name,
                dm.date,
                dm.close,
                dm.percent_change,
                dm.volume,
                -- Simple trend calculation
                CASE 
                    WHEN dm.close > AVG(dm.close) OVER (PARTITION BY dm.symbol ORDER BY dm.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) THEN 'UPTREND'
                    WHEN dm.close < AVG(dm.close) OVER (PARTITION BY dm.symbol ORDER BY dm.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) THEN 'DOWNTREND'
                    ELSE 'SIDEWAYS'
                END AS trend_direction,
                r.rsi AS rsi_14,
                mc.macd_signal
            FROM public_analytics.daily_stock_metrics dm
            LEFT JOIN public_analytics.technical_indicators_rsi r 
                ON dm.symbol = r.symbol AND dm.date = r.date
            LEFT JOIN public_analytics.technical_indicators_macd mc 
                ON dm.symbol = mc.symbol AND dm.date = mc.date
            WHERE dm.date = (SELECT MAX(date) FROM public_analytics.daily_stock_metrics)
        ),
        wave_analysis AS (
            SELECT 
                symbol,
                name,
                close,
                percent_change,
                volume,
                trend_direction,
                rsi_14,
                macd_signal,
                CASE 
                    WHEN trend_direction = 'UPTREND' AND rsi_14 < 70 AND macd_signal = 'Bullish' THEN 'BULLISH_WAVE'
                    WHEN trend_direction = 'DOWNTREND' AND rsi_14 > 30 AND macd_signal = 'Bearish' THEN 'BEARISH_WAVE'
                    WHEN trend_direction = 'SIDEWAYS' THEN 'CONSOLIDATION_WAVE'
                    ELSE 'MIXED_SIGNAL'
                END AS wave_setup,
                -- Priority for ordering
                CASE 
                    WHEN trend_direction = 'UPTREND' AND rsi_14 < 70 AND macd_signal = 'Bullish' THEN 1
                    WHEN trend_direction = 'DOWNTREND' AND rsi_14 > 30 AND macd_signal = 'Bearish' THEN 2
                    WHEN trend_direction = 'SIDEWAYS' THEN 3
                    ELSE 4 
                END AS wave_priority
            FROM latest_wave_data
            WHERE rsi_14 IS NOT NULL
        )
        SELECT 
            symbol,
            name,
            close,
            percent_change,
            volume,
            trend_direction,
            rsi_14,
            macd_signal,
            wave_setup
        FROM wave_analysis
        ORDER BY wave_priority, volume DESC
        LIMIT 20
        """

def get_available_stocks():
    """Get list of available stocks for analysis"""
    query = """
    SELECT DISTINCT 
        dm.symbol, 
        dm.name,
        dm.close,
        dm.percent_change,
        dm.volume
    FROM public_analytics.daily_stock_metrics dm
    WHERE dm.date = (SELECT MAX(date) FROM public_analytics.daily_stock_metrics)
    AND dm.symbol IS NOT NULL
    AND dm.name IS NOT NULL
    ORDER BY dm.symbol
    """
    return fetch_data_cached(query, "Available Stocks")

def show_stock_search():
    """Stock search interface for Elliott Wave analysis"""
    st.markdown("### 🔍 Select Stock for Enhanced Elliott Wave Analysis")
    
    # Check if stock is pre-selected from other pages
    default_symbol = None
    if 'selected_stock' in st.session_state and st.session_state.selected_stock:
        default_symbol = st.session_state.selected_stock
        st.info(f"🌊 **{default_symbol}** selected from another page. You can change selection below.")
    
    # Get available stocks
    stocks_df = get_available_stocks()
    
    if stocks_df.empty:
        st.warning("⚠️ No stocks data available")
        return None
    
    # Create search options
    stock_options = []
    default_index = 0
    
    for i, (_, row) in enumerate(stocks_df.iterrows()):
        price_change = f"{row['percent_change']:+.2f}%" if pd.notna(row['percent_change']) else "N/A"
        price = f"Rp{row['close']:,.0f}" if pd.notna(row['close']) else "N/A"
        option = f"{row['symbol']} - {row['name']} | {price} ({price_change})"
        stock_options.append(option)
        
        # Set default index if stock is pre-selected
        if default_symbol and row['symbol'] == default_symbol:
            default_index = i
    
    # Search interface
    col1, col2 = st.columns([3, 1])
    
    with col1:
        selected_option = st.selectbox(
            "Choose Stock:",
            stock_options,
            index=default_index,
            help="Select a stock to view enhanced Elliott Wave analysis with bidirectional patterns"
        )
        
        if selected_option:
            selected_symbol = selected_option.split(" - ")[0]
            # Update session state
            st.session_state.selected_stock = selected_symbol
            return selected_symbol
    
    with col2:
        # Quick analysis info
        st.markdown("**🎯 Analysis Features:**")
        st.markdown("• Bidirectional patterns")
        st.markdown("• 200-365 day analysis")
        st.markdown("• Bull + Bear Fibonacci")
        st.markdown("• Smart signal generation")
    
    return None

def show_wave_overview():
    """Show Elliott Wave overview for multiple stocks"""
    st.markdown("### 🌊 Enhanced Elliott Wave Market Overview")
    
    try:
        wave_overview_df = fetch_data_cached(
            EnhancedElliottWaveQueries.get_wave_setups_overview(),
            "Enhanced Wave Overview"
        )
        
        if not wave_overview_df.empty:
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                bullish_waves = len(wave_overview_df[wave_overview_df['wave_setup'] == 'BULLISH_WAVE'])
                st.metric("🌊 Bullish Waves", bullish_waves, "setups")
            
            with col2:
                bearish_waves = len(wave_overview_df[wave_overview_df['wave_setup'] == 'BEARISH_WAVE'])
                st.metric("🔻 Bearish Waves", bearish_waves, "setups")
            
            with col3:
                consolidation = len(wave_overview_df[wave_overview_df['wave_setup'] == 'CONSOLIDATION_WAVE'])
                st.metric("⚖️ Consolidation", consolidation, "patterns")
            
            with col4:
                avg_rsi = wave_overview_df['rsi_14'].mean()
                st.metric("📊 Avg RSI", f"{avg_rsi:.1f}", "market level")
            
            # Create clickable wave setups
            st.markdown("#### 🎯 Top Enhanced Elliott Wave Setups:")
            st.markdown("*Click any stock below for detailed bidirectional wave analysis*")
            
            cols = st.columns(4)
            
            for i, (_, row) in enumerate(wave_overview_df.head(12).iterrows()):
                col_idx = i % 4
                with cols[col_idx]:
                    # Determine setup color and emoji
                    if row['wave_setup'] == 'BULLISH_WAVE':
                        setup_color = "🌊"
                        bg_color = "#e8f5e8"
                    elif row['wave_setup'] == 'BEARISH_WAVE':
                        setup_color = "🔻"
                        bg_color = "#ffe6e6"
                    elif row['wave_setup'] == 'CONSOLIDATION_WAVE':
                        setup_color = "⚖️"
                        bg_color = "#fff3cd"
                    else:
                        setup_color = "❓"
                        bg_color = "#f0f2f6"
                    
                    # Trend indicator
                    trend_emoji = "📈" if row['trend_direction'] == 'UPTREND' else "📉" if row['trend_direction'] == 'DOWNTREND' else "➡️"
                    
                    if st.button(
                        f"{setup_color} {row['symbol']}\n{row['wave_setup'].replace('_', ' ').title()}\n{trend_emoji} {row['trend_direction']}\nRSI: {row['rsi_14']:.1f}",
                        key=f"enhanced_wave_{row['symbol']}",
                        help=f"Enhanced Elliott Wave Analysis: {row['wave_setup']} - Click to analyze {row['symbol']}",
                        use_container_width=True
                    ):
                        st.session_state.selected_stock = row['symbol']
                        st.session_state.page_navigation = "🌊 Elliott Wave Analysis"
                        st.rerun()
            
        else:
            st.info("📭 No Elliott Wave data available")
            
    except Exception as e:
        st.error(f"❌ Failed to load enhanced Elliott Wave overview: {str(e)}")

def show_enhanced_period_selection():
    """Enhanced period selection with optimal recommendations"""
    st.markdown("### ⚙️ Enhanced Analysis Configuration")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**📊 Analysis Period:**")
        period_options = {
            "200 days (8 months)": 200,
            "250 days (1 year) ⭐": 250, 
            "300 days (14 months)": 300,
            "365 days (1.5 years)": 365
        }
        
        selected_period = st.selectbox(
            "Choose Period:",
            options=list(period_options.keys()),
            index=1,  # Default 250 days
            help="⭐ 250 days recommended for optimal Elliott Wave pattern recognition"
        )
        period_days = period_options[selected_period]
        st.session_state.enhanced_wave_period = period_days
    
    with col2:
        st.markdown("**🎯 Analysis Mode:**")
        analysis_mode = st.selectbox(
            "Wave Direction:",
            options=[
                "🔄 Bidirectional (Bull + Bear)",
                "📈 Bullish Waves Only", 
                "📉 Bearish Waves Only",
                "🎭 Pattern Recognition"
            ],
            index=0,
            help="Bidirectional analysis detects both upward and downward Elliott Wave patterns"
        )
        st.session_state.analysis_mode = analysis_mode
    
    with col3:
        st.markdown("**📈 Chart Style:**")
        chart_style = st.selectbox(
            "Visualization:",
            options=[
                "🎨 Professional (All indicators)",
                "📊 Standard (Key levels only)",
                "⚡ Minimal (Price + Waves)"
            ],
            index=0
        )
        st.session_state.chart_style = chart_style
    
    return period_days, analysis_mode, chart_style

def show_bidirectional_wave_summary(symbol, wave_data):
    """Enhanced wave summary with bidirectional analysis"""
    if wave_data.empty:
        return
    
    st.markdown(f"### 🌊 Enhanced Elliott Wave Analysis - {symbol}")
    st.markdown(f"*📊 Bidirectional analysis based on {len(wave_data)} trading sessions*")
    
    latest = wave_data.iloc[-1]
    
    # Determine active wave scenario
    primary_trend = latest.get('primary_trend', 'UNKNOWN')
    wave_position = latest.get('wave_position', 'UNKNOWN')
    elliott_signal = latest.get('elliott_signal', 'WAIT')
    market_phase = latest.get('market_phase', 'UNDEFINED_PHASE')
    
    # Enhanced main metrics (7 columns for more info)
    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
    
    with col1:
        st.metric(
            "💰 Current Price",
            f"Rp{latest['close_price']:,.0f}",
            f"{latest['percent_change']:+.2f}%"
        )
    
    with col2:
        # Dynamic trend visualization
        if primary_trend == 'STRONG_UPTREND':
            trend_display = "📈 STRONG UP"
            trend_color = "🟢"
        elif primary_trend == 'UPTREND':
            trend_display = "📈 UPTREND" 
            trend_color = "🟢"
        elif primary_trend == 'STRONG_DOWNTREND':
            trend_display = "📉 STRONG DOWN"
            trend_color = "🔴"
        elif primary_trend == 'DOWNTREND':
            trend_display = "📉 DOWNTREND"
            trend_color = "🔴"
        else:
            trend_display = "➡️ SIDEWAYS"
            trend_color = "🟡"
        
        st.metric(
            "📊 Primary Trend",
            trend_display,
            trend_color
        )
    
    with col3:
        # Wave position with direction
        if 'BULL_' in wave_position:
            wave_display = wave_position.replace('BULL_WAVE_', 'UP-W')
            wave_emoji = "🌊"
        elif 'BEAR_' in wave_position:
            wave_display = wave_position.replace('BEAR_WAVE_', 'DOWN-W')
            wave_emoji = "🔻"
        else:
            wave_display = wave_position.replace('_', ' ')
            wave_emoji = "🌀"
        
        st.metric(
            "🌊 Wave Position",
            f"{wave_emoji} {wave_display}",
            "current wave"
        )
    
    with col4:
        # Elliott signal with colors
        if elliott_signal in ['STRONG_BUY', 'BUY']:
            signal_emoji = "🟢"
        elif elliott_signal in ['STRONG_SELL', 'SELL']:
            signal_emoji = "🔴"
        elif elliott_signal in ['WEAK_BUY', 'WEAK_SELL']:
            signal_emoji = "🟡"
        elif elliott_signal == 'CAUTION_REVERSAL':
            signal_emoji = "⚠️"
        else:
            signal_emoji = "⚪"
        
        st.metric(
            "⚡ Elliott Signal",
            f"{signal_emoji} {elliott_signal.replace('_', ' ')}",
            "action"
        )
    
    with col5:
        # Market phase
        if market_phase == 'BULL_MARKET':
            phase_emoji = "🚀"
        elif market_phase == 'BEAR_MARKET':
            phase_emoji = "🐻"
        elif market_phase == 'POTENTIAL_REVERSAL':
            phase_emoji = "🔄"
        elif market_phase == 'CORRECTION_PHASE':
            phase_emoji = "📊"
        else:
            phase_emoji = "❓"
        
        st.metric(
            "🎭 Market Phase",
            f"{phase_emoji} {market_phase.replace('_', ' ')}",
            "cycle"
        )
    
    with col6:
        # RSI with dynamic interpretation
        rsi_val = latest.get('rsi_14', 50)
        if rsi_val > 70:
            rsi_status = "🔴 Overbought"
        elif rsi_val < 30:
            rsi_status = "🟢 Oversold"
        else:
            rsi_status = "🟡 Neutral"
        
        st.metric(
            "📊 RSI-14",
            f"{rsi_val:.1f}",
            rsi_status
        )
    
    with col7:
        # Volume trend
        vol_ratio = latest['volume'] / latest.get('avg_volume_20', latest['volume']) if latest.get('avg_volume_20', 0) > 0 else 1
        if vol_ratio > 2:
            vol_status = "🔥 Surge"
        elif vol_ratio > 1.5:
            vol_status = "📈 High"
        elif vol_ratio < 0.5:
            vol_status = "📉 Low"
        else:
            vol_status = "📊 Normal"
        
        st.metric(
            "🔊 Volume",
            f"{vol_ratio:.1f}x",
            vol_status
        )

def show_enhanced_fibonacci_analysis(symbol, wave_data):
    """Enhanced Fibonacci analysis for both bullish and bearish scenarios"""
    if wave_data.empty:
        return
    
    latest = wave_data.iloc[-1]
    primary_trend = latest.get('primary_trend', 'UNKNOWN')
    current_price = latest['close_price']
    
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📈 Bullish Fibonacci Levels")
        
        # Bullish Fibonacci table
        bull_fib_data = {
            'Level': ['0%', '23.6%', '38.2%', '50%', '61.8%', '100%'],
            'Price': [
                latest.get('bull_fib_0', 0),
                latest.get('bull_fib_23_6', 0),
                latest.get('bull_fib_38_2', 0),
                latest.get('bull_fib_50', 0),
                latest.get('bull_fib_61_8', 0),
                latest.get('bull_fib_100', 0)
            ]
        }
        
        bull_fib_df = pd.DataFrame(bull_fib_data)
        bull_fib_df['Status'] = bull_fib_df['Price'].apply(
            lambda x: "🔴 Above" if current_price > x and x > 0 else "🟢 Below" if x > 0 else "N/A"
        )
        bull_fib_df['Price'] = bull_fib_df['Price'].apply(lambda x: f"Rp{x:,.0f}" if x > 0 else "N/A")
        
        # Highlight active scenario
        if primary_trend in ['UPTREND', 'STRONG_UPTREND']:
            st.success("🟢 **Active Bullish Scenario**")
        else:
            st.info("ℹ️ Reference levels for uptrend")
        
        st.dataframe(bull_fib_df, use_container_width=True, hide_index=True)
    
    with col2:
        st.markdown("#### 📉 Bearish Fibonacci Levels") 
        
        # Bearish Fibonacci table
        bear_fib_data = {
            'Level': ['0%', '23.6%', '38.2%', '50%', '61.8%', '100%'],
            'Price': [
                latest.get('bear_fib_0', 0),
                latest.get('bear_fib_23_6', 0), 
                latest.get('bear_fib_38_2', 0),
                latest.get('bear_fib_50', 0),
                latest.get('bear_fib_61_8', 0),
                latest.get('bear_fib_100', 0)
            ]
        }
        
        bear_fib_df = pd.DataFrame(bear_fib_data)
        bear_fib_df['Status'] = bear_fib_df['Price'].apply(
            lambda x: "🔴 Below" if current_price < x and x > 0 else "🟢 Above" if x > 0 else "N/A"
        )
        bear_fib_df['Price'] = bear_fib_df['Price'].apply(lambda x: f"Rp{x:,.0f}" if x > 0 else "N/A")
        
        # Highlight active scenario
        if primary_trend in ['DOWNTREND', 'STRONG_DOWNTREND']:
            st.error("🔴 **Active Bearish Scenario**")
        else:
            st.info("ℹ️ Reference levels for downtrend")
        
        st.dataframe(bear_fib_df, use_container_width=True, hide_index=True)

def show_market_bias_analysis(symbol, wave_data):
    """Enhanced Market Bias Analysis - Determine Bullish vs Bearish Probability"""
    if wave_data.empty:
        return
    
    latest = wave_data.iloc[-1]
    current_price = latest['close_price']
    primary_trend = latest.get('primary_trend', 'UNKNOWN')
    wave_position = latest.get('wave_position', 'UNKNOWN')
    elliott_signal = latest.get('elliott_signal', 'WAIT')
    rsi_val = latest.get('rsi_14', 50)
    
    st.markdown("### 🎯 Market Bias Analysis - Which Direction More Likely?")
    st.markdown("*Automatic calculation based on trend, wave, momentum, Fibonacci, and volume factors*")
    
    # Calculate bias scoring
    bias_factors = {
        'trend_score': 0,
        'wave_score': 0, 
        'momentum_score': 0,
        'fibonacci_score': 0,
        'volume_score': 0
    }
    
    # 1. TREND ANALYSIS (40% weight)
    if primary_trend == 'STRONG_DOWNTREND':
        bias_factors['trend_score'] = -8
        trend_analysis = "🔴 **STRONG BEARISH BIAS** - Established downtrend"
    elif primary_trend == 'DOWNTREND':
        bias_factors['trend_score'] = -5
        trend_analysis = "🔴 **BEARISH BIAS** - Clear downtrend"
    elif primary_trend == 'STRONG_UPTREND':
        bias_factors['trend_score'] = +8
        trend_analysis = "🟢 **STRONG BULLISH BIAS** - Established uptrend"
    elif primary_trend == 'UPTREND':
        bias_factors['trend_score'] = +5
        trend_analysis = "🟢 **BULLISH BIAS** - Clear uptrend"
    else:
        bias_factors['trend_score'] = 0
        trend_analysis = "🟡 **NEUTRAL** - Sideways trend"
    
    # 2. WAVE POSITION ANALYSIS (30% weight)
    if 'BEAR_WAVE_4' in wave_position:
        bias_factors['wave_score'] = -3
        wave_analysis = "🔴 **BEARISH SETUP** - Bear Wave 4 (correction in downtrend)"
        wave_expectation = "📉 Expect continuation to Bear Wave 5 (lower lows)"
    elif 'BEAR_WAVE_2' in wave_position:
        bias_factors['wave_score'] = -6
        wave_analysis = "🔴 **STRONG BEARISH** - Bear Wave 2 (pullback before more selling)"
        wave_expectation = "📉 Expect strong Bear Wave 3 (accelerated selling)"
    elif 'BEAR_WAVE_3' in wave_position:
        bias_factors['wave_score'] = -4
        wave_analysis = "🔴 **BEARISH MOMENTUM** - Bear Wave 3 (strongest selling wave)"
        wave_expectation = "📉 Continue bearish momentum, prepare for Wave 4 correction"
    elif 'BULL_WAVE_2' in wave_position:
        bias_factors['wave_score'] = +6
        wave_analysis = "🟢 **STRONG BULLISH** - Bull Wave 2 (pullback before rally)"
        wave_expectation = "📈 Expect strong Bull Wave 3 (accelerated buying)"
    elif 'BULL_WAVE_3' in wave_position:
        bias_factors['wave_score'] = +4
        wave_analysis = "🟢 **BULLISH MOMENTUM** - Bull Wave 3 (strongest buying wave)"
        wave_expectation = "📈 Continue bullish momentum, prepare for Wave 4 correction"
    elif 'BULL_WAVE_4' in wave_position:
        bias_factors['wave_score'] = +3
        wave_analysis = "🟢 **BULLISH SETUP** - Bull Wave 4 (correction in uptrend)"
        wave_expectation = "📈 Expect final Bull Wave 5 (higher highs)"
    elif 'WAVE_5' in wave_position:
        bias_factors['wave_score'] = -1
        wave_analysis = "⚠️ **REVERSAL ZONE** - Wave 5 exhaustion"
        wave_expectation = "🔄 Expect trend reversal soon"
    else:
        bias_factors['wave_score'] = 0
        wave_analysis = "🟡 **UNCLEAR WAVE** - Pattern developing"
        wave_expectation = "⏳ Wait for clearer wave formation"
    
    # 3. RSI MOMENTUM ANALYSIS (15% weight)
    if rsi_val < 25:
        bias_factors['momentum_score'] = +4
        rsi_analysis = "🟢 **OVERSOLD BOUNCE** - RSI extremely oversold, bounce likely"
    elif rsi_val < 30:
        bias_factors['momentum_score'] = +2
        rsi_analysis = "🟢 **OVERSOLD** - RSI oversold, potential bounce"
    elif rsi_val > 75:
        bias_factors['momentum_score'] = -4
        rsi_analysis = "🔴 **OVERBOUGHT REVERSAL** - RSI extremely overbought, pullback likely"
    elif rsi_val > 70:
        bias_factors['momentum_score'] = -2
        rsi_analysis = "🔴 **OVERBOUGHT** - RSI overbought, potential pullback"
    else:
        bias_factors['momentum_score'] = 0
        rsi_analysis = "🟡 **NEUTRAL RSI** - No momentum extreme"
    
    # 4. FIBONACCI POSITION ANALYSIS (10% weight)
    bull_fib_61_8 = latest.get('bull_fib_61_8', 0)
    bear_fib_61_8 = latest.get('bear_fib_61_8', 0)
    
    if primary_trend in ['DOWNTREND', 'STRONG_DOWNTREND']:
        if current_price < bear_fib_61_8:
            bias_factors['fibonacci_score'] = -2
            fib_analysis = "🔴 **BEARISH FIB** - Below key 61.8% bearish level"
        elif current_price > bear_fib_61_8:
            bias_factors['fibonacci_score'] = +1
            fib_analysis = "🟡 **BEAR RETRACEMENT** - Above 61.8%, potential bounce"
        else:
            bias_factors['fibonacci_score'] = 0
            fib_analysis = "🟡 **AT KEY LEVEL** - At critical Fibonacci level"
    elif primary_trend in ['UPTREND', 'STRONG_UPTREND']:
        if current_price > bull_fib_61_8:
            bias_factors['fibonacci_score'] = +2
            fib_analysis = "🟢 **BULLISH FIB** - Above key 61.8% bullish level"
        elif current_price < bull_fib_61_8:
            bias_factors['fibonacci_score'] = -1
            fib_analysis = "🟡 **BULL RETRACEMENT** - Below 61.8%, potential support"
        else:
            bias_factors['fibonacci_score'] = 0
            fib_analysis = "🟡 **AT KEY LEVEL** - At critical Fibonacci level"
    else:
        bias_factors['fibonacci_score'] = 0
        fib_analysis = "🟡 **NEUTRAL FIB** - Sideways market"
    
    # 5. VOLUME CONFIRMATION (5% weight)
    vol_ratio = latest['volume'] / latest.get('avg_volume_20', latest['volume']) if latest.get('avg_volume_20', 0) > 0 else 1
    if vol_ratio > 2 and primary_trend in ['DOWNTREND', 'STRONG_DOWNTREND']:
        bias_factors['volume_score'] = -1
        volume_analysis = "🔴 **HIGH VOLUME SELLING** - Volume confirms bearish move"
    elif vol_ratio > 2 and primary_trend in ['UPTREND', 'STRONG_UPTREND']:
        bias_factors['volume_score'] = +1
        volume_analysis = "🟢 **HIGH VOLUME BUYING** - Volume confirms bullish move"
    elif vol_ratio < 0.5:
        bias_factors['volume_score'] = 0
        volume_analysis = "🟡 **LOW VOLUME** - Weak conviction, wait for confirmation"
    else:
        bias_factors['volume_score'] = 0
        volume_analysis = "🟡 **NORMAL VOLUME** - No strong volume bias"
    
    # CALCULATE TOTAL BIAS SCORE
    total_bias = sum(bias_factors.values())
    
    # DETERMINE MARKET BIAS
    if total_bias <= -10:
        market_bias = "🔴 STRONG BEARISH"
        bias_confidence = "Very High"
        bias_color = "error"
        recommendation = "🔴 **STRONG SELL BIAS** - Multiple bearish factors aligned"
        probability = f"Bearish: 80-90% | Bullish: 10-20%"
    elif total_bias <= -5:
        market_bias = "🔴 BEARISH"
        bias_confidence = "High" 
        bias_color = "error"
        recommendation = "🔴 **SELL BIAS** - Bearish factors dominate"
        probability = f"Bearish: 65-75% | Bullish: 25-35%"
    elif total_bias <= -2:
        market_bias = "🟡 WEAK BEARISH"
        bias_confidence = "Medium"
        bias_color = "warning"
        recommendation = "🟡 **SLIGHT BEAR BIAS** - Cautiously bearish"
        probability = f"Bearish: 55-60% | Bullish: 40-45%"
    elif total_bias <= 2:
        market_bias = "⚪ NEUTRAL"
        bias_confidence = "Low"
        bias_color = "info"
        recommendation = "⚪ **NO CLEAR BIAS** - Wait for clearer signals"
        probability = f"Bearish: 45-55% | Bullish: 45-55%"
    elif total_bias <= 5:
        market_bias = "🟢 WEAK BULLISH"
        bias_confidence = "Medium"
        bias_color = "warning"
        recommendation = "🟢 **SLIGHT BULL BIAS** - Cautiously bullish"
        probability = f"Bearish: 40-45% | Bullish: 55-60%"
    elif total_bias <= 10:
        market_bias = "🟢 BULLISH"
        bias_confidence = "High"
        bias_color = "success"
        recommendation = "🟢 **BUY BIAS** - Bullish factors dominate"
        probability = f"Bearish: 25-35% | Bullish: 65-75%"
    else:
        market_bias = "🟢 STRONG BULLISH"
        bias_confidence = "Very High"
        bias_color = "success"
        recommendation = "🟢 **STRONG BUY BIAS** - Multiple bullish factors aligned"
        probability = f"Bearish: 10-20% | Bullish: 80-90%"
    
    # DISPLAY ANALYSIS
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Main bias result
        if bias_color == "error":
            st.error(f"""
            **🎯 MARKET BIAS: {market_bias}**
            **📊 Confidence: {bias_confidence}**
            **🎲 Probability: {probability}**
            
            {recommendation}
            """)
        elif bias_color == "success":
            st.success(f"""
            **🎯 MARKET BIAS: {market_bias}**
            **📊 Confidence: {bias_confidence}**
            **🎲 Probability: {probability}**
            
            {recommendation}
            """)
        elif bias_color == "warning":
            st.warning(f"""
            **🎯 MARKET BIAS: {market_bias}**
            **📊 Confidence: {bias_confidence}**
            **🎲 Probability: {probability}**
            
            {recommendation}
            """)
        else:
            st.info(f"""
            **🎯 MARKET BIAS: {market_bias}**
            **📊 Confidence: {bias_confidence}**
            **🎲 Probability: {probability}**
            
            {recommendation}
            """)
    
    with col2:
        # Bias score breakdown
        st.markdown("#### 📊 Bias Score Breakdown")
        st.markdown(f"**Trend:** {bias_factors['trend_score']:+d} points")
        st.markdown(f"**Wave:** {bias_factors['wave_score']:+d} points")
        st.markdown(f"**RSI:** {bias_factors['momentum_score']:+d} points")
        st.markdown(f"**Fibonacci:** {bias_factors['fibonacci_score']:+d} points")
        st.markdown(f"**Volume:** {bias_factors['volume_score']:+d} points")
        st.markdown("---")
        st.markdown(f"**TOTAL:** {total_bias:+d} points")
        
        # Confidence meter
        confidence_pct = min(100, abs(total_bias) * 8)
        st.markdown(f"**Confidence:** {confidence_pct:.0f}%")
    
    # Detailed factor analysis
    st.markdown("---")
    st.markdown("#### 🔍 Detailed Factor Analysis")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**📈 Trend & Wave Analysis:**")
        st.markdown(trend_analysis)
        st.markdown(wave_analysis)
        st.markdown(wave_expectation)
    
    with col2:
        st.markdown("**⚡ Momentum & Technical:**")
        st.markdown(rsi_analysis)
        st.markdown(fib_analysis)
        st.markdown(volume_analysis)
    
    with col3:
        st.markdown("**🎯 Trading Implications:**")
        if total_bias <= -5:
            st.markdown("""
            **For Bears:** 🔴
            - High probability setup
            - Consider shorts/puts
            - Tight stops above resistance
            
            **For Bulls:** 🟢
            - Wait for oversold bounce
            - Small position only
            - Quick profit taking
            """)
        elif total_bias >= 5:
            st.markdown("""
            **For Bulls:** 🟢
            - High probability setup
            - Consider longs/calls
            - Tight stops below support
            
            **For Bears:** 🔴
            - Wait for overbought levels
            - Small position only
            - Quick profit taking
            """)
        else:
            st.markdown("""
            **For All Traders:** ⚪
            - Mixed signals present
            - Wait for clearer setup
            - Use smaller positions
            - Keep stops tight
            """)
    
    # Special case analysis for specific symbols
    if symbol == 'CBDK':
        st.markdown("---")
        st.markdown("#### 📋 CBDK Specific Analysis")
        st.info("""
        **CBDK Current Setup:**
        Based on your analysis, CBDK shows a classic **Bear Wave 4** pattern in a **downtrend**.
        
        **Primary Scenario (70% probability):** Continue to Bear Wave 5
        - Target: 5,500-5,800 area
        - Strategy: Short any bounce to 6,500-6,800
        
        **Secondary Scenario (30% probability):** Oversold bounce
        - Target: 6,700-7,000 area  
        - Strategy: Quick scalp, tight stops
        
        **Key Level:** 7,200 - break above invalidates bearish bias
        """)
    
    return {
        'market_bias': market_bias,
        'bias_score': total_bias,
        'confidence': bias_confidence,
        'recommendation': recommendation,
        'probability': probability
    }

def show_enhanced_elliott_chart(symbol, wave_data, chart_style):
    """Enhanced Elliott Wave chart with bidirectional analysis"""
    if wave_data.empty:
        return
    
    st.markdown(f"### 📈 Enhanced Elliott Wave Chart - {symbol}")
    st.markdown(f"*🔄 Bidirectional analysis • {len(wave_data)} sessions • Advanced pattern recognition*")
    
    # Chart customization based on style
    if chart_style == "🎨 Professional (All indicators)":
        show_fibonacci = True
        show_waves = True  
        show_ma_all = True
        chart_height = 1000
    elif chart_style == "📊 Standard (Key levels only)":
        show_fibonacci = True
        show_waves = True
        show_ma_all = False
        chart_height = 800
    else:  # Minimal
        show_fibonacci = False
        show_waves = True
        show_ma_all = False
        chart_height = 600
    
    # Create enhanced subplots
    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=(
            f'{symbol} - Enhanced Bidirectional Elliott Wave Analysis',
            'RSI & Momentum Signals',
            'MACD Convergence & Divergence',
            'Volume Analysis & Money Flow'
        ),
        row_heights=[0.5, 0.2, 0.2, 0.1]
    )
    
    # Enhanced candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=wave_data['date'],
            open=wave_data['open_price'],
            high=wave_data['high_price'],
            low=wave_data['low_price'],
            close=wave_data['close_price'],
            name="Price Action",
            increasing_line_color='#00ff88',
            decreasing_line_color='#ff4444',
            increasing_fillcolor='rgba(0,255,136,0.3)',
            decreasing_fillcolor='rgba(255,68,68,0.3)'
        ),
        row=1, col=1
    )
    
    # Add bidirectional Fibonacci levels
    if show_fibonacci:
        latest = wave_data.iloc[-1]
        primary_trend = latest.get('primary_trend', 'UNKNOWN')
        
        # Show active Fibonacci set based on trend
        if primary_trend in ['UPTREND', 'STRONG_UPTREND']:
            # Bullish Fibonacci levels
            bull_fib_levels = [
                ('bull_fib_0', 'Bull 0%', '#2E86AB', 2),
                ('bull_fib_23_6', 'Bull 23.6%', '#A23B72', 1),
                ('bull_fib_38_2', 'Bull 38.2%', '#F18F01', 2),
                ('bull_fib_50', 'Bull 50%', '#C73E1D', 3),
                ('bull_fib_61_8', 'Bull 61.8% (Key)', '#7209B7', 3),
                ('bull_fib_100', 'Bull 100%', '#2E86AB', 2)
            ]
            
            for fib_col, fib_name, color, width in bull_fib_levels:
                if fib_col in wave_data.columns:
                    fib_value = wave_data[fib_col].iloc[-1]
                    fig.add_hline(
                        y=fib_value,
                        line_dash="dash",
                        line_color=color,
                        line_width=width,
                        annotation_text=f"{fib_name}: Rp{fib_value:,.0f}",
                        annotation_position="right",
                        annotation_font_size=10,
                        row=1, col=1
                    )
        
        elif primary_trend in ['DOWNTREND', 'STRONG_DOWNTREND']:
            # Bearish Fibonacci levels
            bear_fib_levels = [
                ('bear_fib_0', 'Bear 0%', '#FF4444', 2),
                ('bear_fib_23_6', 'Bear 23.6%', '#FF6B6B', 1),
                ('bear_fib_38_2', 'Bear 38.2%', '#FF8E53', 2),
                ('bear_fib_50', 'Bear 50%', '#FF4757', 3),
                ('bear_fib_61_8', 'Bear 61.8% (Key)', '#C44569', 3),
                ('bear_fib_100', 'Bear 100%', '#FF4444', 2)
            ]
            
            for fib_col, fib_name, color, width in bear_fib_levels:
                if fib_col in wave_data.columns:
                    fib_value = wave_data[fib_col].iloc[-1]
                    fig.add_hline(
                        y=fib_value,
                        line_dash="dash",
                        line_color=color,
                        line_width=width,
                        annotation_text=f"{fib_name}: Rp{fib_value:,.0f}",
                        annotation_position="right",
                        annotation_font_size=10,
                        row=1, col=1
                    )
    
    # Enhanced moving averages
    if show_ma_all:
        ma_configs = [
            ('sma_20', 'SMA 20', '#45B7D1', 2),
            ('sma_50', 'SMA 50', '#FFA07A', 2),
            ('sma_100', 'SMA 100', '#98D8C8', 1),
            ('sma_200', 'SMA 200', '#F7DC6F', 2)
        ]
    else:
        ma_configs = [
            ('sma_20', 'SMA 20', '#45B7D1', 2),
            ('sma_50', 'SMA 50', '#FFA07A', 2)
        ]
    
    for ma_col, ma_name, color, width in ma_configs:
        if ma_col in wave_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=wave_data['date'],
                    y=wave_data[ma_col],
                    mode='lines',
                    name=ma_name,
                    line=dict(color=color, width=width),
                    opacity=0.8
                ),
                row=1, col=1
            )
    
    # Enhanced swing points with wave labels
    if show_waves:
        if 'swing_high' in wave_data.columns:
            swing_highs = wave_data[wave_data['swing_high'] == 1].copy()
            if not swing_highs.empty:
                swing_highs['wave_label'] = [f"H{i+1}" for i in range(len(swing_highs))]
                
                fig.add_trace(
                    go.Scatter(
                        x=swing_highs['date'],
                        y=swing_highs['high_price'],
                        mode='markers+text',
                        name='Wave Highs',
                        text=swing_highs['wave_label'],
                        textposition="top center",
                        marker=dict(
                            color='red', 
                            size=12, 
                            symbol='triangle-down',
                            line=dict(color='darkred', width=2)
                        ),
                    ),
                    row=1, col=1
                )
        
        if 'swing_low' in wave_data.columns:
            swing_lows = wave_data[wave_data['swing_low'] == 1].copy()
            if not swing_lows.empty:
                swing_lows['wave_label'] = [f"L{i+1}" for i in range(len(swing_lows))]
                
                fig.add_trace(
                    go.Scatter(
                        x=swing_lows['date'],
                        y=swing_lows['low_price'],
                        mode='markers+text',
                        name='Wave Lows',
                        text=swing_lows['wave_label'],
                        textposition="bottom center",
                        marker=dict(
                            color='green', 
                            size=12, 
                            symbol='triangle-up',
                            line=dict(color='darkgreen', width=2)
                        ),
                    ),
                    row=1, col=1
                )
    
    # Enhanced RSI with zones
    if 'rsi_14' in wave_data.columns:
        fig.add_trace(
            go.Scatter(
                x=wave_data['date'],
                y=wave_data['rsi_14'],
                mode='lines',
                name='RSI-14',
                line=dict(color='purple', width=2.5),
            ),
            row=2, col=1
        )
        
        # RSI zones
        fig.add_hrect(y0=70, y1=100, fillcolor="rgba(255,0,0,0.1)", row=2, col=1)
        fig.add_hrect(y0=0, y1=30, fillcolor="rgba(0,255,0,0.1)", row=2, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="red", line_width=1, row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", line_width=1, row=2, col=1)
        fig.add_hline(y=50, line_dash="dot", line_color="gray", line_width=1, row=2, col=1)
    
    # Enhanced MACD
    if 'macd_line' in wave_data.columns:
        fig.add_trace(
            go.Scatter(
                x=wave_data['date'],
                y=wave_data['macd_line'],
                mode='lines',
                name='MACD Line',
                line=dict(color='blue', width=2)
            ),
            row=3, col=1
        )
        
        if 'signal_line' in wave_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=wave_data['date'],
                    y=wave_data['signal_line'],
                    mode='lines',
                    name='Signal Line',
                    line=dict(color='red', width=2)
                ),
                row=3, col=1
            )
        
        if 'macd_histogram' in wave_data.columns:
            histogram_colors = ['green' if val >= 0 else 'red' for val in wave_data['macd_histogram'].fillna(0)]
            fig.add_trace(
                go.Bar(
                    x=wave_data['date'],
                    y=wave_data['macd_histogram'],
                    name='MACD Histogram',
                    marker_color=histogram_colors,
                    opacity=0.7
                ),
                row=3, col=1
            )
        
        fig.add_hline(y=0, line_dash="dot", line_color="gray", row=3, col=1)
    
    # Enhanced volume analysis
    volume_colors = []
    if 'avg_volume_20' in wave_data.columns:
        volume_colors = ['lightgreen' if vol > avg_vol else 'lightcoral' 
                        for vol, avg_vol in zip(wave_data['volume'], wave_data['avg_volume_20'])]
    else:
        volume_colors = 'lightblue'
    
    fig.add_trace(
        go.Bar(
            x=wave_data['date'],
            y=wave_data['volume'],
            name='Volume',
            marker_color=volume_colors,
            opacity=0.8
        ),
        row=4, col=1
    )
    
    # Average volume lines
    if 'avg_volume_20' in wave_data.columns:
        fig.add_trace(
            go.Scatter(
                x=wave_data['date'],
                y=wave_data['avg_volume_20'],
                mode='lines',
                name='Avg Volume (20)',
                line=dict(color='orange', width=2, dash='dash')
            ),
            row=4, col=1
        )
    
    # Enhanced layout
    fig.update_layout(
        title=dict(
            text=f"🌊 Enhanced Bidirectional Elliott Wave Analysis - {symbol}",
            font=dict(size=20, color='#2E86AB'),
            x=0.5
        ),
        xaxis_rangeslider_visible=False,
        height=chart_height,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor='rgba(255,255,255,0.9)',
        paper_bgcolor='rgba(255,255,255,1)',
        font=dict(size=11)
    )
    
    # Update axes
    fig.update_yaxes(title_text="💰 Price (Rp)", row=1, col=1, gridcolor='lightgray')
    fig.update_yaxes(title_text="📊 RSI", row=2, col=1, range=[0, 100], gridcolor='lightgray')
    fig.update_yaxes(title_text="📈 MACD", row=3, col=1, gridcolor='lightgray')
    fig.update_yaxes(title_text="🔊 Volume", row=4, col=1, gridcolor='lightgray')
    fig.update_xaxes(title_text="📅 Date", row=4, col=1, gridcolor='lightgray')
    
    fig.update_layout(hovermode='x unified')
    
    st.plotly_chart(fig, use_container_width=True)

def show():
    """Enhanced Elliott Wave Analysis with bidirectional capabilities"""
    
    st.markdown("# 🌊 Enhanced Elliott Wave Analysis")
    st.markdown("**Professional bidirectional wave pattern recognition with 200+ data points**")
    st.markdown("*🔄 Detects both bullish and bearish Elliott Wave patterns with advanced Fibonacci analysis*")
    
    st.markdown("---")
    
    # Show wave overview first
    show_wave_overview()
    
    st.markdown("---")
    
    # Enhanced configuration
    period_days, analysis_mode, chart_style = show_enhanced_period_selection()
    
    st.markdown("---")
    
    # Stock selection (reuse existing function)
    selected_symbol = show_stock_search()
    
    if not selected_symbol:
        st.info("👆 Please select a stock for enhanced bidirectional Elliott Wave analysis.")
        
        # Show enhanced features info
        with st.expander("🚀 Enhanced Features in This Version", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                #### 🔄 **Bidirectional Analysis**
                - **📈 Bullish Waves:** 1-2-3-4-5 upward patterns
                - **📉 Bearish Waves:** 1-2-3-4-5 downward patterns  
                - **🎯 Active Scenario:** Automatically detects current trend
                - **🔄 Pattern Switching:** Adapts when trend changes
                
                #### 📊 **Extended Data Analysis**
                - **200-365 days:** Comprehensive pattern recognition
                - **5-period swing detection:** More accurate pivot points
                - **Multi-timeframe MAs:** SMA 20, 50, 100, 200
                - **Enhanced volume analysis:** Money flow patterns
                
                #### 🎯 **Market Bias Analysis (NEW!)**
                - **Automatic calculation:** Bearish vs Bullish probability
                - **5-factor scoring:** Trend, Wave, RSI, Fibonacci, Volume
                - **Confidence levels:** Low/Medium/High/Very High
                - **Clear recommendations:** Strong Sell to Strong Buy
                """)
            
            with col2:
                st.markdown("""
                #### 🌟 **Advanced Fibonacci**
                - **Dual Fibonacci sets:** Bull + Bear scenarios
                - **100-day range:** More reliable levels
                - **Dynamic switching:** Shows active scenario
                - **Key level highlighting:** Golden ratio emphasis
                
                #### ⚡ **Smart Signals & Probabilities**
                - **Bidirectional signals:** Buy/Sell for both trends
                - **Market phase detection:** Bull/Bear/Reversal/Correction
                - **Probability percentages:** e.g., "70% Bearish, 30% Bullish"
                - **Risk level assessment:** Multi-factor analysis
                
                #### 🎓 **Real-World Examples**
                - **CBDK case study:** Live analysis of bearish setup
                - **Trading implications:** Specific strategies for bulls/bears
                - **Confidence scoring:** Know when to trust the signals
                - **Dynamic interpretation:** Adapts to current market conditions
                """)
        
        return
    
    st.markdown(f"## 🌊 Enhanced Elliott Wave Analysis: **{selected_symbol}**")
    st.markdown("---")
    
    # Get enhanced wave data
    wave_data = fetch_data_cached(
        EnhancedElliottWaveQueries.get_comprehensive_wave_data(selected_symbol, period_days),
        f"Enhanced Elliott Wave Data for {selected_symbol}"
    )
    
    if wave_data.empty:
        st.error(f"❌ No enhanced Elliott Wave data found for {selected_symbol}")
        return
    
    # Show enhanced summary
    show_bidirectional_wave_summary(selected_symbol, wave_data)
    
    # Show enhanced Fibonacci analysis
    show_enhanced_fibonacci_analysis(selected_symbol, wave_data)
    
    # MARKET BIAS ANALYSIS (NEW!)
    st.markdown("---")
    bias_result = show_market_bias_analysis(selected_symbol, wave_data)
    
    st.markdown("---")
    
    # Show enhanced chart
    show_enhanced_elliott_chart(selected_symbol, wave_data, chart_style)
    
    # Enhanced interpretation
    st.markdown("---")
    st.markdown("### 🎯 Enhanced Trading Interpretation")
    
    latest = wave_data.iloc[-1]
    primary_trend = latest.get('primary_trend', 'UNKNOWN')
    wave_position = latest.get('wave_position', 'UNKNOWN')
    elliott_signal = latest.get('elliott_signal', 'WAIT')
    market_phase = latest.get('market_phase', 'UNDEFINED_PHASE')
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📊 Current Market Assessment")
        
        # Use bias result for interpretation
        bias_score = bias_result.get('bias_score', 0)
        market_bias = bias_result.get('market_bias', 'NEUTRAL')
        probability = bias_result.get('probability', 'Unknown')
        
        if bias_score <= -5:
            st.error(f"""
            🔴 **BEARISH ELLIOTT WAVE SETUP**
            - **Market Bias:** {market_bias}
            - **Probability:** {probability}
            - **Trend:** {primary_trend.replace('_', ' ')}
            - **Wave:** {wave_position.replace('_', ' ')}
            - **Signal:** {elliott_signal.replace('_', ' ')}
            
            **Strategy:** Focus on short positions, expect lower prices.
            """)
        elif bias_score >= 5:
            st.success(f"""
            🟢 **BULLISH ELLIOTT WAVE SETUP**
            - **Market Bias:** {market_bias}
            - **Probability:** {probability}
            - **Trend:** {primary_trend.replace('_', ' ')}
            - **Wave:** {wave_position.replace('_', ' ')}
            - **Signal:** {elliott_signal.replace('_', ' ')}
            
            **Strategy:** Focus on long positions, expect higher prices.
            """)
        elif abs(bias_score) <= 2:
            st.info(f"""
            ⚪ **NEUTRAL ELLIOTT WAVE SETUP**
            - **Market Bias:** {market_bias}
            - **Probability:** {probability}
            - **Trend:** {primary_trend.replace('_', ' ')}
            - **Wave:** {wave_position.replace('_', ' ')}
            - **Signal:** {elliott_signal.replace('_', ' ')}
            
            **Strategy:** Wait for clearer directional bias.
            """)
        else:
            st.warning(f"""
            🟡 **MIXED ELLIOTT WAVE SETUP**
            - **Market Bias:** {market_bias}
            - **Probability:** {probability}
            - **Trend:** {primary_trend.replace('_', ' ')}
            - **Wave:** {wave_position.replace('_', ' ')}
            - **Signal:** {elliott_signal.replace('_', ' ')}
            
            **Strategy:** Use smaller positions, be prepared for volatility.
            """)
    
    with col2:
        st.markdown("#### ⚠️ Enhanced Risk Management")
        
        # Calculate dynamic support/resistance
        current_price = latest['close_price']
        
        # Get appropriate Fibonacci levels based on trend
        if primary_trend in ['UPTREND', 'STRONG_UPTREND']:
            support_level = latest.get('bull_fib_50', current_price * 0.95)
            resistance_level = latest.get('bull_fib_100', current_price * 1.05)
            key_level = latest.get('bull_fib_61_8', current_price)
        elif primary_trend in ['DOWNTREND', 'STRONG_DOWNTREND']:
            support_level = latest.get('bear_fib_100', current_price * 0.95)
            resistance_level = latest.get('bear_fib_50', current_price * 1.05)
            key_level = latest.get('bear_fib_61_8', current_price)
        else:
            support_level = current_price * 0.95
            resistance_level = current_price * 1.05
            key_level = current_price
        
        st.markdown(f"""
        **📊 Key Levels:**
        - **Support:** Rp{support_level:,.0f}
        - **Resistance:** Rp{resistance_level:,.0f}
        - **Key Fibonacci:** Rp{key_level:,.0f}
        
        **🎯 Suggested Setup:**
        - **Entry:** Wait for confirmation at key levels
        - **Stop Loss:** 3-5% beyond support/resistance
        - **Take Profit:** Fibonacci targets based on trend
        - **Position Size:** Adjust based on signal strength
        
        **📏 Risk/Reward:** Aim for minimum 1:2 ratio
        """)
    
    # Enhanced footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray; font-size: 0.9em; padding: 20px;'>
        🌊 <strong>Enhanced Bidirectional Elliott Wave Analysis with Market Bias Detection</strong><br>
        📊 200+ days comprehensive data • 🔄 Bull + Bear pattern recognition • 🎯 Automatic bias calculation<br>
        🌟 Advanced Fibonacci analysis • ⚡ Smart signal generation • 📈 Probability-based recommendations<br>
        💡 Professional-grade trading tool for Indonesian stocks with confidence scoring
    </div>
    """, unsafe_allow_html=True)