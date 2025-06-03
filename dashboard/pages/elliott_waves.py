# ============================================================================
# dashboard/pages/elliott_waves.py - Elliott Wave Analysis
# ============================================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
import numpy as np
from datetime import datetime, timedelta

# Import utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database import fetch_data_cached, execute_query_safe
from charts import ChartFactory

class ElliottWaveQueries:
    """Queries for Elliott Wave Analysis"""
    
    @staticmethod
    def get_stock_data_for_waves(symbol, period_days=90):
        """Get comprehensive stock data for Elliott Wave analysis"""
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
        
        -- Moving Averages untuk trend identification
        moving_averages AS (
            SELECT *,
                AVG(close_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sma_5,
                AVG(close_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS sma_10,
                AVG(close_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20,
                AVG(close_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_50,
                AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_volume_20
            FROM base_data
        ),
        
        -- Technical indicators
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
        
        -- Elliott Wave Analysis
        wave_analysis AS (
            SELECT *,
                -- Trend identification
                CASE 
                    WHEN close_price > sma_20 AND sma_20 > LAG(sma_20, 5) OVER (PARTITION BY symbol ORDER BY date) THEN 'UPTREND'
                    WHEN close_price < sma_20 AND sma_20 < LAG(sma_20, 5) OVER (PARTITION BY symbol ORDER BY date) THEN 'DOWNTREND'
                    ELSE 'SIDEWAYS'
                END AS trend_direction,
                
                -- High/Low detection for wave pivots
                CASE 
                    WHEN high_price > LAG(high_price, 1) OVER (PARTITION BY symbol ORDER BY date) 
                         AND high_price > LEAD(high_price, 1) OVER (PARTITION BY symbol ORDER BY date) 
                         AND high_price > LAG(high_price, 2) OVER (PARTITION BY symbol ORDER BY date)
                         AND high_price > LEAD(high_price, 2) OVER (PARTITION BY symbol ORDER BY date)
                    THEN 1 ELSE 0 
                END AS swing_high,
                
                CASE 
                    WHEN low_price < LAG(low_price, 1) OVER (PARTITION BY symbol ORDER BY date) 
                         AND low_price < LEAD(low_price, 1) OVER (PARTITION BY symbol ORDER BY date)
                         AND low_price < LAG(low_price, 2) OVER (PARTITION BY symbol ORDER BY date)
                         AND low_price < LEAD(low_price, 2) OVER (PARTITION BY symbol ORDER BY date)
                    THEN 1 ELSE 0 
                END AS swing_low
            FROM technical_data
        ),
        
        -- Support & Resistance dengan Fibonacci
        fibonacci_levels AS (
            SELECT *,
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS support_30,
                MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS resistance_30,
                
                -- Fibonacci levels (using 30-day high/low untuk wave analysis)
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS fib_0,
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) + 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) * 0.236 AS fib_23_6,
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) + 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) * 0.382 AS fib_38_2,
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) + 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) * 0.50 AS fib_50,
                MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) + 
                (MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) - 
                 MIN(low_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) * 0.618 AS fib_61_8,
                MAX(high_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS fib_100
            FROM wave_analysis
        ),
        
        -- Wave Signals
        wave_signals AS (
            SELECT *,
                -- Elliott Wave position
                CASE 
                    WHEN close_price <= fib_23_6 THEN 'WAVE_1_OR_A'
                    WHEN close_price > fib_23_6 AND close_price <= fib_38_2 THEN 'WAVE_2_OR_B'
                    WHEN close_price > fib_38_2 AND close_price <= fib_61_8 THEN 'WAVE_3_OR_C'
                    WHEN close_price > fib_61_8 AND close_price <= fib_100 THEN 'WAVE_4_OR_D'
                    WHEN close_price > fib_100 THEN 'WAVE_5_OR_E'
                    ELSE 'UNDEFINED'
                END AS wave_position,
                
                -- Elliott Wave Signals
                CASE 
                    WHEN trend_direction = 'UPTREND' AND close_price > fib_61_8 AND rsi_14 < 80 THEN 'WAVE_BUY'
                    WHEN trend_direction = 'DOWNTREND' AND close_price < fib_38_2 AND rsi_14 > 20 THEN 'WAVE_SELL'
                    WHEN close_price BETWEEN fib_38_2 AND fib_61_8 THEN 'WAVE_CONSOLIDATION'
                    ELSE 'WAVE_WAIT'
                END AS elliott_signal,
                
                -- Wave momentum
                CASE 
                    WHEN swing_high = 1 AND trend_direction = 'UPTREND' THEN 'POTENTIAL_TOP'
                    WHEN swing_low = 1 AND trend_direction = 'DOWNTREND' THEN 'POTENTIAL_BOTTOM'
                    WHEN swing_high = 1 OR swing_low = 1 THEN 'WAVE_PIVOT'
                    ELSE 'WAVE_CONTINUATION'
                END AS wave_momentum
            FROM fibonacci_levels
        )
        
        SELECT * FROM wave_signals
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
    st.markdown("### 🔍 Select Stock for Elliott Wave Analysis")
    
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
            help="Select a stock to view Elliott Wave analysis including wave patterns, Fibonacci levels, and trading signals"
        )
        
        if selected_option:
            selected_symbol = selected_option.split(" - ")[0]
            # Update session state
            st.session_state.selected_stock = selected_symbol
            return selected_symbol
    
    with col2:
        # Analysis period
        st.markdown("**Analysis Period:**")
        period = st.selectbox(
            "Days:",
            options=[30, 60, 90, 120],
            index=2,  # Default 90 days
            help="Select period for Elliott Wave analysis"
        )
        st.session_state.wave_period = period
    
    return None

def calculate_wave_metrics(df):
    """Calculate Elliott Wave metrics from price data"""
    if df.empty:
        return {}
    
    latest = df.iloc[-1]
    
    # Calculate wave range
    wave_high = df['high_price'].max()
    wave_low = df['low_price'].min()
    wave_range = wave_high - wave_low
    
    # Current position in wave
    current_position = (latest['close_price'] - wave_low) / wave_range if wave_range > 0 else 0
    
    # Wave momentum (last 5 days)
    recent_df = df.tail(5)
    momentum = "Bullish" if recent_df['close_price'].iloc[-1] > recent_df['close_price'].iloc[0] else "Bearish"
    
    # Count swing points
    swing_highs = df['swing_high'].sum() if 'swing_high' in df.columns else 0
    swing_lows = df['swing_low'].sum() if 'swing_low' in df.columns else 0
    
    return {
        'wave_high': wave_high,
        'wave_low': wave_low,
        'wave_range': wave_range,
        'current_position': current_position,
        'momentum': momentum,
        'swing_highs': swing_highs,
        'swing_lows': swing_lows,
        'trend_direction': latest.get('trend_direction', 'Unknown'),
        'wave_position': latest.get('wave_position', 'Unknown'),
        'elliott_signal': latest.get('elliott_signal', 'Unknown')
    }

def show_wave_overview():
    """Show Elliott Wave overview for multiple stocks"""
    st.markdown("### 🌊 Elliott Wave Market Overview")
    
    try:
        wave_overview_df = fetch_data_cached(
            ElliottWaveQueries.get_wave_setups_overview(),
            "Wave Overview"
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
            st.markdown("#### 🎯 Top Elliott Wave Setups:")
            st.markdown("*Click any stock below for detailed wave analysis*")
            
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
                        key=f"wave_{row['symbol']}",
                        help=f"Elliott Wave Analysis: {row['wave_setup']} - Click to analyze {row['symbol']}",
                        use_container_width=True
                    ):
                        st.session_state.selected_stock = row['symbol']
                        st.session_state.page_navigation = "🌊 Elliott Wave Analysis"
                        st.rerun()
            
        else:
            st.info("📭 No Elliott Wave data available")
            
    except Exception as e:
        st.error(f"❌ Failed to load Elliott Wave overview: {str(e)}")

def show_elliott_wave_chart(symbol, wave_data):
    """Show comprehensive Elliott Wave chart"""
    if wave_data.empty:
        return
    
    st.markdown(f"### 📈 Elliott Wave Chart - {symbol}")
    
    # Create subplots
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(
            f'{symbol} - Price & Elliott Wave Analysis',
            'RSI (14) with Wave Signals',
            'Volume'
        ),
        row_heights=[0.6, 0.25, 0.15]
    )
    
    # Main price chart with candlesticks
    fig.add_trace(
        go.Candlestick(
            x=wave_data['date'],
            open=wave_data['open_price'],
            high=wave_data['high_price'],
            low=wave_data['low_price'],
            close=wave_data['close_price'],
            name="Price",
            increasing_line_color='green',
            decreasing_line_color='red'
        ),
        row=1, col=1
    )
    
    # Add Fibonacci levels
    if 'fib_23_6' in wave_data.columns:
        latest_date = wave_data['date'].iloc[-1]
        
        fib_levels = [
            ('fib_0', 'Fib 0%', 'blue'),
            ('fib_23_6', 'Fib 23.6%', 'green'),
            ('fib_38_2', 'Fib 38.2%', 'orange'),
            ('fib_50', 'Fib 50%', 'red'),
            ('fib_61_8', 'Fib 61.8%', 'purple'),
            ('fib_100', 'Fib 100%', 'blue')
        ]
        
        for fib_col, fib_name, color in fib_levels:
            if fib_col in wave_data.columns:
                fib_value = wave_data[fib_col].iloc[-1]
                fig.add_hline(
                    y=fib_value,
                    line_dash="dash",
                    line_color=color,
                    annotation_text=f"{fib_name}: {fib_value:,.0f}",
                    annotation_position="right",
                    row=1, col=1
                )
    
    # Add moving averages
    if 'sma_20' in wave_data.columns:
        fig.add_trace(
            go.Scatter(
                x=wave_data['date'],
                y=wave_data['sma_20'],
                mode='lines',
                name='SMA 20',
                line=dict(color='blue', width=1),
                opacity=0.7
            ),
            row=1, col=1
        )
    
    if 'sma_50' in wave_data.columns:
        fig.add_trace(
            go.Scatter(
                x=wave_data['date'],
                y=wave_data['sma_50'],
                mode='lines',
                name='SMA 50',
                line=dict(color='red', width=1),
                opacity=0.7
            ),
            row=1, col=1
        )
    
    # Mark swing points
    if 'swing_high' in wave_data.columns:
        swing_highs = wave_data[wave_data['swing_high'] == 1]
        if not swing_highs.empty:
            fig.add_trace(
                go.Scatter(
                    x=swing_highs['date'],
                    y=swing_highs['high_price'],
                    mode='markers',
                    name='Swing Highs',
                    marker=dict(color='red', size=8, symbol='triangle-down'),
                ),
                row=1, col=1
            )
    
    if 'swing_low' in wave_data.columns:
        swing_lows = wave_data[wave_data['swing_low'] == 1]
        if not swing_lows.empty:
            fig.add_trace(
                go.Scatter(
                    x=swing_lows['date'],
                    y=swing_lows['low_price'],
                    mode='markers',
                    name='Swing Lows',
                    marker=dict(color='green', size=8, symbol='triangle-up'),
                ),
                row=1, col=1
            )
    
    # RSI with wave signals
    if 'rsi_14' in wave_data.columns:
        fig.add_trace(
            go.Scatter(
                x=wave_data['date'],
                y=wave_data['rsi_14'],
                mode='lines',
                name='RSI',
                line=dict(color='purple', width=2)
            ),
            row=2, col=1
        )
        
        # RSI levels
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
        fig.add_hline(y=50, line_dash="dot", line_color="gray", row=2, col=1)
    
    # Volume
    fig.add_trace(
        go.Bar(
            x=wave_data['date'],
            y=wave_data['volume'],
            name='Volume',
            marker_color='lightblue',
            opacity=0.7
        ),
        row=3, col=1
    )
    
    # Update layout
    fig.update_layout(
        title=f"Elliott Wave Analysis - {symbol}",
        xaxis_rangeslider_visible=False,
        height=800,
        showlegend=True
    )
    
    fig.update_yaxes(title_text="Price (Rp)", row=1, col=1)
    fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100])
    fig.update_yaxes(title_text="Volume", row=3, col=1)
    fig.update_xaxes(title_text="Date", row=3, col=1)
    
    st.plotly_chart(fig, use_container_width=True)

def show_wave_analysis_summary(symbol, wave_data):
    """Show Elliott Wave analysis summary"""
    if wave_data.empty:
        return
    
    st.markdown(f"### 🌊 Elliott Wave Analysis Summary - {symbol}")
    
    # Calculate metrics
    metrics = calculate_wave_metrics(wave_data)
    latest = wave_data.iloc[-1]
    
    # Main metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "💰 Current Price",
            f"Rp{latest['close_price']:,.0f}",
            f"{latest['percent_change']:+.2f}%"
        )
    
    with col2:
        wave_position_pct = metrics['current_position'] * 100
        st.metric(
            "🌊 Wave Position",
            f"{wave_position_pct:.1f}%",
            "of range"
        )
    
    with col3:
        st.metric(
            "📊 Wave Range",
            f"Rp{metrics['wave_range']:,.0f}",
            f"H: {metrics['wave_high']:,.0f} L: {metrics['wave_low']:,.0f}"
        )
    
    with col4:
        trend_emoji = "📈" if metrics['trend_direction'] == 'UPTREND' else "📉" if metrics['trend_direction'] == 'DOWNTREND' else "➡️"
        st.metric(
            "📈 Trend",
            f"{trend_emoji} {metrics['trend_direction']}",
            f"{metrics['momentum']} momentum"
        )
    
    with col5:
        elliott_signal = metrics['elliott_signal']
        signal_emoji = "🟢" if 'BUY' in elliott_signal else "🔴" if 'SELL' in elliott_signal else "🟡"
        st.metric(
            "🌊 Elliott Signal",
            f"{signal_emoji} {elliott_signal}",
            "wave signal"
        )
    
    # Wave analysis details
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📊 Fibonacci Analysis")
        
        if 'fib_23_6' in wave_data.columns:
            latest_price = latest['close_price']
            
            fib_data = {
                'Level': ['0%', '23.6%', '38.2%', '50%', '61.8%', '100%'],
                'Price': [
                    latest.get('fib_0', 0),
                    latest.get('fib_23_6', 0),
                    latest.get('fib_38_2', 0),
                    latest.get('fib_50', 0),
                    latest.get('fib_61_8', 0),
                    latest.get('fib_100', 0)
                ]
            }
            
            fib_df = pd.DataFrame(fib_data)
            fib_df['Price'] = fib_df['Price'].apply(lambda x: f"Rp{x:,.0f}")
            fib_df['Status'] = fib_df['Level'].apply(
                lambda x: "🔴 Above" if latest_price > latest.get(f"fib_{x.replace('%', '').replace('.', '_')}", 0) 
                else "🟢 Below"
            )
            
            st.dataframe(fib_df, use_container_width=True, hide_index=True)
    
    with col2:
        st.markdown("#### 🎯 Trading Signals")
        
        # Current wave position
        wave_pos = latest.get('wave_position', 'Unknown')
        elliott_sig = latest.get('elliott_signal', 'Unknown')
        
        # Signal interpretation
        if 'BUY' in elliott_sig:
            st.success(f"🟢 **{elliott_sig}** - Potential bullish setup")
        elif 'SELL' in elliott_sig:
            st.error(f"🔴 **{elliott_sig}** - Potential bearish setup")
        elif 'CONSOLIDATION' in elliott_sig:
            st.warning(f"🟡 **{elliott_sig}** - Range-bound trading")
        else:
            st.info(f"⚪ **{elliott_sig}** - Wait for clearer signal")
        
        # Wave position interpretation
        st.markdown(f"**Wave Position:** {wave_pos}")
        
        if 'WAVE_3' in wave_pos or 'WAVE_5' in wave_pos:
            st.markdown("🔥 **Strong momentum wave** - Ride the trend")
        elif 'WAVE_2' in wave_pos or 'WAVE_4' in wave_pos:
            st.markdown("🔄 **Correction wave** - Prepare for reversal")
        elif 'WAVE_1' in wave_pos:
            st.markdown("🚀 **Initial impulse** - Early trend confirmation")
        
        # Risk management
        st.markdown("#### ⚠️ Risk Management")
        support_level = latest.get('support_30', latest['close_price'] * 0.95)
        resistance_level = latest.get('resistance_30', latest['close_price'] * 1.05)
        
        st.markdown(f"**Support:** Rp{support_level:,.0f}")
        st.markdown(f"**Resistance:** Rp{resistance_level:,.0f}")
        st.markdown(f"**Stop Loss:** Rp{latest['close_price'] * 0.97:,.0f} (-3%)")
        st.markdown(f"**Take Profit:** Rp{latest['close_price'] * 1.06:,.0f} (+6%)")

def show():
    """Main function for Elliott Wave Analysis page"""
    
    st.markdown("# 🌊 Elliott Wave Analysis")
    st.markdown("Professional wave pattern recognition, Fibonacci analysis, and Elliott Wave trading signals for Indonesian stocks.")
    
    st.markdown("---")
    
    # Show wave overview first
    show_wave_overview()
    
    st.markdown("---")
    
    # Stock selection
    selected_symbol = show_stock_search()
    
    if not selected_symbol:
        st.info("👆 Please select a stock from the dropdown above to begin Elliott Wave analysis.")
        
        # Show educational content
        with st.expander("📚 Learn About Elliott Wave Theory", expanded=False):
            st.markdown("""
            ### 🌊 Elliott Wave Theory Basics
            
            **Impulse Waves (5-wave pattern):**
            - **Wave 1**: Initial move in new direction
            - **Wave 2**: Corrective wave (retraces 50-61.8% of Wave 1)
            - **Wave 3**: Strongest wave (usually 161.8% of Wave 1)
            - **Wave 4**: Corrective wave (retraces 23.6-38.2% of Wave 3)
            - **Wave 5**: Final wave (often equal to Wave 1)
            
            **Corrective Waves (3-wave pattern):**
            - **Wave A**: First leg down
            - **Wave B**: Corrective bounce
            - **Wave C**: Final leg down
            
            **Key Fibonacci Levels:**
            - **23.6%**: Minor support/resistance
            - **38.2%**: Strong retracement level
            - **50%**: Psychological level
            - **61.8%**: Golden ratio - strongest level
            - **100%**: Complete retracement
            
            **Trading Rules:**
            - Wave 2 never retraces more than 100% of Wave 1
            - Wave 3 is never the shortest wave
            - Wave 4 never overlaps Wave 1
            """)
        
        return
    
    st.markdown(f"## 🌊 Elliott Wave Analysis: **{selected_symbol}**")
    st.markdown("---")
    
    # Get period from session state or default
    period = st.session_state.get('wave_period', 90)
    
    # Get Elliott Wave data
    wave_data = fetch_data_cached(
        ElliottWaveQueries.get_stock_data_for_waves(selected_symbol, period),
        f"Elliott Wave Data for {selected_symbol}"
    )
    
    if wave_data.empty:
        st.error(f"❌ No Elliott Wave data found for {selected_symbol}")
        return
    
    # Show analysis summary
    show_wave_analysis_summary(selected_symbol, wave_data)
    
    st.markdown("---")
    
    # Show Elliott Wave chart
    show_elliott_wave_chart(selected_symbol, wave_data)
    
    # Add detailed wave analysis
    st.markdown("---")
    st.markdown("### 📋 Detailed Wave Analysis")
    
    with st.expander(f"🔍 View Detailed Elliott Wave Data for {selected_symbol}", expanded=False):
        # Show recent wave data
        display_cols = ['date', 'close_price', 'volume', 'trend_direction', 'wave_position', 'elliott_signal']
        recent_data = wave_data[display_cols].tail(10).copy()
        
        recent_data['date'] = pd.to_datetime(recent_data['date']).dt.strftime('%Y-%m-%d')
        recent_data['close_price'] = recent_data['close_price'].apply(lambda x: f"Rp{x:,.0f}")
        recent_data['volume'] = recent_data['volume'].apply(lambda x: f"{x:,.0f}")
        
        recent_data.columns = ['Date', 'Close Price', 'Volume', 'Trend', 'Wave Position', 'Elliott Signal']
        
        st.dataframe(recent_data, use_container_width=True, hide_index=True)
    
    # Add wave theory explanation specific to current stock
    with st.expander(f"🎓 Elliott Wave Interpretation for {selected_symbol}", expanded=False):
        latest = wave_data.iloc[-1]
        wave_position = latest.get('wave_position', 'Unknown')
        elliott_signal = latest.get('elliott_signal', 'Unknown')
        trend = latest.get('trend_direction', 'Unknown')
        
        st.markdown(f"""
        **Current Analysis for {selected_symbol}:**
        
        **Wave Position:** {wave_position}
        **Elliott Signal:** {elliott_signal}
        **Trend Direction:** {trend}
        
        **Interpretation:**
        """)
        
        if 'WAVE_1' in wave_position:
            st.markdown("""
            - 🚀 **Wave 1/A**: Initial impulse or first corrective leg
            - **Strategy**: Early entry opportunity, but wait for confirmation
            - **Risk**: High, as this could be false breakout
            """)
        elif 'WAVE_2' in wave_position:
            st.markdown("""
            - 🔄 **Wave 2/B**: Corrective phase
            - **Strategy**: Prepare for Wave 3, this is buying opportunity if uptrend
            - **Risk**: Medium, good entry point after retracement
            """)
        elif 'WAVE_3' in wave_position:
            st.markdown("""
            - 🔥 **Wave 3/C**: Strongest wave in sequence
            - **Strategy**: Ride the momentum, strongest profit potential
            - **Risk**: Low for trend following, high returns expected
            """)
        elif 'WAVE_4' in wave_position:
            st.markdown("""
            - ⚖️ **Wave 4/D**: Complex correction
            - **Strategy**: Prepare for final Wave 5, or end of pattern
            - **Risk**: Medium, choppy price action expected
            """)
        elif 'WAVE_5' in wave_position:
            st.markdown("""
            - 🎯 **Wave 5/E**: Final wave
            - **Strategy**: Take profits, prepare for major reversal
            - **Risk**: High, momentum typically weaker than Wave 3
            """)
        
        st.markdown(f"""
        **Fibonacci Levels for {selected_symbol}:**
        - Use these levels for entry/exit points
        - 61.8% retracement is strongest support/resistance
        - 38.2% level good for stop loss placement
        """)
    
    # Footer with disclaimer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray; font-size: 0.9em; padding: 20px;'>
        🌊 <strong>Elliott Wave Analysis</strong> is based on pattern recognition and probability<br>
        💡 Always combine with other technical indicators and risk management
    </div>
    """, unsafe_allow_html=True)