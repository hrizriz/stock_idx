# ============================================================================
# dashboard/pages/individual_stock_analysis.py - Comprehensive Stock Analysis
# ============================================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
from datetime import datetime, timedelta

# Import utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database import fetch_data_cached, execute_query_safe
from charts import ChartFactory

class StockAnalysisQueries:
    """Queries based on reporting_and_alerting.py DAG"""
    
    @staticmethod
    def get_stock_movement_analysis(symbol):
        """Based on send_stock_movement_alert function"""
        return f"""
        WITH latest_date AS (
            SELECT MAX(date) as max_date 
            FROM public_analytics.daily_stock_metrics
        ),
        stock_movement AS (
            SELECT 
                symbol,
                name,
                date,
                close,
                prev_close,
                percent_change,
                volume,
                value,
                high,
                low,
                open_price,
                foreign_buy,
                foreign_sell,
                (foreign_buy - foreign_sell) as foreign_net
            FROM public_analytics.daily_stock_metrics
            WHERE symbol = '{symbol}'
            AND date >= (SELECT max_date - INTERVAL '30 days' FROM latest_date)
            ORDER BY date DESC
        )
        SELECT * FROM stock_movement
        """
    
    @staticmethod
    def get_technical_signals(symbol):
        """Based on send_technical_signal_report function"""
        return f"""
        WITH latest_technical AS (
            SELECT MAX(r.date) as max_date 
            FROM public_analytics.technical_indicators_rsi r
            WHERE r.symbol = '{symbol}'
        )
        SELECT 
            r.symbol,
            r.date,
            r.rsi,
            r.rsi_signal,
            m.macd_line,
            m.signal_line,
            m.macd_histogram,
            m.macd_signal,
            dm.close,
            dm.volume,
            dm.percent_change,
            -- Combined signal strength
            CASE 
                WHEN r.rsi < 30 AND m.macd_signal = 'Bullish' THEN 'Strong Buy'
                WHEN r.rsi > 70 AND m.macd_signal = 'Bearish' THEN 'Strong Sell'
                WHEN r.rsi < 30 THEN 'Buy Signal'
                WHEN r.rsi > 70 THEN 'Sell Signal'
                WHEN m.macd_signal = 'Bullish' THEN 'Weak Buy'
                WHEN m.macd_signal = 'Bearish' THEN 'Weak Sell'
                ELSE 'Hold'
            END as recommendation
        FROM public_analytics.technical_indicators_rsi r
        JOIN public_analytics.technical_indicators_macd m 
            ON r.symbol = m.symbol AND r.date = m.date
        JOIN public_analytics.daily_stock_metrics dm
            ON r.symbol = dm.symbol AND r.date = dm.date
        WHERE r.symbol = '{symbol}'
        AND r.date >= (SELECT max_date - INTERVAL '30 days' FROM latest_technical)
        ORDER BY r.date DESC
        """
    
    @staticmethod
    def get_sentiment_analysis(symbol):
        """Based on send_news_sentiment_report function"""
        return f"""
        WITH sentiment_data AS (
            SELECT 
                ticker as symbol,
                date,
                avg_sentiment,
                news_count,
                positive_count,
                negative_count,
                neutral_count,
                CASE WHEN news_count > 0 THEN (positive_count::float / news_count) * 100 ELSE 0 END as positive_percentage,
                CASE 
                    WHEN avg_sentiment > 0.5 THEN 'Strong Buy'
                    WHEN avg_sentiment > 0.2 THEN 'Buy'
                    WHEN avg_sentiment < -0.5 THEN 'Strong Sell'
                    WHEN avg_sentiment < -0.2 THEN 'Sell'
                    ELSE 'Hold/No Signal'
                END as trading_signal
            FROM public.detik_ticker_sentiment
            WHERE ticker = '{symbol}'
            AND date >= CURRENT_DATE - INTERVAL '30 days'
            AND news_count >= 1
        ),
        newsapi_sentiment AS (
            SELECT 
                ticker as symbol,
                date,
                avg_sentiment as newsapi_sentiment,
                news_count as newsapi_count,
                'NewsAPI' as source
            FROM public.newsapi_ticker_sentiment
            WHERE ticker = '{symbol}'
            AND date >= CURRENT_DATE - INTERVAL '30 days'
            AND news_count >= 1
        )
        SELECT 
            COALESCE(s.symbol, n.symbol) as symbol,
            COALESCE(s.date, n.date) as date,
            s.avg_sentiment as detik_sentiment,
            s.news_count as detik_count,
            s.positive_count,
            s.negative_count,
            s.positive_percentage,
            s.trading_signal,
            n.newsapi_sentiment,
            n.newsapi_count
        FROM sentiment_data s
        FULL OUTER JOIN newsapi_sentiment n ON s.symbol = n.symbol AND s.date = n.date
        ORDER BY COALESCE(s.date, n.date) DESC
        """
    
    @staticmethod
    def get_bandarmology_analysis(symbol):
        """Based on send_bandarmology_report function"""
        return f"""
        WITH volume_analysis AS (
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
            WHERE symbol = '{symbol}'
            AND date >= CURRENT_DATE - INTERVAL '30 days'
        ),
        accumulation_signals AS (
            SELECT 
                v.symbol,
                v.date,
                v.close,
                v.volume,
                v.avg_volume_20d,
                v.percent_change,
                v.foreign_net,
                CASE WHEN v.volume > 2 * v.avg_volume_20d THEN 1 ELSE 0 END AS volume_spike,
                CASE WHEN v.foreign_net > 0 THEN 1 ELSE 0 END AS foreign_buying,
                CASE WHEN v.bid_volume > 1.5 * v.offer_volume THEN 1 ELSE 0 END AS strong_bid,
                CASE WHEN v.volume > 1.5 * v.avg_volume_20d AND ABS(v.percent_change) < 1.0 THEN 1 ELSE 0 END AS hidden_buying,
                CASE WHEN v.close > v.prev_close THEN 1 ELSE 0 END AS positive_close
            FROM volume_analysis v
        ),
        final_scores AS (
            SELECT 
                a.symbol,
                a.date,
                a.close,
                a.percent_change,
                a.volume,
                a.avg_volume_20d,
                a.volume/NULLIF(a.avg_volume_20d, 0) AS volume_ratio,
                a.foreign_net,
                (a.volume_spike + a.foreign_buying + a.strong_bid + a.hidden_buying + a.positive_close) AS bandar_score,
                CASE
                    WHEN (a.volume_spike + a.foreign_buying + a.strong_bid + a.hidden_buying + a.positive_close) >= 4 THEN 'Sangat Kuat'
                    WHEN (a.volume_spike + a.foreign_buying + a.strong_bid + a.hidden_buying + a.positive_close) = 3 THEN 'Kuat'
                    WHEN (a.volume_spike + a.foreign_buying + a.strong_bid + a.hidden_buying + a.positive_close) = 2 THEN 'Sedang'
                    ELSE 'Lemah'
                END AS signal_strength
            FROM accumulation_signals a
        )
        SELECT * FROM final_scores
        ORDER BY date DESC
        """
    
    @staticmethod
    def get_accumulation_distribution(symbol):
        """Based on send_accumulation_distribution_report function"""
        return f"""
        WITH mfv_calculation AS (
            SELECT
                symbol,
                date,
                close,
                high,
                low,
                volume,
                CASE
                    WHEN (high - low) = 0 THEN 0
                    ELSE ((close - low) - (high - close)) / (high - low) * volume
                END AS money_flow_volume
            FROM public_analytics.daily_stock_metrics
            WHERE symbol = '{symbol}'
            AND date >= CURRENT_DATE - INTERVAL '40 days'
        ),
        ad_line AS (
            SELECT
                symbol,
                date,
                close,
                money_flow_volume,
                SUM(money_flow_volume) OVER (PARTITION BY symbol ORDER BY date) AS ad_line_cumulative
            FROM mfv_calculation
        )
        SELECT * FROM ad_line
        ORDER BY date DESC
        """
    
    @staticmethod
    def get_high_probability_signals(symbol):
        """Based on send_high_probability_signals function"""
        return f"""
        SELECT 
            s.symbol,
            s.date,
            s.buy_score,
            s.signal_strength,
            s.winning_probability,
            m.name,
            m.close,
            m.volume,
            r.rsi,
            mc.macd_signal
        FROM public_analytics.advanced_trading_signals s
        JOIN public.daily_stock_summary m 
            ON s.symbol = m.symbol AND s.date = m.date
        LEFT JOIN public_analytics.technical_indicators_rsi r 
            ON s.symbol = r.symbol AND s.date = r.date
        LEFT JOIN public_analytics.technical_indicators_macd mc 
            ON s.symbol = mc.symbol AND s.date = mc.date
        WHERE s.symbol = '{symbol}'
        AND s.date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY s.date DESC
        """

def get_available_stocks():
    """Get list of available stocks for search"""
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
    """Stock search interface"""
    st.markdown("### 🔍 Select Stock for Comprehensive Analysis")
    
    # Get available stocks
    stocks_df = get_available_stocks()
    
    if stocks_df.empty:
        st.warning("⚠️ No stocks data available")
        return None
    
    # Create search options
    stock_options = []
    for _, row in stocks_df.iterrows():
        price_change = f"{row['percent_change']:+.2f}%" if pd.notna(row['percent_change']) else "N/A"
        price = f"Rp{row['close']:,.0f}" if pd.notna(row['close']) else "N/A"
        option = f"{row['symbol']} - {row['name']} | {price} ({price_change})"
        stock_options.append(option)
    
    # Search interface
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        selected_option = st.selectbox(
            "Choose Stock:",
            stock_options,
            index=0,
            help="Select a stock to view comprehensive analysis including technical, sentiment, and trading signals"
        )
        
        if selected_option:
            selected_symbol = selected_option.split(" - ")[0]
            return selected_symbol
    
    with col2:
        # Quick filters
        st.markdown("**Quick Filters:**")
        show_gainers = st.checkbox("🟢 Gainers Only")
        show_losers = st.checkbox("🔴 Losers Only")
    
    with col3:
        # Volume filter
        st.markdown("**Volume Filter:**")
        min_volume = st.number_input("Min Volume (M)", value=0.0, step=0.1)
    
    return None

def show_stock_metrics(symbol, stock_data):
    """Show key stock metrics"""
    if stock_data.empty:
        return
    
    latest = stock_data.iloc[0]
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        price_change = latest['percent_change'] if pd.notna(latest['percent_change']) else 0
        st.metric(
            "💰 Current Price",
            f"Rp{latest['close']:,.0f}",
            f"{price_change:+.2f}%"
        )
    
    with col2:
        volume = latest['volume'] if pd.notna(latest['volume']) else 0
        st.metric(
            "🔊 Volume",
            f"{volume:,.0f}",
            "lots"
        )
    
    with col3:
        foreign_net = latest['foreign_net'] if pd.notna(latest['foreign_net']) else 0
        foreign_color = "🟢" if foreign_net > 0 else "🔴" if foreign_net < 0 else "⚪"
        st.metric(
            "🌍 Foreign Flow",
            f"{foreign_net:,.0f}",
            f"{foreign_color} Net"
        )
    
    with col4:
        day_range = latest['high'] - latest['low'] if pd.notna(latest['high']) and pd.notna(latest['low']) else 0
        st.metric(
            "📏 Day Range",
            f"Rp{day_range:,.0f}",
            f"H: {latest['high']:,.0f} L: {latest['low']:,.0f}" if pd.notna(latest['high']) else "N/A"
        )
    
    with col5:
        value = latest['value'] if pd.notna(latest['value']) else 0
        value_b = value / 1e9
        st.metric(
            "💹 Trading Value",
            f"Rp{value_b:.2f}B",
            "today"
        )

def show_technical_analysis_summary(symbol):
    """Show technical analysis summary"""
    st.markdown("### 📈 Technical Analysis Summary")
    
    technical_data = fetch_data_cached(
        StockAnalysisQueries.get_technical_signals(symbol),
        f"Technical Signals for {symbol}"
    )
    
    if technical_data.empty:
        st.warning(f"⚠️ No technical data available for {symbol}")
        return
    
    latest_tech = technical_data.iloc[0]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # RSI Analysis
        st.markdown("#### 📊 RSI Analysis")
        rsi_val = latest_tech['rsi'] if pd.notna(latest_tech['rsi']) else 50
        rsi_signal = latest_tech['rsi_signal'] if pd.notna(latest_tech['rsi_signal']) else 'Neutral'
        
        # RSI gauge
        fig_rsi = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = rsi_val,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': f"RSI: {rsi_signal}"},
            gauge = {
                'axis': {'range': [None, 100]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 30], 'color': "lightgreen"},
                    {'range': [30, 70], 'color': "lightyellow"},
                    {'range': [70, 100], 'color': "lightcoral"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': rsi_val
                }
            }
        ))
        fig_rsi.update_layout(height=300)
        st.plotly_chart(fig_rsi, use_container_width=True)
    
    with col2:
        # MACD Analysis
        st.markdown("#### 📈 MACD Analysis")
        macd_signal = latest_tech['macd_signal'] if pd.notna(latest_tech['macd_signal']) else 'Neutral'
        macd_line = latest_tech['macd_line'] if pd.notna(latest_tech['macd_line']) else 0
        signal_line = latest_tech['signal_line'] if pd.notna(latest_tech['signal_line']) else 0
        
        st.metric("MACD Signal", macd_signal)
        st.metric("MACD Line", f"{macd_line:.4f}")
        st.metric("Signal Line", f"{signal_line:.4f}")
        st.metric("Histogram", f"{macd_line - signal_line:.4f}")
    
    # Overall recommendation
    recommendation = latest_tech['recommendation'] if pd.notna(latest_tech['recommendation']) else 'Hold'
    
    if recommendation in ['Strong Buy', 'Buy Signal']:
        st.success(f"🟢 **Recommendation: {recommendation}**")
    elif recommendation in ['Strong Sell', 'Sell Signal']:
        st.error(f"🔴 **Recommendation: {recommendation}**")
    elif recommendation in ['Weak Buy', 'Weak Sell']:
        st.warning(f"🟡 **Recommendation: {recommendation}**")
    else:
        st.info(f"⚪ **Recommendation: {recommendation}**")

def show_sentiment_analysis(symbol):
    """Show sentiment analysis"""
    st.markdown("### 📰 News Sentiment Analysis")
    
    sentiment_data = fetch_data_cached(
        StockAnalysisQueries.get_sentiment_analysis(symbol),
        f"Sentiment Analysis for {symbol}"
    )
    
    if sentiment_data.empty:
        st.warning(f"⚠️ No sentiment data available for {symbol}")
        return
    
    # Latest sentiment
    latest_sentiment = sentiment_data.iloc[0]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        detik_sentiment = latest_sentiment['detik_sentiment'] if pd.notna(latest_sentiment['detik_sentiment']) else 0
        detik_count = latest_sentiment['detik_count'] if pd.notna(latest_sentiment['detik_count']) else 0
        
        sentiment_color = "🟢" if detik_sentiment > 0.2 else "🔴" if detik_sentiment < -0.2 else "🟡"
        st.metric(
            "📰 Detik Sentiment",
            f"{detik_sentiment:.3f} {sentiment_color}",
            f"{detik_count} berita"
        )
    
    with col2:
        newsapi_sentiment = latest_sentiment['newsapi_sentiment'] if pd.notna(latest_sentiment['newsapi_sentiment']) else 0
        newsapi_count = latest_sentiment['newsapi_count'] if pd.notna(latest_sentiment['newsapi_count']) else 0
        
        newsapi_color = "🟢" if newsapi_sentiment > 0.2 else "🔴" if newsapi_sentiment < -0.2 else "🟡"
        st.metric(
            "🌐 NewsAPI Sentiment",
            f"{newsapi_sentiment:.3f} {newsapi_color}",
            f"{newsapi_count} berita"
        )
    
    with col3:
        trading_signal = latest_sentiment['trading_signal'] if pd.notna(latest_sentiment['trading_signal']) else 'Hold'
        positive_pct = latest_sentiment['positive_percentage'] if pd.notna(latest_sentiment['positive_percentage']) else 0
        
        st.metric(
            "📊 Trading Signal",
            trading_signal,
            f"{positive_pct:.1f}% positive"
        )
    
    # Sentiment trend chart
    if len(sentiment_data) > 1:
        fig_sentiment = go.Figure()
        
        # Detik sentiment line
        fig_sentiment.add_trace(go.Scatter(
            x=sentiment_data['date'],
            y=sentiment_data['detik_sentiment'],
            mode='lines+markers',
            name='Detik Sentiment',
            line=dict(color='blue', width=2)
        ))
        
        # NewsAPI sentiment line
        fig_sentiment.add_trace(go.Scatter(
            x=sentiment_data['date'],
            y=sentiment_data['newsapi_sentiment'],
            mode='lines+markers',
            name='NewsAPI Sentiment',
            line=dict(color='red', width=2)
        ))
        
        fig_sentiment.add_hline(y=0, line_dash="dash", line_color="gray")
        fig_sentiment.add_hline(y=0.2, line_dash="dot", line_color="green", annotation_text="Positive Threshold")
        fig_sentiment.add_hline(y=-0.2, line_dash="dot", line_color="red", annotation_text="Negative Threshold")
        
        fig_sentiment.update_layout(
            title=f"Sentiment Trend - {symbol}",
            xaxis_title="Date",
            yaxis_title="Sentiment Score",
            height=400
        )
        
        st.plotly_chart(fig_sentiment, use_container_width=True)

def show_bandarmology_analysis(symbol):
    """Show bandarmology analysis"""
    st.markdown("### 🔍 Bandarmology Analysis")
    
    bandar_data = fetch_data_cached(
        StockAnalysisQueries.get_bandarmology_analysis(symbol),
        f"Bandarmology Analysis for {symbol}"
    )
    
    if bandar_data.empty:
        st.warning(f"⚠️ No bandarmology data available for {symbol}")
        return
    
    latest_bandar = bandar_data.iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        bandar_score = latest_bandar['bandar_score'] if pd.notna(latest_bandar['bandar_score']) else 0
        signal_strength = latest_bandar['signal_strength'] if pd.notna(latest_bandar['signal_strength']) else 'Lemah'
        
        score_color = "🟢" if bandar_score >= 4 else "🟡" if bandar_score >= 2 else "🔴"
        st.metric(
            "🎯 Bandar Score",
            f"{bandar_score}/5 {score_color}",
            signal_strength
        )
    
    with col2:
        volume_ratio = latest_bandar['volume_ratio'] if pd.notna(latest_bandar['volume_ratio']) else 1
        st.metric(
            "📊 Volume Ratio",
            f"{volume_ratio:.2f}x",
            "vs 20-day avg"
        )
    
    with col3:
        foreign_net = latest_bandar['foreign_net'] if pd.notna(latest_bandar['foreign_net']) else 0
        foreign_trend = "🟢 Buying" if foreign_net > 0 else "🔴 Selling" if foreign_net < 0 else "⚪ Neutral"
        st.metric(
            "🌍 Foreign Flow",
            f"{foreign_net:,.0f}",
            foreign_trend
        )
    
    with col4:
        price_change = latest_bandar['percent_change'] if pd.notna(latest_bandar['percent_change']) else 0
        st.metric(
            "💹 Price Movement",
            f"{price_change:+.2f}%",
            "latest session"
        )
    
    # Bandar score trend
    if len(bandar_data) > 1:
        fig_bandar = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            subplot_titles=('Bandar Score Over Time', 'Volume Ratio'),
            row_heights=[0.6, 0.4]
        )
        
        # Bandar score
        fig_bandar.add_trace(
            go.Scatter(
                x=bandar_data['date'],
                y=bandar_data['bandar_score'],
                mode='lines+markers',
                name='Bandar Score',
                line=dict(color='purple', width=3)
            ),
            row=1, col=1
        )
        
        # Volume ratio
        fig_bandar.add_trace(
            go.Bar(
                x=bandar_data['date'],
                y=bandar_data['volume_ratio'],
                name='Volume Ratio',
                marker_color='lightblue'
            ),
            row=2, col=1
        )
        
        fig_bandar.update_layout(height=500, title=f"Bandarmology Analysis - {symbol}")
        st.plotly_chart(fig_bandar, use_container_width=True)

def show_accumulation_distribution(symbol):
    """Show A/D Line analysis"""
    st.markdown("### 📊 Accumulation/Distribution Analysis")
    
    ad_data = fetch_data_cached(
        StockAnalysisQueries.get_accumulation_distribution(symbol),
        f"A/D Line for {symbol}"
    )
    
    if ad_data.empty:
        st.warning(f"⚠️ No A/D Line data available for {symbol}")
        return
    
    latest_ad = ad_data.iloc[0]
    
    col1, col2 = st.columns(2)
    
    with col1:
        money_flow = latest_ad['money_flow_volume'] if pd.notna(latest_ad['money_flow_volume']) else 0
        flow_direction = "🟢 Accumulation" if money_flow > 0 else "🔴 Distribution" if money_flow < 0 else "⚪ Neutral"
        
        st.metric(
            "💰 Money Flow Volume",
            f"{money_flow:,.0f}",
            flow_direction
        )
    
    with col2:
        ad_line = latest_ad['ad_line_cumulative'] if pd.notna(latest_ad['ad_line_cumulative']) else 0
        st.metric(
            "📈 A/D Line",
            f"{ad_line:,.0f}",
            "cumulative"
        )
    
    # A/D Line chart
    if len(ad_data) > 1:
        fig_ad = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            subplot_titles=('Price vs A/D Line', 'Money Flow Volume'),
            row_heights=[0.7, 0.3]
        )
        
        # Price line
        fig_ad.add_trace(
            go.Scatter(
                x=ad_data['date'],
                y=ad_data['close'],
                mode='lines',
                name='Price',
                line=dict(color='blue', width=2),
                yaxis='y'
            ),
            row=1, col=1
        )
        
        # A/D Line
        fig_ad.add_trace(
            go.Scatter(
                x=ad_data['date'],
                y=ad_data['ad_line_cumulative'],
                mode='lines',
                name='A/D Line',
                line=dict(color='orange', width=2),
                yaxis='y2'
            ),
            row=1, col=1
        )
        
        # Money Flow Volume
        colors = ['green' if x > 0 else 'red' for x in ad_data['money_flow_volume']]
        fig_ad.add_trace(
            go.Bar(
                x=ad_data['date'],
                y=ad_data['money_flow_volume'],
                name='Money Flow',
                marker_color=colors
            ),
            row=2, col=1
        )
        
        fig_ad.update_layout(height=600, title=f"A/D Line Analysis - {symbol}")
        fig_ad.update_yaxes(title_text="Price (Rp)", secondary_y=False, row=1, col=1)
        fig_ad.update_yaxes(title_text="A/D Line", secondary_y=True, row=1, col=1)
        
        st.plotly_chart(fig_ad, use_container_width=True)

def show_high_probability_signals(symbol):
    """Show high probability trading signals"""
    st.markdown("### 🎯 High Probability Trading Signals")
    
    try:
        signals_data = fetch_data_cached(
            StockAnalysisQueries.get_high_probability_signals(symbol),
            f"High Probability Signals for {symbol}"
        )
        
        if signals_data.empty:
            st.info(f"ℹ️ No high probability signals data available for {symbol}")
            st.markdown("💡 This feature requires the `advanced_trading_signals` table from your analytics pipeline.")
            return
        
        latest_signal = signals_data.iloc[0]
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            buy_score = latest_signal['buy_score'] if pd.notna(latest_signal['buy_score']) else 0
            st.metric(
                "🎯 Buy Score",
                f"{buy_score}/10",
                "algorithm score"
            )
        
        with col2:
            probability = latest_signal['winning_probability'] if pd.notna(latest_signal['winning_probability']) else 0
            prob_color = "🟢" if probability >= 0.8 else "🟡" if probability >= 0.6 else "🔴"
            st.metric(
                "📊 Win Probability",
                f"{probability:.1%} {prob_color}",
                "historical accuracy"
            )
        
        with col3:
            signal_strength = latest_signal['signal_strength'] if pd.notna(latest_signal['signal_strength']) else 'Weak'
            st.metric(
                "💪 Signal Strength",
                signal_strength,
                "confidence level"
            )
        
        # Show signals history
        if len(signals_data) > 1:
            st.markdown("#### 📈 Signals History")
            
            # Filter high probability signals
            high_prob = signals_data[signals_data['winning_probability'] >= 0.7]
            
            if not high_prob.empty:
                st.dataframe(
                    high_prob[['date', 'buy_score', 'winning_probability', 'signal_strength']].head(10),
                    use_container_width=True
                )
            else:
                st.info("No high probability signals (>70%) found in recent data")
    
    except Exception as e:
        st.info(f"ℹ️ Advanced trading signals not available: {str(e)}")

def show():
    """Main function for Individual Stock Analysis page"""
    
    st.markdown("# 🎯 Individual Stock Analysis")
    st.markdown("Comprehensive analysis combining technical indicators, trading signals, sentiment analysis, and smart money tracking for individual stocks.")
    
    st.markdown("---")
    
    # Stock selection
    selected_symbol = show_stock_search()
    
    if not selected_symbol:
        st.info("👆 Please select a stock from the dropdown above to begin comprehensive analysis.")
        return
    
    st.markdown(f"## 📊 Comprehensive Analysis: **{selected_symbol}**")
    st.markdown("---")
    
    # Get stock movement data for basic info
    stock_data = fetch_data_cached(
        StockAnalysisQueries.get_stock_movement_analysis(selected_symbol),
        f"Stock Movement for {selected_symbol}"
    )
    
    if stock_data.empty:
        st.error(f"❌ No data found for {selected_symbol}")
        return
    
    # Stock metrics overview
    show_stock_metrics(selected_symbol, stock_data)
    
    st.markdown("---")
    
    # Create tabs for different analysis types
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Technical Analysis", 
        "📰 Sentiment Analysis", 
        "🔍 Bandarmology", 
        "📊 A/D Line",
        "🎯 AI Signals"
    ])
    
    with tab1:
        show_technical_analysis_summary(selected_symbol)
    
    with tab2:
        show_sentiment_analysis(selected_symbol)
    
    with tab3:
        show_bandarmology_analysis(selected_symbol)
    
    with tab4:
        show_accumulation_distribution(selected_symbol)
    
    with tab5:
        show_high_probability_signals(selected_symbol)
    
    # Add comprehensive summary at the bottom
    st.markdown("---")
    st.markdown("### 📋 Analysis Summary")
    
    with st.expander("🔍 View Complete Analysis Summary", expanded=False):
        st.markdown(f"""
        **Stock: {selected_symbol}**
        
        This comprehensive analysis combines multiple data sources and analytical methods:
        
        **📈 Technical Analysis:**
        - RSI (Relative Strength Index) for momentum analysis
        - MACD (Moving Average Convergence Divergence) for trend analysis
        - Combined technical recommendation based on both indicators
        
        **📰 Sentiment Analysis:**
        - News sentiment from Detik and NewsAPI sources
        - Positive/negative sentiment percentages
        - Trading signals derived from news sentiment
        
        **🔍 Bandarmology Analysis:**
        - Smart money flow detection using volume analysis
        - Foreign investor activity tracking
        - Accumulation/distribution pattern recognition
        
        **📊 A/D Line Analysis:**
        - Money flow volume tracking
        - Cumulative accumulation/distribution line
        - Price vs money flow correlation
        
        **🎯 AI Trading Signals:**
        - High probability signals from machine learning models
        - Win probability percentages based on historical data
        - Algorithmic buy/sell score recommendations
        
        **Data Sources:**
        - Real-time stock price data
        - Technical indicators (RSI, MACD)
        - News sentiment analysis
        - Foreign investor flow data
        - Volume and money flow analysis
        - AI-generated trading signals
        """)