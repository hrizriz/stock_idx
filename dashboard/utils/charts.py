
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd

class ChartFactory:
    """Factory class for creating various chart types"""
    
    @staticmethod
    def create_candlestick_with_indicators(data, symbol, period):
        """Create comprehensive candlestick chart with RSI and MACD"""
        
        fig = make_subplots(
            rows=4, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            subplot_titles=(
                f'{symbol} - Price & Volume',
                'RSI (14)',
                'MACD',
                'Volume'
            ),
            row_heights=[0.5, 0.2, 0.2, 0.1]
        )
        
        # Candlestick chart
        fig.add_trace(
            go.Candlestick(
                x=data['date'],
                open=data['open_price'],
                high=data['high'],
                low=data['low'],
                close=data['close'],
                name="Price",
                increasing_line_color='green',
                decreasing_line_color='red'
            ),
            row=1, col=1
        )
        
        # RSI
        fig.add_trace(
            go.Scatter(
                x=data['date'],
                y=data['rsi'],
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
        
        # MACD
        fig.add_trace(
            go.Scatter(
                x=data['date'],
                y=data['macd_line'],
                mode='lines',
                name='MACD Line',
                line=dict(color='blue', width=2)
            ),
            row=3, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=data['date'],
                y=data['signal_line'],
                mode='lines',
                name='Signal Line',
                line=dict(color='red', width=2)
            ),
            row=3, col=1
        )
        
        # MACD Histogram
        colors = ['green' if val >= 0 else 'red' for val in data['macd_histogram'].fillna(0)]
        fig.add_trace(
            go.Bar(
                x=data['date'],
                y=data['macd_histogram'],
                name='MACD Histogram',
                marker_color=colors,
                opacity=0.6
            ),
            row=3, col=1
        )
        
        # Volume
        fig.add_trace(
            go.Bar(
                x=data['date'],
                y=data['volume'],
                name='Volume',
                marker_color='lightblue',
                opacity=0.7
            ),
            row=4, col=1
        )
        
        # Update layout
        fig.update_layout(
            title=f"Technical Analysis - {symbol} ({period})",
            xaxis_rangeslider_visible=False,
            height=800,
            showlegend=True
        )
        
        fig.update_yaxes(title_text="Price (Rp)", row=1, col=1)
        fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100])
        fig.update_yaxes(title_text="MACD", row=3, col=1)
        fig.update_yaxes(title_text="Volume", row=4, col=1)
        fig.update_xaxes(title_text="Date", row=4, col=1)
        
        return fig
    
    @staticmethod
    def create_top_movers_chart(data, title="Top Movers"):
        """Create top movers bar chart"""
        fig = px.bar(
            data,
            x='percent_change',
            y='symbol',
            color='percent_change',
            color_continuous_scale='RdYlGn',
            color_continuous_midpoint=0,
            title=title,
            labels={'percent_change': 'Percent Change (%)', 'symbol': 'Stock Symbol'},
            hover_data=['name', 'close', 'volume'] if 'name' in data.columns else None
        )
        fig.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
        return fig