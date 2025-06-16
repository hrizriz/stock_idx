import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

class StockAnalyzer:
    def __init__(self, symbol):
        self.symbol = symbol
        self.data = None
        
    def fetch_data(self, period="1y"):
        """Fetch stock data from yfinance"""
        try:
            self.data = yf.download(self.symbol, period=period)
            return True
        except Exception as e:
            print(f"Error fetching data: {e}")
            return False
            
    def get_basic_info(self):
        """Get basic stock information"""
        if self.data is None:
            return None
            
        latest = self.data.iloc[-1]
        return {
            'symbol': self.symbol,
            'last_price': latest['Close'],
            'change': latest['Close'] - self.data.iloc[-2]['Close'],
            'volume': latest['Volume']
        } 