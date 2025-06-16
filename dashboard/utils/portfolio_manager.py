"""
Portfolio Manager Bridge
Menghubungkan dashboard utama dengan portfolio tracker
"""

import sys
import os
import streamlit as st

# Add portfolio tracker path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'portofolio_tracker'))

class PortfolioManager:
    def __init__(self):
        self.portfolio_available = False
        self._check_portfolio_availability()
    
    def _check_portfolio_availability(self):
        """Check if portfolio tracker modules are available"""
        try:
            from utils.database import PortfolioDatabase
            from utils.portfolio_calculator import PortfolioCalculator
            
            # Test database connection
            db = PortfolioDatabase()
            if db.check_connection():
                self.portfolio_available = True
                self.db = db
                self.calculator = PortfolioCalculator()
        except ImportError as e:
            st.warning(f"Portfolio modules not available: {e}")
            self.portfolio_available = False
        except Exception as e:
            st.warning(f"Portfolio connection error: {e}")
            self.portfolio_available = False
    
    def is_available(self):
        """Check if portfolio tracker is available"""
        return self.portfolio_available
    
    def get_portfolio_summary(self, user_id="default_user"):
        """Get portfolio summary for user"""
        if not self.portfolio_available:
            return {}
        
        try:
            return self.calculator.calculate_portfolio_metrics(user_id)
        except Exception as e:
            st.error(f"Error getting portfolio summary: {e}")
            return {}
    
    def get_holdings_for_stock(self, symbol, user_id="default_user"):
        """Get holdings information for specific stock"""
        if not self.portfolio_available:
            return None
        
        try:
            holdings = self.db.get_current_holdings(user_id)
            if not holdings.empty:
                stock_holdings = holdings[holdings['symbol'] == symbol]
                if not stock_holdings.empty:
                    return stock_holdings.iloc[0].to_dict()
            return None
        except Exception as e:
            st.error(f"Error getting holdings for {symbol}: {e}")
            return None
    
    def add_quick_transaction(self, transaction_data, user_id="default_user"):
        """Add transaction from other pages"""
        if not self.portfolio_available:
            return False
        
        try:
            return self.db.add_transaction(user_id, transaction_data)
        except Exception as e:
            st.error(f"Error adding transaction: {e}")
            return False

# Global portfolio manager instance
portfolio_manager = PortfolioManager()
