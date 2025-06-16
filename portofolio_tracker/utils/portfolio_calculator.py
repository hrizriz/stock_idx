"""
Portfolio Calculator - Simplified Version
"""
from datetime import date
import streamlit as st

try:
    from .database import PortfolioDatabase
except ImportError:
    from database import PortfolioDatabase

class PortfolioCalculator:
    def __init__(self):
        self.db = PortfolioDatabase()
    
    def calculate_portfolio_metrics(self, user_id):
        holdings = self.db.get_current_holdings(user_id)
        
        if holdings.empty:
            return {}
        
        metrics = {
            'total_value': holdings['current_value'].sum() if 'current_value' in holdings.columns else holdings['total_cost'].sum(),
            'total_cost': holdings['total_cost'].sum(),
            'total_pnl': holdings['unrealized_pnl'].sum() if 'unrealized_pnl' in holdings.columns else 0,
            'total_stocks': len(holdings),
            'winners': len(holdings[holdings['unrealized_pnl'] > 0]) if 'unrealized_pnl' in holdings.columns else 0,
            'losers': len(holdings[holdings['unrealized_pnl'] < 0]) if 'unrealized_pnl' in holdings.columns else 0
        }
        
        metrics['total_pnl_pct'] = (metrics['total_pnl'] / metrics['total_cost'] * 100) if metrics['total_cost'] > 0 else 0
        metrics['win_rate'] = (metrics['winners'] / metrics['total_stocks'] * 100) if metrics['total_stocks'] > 0 else 0
        
        return metrics
    
    def recalculate_portfolio(self, user_id):
        try:
            # Update current values berdasarkan average price
            conn = self.db.get_connection()
            if conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE portfolio.holdings 
                        SET current_value = total_quantity * COALESCE(current_price, average_price),
                            unrealized_pnl = (total_quantity * COALESCE(current_price, average_price)) - total_cost,
                            unrealized_pnl_pct = CASE 
                                WHEN total_cost > 0 
                                THEN ((total_quantity * COALESCE(current_price, average_price)) - total_cost) / total_cost * 100 
                                ELSE 0 
                            END
                        WHERE user_id = %s
                    """, (user_id,))
                conn.commit()
                conn.close()
            return True
        except Exception as e:
            st.error(f"Error recalculating portfolio: {e}")
            return False
