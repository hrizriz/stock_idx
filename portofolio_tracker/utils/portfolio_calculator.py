# ============================================================================
# PORTFOLIO CALCULATOR
# File: portofolio_tracker/utils/portfolio_calculator.py
# ============================================================================

from datetime import datetime, date
import pandas as pd
import streamlit as st
import sys
import os

# Import database utilities
try:
    from .database import PortfolioDatabase
except ImportError:
    try:
        from portofolio_tracker.utils.database import PortfolioDatabase
    except ImportError:
        from database import PortfolioDatabase

# Import main dashboard database utilities for price fetching
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'dashboard', 'utils'))
try:
    from database import fetch_data_cached
    MAIN_DB_AVAILABLE = True
except ImportError:
    MAIN_DB_AVAILABLE = False

class PortfolioCalculator:
    def __init__(self):
        self.db = PortfolioDatabase()
    
    def recalculate_portfolio(self, user_id):
        """Recalculate entire portfolio metrics"""
        try:
            self._update_holdings_metrics(user_id)
            self._update_portfolio_summary(user_id)
            return True
        except Exception as e:
            st.error(f"Error recalculating portfolio: {e}")
            return False
    
    def _update_holdings_metrics(self, user_id):
        """Update current value and P&L for all holdings"""
        conn = self.db.get_connection()
        if not conn:
            return
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE portfolio.holdings 
                    SET current_value = total_quantity * COALESCE(current_price, average_price),
                        unrealized_pnl = (total_quantity * COALESCE(current_price, average_price)) - total_cost,
                        unrealized_pnl_pct = CASE 
                            WHEN total_cost > 0 
                            THEN ((total_quantity * COALESCE(current_price, average_price)) - total_cost) / total_cost * 100 
                            ELSE 0 
                        END,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE user_id = %s
                """, (user_id,))
            
            conn.commit()
        except Exception as e:
            st.error(f"Error updating holdings metrics: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def _update_portfolio_summary(self, user_id):
        """Update portfolio summary for today"""
        conn = self.db.get_connection()
        if not conn:
            return
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COALESCE(SUM(total_cost), 0) as total_invested,
                        COALESCE(SUM(current_value), 0) as current_value,
                        COALESCE(SUM(unrealized_pnl), 0) as unrealized_pnl,
                        COUNT(*) as total_stocks
                    FROM portfolio.holdings 
                    WHERE user_id = %s AND total_quantity > 0
                """, (user_id,))
                
                result = cursor.fetchone()
                if result:
                    total_invested, current_value, unrealized_pnl, total_stocks = result
                    total_pnl = unrealized_pnl
                    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0
                    today = date.today()
                    
                    cursor.execute("""
                        INSERT INTO portfolio.portfolio_summary 
                        (user_id, summary_date, total_invested, current_value, 
                         total_pnl, total_pnl_pct, realized_pnl, unrealized_pnl, total_stocks)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, summary_date)
                        DO UPDATE SET
                            total_invested = EXCLUDED.total_invested,
                            current_value = EXCLUDED.current_value,
                            total_pnl = EXCLUDED.total_pnl,
                            total_pnl_pct = EXCLUDED.total_pnl_pct,
                            realized_pnl = EXCLUDED.realized_pnl,
                            unrealized_pnl = EXCLUDED.unrealized_pnl,
                            total_stocks = EXCLUDED.total_stocks
                    """, (user_id, today, total_invested, current_value, 
                         total_pnl, total_pnl_pct, 0, unrealized_pnl, total_stocks))
            
            conn.commit()
        except Exception as e:
            st.error(f"Error updating portfolio summary: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def calculate_portfolio_metrics(self, user_id):
        """Calculate comprehensive portfolio metrics"""
        holdings = self.db.get_current_holdings(user_id)
        
        if holdings.empty:
            return {}
        
        metrics = {
            'total_value': holdings['current_value'].sum(),
            'total_cost': holdings['total_cost'].sum(),
            'total_pnl': holdings['unrealized_pnl'].sum(),
            'total_stocks': len(holdings),
            'winners': len(holdings[holdings['unrealized_pnl'] > 0]),
            'losers': len(holdings[holdings['unrealized_pnl'] < 0])
        }
        
        metrics['total_pnl_pct'] = (metrics['total_pnl'] / metrics['total_cost'] * 100) if metrics['total_cost'] > 0 else 0
        metrics['win_rate'] = (metrics['winners'] / metrics['total_stocks'] * 100) if metrics['total_stocks'] > 0 else 0
        
        return metrics
