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
    # Fallback for relative import issues
    from portofolio_tracker.utils.database import PortfolioDatabase

# Import main dashboard database utilities for price fetching
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'dashboard', 'utils'))
try:
    from database import fetch_data_cached
    MAIN_DB_AVAILABLE = True
except ImportError:
    MAIN_DB_AVAILABLE = False
    st.warning("⚠️ Main database utilities not available. Price updates will be limited.")

class PortfolioCalculator:
    def __init__(self):
        self.db = PortfolioDatabase()
    
    def recalculate_portfolio(self, user_id):
        """Recalculate entire portfolio metrics"""
        try:
            # Update current prices from market data
            self._sync_current_prices(user_id)
            
            # Update holdings metrics
            self._update_holdings_metrics(user_id)
            
            # Update portfolio summary
            self._update_portfolio_summary(user_id)
            
            return True
        except Exception as e:
            st.error(f"Error recalculating portfolio: {e}")
            return False
    
    def _sync_current_prices(self, user_id):
        """Sync current prices from main database"""
        if not MAIN_DB_AVAILABLE:
            return
        
        try:
            # Get symbols in portfolio
            holdings = self.db.get_current_holdings(user_id)
            if holdings.empty:
                return
            
            symbols = holdings['symbol'].tolist()
            symbols_str = "', '".join(symbols)
            
            # Get latest prices from main database
            price_query = f"""
            SELECT symbol, close as current_price, date
            FROM daily_stock_summary
            WHERE symbol IN ('{symbols_str}')
            AND date = (SELECT MAX(date) FROM daily_stock_summary)
            """
            
            current_prices = fetch_data_cached(price_query, "Portfolio Price Sync")
            
            if not current_prices.empty:
                # Update prices in portfolio database
                price_updates = {}
                for _, price_row in current_prices.iterrows():
                    price_updates[price_row['symbol']] = price_row['current_price']
                
                self.db.update_all_holdings_prices(price_updates)
                
                # Also save to price history
                self._save_price_history(current_prices)
        
        except Exception as e:
            st.warning(f"Could not sync prices from main database: {e}")
    
    def _save_price_history(self, price_data):
        """Save current prices to price history table"""
        conn = self.db.get_connection()
        if not conn:
            return
        
        try:
            with conn.cursor() as cursor:
                today = date.today()
                
                for _, row in price_data.iterrows():
                    cursor.execute("""
                        INSERT INTO portfolio.price_history (symbol, date, close_price)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (symbol, date) 
                        DO UPDATE SET close_price = EXCLUDED.close_price
                    """, (row['symbol'], today, row['current_price']))
            
            conn.commit()
        
        except Exception as e:
            st.warning(f"Could not save price history: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def _update_holdings_metrics(self, user_id):
        """Update current value and P&L for all holdings"""
        conn = self.db.get_connection()
        if not conn:
            return
        
        try:
            with conn.cursor() as cursor:
                # Update current values where current_price is available
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
                # Calculate portfolio totals
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
                    
                    # Calculate realized P&L from sell transactions
                    realized_pnl = self._calculate_realized_pnl(user_id, cursor)
                    
                    total_pnl = realized_pnl + unrealized_pnl
                    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0
                    
                    # Insert or update today's summary
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
                            total_stocks = EXCLUDED.total_stocks,
                            created_at = CURRENT_TIMESTAMP
                    """, (user_id, today, total_invested, current_value, 
                         total_pnl, total_pnl_pct, realized_pnl, unrealized_pnl, total_stocks))
            
            conn.commit()
        
        except Exception as e:
            st.error(f"Error updating portfolio summary: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def _calculate_realized_pnl(self, user_id, cursor):
        """Calculate realized P&L using FIFO method"""
        try:
            # Get all transactions ordered by date
            cursor.execute("""
                SELECT symbol, trade_date, transaction_type, quantity, price, total_value
                FROM portfolio.transactions
                WHERE user_id = %s
                ORDER BY symbol, trade_date, id
            """, (user_id,))
            
            transactions = cursor.fetchall()
            
            # Group by symbol
            symbol_transactions = {}
            for trans in transactions:
                symbol = trans[0]
                if symbol not in symbol_transactions:
                    symbol_transactions[symbol] = []
                symbol_transactions[symbol].append(trans)
            
            total_realized_pnl = 0
            
            # Calculate realized P&L for each symbol using FIFO
            for symbol, symbol_trans in symbol_transactions.items():
                realized_pnl = self._calculate_symbol_realized_pnl(symbol_trans)
                total_realized_pnl += realized_pnl
            
            return total_realized_pnl
        
        except Exception as e:
            st.warning(f"Error calculating realized P&L: {e}")
            return 0
    
    def _calculate_symbol_realized_pnl(self, transactions):
        """Calculate realized P&L for a single symbol using FIFO"""
        buy_queue = []  # Queue of (quantity, price) for FIFO
        realized_pnl = 0
        
        for trans in transactions:
            symbol, trade_date, trans_type, quantity, price, total_value = trans
            
            if trans_type == 'BUY':
                # Add to buy queue
                buy_queue.append((quantity, price))
            
            elif trans_type == 'SELL':
                # Match with earliest buys (FIFO)
                remaining_sell_qty = quantity
                sell_price = price
                
                while remaining_sell_qty > 0 and buy_queue:
                    buy_qty, buy_price = buy_queue[0]
                    
                    if buy_qty <= remaining_sell_qty:
                        # Use entire buy lot
                        pnl = buy_qty * (sell_price - buy_price)
                        realized_pnl += pnl
                        remaining_sell_qty -= buy_qty
                        buy_queue.pop(0)
                    else:
                        # Partial use of buy lot
                        pnl = remaining_sell_qty * (sell_price - buy_price)
                        realized_pnl += pnl
                        # Update remaining buy quantity
                        buy_queue[0] = (buy_qty - remaining_sell_qty, buy_price)
                        remaining_sell_qty = 0
        
        return realized_pnl
    
    def calculate_portfolio_metrics(self, user_id):
        """Calculate comprehensive portfolio metrics"""
        holdings = self.db.get_current_holdings(user_id)
        summary_history = self.db.get_portfolio_summary(user_id)
        
        if holdings.empty:
            return {}
        
        metrics = {}
        
        # Basic metrics
        metrics['total_value'] = holdings['current_value'].sum()
        metrics['total_cost'] = holdings['total_cost'].sum()
        metrics['total_pnl'] = holdings['unrealized_pnl'].sum()
        metrics['total_pnl_pct'] = (metrics['total_pnl'] / metrics['total_cost'] * 100) if metrics['total_cost'] > 0 else 0
        
        # Holdings analysis
        metrics['total_stocks'] = len(holdings)
        metrics['winners'] = len(holdings[holdings['unrealized_pnl'] > 0])
        metrics['losers'] = len(holdings[holdings['unrealized_pnl'] < 0])
        metrics['win_rate'] = (metrics['winners'] / metrics['total_stocks'] * 100) if metrics['total_stocks'] > 0 else 0
        
        # Best/worst performers
        if not holdings.empty:
            best_performer = holdings.loc[holdings['unrealized_pnl_pct'].idxmax()]
            worst_performer = holdings.loc[holdings['unrealized_pnl_pct'].idxmin()]
            
            metrics['best_performer'] = {
                'symbol': best_performer['symbol'],
                'pnl_pct': best_performer['unrealized_pnl_pct']
            }
            metrics['worst_performer'] = {
                'symbol': worst_performer['symbol'],
                'pnl_pct': worst_performer['unrealized_pnl_pct']
            }
        
        # Concentration analysis
        holdings_sorted = holdings.sort_values('current_value', ascending=False)
        top_5_value = holdings_sorted.head(5)['current_value'].sum()
        metrics['top_5_concentration'] = (top_5_value / metrics['total_value'] * 100) if metrics['total_value'] > 0 else 0
        
        # Historical performance
        if not summary_history.empty and len(summary_history) > 1:
            latest = summary_history.iloc[-1]
            previous = summary_history.iloc[-2]
            
            metrics['daily_change'] = latest['current_value'] - previous['current_value']
            metrics['daily_change_pct'] = (metrics['daily_change'] / previous['current_value'] * 100) if previous['current_value'] > 0 else 0
            
            # Calculate max drawdown
            metrics['max_drawdown'] = self._calculate_max_drawdown(summary_history)
        
        return metrics
    
    def _calculate_max_drawdown(self, summary_history):
        """Calculate maximum drawdown from portfolio value history"""
        if len(summary_history) < 2:
            return 0
        
        values = summary_history['current_value'].values
        peak = values[0]
        max_drawdown = 0
        
        for value in values:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak * 100 if peak > 0 else 0
            max_drawdown = max(max_drawdown, drawdown)
        
        return max_drawdown
    
    def calculate_stock_performance_ranking(self, user_id):
        """Calculate performance ranking for all stocks in portfolio"""
        holdings = self.db.get_current_holdings(user_id)
        
        if holdings.empty:
            return pd.DataFrame()
        
        # Add performance rankings
        holdings_ranked = holdings.copy()
        holdings_ranked['value_rank'] = holdings_ranked['current_value'].rank(ascending=False)
        holdings_ranked['pnl_rank'] = holdings_ranked['unrealized_pnl'].rank(ascending=False)
        holdings_ranked['pnl_pct_rank'] = holdings_ranked['unrealized_pnl_pct'].rank(ascending=False)
        
        # Add weight in portfolio
        total_value = holdings_ranked['current_value'].sum()
        holdings_ranked['weight_pct'] = holdings_ranked['current_value'] / total_value * 100
        
        return holdings_ranked.sort_values('unrealized_pnl_pct', ascending=False)
    
    def generate_portfolio_report(self, user_id):
        """Generate comprehensive portfolio report"""
        report = {}
        
        # Basic portfolio data
        report['holdings'] = self.db.get_current_holdings(user_id)
        report['transactions'] = self.db.get_all_transactions(user_id)
        report['summary_history'] = self.db.get_portfolio_summary(user_id)
        
        # Calculated metrics
        report['metrics'] = self.calculate_portfolio_metrics(user_id)
        report['performance_ranking'] = self.calculate_stock_performance_ranking(user_id)
        
        # Risk analysis
        report['risk_metrics'] = self._calculate_risk_metrics(report['holdings'])
        
        return report
    
    def _calculate_risk_metrics(self, holdings):
        """Calculate portfolio risk metrics"""
        if holdings.empty:
            return {}
        
        risk_metrics = {}
        
        # Concentration risk
        total_value = holdings['current_value'].sum()
        largest_position_pct = holdings['current_value'].max() / total_value * 100 if total_value > 0 else 0
        risk_metrics['largest_position_pct'] = largest_position_pct
        
        # Volatility estimate (based on P&L percentages)
        pnl_pcts = holdings['unrealized_pnl_pct'].dropna()
        if not pnl_pcts.empty:
            risk_metrics['portfolio_volatility'] = pnl_pcts.std()
            risk_metrics['value_at_risk_95'] = pnl_pcts.quantile(0.05)  # 5th percentile
        
        # Diversification score (simple measure)
        num_stocks = len(holdings)
        if num_stocks > 0:
            # Herfindahl index for concentration
            weights = holdings['current_value'] / total_value
            herfindahl = (weights ** 2).sum()
            risk_metrics['diversification_score'] = (1 - herfindahl) * 100
        
        return risk_metrics
    
    def update_portfolio_from_market_data(self, user_id):
        """Update entire portfolio with latest market data"""
        try:
            # Sync prices from main database
            self._sync_current_prices(user_id)
            
            # Recalculate everything
            self.recalculate_portfolio(user_id)
            
            return True
        
        except Exception as e:
            st.error(f"Error updating portfolio from market data: {e}")
            return False