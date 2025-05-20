import pandas as pd
import numpy as np
import logging
import json
from datetime import datetime
from airflow.models import Variable
from .database import get_database_connection, get_latest_stock_date
from .telegram import send_telegram_message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_performance_report(report_type='DAILY', lookback_days=60):
    """
    Send performance report for previous trading signals
    
    Parameters:
    report_type (str): Report type - DAILY, WEEKLY, or MONTHLY
    lookback_days (int): Days to look back for performance data
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    latest_date = get_latest_stock_date()
    
    try:
        backtest_table = f"public_analytics.backtest_results_{report_type.lower()}" if report_type != 'DAILY' else "public_analytics.backtest_results"
        signals_table = f"public_analytics.advanced_trading_signals_{report_type.lower()}" if report_type != 'DAILY' else "public_analytics.advanced_trading_signals"
        
        cursor = conn.cursor()
        cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = '{'backtest_results' if report_type == 'DAILY' else f"backtest_results_{report_type.lower()}"}'
        );
        """)
        backtest_table_exists = cursor.fetchone()[0]
        
        cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = '{'advanced_trading_signals' if report_type == 'DAILY' else f"advanced_trading_signals_{report_type.lower()}"}'
        );
        """)
        signals_table_exists = cursor.fetchone()[0]
        
        if not backtest_table_exists or not signals_table_exists:
            cursor.close()
            conn.close()
            return f"Required tables don't exist yet. Skipping {report_type} performance report."
        
        sql = f"""
        WITH backtest_results AS (
            SELECT 
                symbol,
                signal_date,
                buy_score,
                winning_probability,
                entry_price,
                exit_price,
                percent_change_5d,
                is_win
            FROM {backtest_table}
            WHERE signal_date >= '{latest_date}'::date - INTERVAL '{lookback_days} days'
        )
        SELECT 
            buy_score,
            COUNT(*) as total_signals,
            SUM(CASE WHEN is_win THEN 1 ELSE 0 END) as win_count,
            ROUND(SUM(CASE WHEN is_win THEN 1 ELSE 0 END)::float / COUNT(*) * 100, 1) as win_rate,
            ROUND(AVG(percent_change_5d), 2) as avg_return
        FROM backtest_results
        GROUP BY buy_score
        ORDER BY buy_score DESC
        """
        
        try:
            performance_df = pd.read_sql(sql, conn)
        except:
            performance_df = pd.DataFrame()
            logger.warning(f"{backtest_table} table does not exist yet")
        
        hold_period = 5  # Default for DAILY
        if report_type == 'WEEKLY':
            hold_period = 10
        elif report_type == 'MONTHLY':
            hold_period = 20
            
        recent_sql = f"""
        WITH completed_signals AS (
            SELECT 
                s.symbol,
                s.date as signal_date,
                s.buy_score,
                s.winning_probability,
                m1.close as entry_price,
                m2.close as exit_price,
                ROUND(((m2.close - m1.close) / m1.close * 100)::numeric, 2) as percent_change,
                CASE WHEN ((m2.close - m1.close) / m1.close * 100) > 2 THEN TRUE ELSE FALSE END as is_win,
                m1.name
            FROM {signals_table} s
            JOIN public.daily_stock_summary m1 
                ON s.symbol = m1.symbol AND s.date = m1.date
            JOIN public.daily_stock_summary m2
                ON s.symbol = m2.symbol AND m2.date = s.date + INTERVAL '{hold_period} days'
            WHERE s.date BETWEEN '{latest_date}'::date - INTERVAL '{lookback_days} days' AND '{latest_date}'::date - INTERVAL '{hold_period} days'
            AND s.winning_probability >= 0.7
        )
        SELECT * FROM completed_signals
        ORDER BY signal_date DESC, percent_change DESC
        LIMIT 10
        """
        
        try:
            recent_df = pd.read_sql(recent_sql, conn)
        except:
            recent_df = pd.DataFrame()
            logger.warning("No completed signals found")
            
        conn.close()
    except Exception as e:
        logger.error(f"Error querying performance data: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error querying performance data: {str(e)}"
    
    message = f"📊 *LAPORAN PERFORMA SINYAL TRADING {report_type}* 📊\n\n"
    
    if not performance_df.empty:
        message += "*Statistik Win Rate Berdasarkan Skor:*\n\n"
        message += "| Skor | Jumlah | Win Rate | Avg Return |\n"
        message += "|------|--------|----------|------------|\n"
        
        for _, row in performance_df.iterrows():
            message += f"| {row['buy_score']} | {row['total_signals']} | {row['win_rate']}% | {row['avg_return']}% |\n"
        
        total_signals = performance_df['total_signals'].sum()
        total_wins = performance_df['win_count'].sum()
        overall_win_rate = (total_wins / total_signals * 100) if total_signals > 0 else 0
        avg_return = performance_df['avg_return'].mean()
        
        message += f"\n*Overall:* {total_signals} sinyal, Win Rate {overall_win_rate:.1f}%, Return Rata-rata {avg_return:.2f}%\n\n"
    else:
        message += "*Belum ada data backtest yang cukup*\n\n"
    
    if not recent_df.empty:
        message += "*Performance Sinyal Terbaru:*\n\n"
        
        win_count = 0
        for i, row in enumerate(recent_df.itertuples(), 1):
            signal_date = row.signal_date
            if isinstance(signal_date, pd.Timestamp):
                signal_date = signal_date.strftime('%Y-%m-%d')
                
            emoji = "✅" if row.is_win else "❌"
            
            message += f"{emoji} *{row.symbol}* ({row.name}): {row.percent_change}%\n"
            message += f"   Tanggal: {signal_date} | Skor: {row.buy_score} | Win Prob: {row.winning_probability*100:.0f}%\n"
            message += f"   Entry: Rp{row.entry_price:,.0f} → Exit: Rp{row.exit_price:,.0f}\n\n"
            
            if row.is_win:
                win_count += 1
        
        recent_win_rate = (win_count / len(recent_df) * 100)
        message += f"*Win Rate {lookback_days} Hari Terakhir:* {recent_win_rate:.1f}% ({win_count}/{len(recent_df)})\n\n"
    else:
        message += f"*Belum ada sinyal yang complete dalam {lookback_days} hari terakhir*\n\n"
    
    message += "*Tips Meningkatkan Win Rate:*\n"
    
    if report_type == 'WEEKLY':
        message += "1. Utamakan saham dengan skor 7+ (win rate >80%)\n"
        message += "2. Perhatikan tren mingguan dan level support/resistance\n"
        message += "3. Gunakan money management yang ketat (max 3% risiko per trade)\n"
        message += "4. Take profit bertahap pada +7% dan +15%\n"
    elif report_type == 'MONTHLY':
        message += "1. Pilih saham dengan fundamental kuat dan skor 6+ (win rate >80%)\n"
        message += "2. Analisis tren bulanan dan siklus pasar secara keseluruhan\n"
        message += "3. Gunakan money management yang ketat (max 5% risiko per trade)\n"
        message += "4. Take profit bertahap pada +10% dan +20%\n"
    else:
        message += "1. Utamakan saham dengan skor 8+ (win rate >85%)\n"
        message += "2. Cari konfirmasi volume pada breakout\n"
        message += "3. Gunakan money management yang ketat (max 2% risiko per trade)\n"
        message += "4. Take profit bertahap pada +5% dan +10%\n"
    
    result = send_telegram_message(message)
    if "successfully" in result:
        return f"Performance report for {report_type} sent successfully"
    else:
        return result

def calculate_advanced_indicators(lookback_period=300, signal_type='DAILY'):
    """
    Menghitung indikator tingkat lanjut dan membuat sistem penyaringan multi-layer
    
    Parameters:
    lookback_period (int): Number of days to look back for analysis
    signal_type (str): Signal type - DAILY, WEEKLY, or MONTHLY
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Failed to connect to database: {str(e)}"
    
    latest_date = get_latest_stock_date()
    
    query = f"""
    SELECT 
        d.symbol, 
        d.date,
        d.close,
        d.open_price as open,
        d.high,
        d.low,
        d.volume,
        d.name,
        d.prev_close,
        d.avg_volume_30d,
        CASE 
            WHEN d.prev_close > 0 THEN (d.volume / d.avg_volume_30d)
            ELSE NULL
        END as volume_ratio,
        CASE 
            WHEN d.prev_close > 0 THEN (d.close - d.prev_close) / d.prev_close * 100
            ELSE NULL
        END as daily_change
    FROM public.daily_stock_summary d
    WHERE d.date >= '{latest_date}'::date - INTERVAL '{lookback_period} days'
    ORDER BY d.symbol, d.date
    """
    
    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        logger.error(f"Error querying stock data: {str(e)}")
        conn.close()
        return f"Error querying stock data: {str(e)}"
    
    if df.empty:
        logger.warning(f"No stock data for calculating advanced indicators {signal_type}")
        conn.close()
        return f"No data for {signal_type}"
    
    signals_table = f"public_analytics.advanced_trading_signals_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.advanced_trading_signals"
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {signals_table} (
            symbol TEXT,
            date DATE,
            buy_score INTEGER,
            selling_score INTEGER,
            winning_probability NUMERIC,
            signal_strength TEXT,
            indicators_triggered TEXT[],
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, date)
        )
        """)
        conn.commit()
    except Exception as e:
        logger.error(f"Error creating signals table: {str(e)}")
        cursor.close()
        conn.close()
        return f"Error creating signals table: {str(e)}"
    
    try:
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = 'indicator_performance_metrics'
        );
        """)
        metrics_table_exists = cursor.fetchone()[0]
        
        indicator_weights = {}
        
        if metrics_table_exists:
            backtest_sql = """
            SELECT 
                indicator_name,
                AVG(win_rate) as avg_win_rate
            FROM indicator_performance_metrics
            GROUP BY indicator_name
            """
            
            try:
                weights_df = pd.read_sql(backtest_sql, conn)
                for _, row in weights_df.iterrows():
                    indicator_weights[row['indicator_name']] = row['avg_win_rate']
            except Exception as e:
                logger.warning(f"Error fetching indicator weights: {str(e)}")
                indicator_weights = {
                    'rsi_oversold': 1.0,
                    'macd_bullish': 1.0,
                    'volume_shock': 1.0,
                    'demand_zone': 2.0,
                    'foreign_flow_positive': 1.0,
                    'bullish_engulfing': 2.0,
                    'uptrend_structure': 2.0,
                    'bollinger_oversold': 1.0,
                    'adx_strong': 1.0,
                    'fibonacci_support': 1.0
                }
        else:
            indicator_weights = {
                'rsi_oversold': 1.0,
                'macd_bullish': 1.0,
                'volume_shock': 1.0,
                'demand_zone': 2.0,
                'foreign_flow_positive': 1.0,
                'bullish_engulfing': 2.0,
                'uptrend_structure': 2.0,
                'bollinger_oversold': 1.0,
                'adx_strong': 1.0,
                'fibonacci_support': 1.0
            }
        
        results = []
        processed_count = 0
        
        for symbol, group in df.groupby('symbol'):
            if len(group) < 20:  # Need enough data
                continue
                
            group = group.sort_values('date')
            
            try:
                group['avg_volume_10d'] = group['volume'].rolling(window=10).mean()
                group['volume_shock'] = group['volume'] > group['avg_volume_10d'] * 3
                
                group['high_20d'] = group['high'].rolling(window=20).max()
                group['low_20d'] = group['low'].rolling(window=20).min()
                
                group['near_support'] = group['close'] < group['low_20d'] * 1.03
                group['near_resistance'] = group['close'] > group['high_20d'] * 0.97
                
                group['prev_open'] = group['open'].shift(1)
                group['prev_close'] = group['close'].shift(1)
                group['bullish_engulfing'] = (
                    (group['open'] < group['prev_close']) & 
                    (group['close'] > group['prev_open']) &
                    (group['close'] > group['open']) &
                    (group['prev_close'] < group['prev_open'])
                )
                
                if signal_type == 'WEEKLY':
                    latest_data = group.tail(5)
                elif signal_type == 'MONTHLY':
                    latest_data = group.tail(20)
                else:
                    latest_data = group.tail(2)
                
                for i, (idx, row) in enumerate(latest_data.iterrows()):
                    if i != len(latest_data) - 1:
                        continue
                    
                    try:
                        rsi_table = f"public_analytics.technical_indicators_rsi_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_rsi"
                        cursor.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public_analytics' 
                            AND table_name = '{'technical_indicators_rsi' if signal_type == 'DAILY' else f"technical_indicators_rsi_{signal_type.lower()}"}'
                        );
                        """)
                        rsi_table_exists = cursor.fetchone()[0]
                        
                        macd_table = f"public_analytics.technical_indicators_macd_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_macd"
                        cursor.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public_analytics' 
                            AND table_name = '{'technical_indicators_macd' if signal_type == 'DAILY' else f"technical_indicators_macd_{signal_type.lower()}"}'
                        );
                        """)
                        macd_table_exists = cursor.fetchone()[0]
                        
                        bb_table = f"public_analytics.technical_indicators_bollinger_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_bollinger"
                        cursor.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public_analytics' 
                            AND table_name = '{'technical_indicators_bollinger' if signal_type == 'DAILY' else f"technical_indicators_bollinger_{signal_type.lower()}"}'
                        );
                        """)
                        bb_table_exists = cursor.fetchone()[0]
                        
                        rsi_df = pd.DataFrame()
                        macd_df = pd.DataFrame()
                        bb_df = pd.DataFrame()
                        
                        if rsi_table_exists:
                            rsi_query = f"""
                            SELECT rsi, rsi_signal 
                            FROM {rsi_table}
                            WHERE symbol = '{symbol}' AND date = '{row['date']}'
                            """
                            rsi_df = pd.read_sql(rsi_query, conn)
                        
                        if macd_table_exists:
                            macd_query = f"""
                            SELECT macd_line, signal_line, macd_histogram, macd_signal
                            FROM {macd_table}
                            WHERE symbol = '{symbol}' AND date = '{row['date']}'
                            """
                            macd_df = pd.read_sql(macd_query, conn)
                        
                        if bb_table_exists:
                            bb_query = f"""
                            SELECT middle_band, upper_band, lower_band, percent_b, bb_signal
                            FROM {bb_table}
                            WHERE symbol = '{symbol}' AND date = '{row['date']}'
                            """
                            bb_df = pd.read_sql(bb_query, conn)
                        
                        buy_score = 0
                        indicators_triggered = []
                        
                        if not rsi_df.empty and 'rsi_signal' in rsi_df.columns and rsi_df.iloc[0]['rsi_signal'] == 'Oversold':
                            buy_score += indicator_weights.get('rsi_oversold', 1.0)
                            indicators_triggered.append('RSI Oversold')
                        
                        if not macd_df.empty and 'macd_signal' in macd_df.columns and macd_df.iloc[0]['macd_signal'] == 'Bullish':
                            buy_score += indicator_weights.get('macd_bullish', 1.0)
                            indicators_triggered.append('MACD Bullish')
                        
                        if row['volume_shock']:
                            buy_score += indicator_weights.get('volume_shock', 1.0)
                            indicators_triggered.append('Volume Shock')
                        
                        if row['near_support']:
                            buy_score += indicator_weights.get('demand_zone', 2.0)
                            indicators_triggered.append('Near Support')
                        
                        if row['bullish_engulfing']:
                            buy_score += indicator_weights.get('bullish_engulfing', 2.0)
                            indicators_triggered.append('Bullish Engulfing')
                        
                        if not bb_df.empty and 'bb_signal' in bb_df.columns and (bb_df.iloc[0]['bb_signal'] == 'Oversold' or bb_df.iloc[0]['bb_signal'] == 'Near Oversold'):
                            buy_score += indicator_weights.get('bollinger_oversold', 1.0)
                            indicators_triggered.append('Bollinger Oversold')
                        
                        max_possible_score = sum([
                            indicator_weights.get('rsi_oversold', 1.0),
                            indicator_weights.get('macd_bullish', 1.0),
                            indicator_weights.get('volume_shock', 1.0),
                            indicator_weights.get('demand_zone', 2.0),
                            indicator_weights.get('bullish_engulfing', 2.0),
                            indicator_weights.get('bollinger_oversold', 1.0)
                        ])
                        
                        normalized_score = min(10, round(buy_score * 10 / max_possible_score))
                        
                        if normalized_score >= 8:
                            signal_strength = 'Strong'
                        elif normalized_score >= 6:
                            signal_strength = 'Moderate'
                        elif normalized_score >= 4:
                            signal_strength = 'Weak'
                        else:
                            signal_strength = 'No Signal'
                        
                        backtest_table = f"public_analytics.backtest_results_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.backtest_results"
                        cursor.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public_analytics' 
                            AND table_name = '{'backtest_results' if signal_type == 'DAILY' else f"backtest_results_{signal_type.lower()}"}'
                        );
                        """)
                        backtest_table_exists = cursor.fetchone()[0]
                        
                        if backtest_table_exists:
                            try:
                                win_prob_query = f"""
                                SELECT
                                    AVG(CASE WHEN is_win THEN 1.0 ELSE 0.0 END) as win_rate
                                FROM
                                    {backtest_table}
                                WHERE
                                    buy_score = {normalized_score}
                                """
                                win_prob_df = pd.read_sql(win_prob_query, conn)
                                if not win_prob_df.empty and not pd.isna(win_prob_df.iloc[0]['win_rate']):
                                    winning_probability = win_prob_df.iloc[0]['win_rate']
                                else:
                                    winning_probability = min(0.95, 0.5 + normalized_score * 0.05)
                            except Exception as e:
                                logger.warning(f"Error calculating win probability for {symbol}: {str(e)}")
                                winning_probability = min(0.95, 0.5 + normalized_score * 0.05)
                        else:
                            winning_probability = min(0.95, 0.5 + normalized_score * 0.05)
                        
                        if normalized_score >= 3:  # Minimum score threshold
                            results.append({
                                'symbol': symbol,
                                'date': row['date'],
                                'buy_score': normalized_score,
                                'selling_score': 0,  # Not implemented yet
                                'winning_probability': winning_probability,
                                'signal_strength': signal_strength,
                                'indicators_triggered': indicators_triggered
                            })
                        
                        processed_count += 1
                        
                    except Exception as e:
                        logger.warning(f"Error processing technical data for {symbol}: {str(e)}")
                        continue
                
            except Exception as e:
                logger.warning(f"Error calculating indicators for {symbol}: {str(e)}")
                continue
        
        if results:
            for result in results:
                cursor.execute(f"""
                INSERT INTO {signals_table}
                (symbol, date, buy_score, selling_score, winning_probability, signal_strength, indicators_triggered)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) 
                DO UPDATE SET
                    buy_score = EXCLUDED.buy_score,
                    selling_score = EXCLUDED.selling_score,
                    winning_probability = EXCLUDED.winning_probability,
                    signal_strength = EXCLUDED.signal_strength,
                    indicators_triggered = EXCLUDED.indicators_triggered,
                    created_at = CURRENT_TIMESTAMP
                """, (
                    result['symbol'],
                    result['date'],
                    result['buy_score'],
                    result['selling_score'],
                    result['winning_probability'],
                    result['signal_strength'],
                    result['indicators_triggered']
                ))
            
            conn.commit()
        
        cursor.close()
        conn.close()
        
        return f"Successfully calculated advanced indicators for {processed_count} stocks"
    except Exception as e:
        logger.error(f"Error in calculate_advanced_indicators: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error calculating advanced indicators: {str(e)}"

def filter_by_volatility_liquidity(analysis_period=30, signal_type='DAILY', volatility_min=1.0, volatility_max=4.0, volume_min=1000000):
    """
    Filter stocks by volatility and liquidity criteria
    
    Parameters:
    analysis_period (int): Number of days to analyze
    signal_type (str): Signal type - DAILY, WEEKLY, or MONTHLY
    volatility_min (float): Minimum volatility percent
    volatility_max (float): Maximum volatility percent 
    volume_min (int): Minimum average daily volume
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    latest_date = get_latest_stock_date()
    
    try:
        query = f"""
        WITH stock_metrics AS (
            SELECT 
                symbol,
                MAX(high) as max_high,
                MIN(low) as min_low,
                AVG(volume) as avg_volume,
                STDDEV(close) as std_dev,
                AVG(close) as avg_price,
                MAX(date) as last_date,
                MIN(date) as first_date
            FROM public.daily_stock_summary
            WHERE date >= '{latest_date}'::date - INTERVAL '{analysis_period} days'
            GROUP BY symbol
        )
        SELECT 
            symbol,
            ((max_high - min_low) / min_low * 100) as volatility_pct,
            avg_volume,
            (std_dev / avg_price * 100) as price_volatility,
            last_date,
            first_date,
            EXTRACT(DAY FROM (last_date - first_date)) as days_range
        FROM stock_metrics
        WHERE avg_volume >= {volume_min}
        AND ((max_high - min_low) / min_low * 100) BETWEEN {volatility_min} AND {volatility_max}
        ORDER BY volatility_pct DESC, avg_volume DESC
        """
        
        filtered_stocks = pd.read_sql(query, conn)
        
        table_name = f"public_analytics.filtered_stocks_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.filtered_stocks"
        
        cursor = conn.cursor()
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT PRIMARY KEY,
            volatility_pct NUMERIC,
            avg_volume NUMERIC,
            price_volatility NUMERIC,
            filter_date DATE DEFAULT CURRENT_DATE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        cursor.execute(f"DELETE FROM {table_name}")
        
        for _, row in filtered_stocks.iterrows():
            cursor.execute(f"""
            INSERT INTO {table_name} (symbol, volatility_pct, avg_volume, price_volatility)
            VALUES (%s, %s, %s, %s)
            """, (
                row['symbol'],
                row['volatility_pct'],
                row['avg_volume'],
                row['price_volatility']
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return f"Successfully filtered {len(filtered_stocks)} stocks for {signal_type} analysis"
    except Exception as e:
        logger.error(f"Error filtering stocks: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error filtering stocks: {str(e)}"

def backtest_trading_signals(test_period=180, hold_period=5, signal_type='DAILY', min_win_rate=0.6):
    """
    Backtest trading signals and calculate win rates
    
    Parameters:
    test_period (int): Number of days to look back for testing
    hold_period (int): Number of days to hold position
    signal_type (str): Signal type - DAILY, WEEKLY, or MONTHLY
    min_win_rate (float): Minimum win rate to consider
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    signals_table = f"public_analytics.advanced_trading_signals_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.advanced_trading_signals"
    backtest_table = f"public_analytics.backtest_results_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.backtest_results"
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = '{'advanced_trading_signals' if signal_type == 'DAILY' else f"advanced_trading_signals_{signal_type.lower()}"}'
        );
        """)
        signals_table_exists = cursor.fetchone()[0]
        
        if not signals_table_exists:
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {signals_table} (
                symbol TEXT,
                date DATE,
                buy_score INTEGER,
                selling_score INTEGER,
                winning_probability NUMERIC,
                signal_strength TEXT,
                indicators_triggered TEXT[],
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, date)
            )
            """)
            conn.commit()
            
            cursor.close()
            conn.close()
            return f"Created {signals_table} table, but no data available for backtest yet"
    except Exception as e:
        logger.error(f"Error checking signals table: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error checking signals table: {str(e)}"
    
    try:
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {backtest_table} (
            id SERIAL PRIMARY KEY,
            symbol TEXT,
            signal_date DATE,
            buy_score INTEGER,
            winning_probability NUMERIC,
            entry_price NUMERIC,
            exit_price NUMERIC,
            percent_change_5d NUMERIC,
            is_win BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
    except Exception as e:
        logger.error(f"Error creating backtest table: {str(e)}")
        cursor.close()
        conn.close()
        return f"Error creating backtest table: {str(e)}"
    
    latest_date = get_latest_stock_date()
    
    try:
        query = f"""
        WITH historical_signals AS (
            SELECT 
                s.symbol,
                s.date as signal_date,
                s.buy_score,
                s.winning_probability,
                ROW_NUMBER() OVER (PARTITION BY s.symbol ORDER BY s.date DESC) as signal_rank
            FROM {signals_table} s
            WHERE s.date BETWEEN '{latest_date}'::date - INTERVAL '{test_period} days' AND '{latest_date}'::date - INTERVAL '{hold_period} days'
            -- Only include signals with enough strength
            AND s.buy_score >= 5
        )
        SELECT 
            h.symbol,
            h.signal_date,
            h.buy_score,
            h.winning_probability,
            e.close as entry_price,
            x.close as exit_price,
            ((x.close - e.close) / e.close * 100) as percent_change
        FROM historical_signals h
        -- Join with entry price
        JOIN public.daily_stock_summary e 
            ON h.symbol = e.symbol AND h.signal_date = e.date
        -- Join with exit price after hold_period days
        JOIN public.daily_stock_summary x
            ON h.symbol = x.symbol AND x.date = h.signal_date + INTERVAL '{hold_period} days'
        ORDER BY h.symbol, h.signal_date DESC
        """
        
        try:
            backtest_df = pd.read_sql(query, conn)
        except Exception as e:
            logger.warning(f"Error querying historical signals: {str(e)}")
            cursor.close()
            conn.close()
            return f"No historical signals found for {signal_type} backtest"
        
        if backtest_df.empty:
            logger.warning(f"No historical signals found for {signal_type} backtest")
            cursor.close()
            conn.close()
            return f"No data for {signal_type} backtest"
        
        if signal_type == 'WEEKLY':
            win_threshold = 3.0  # Higher for weekly
        elif signal_type == 'MONTHLY':
            win_threshold = 5.0  # Even higher for monthly
        else:
            win_threshold = 2.0  # Default for daily
            
        backtest_df['is_win'] = backtest_df['percent_change'] > win_threshold
        
        for _, row in backtest_df.iterrows():
            cursor.execute(f"""
            INSERT INTO {backtest_table}
            (symbol, signal_date, buy_score, winning_probability, entry_price, exit_price, percent_change_5d, is_win)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """, (
                row['symbol'],
                row['signal_date'],
                row['buy_score'],
                row['winning_probability'],
                row['entry_price'],
                row['exit_price'],
                row['percent_change'],
                row['is_win']
            ))
        
        conn.commit()
        
        win_rates_query = f"""
        SELECT 
            buy_score,
            COUNT(*) as total_signals,
            SUM(CASE WHEN is_win THEN 1 ELSE 0 END) as win_count,
            ROUND(SUM(CASE WHEN is_win THEN 1 ELSE 0 END)::float / COUNT(*) * 100, 1) as win_rate,
            ROUND(AVG(percent_change_5d), 2) as avg_return
        FROM {backtest_table}
        GROUP BY buy_score
        ORDER BY buy_score DESC
        """
        
        win_rates_df = pd.read_sql(win_rates_query, conn)
        
        logger.info(f"Backtest results for {signal_type}:")
        for _, row in win_rates_df.iterrows():
            logger.info(f"Score {row['buy_score']}: {row['win_rate']}% win rate, {row['avg_return']}% avg return, {row['total_signals']} signals")
        
        update_query = f"""
        WITH win_rates AS (
            SELECT 
                buy_score,
                ROUND(SUM(CASE WHEN is_win THEN 1 ELSE 0 END)::float / COUNT(*), 4) as actual_win_rate
            FROM {backtest_table}
            GROUP BY buy_score
        )
        UPDATE {signals_table} s
        SET winning_probability = CASE
                WHEN w.actual_win_rate IS NOT NULL THEN w.actual_win_rate
                ELSE s.winning_probability
            END
        FROM win_rates w
        WHERE s.buy_score = w.buy_score
        AND s.date >= '{latest_date}'::date - INTERVAL '7 days'
        """
        
        cursor.execute(update_query)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return f"Successfully backtested {len(backtest_df)} historical signals for {signal_type}"
    except Exception as e:
        logger.error(f"Error in backtest: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error in backtest: {str(e)}"

def send_high_probability_signals(signal_type='DAILY', min_probability=0.8):
    """
    Send high probability trading signals report to Telegram
    
    Parameters:
    signal_type (str): Signal type - DAILY, WEEKLY, or MONTHLY
    min_probability (float): Minimum probability threshold
    """
    try:
        conn = get_database_connection()
        
        signals_table = f"public_analytics.advanced_trading_signals_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.advanced_trading_signals"
        rsi_table = f"public_analytics.technical_indicators_rsi_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_rsi"
        macd_table = f"public_analytics.technical_indicators_macd_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_macd"
        
        cursor = conn.cursor()
        cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = '{'advanced_trading_signals' if signal_type == 'DAILY' else f"advanced_trading_signals_{signal_type.lower()}"}'
        );
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.warning(f"Table {signals_table} does not exist")
            cursor.close()
            conn.close()
            return f"Table {signals_table} does not exist yet. Skipping report."
            
        latest_date = get_latest_stock_date()
        if not latest_date:
            logger.warning("No date data available")
            cursor.close()
            conn.close()
            return "No date data available"
            
        date_filter = latest_date.strftime('%Y-%m-%d')
        
        cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = '{'technical_indicators_rsi' if signal_type == 'DAILY' else f"technical_indicators_rsi_{signal_type.lower()}"}'
        );
        """)
        rsi_table_exists = cursor.fetchone()[0]
        
        cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = '{'technical_indicators_macd' if signal_type == 'DAILY' else f"technical_indicators_macd_{signal_type.lower()}"}'
        );
        """)
        macd_table_exists = cursor.fetchone()[0]
        
        left_join_rsi = f"LEFT JOIN {rsi_table} r ON s.symbol = r.symbol AND s.date = r.date" if rsi_table_exists else ""
        left_join_macd = f"LEFT JOIN {macd_table} mc ON s.symbol = mc.symbol AND s.date = mc.date" if macd_table_exists else ""
        
        select_rsi = "r.rsi," if rsi_table_exists else ""
        select_macd = "mc.macd_signal" if macd_table_exists else ""
        
        signals_sql = f"""
        WITH stock_info AS (
            SELECT 
                s.symbol,
                s.date,
                s.buy_score,
                s.signal_strength,
                s.winning_probability,
                m.name,
                m.close,
                m.volume
                {', ' + select_rsi if select_rsi else ''}
                {', ' + select_macd if select_macd else ''}
            FROM {signals_table} s
            JOIN public.daily_stock_summary m 
                ON s.symbol = m.symbol AND s.date = m.date
            {left_join_rsi}
            {left_join_macd}
            WHERE s.date = '{date_filter}'
              AND s.winning_probability >= {min_probability}
              AND s.buy_score >= 5
        )
        SELECT * FROM stock_info
        ORDER BY buy_score DESC, winning_probability DESC
        LIMIT 10
        """
        
        try:
            signals_df = pd.read_sql(signals_sql, conn)
        except Exception as e:
            logger.error(f"Error querying high probability signals: {str(e)}")
            cursor.close()
            conn.close()
            return f"Error querying signals: {str(e)}"
        
        if signals_df.empty:
            logger.warning(f"No high probability signals for date {date_filter}")
            cursor.close()
            conn.close()
            return f"No high probability signals found for date {date_filter}"
        
        timeframe_label = {
            'DAILY': 'HARIAN (1-5 HARI)',
            'WEEKLY': 'MINGGUAN (1-3 MINGGU)',
            'MONTHLY': 'BULANAN (1-3 BULAN)'
        }.get(signal_type, signal_type)
        
        message = f"🎯 *SINYAL TRADING {timeframe_label} ({date_filter})* 🎯\n\n"
        message += "Saham-saham berikut menunjukkan pola dengan probabilitas kemenangan tinggi:\n\n"
        
        for i, row in enumerate(signals_df.itertuples(), 1):
            message += f"{i}. *{row.symbol}* ({row.name})\n"
            message += f"   Harga: Rp{row.close:,.0f} | Buy Score: {row.buy_score}/10\n"
            message += f"   Probabilitas: {row.winning_probability:.2%}"
            
            if hasattr(row, 'rsi') and not pd.isna(row.rsi):
                message += f" | RSI: {row.rsi:.1f}"
            
            message += "\n"
                
            if hasattr(row, 'signal_strength') and row.signal_strength:
                message += f"   Kekuatan Sinyal: {row.signal_strength}\n"
                
            if hasattr(row, 'macd_signal') and row.macd_signal:
                message += f"   MACD: {row.macd_signal}\n"
            
            if signal_type == 'WEEKLY':
                tp1 = row.close * 1.08  # 8% target
                tp2 = row.close * 1.15  # 15% target
                sl = row.close * 0.95   # 5% stop loss
            elif signal_type == 'MONTHLY':
                tp1 = row.close * 1.12  # 12% target
                tp2 = row.close * 1.20  # 20% target
                sl = row.close * 0.92   # 8% stop loss
            else:
                tp1 = row.close * 1.05  # 5% target
                tp2 = row.close * 1.10  # 10% target
                sl = row.close * 0.97   # 3% stop loss
            
            message += f"   🎯 TP1: Rp{tp1:,.0f} | TP2: Rp{tp2:,.0f}\n"
            message += f"   🛑 SL: Rp{sl:,.0f}\n\n"
        
        message += "*Tips Trading:*\n"
        
        if signal_type == 'WEEKLY':
            message += "• Gunakan timeframe H4 atau D1 untuk entry\n"
            message += "• Perhatikan support/resistance level mingguan\n"
            message += "• Target profit 8-15% dalam 1-3 minggu\n"
        elif signal_type == 'MONTHLY':
            message += "• Gunakan timeframe D1 atau W1 untuk entry\n"
            message += "• Perhatikan tren jangka menengah sektor saham\n"
            message += "• Target profit 12-20% dalam 1-3 bulan\n"
        else:
            message += "• Gunakan timeframe H1 atau H4 untuk entry\n"
            message += "• Perhatikan level support/resistance terdekat\n"
            message += "• Target profit 5-10% dalam 1-5 hari\n"
        
        message += "\n*Disclaimer:* Sinyal ini dihasilkan dari algoritma dan tidak menjamin keberhasilan. Selalu lakukan analisis tambahan sebelum mengambil keputusan investasi."
        
        result = send_telegram_message(message)
        cursor.close()
        conn.close()
        
        if "successfully" in result:
            return f"High probability {signal_type} signals report sent: {len(signals_df)} stocks"
        else:
            return result
    except Exception as e:
        logger.error(f"Error in send_high_probability_signals: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error: {str(e)}"
