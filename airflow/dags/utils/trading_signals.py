import pandas as pd
import numpy as np
import logging
import json
from datetime import datetime
from airflow.models import Variable
from .database import get_database_connection, get_latest_stock_date
from .telegram import send_telegram_message

# Configure logging
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
    
    # Get latest date
    latest_date = get_latest_stock_date()
    
    # Query to get previous signal performance (backtest)
    try:
        # Table names based on report_type
        backtest_table = f"public_analytics.backtest_results_{report_type.lower()}" if report_type != 'DAILY' else "public_analytics.backtest_results"
        signals_table = f"public_analytics.advanced_trading_signals_{report_type.lower()}" if report_type != 'DAILY' else "public_analytics.advanced_trading_signals"
        
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
            # If backtest_results table doesn't exist yet
            performance_df = pd.DataFrame()
            logger.warning(f"{backtest_table} table does not exist yet")
        
        # Query for recent completed signals (5+ days)
        # Adjust hold period based on report_type
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
            # If no completed signals yet
            recent_df = pd.DataFrame()
            logger.warning("No completed signals found")
            
        conn.close()
    except Exception as e:
        logger.error(f"Error querying performance data: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error querying performance data: {str(e)}"
    
    # Create Telegram message
    message = f"📊 *LAPORAN PERFORMA SINYAL TRADING {report_type}* 📊\n\n"
    
    # Part 1: Win rate statistics
    if not performance_df.empty:
        message += "*Statistik Win Rate Berdasarkan Skor:*\n\n"
        message += "| Skor | Jumlah | Win Rate | Avg Return |\n"
        message += "|------|--------|----------|------------|\n"
        
        for _, row in performance_df.iterrows():
            message += f"| {row['buy_score']} | {row['total_signals']} | {row['win_rate']}% | {row['avg_return']}% |\n"
        
        # Overall statistics
        total_signals = performance_df['total_signals'].sum()
        total_wins = performance_df['win_count'].sum()
        overall_win_rate = (total_wins / total_signals * 100) if total_signals > 0 else 0
        avg_return = performance_df['avg_return'].mean()
        
        message += f"\n*Overall:* {total_signals} sinyal, Win Rate {overall_win_rate:.1f}%, Return Rata-rata {avg_return:.2f}%\n\n"
    else:
        message += "*Belum ada data backtest yang cukup*\n\n"
    
    # Part 2: Recent completed signals performance
    if not recent_df.empty:
        message += "*Performance Sinyal Terbaru:*\n\n"
        
        win_count = 0
        for i, row in enumerate(recent_df.itertuples(), 1):
            # Format date
            signal_date = row.signal_date
            if isinstance(signal_date, pd.Timestamp):
                signal_date = signal_date.strftime('%Y-%m-%d')
                
            # Emoji based on win/loss
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
    
    # Part 3: Tips to improve win rate - adjust based on report_type
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
    
    # Send to Telegram
    result = send_telegram_message(message)
    if "successfully" in result:
        return f"Performance report for {report_type} sent successfully"
    else:
        return result
    
    def calculate_advanced_indicators(lookback_period=300, signal_type='DAILY'):
    """
    Menghitung indikator tingkat lanjut dan membuat sistem penyaringan multi-layer
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Failed to connect to database: {str(e)}"
    
    # Dapatkan tanggal terakhir
    latest_date = get_latest_stock_date()
    
    # Kode yang sudah ada untuk mengambil data...
    
    # Perbaiki scoring system untuk lebih dinamis
    # Sebagai contoh, tambahkan dynamic scoring berdasarkan backtest results
    try:
        # Dynamic scoring dari backtest results
        backtest_sql = """
        SELECT 
            indicator_name,
            AVG(win_rate) as avg_win_rate
        FROM indicator_performance_metrics
        GROUP BY indicator_name
        """
        
        indicator_weights = {}
        try:
            weights_df = pd.read_sql(backtest_sql, conn)
            for _, row in weights_df.iterrows():
                indicator_weights[row['indicator_name']] = row['avg_win_rate']
        except Exception as e:
            logger.warning(f"Error fetching indicator weights: {str(e)}")
            # Default weights jika tidak ada data metrik
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
        
        # Gunakan weights dalam scoring
        # Contoh implementasi:
        for symbol, group in df.groupby('symbol'):
            # Kode yang sudah ada...
            
            # Modifikasi scoring untuk menggunakan weights
            for i, (idx, row) in enumerate(latest_data.iterrows()):
                # Hanya proses hari terakhir
                if i != len(latest_data) - 1:
                    continue
                
                # Gabungkan data teknikal dari tabel lain
                try:
                    # Query untuk ambil RSI terbaru
                    rsi_query = f"""
                    SELECT rsi, rsi_signal 
                    FROM public_analytics.technical_indicators_rsi 
                    WHERE symbol = '{symbol}' AND date = '{row['date']}'
                    """
                    rsi_df = pd.read_sql(rsi_query, conn)
                    
                    # Kode query lainnya...
                    
                    # Skor Kumulatif untuk Sinyal Beli dengan weights
                    buy_score = 0
                    
                    # 1. RSI Oversold
                    if not rsi_df.empty and rsi_df.iloc[0]['rsi_signal'] == 'Oversold':
                        buy_score += indicator_weights.get('rsi_oversold', 1.0)
                    
                    # 2. MACD Bullish
                    if not macd_df.empty and macd_df.iloc[0]['macd_signal'] == 'Bullish':
                        buy_score += indicator_weights.get('macd_bullish', 1.0)
                    
                    # 3. Volume Shock
                    if row['volume_shock']:
                        buy_score += indicator_weights.get('volume_shock', 1.0)
                    
                    # Lanjutkan dengan skor lainnya...
                    
                    # Normalisasi skor ke skala 0-10
                    max_possible_score = sum(indicator_weights.values())
                    normalized_score = min(10, round(buy_score / max_possible_score * 10))
                    
                    # Update buy_score dengan nilai yang dinormalisasi
                    buy_score = normalized_score
                    
                    # Kode selanjutnya tetap sama...
                    
                except Exception as e:
                    logger.warning(f"Error processing technical indicators for {symbol}: {str(e)}")
                    continue
        
    except Exception as e:
        logger.warning(f"Error getting dynamic weights: {str(e)}. Using default weights.")
        # Lanjutkan dengan kode yang sudah ada...

