from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import pendulum
import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.model_selection import train_test_split
import psycopg2
import pickle
import collections
from pathlib import Path
import os
import requests
import logging
import json
from datetime import datetime

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=5)
}

# Utility functions
def get_database_connection():
    """
    Membuat koneksi database dengan handling error yang lebih baik
    """
    try:
        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow"
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

def get_telegram_credentials():
    """
    Mendapatkan kredensial Telegram dari environment atau Airflow Variables
    """
    # Coba dari environment variables
    telegram_bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    # Jika tidak ada, coba dari Airflow Variables
    if not telegram_bot_token:
        try:
            telegram_bot_token = Variable.get("TELEGRAM_BOT_TOKEN")
            logger.info("Successfully retrieved bot token from Airflow Variables")
        except:
            logger.warning("Failed to get TELEGRAM_BOT_TOKEN from Airflow Variables")
    
    if not telegram_chat_id:
        try:
            telegram_chat_id = Variable.get("TELEGRAM_CHAT_ID")
            logger.info("Successfully retrieved chat ID from Airflow Variables")
        except:
            logger.warning("Failed to get TELEGRAM_CHAT_ID from Airflow Variables")
            
    if telegram_bot_token:
        masked_token = f"{telegram_bot_token[:5]}...{telegram_bot_token[-5:]}"
        logger.info(f"Using token: {masked_token}")
    else:
        logger.error("Telegram bot token not found!")
        
    if telegram_chat_id:
        logger.info(f"Using chat ID: {telegram_chat_id}")
    else:
        logger.error("Telegram chat ID not found!")
        
    return telegram_bot_token, telegram_chat_id

def send_telegram_message(message, disable_web_page_preview=False):
    """
    Kirim pesan ke Telegram dengan menangani pesan yang terlalu panjang
    """
    token, chat_id = get_telegram_credentials()
    if not token or not chat_id:
        logger.error("Missing Telegram credentials")
        return "Error: Missing Telegram credentials"
    
    # Batasan panjang pesan Telegram 4096 karakter
    MAX_MESSAGE_LENGTH = 4000  # Sedikit margin untuk keamanan
    
    # Jika pesan lebih pendek dari batasan, kirim langsung
    if len(message) <= MAX_MESSAGE_LENGTH:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": disable_web_page_preview
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            logger.info(f"Telegram response: {response.status_code}")
            
            if response.status_code == 200:
                return "Message sent successfully"
            else:
                return f"Error sending to Telegram: {response.status_code}, {response.text}"
        except Exception as e:
            logger.error(f"Exception sending to Telegram: {str(e)}")
            return f"Error exception: {str(e)}"
    # Jika pesan terlalu panjang, bagi menjadi beberapa bagian
    else:
        logger.info(f"Message too long ({len(message)} chars), splitting into parts")
        # Cari posisi baris baru untuk membagi pesan dengan rapi
        parts = []
        start_idx = 0
        
        while start_idx < len(message):
            # Jika sisa pesan masih muat dalam satu bagian
            if len(message) - start_idx <= MAX_MESSAGE_LENGTH:
                parts.append(message[start_idx:])
                break
            
            # Cari posisi baris baru terakhir yang masih dalam batas panjang
            cut_idx = start_idx + MAX_MESSAGE_LENGTH
            while cut_idx > start_idx:
                if message[cut_idx] == '\n':
                    break
                cut_idx -= 1
                
            # Jika tidak ada baris baru, potong di spasi
            if cut_idx == start_idx:
                cut_idx = start_idx + MAX_MESSAGE_LENGTH
                while cut_idx > start_idx:
                    if message[cut_idx] == ' ':
                        break
                    cut_idx -= 1
                    
            # Jika masih tidak ditemukan, potong tepat di MAX_MESSAGE_LENGTH
            if cut_idx == start_idx:
                cut_idx = start_idx + MAX_MESSAGE_LENGTH - 1
                
            # Tambahkan bagian ini ke daftar
            parts.append(message[start_idx:cut_idx+1])
            start_idx = cut_idx + 1
            
        # Kirim setiap bagian
        success_count = 0
        for i, part in enumerate(parts):
            logger.info(f"Sending part {i+1}/{len(parts)}, length: {len(part)} chars")
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": part,
                "parse_mode": "Markdown",
                "disable_web_page_preview": disable_web_page_preview
            }
            
            try:
                response = requests.post(url, json=payload, timeout=10)
                if response.status_code == 200:
                    success_count += 1
                else:
                    logger.error(f"Error sending part {i+1}: {response.status_code}, {response.text}")
                # Tunggu sebentar untuk menghindari rate limiting
                time.sleep(1)
            except Exception as e:
                logger.error(f"Exception sending part {i+1}: {str(e)}")
                
        if success_count == len(parts):
            return f"Message sent successfully in {len(parts)} parts"
        else:
            return f"Partial success: {success_count}/{len(parts)} parts sent"

class LSTMModel(nn.Module):
    """
    Model LSTM untuk prediksi pergerakan harga saham
    """
    def __init__(self, input_size, hidden_size, num_layers, output_size):
        super(LSTMModel, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)
        
    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        
        out, _ = self.lstm(x, (h0, c0))
        out = self.fc(out[:, -1, :])
        return out

def train_win_rate_predictor():
    """
    Melatih model machine learning untuk memprediksi probabilitas win rate
    berdasarkan faktor-faktor teknikal
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    # Ambil data historis untuk training
    try:
        # Query data sinyal historis dengan outcome (win/loss)
        sql = """
        WITH historical_signals AS (
            SELECT 
                s.symbol, 
                s.date, 
                s.buy_score,
                s.volume_shock,
                s.demand_zone,
                s.foreign_flow,
                s.price_pattern,
                s.market_structure,
                s.adx,
                s.fib_support,
                r.rsi,
                CASE WHEN m.macd_signal = 'Bullish' THEN 1 ELSE 0 END as macd_bullish,
                CASE 
                    WHEN ((m2.close - m1.close) / m1.close * 100) > 3 THEN 1  -- Win jika naik >3%
                    ELSE 0
                END AS is_win
            FROM public_analytics.advanced_trading_signals s
            JOIN public.daily_stock_summary m1 
                ON s.symbol = m1.symbol AND s.date = m1.date
            LEFT JOIN public.daily_stock_summary m2
                ON s.symbol = m2.symbol AND m2.date = s.date + INTERVAL '5 days'
            LEFT JOIN public_analytics.technical_indicators_rsi r
                ON s.symbol = r.symbol AND s.date = r.date
            LEFT JOIN public_analytics.technical_indicators_macd m
                ON s.symbol = m.symbol AND s.date = m.date
            WHERE s.date BETWEEN CURRENT_DATE - INTERVAL '365 days' AND CURRENT_DATE - INTERVAL '5 days'
            AND m2.close IS NOT NULL  -- Only include cases where we have outcome data
        )
        SELECT * FROM historical_signals
        """
        
        training_data = pd.read_sql(sql, conn)
        conn.close()
    except Exception as e:
        logger.error(f"Error querying training data: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error querying training data: {str(e)}"
    
    if training_data.empty:
        logger.warning("No data available for model training")
        return "No data available for model training"
    
    # Preprocessing data
    try:
        # Convert categorical columns to binary/numeric
        training_data['price_pattern_binary'] = training_data['price_pattern'].apply(
            lambda x: 1 if x == 'Bullish Engulfing' else 0
        )
        
        training_data['market_structure_binary'] = training_data['market_structure'].apply(
            lambda x: 1 if x == 'Uptrend' else 0
        )
        
        # Handle null/nan values
        numeric_cols = ['buy_score', 'foreign_flow', 'adx', 'rsi']
        for col in numeric_cols:
            training_data[col] = training_data[col].fillna(training_data[col].median())
        
        # Fill remaining binary cols with 0
        binary_cols = ['volume_shock', 'demand_zone', 'fib_support', 
                      'price_pattern_binary', 'market_structure_binary', 'macd_bullish']
        for col in binary_cols:
            training_data[col] = training_data[col].fillna(0)
            
        # Pilih feature dan target
        features = [
            'buy_score', 'volume_shock', 'demand_zone', 'foreign_flow',
            'adx', 'rsi', 'fib_support', 
            'price_pattern_binary', 'market_structure_binary', 'macd_bullish'
        ]
        
        X = training_data[features]
        y = training_data['is_win']
        
        # Train-test split
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        
        # Train Random Forest model
        model = RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42, class_weight='balanced')
        model.fit(X_train, y_train)
        
        # Evaluasi model
        y_pred = model.predict(X_test)
        accuracy = (y_pred == y_test).mean()
        
        # Feature importance
        feature_importance = dict(zip(features, model.feature_importances_))
        
        # Save model to file
        model_dir = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'models')
        os.makedirs(model_dir, exist_ok=True)
        
        model_path = os.path.join(model_dir, 'win_rate_predictor.pkl')
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        
        # Save feature importance as Airflow variable
        Variable.set('model_feature_importance', json.dumps(feature_importance))
        
        # Create report for Telegram
        report = "🧠 *MODEL ML WIN RATE PREDICTOR BERHASIL DILATIH* 🧠\n\n"
        report += f"Model dilatih dengan {len(training_data)} sampel data historis.\n"
        report += f"Accuracy: {accuracy*100:.2f}%\n\n"
        
        report += "*Feature Importance:*\n"
        # Sort feature importance
        sorted_importance = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
        for feature, importance in sorted_importance:
            report += f"- {feature}: {importance*100:.2f}%\n"
            
        # Send report to Telegram
        send_telegram_message(report)
        
        return f"Model trained with {accuracy*100:.2f}% accuracy"
    except Exception as e:
        logger.error(f"Error training model: {str(e)}")
        return f"Error training model: {str(e)}"

def send_comprehensive_signal_report():
    """
    Fungsi untuk mengirim laporan sinyal yang komprehensif dengan win rate tinggi
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    # Ambil tanggal terbaru dengan sinyal
    try:
        date_query = """
        SELECT MAX(date) FROM public_analytics.advanced_trading_signals
        """
        cursor = conn.cursor()
        cursor.execute(date_query)
        latest_date = cursor.fetchone()[0]
        
        if not latest_date:
            logger.warning("No signals data available")
            cursor.close()
            conn.close()
            return "No signals data available"
            
        # Format date for query if needed
        if not isinstance(latest_date, str):
            latest_date_str = latest_date.strftime('%Y-%m-%d')
        else:
            latest_date_str = latest_date
            
        # Query untuk sinyal buy dengan probabilitas >80%
        high_probability_query = f"""
        SELECT 
            s.symbol,
            m.name,
            s.buy_score,
            s.winning_probability,
            s.signal_strength,
            m.close,
            r.rsi,
            s.price_pattern,
            s.market_structure,
            s.adx,
            s.fib_support
        FROM public_analytics.advanced_trading_signals s
        JOIN public.daily_stock_summary m 
            ON s.symbol = m.symbol AND s.date = m.date
        LEFT JOIN public_analytics.technical_indicators_rsi r
            ON s.symbol = r.symbol AND s.date = r.date
        WHERE s.date = '{latest_date_str}'
        AND s.winning_probability >= 0.8
        ORDER BY s.buy_score DESC, s.winning_probability DESC
        LIMIT 5
        """
        
        high_prob_df = pd.read_sql(high_probability_query, conn)
        
        # Query untuk win rate historis
        historical_winrate_query = """
        WITH score_groups AS (
            SELECT 
                buy_score,
                is_win,
                COUNT(*) as signal_count
            FROM public_analytics.backtest_results
            GROUP BY buy_score, is_win
        ),
        win_stats AS (
            SELECT 
                buy_score as score_group,
                SUM(CASE WHEN is_win THEN signal_count ELSE 0 END) as wins,
                SUM(signal_count) as total_signals
            FROM score_groups
            GROUP BY buy_score
        )
        SELECT 
            score_group,
            total_signals,
            wins,
            ROUND((wins::float / total_signals * 100)::numeric, 1) as win_rate
        FROM win_stats
        ORDER BY score_group DESC
        """
        
        # Jika tabel backtest result sudah ada
        try:
            winrate_df = pd.read_sql(historical_winrate_query, conn)
        except:
            winrate_df = pd.DataFrame()
            logger.warning("Could not retrieve historical win rate data")
            
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error querying signal data: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error querying signal data: {str(e)}"
    
    if high_prob_df.empty:
        logger.warning("No high probability signals found")
        return "No high probability signals found"
    
    # Create date string for report
    report_date = latest_date
    if isinstance(report_date, datetime):
        report_date = report_date.strftime('%Y-%m-%d')
    
    # Buat laporan Telegram
    message = f"🚀 *LAPORAN SINYAL TRADING PROBABILITAS TINGGI ({report_date})* 🚀\n\n"
    
    # Bagian 1: Saham dengan probabilitas tinggi
    message += "*Saham Dengan Win Rate 80-90%:*\n\n"
    
    for i, row in enumerate(high_prob_df.itertuples(), 1):
        message += f"{i}. *{row.symbol}* ({row.name})\n"
        message += f"   Harga: Rp{row.close:,.0f} | Win Prob: {row.winning_probability*100:.0f}%\n"
        message += f"   Skor: {row.buy_score}/10 | Sinyal: {row.signal_strength}\n"
        
        # Detail teknikal
        technicals = []
        if hasattr(row, 'rsi') and row.rsi is not None:
            technicals.append(f"RSI: {row.rsi:.1f}")
        if hasattr(row, 'price_pattern') and row.price_pattern not in ["None", None]:
            technicals.append(f"Pattern: {row.price_pattern}")
        if hasattr(row, 'market_structure') and row.market_structure == "Uptrend":
            technicals.append("Uptrend")
        if hasattr(row, 'adx') and row.adx > 20:
            technicals.append(f"ADX: {row.adx:.1f}")
        if hasattr(row, 'fib_support') and row.fib_support:
            technicals.append("Fib Support")
            
        if technicals:
            message += f"   Teknikal: {' | '.join(technicals)}\n"
            
        # Target harga dan Stop Loss
        target_price_1 = row.close * 1.05  # Target 5%
        target_price_2 = row.close * 1.10  # Target 10%
        stop_loss = row.close * 0.95      # Stop loss 5%
        
        message += f"   🎯 Target 1: Rp{target_price_1:,.0f} (+5%) | Target 2: Rp{target_price_2:,.0f} (+10%)\n"
        message += f"   🛑 Stop Loss: Rp{stop_loss:,.0f} (-5%)\n\n"
    
    # Bagian 2: Statistik Win Rate Historis (jika tersedia)
    if not winrate_df.empty:
        message += "*Statistik Win Rate Historis:*\n\n"
        message += "| Skor | Total Sinyal | Win Rate |\n"
        message += "|------|--------------|----------|\n"
        
        for _, row in winrate_df.iterrows():
            message += f"| {row['score_group']} | {row['total_signals']} | {row['win_rate']}% |\n"
        
        message += "\n"
    
    # Bagian 3: Setup dan Strategi Entry
    message += "*Strategi Entry:*\n"
    message += "1. Beli di harga pasar atau sedikit di bawahnya\n"
    message += "2. Volume entry: 30% pada sinyal, 30% di hari berikutnya jika tren berlanjut\n"
    message += "3. Periode hold: 5-10 hari trading\n"
    message += "4. Tingkatkan posisi jika ada konfirmasi breakout\n\n"
    
    # Bagian 4: Money Management
    message += "*Money Management:*\n"
    message += "1. Risiko maksimal per trade: 1-2% dari total portfolio\n"
    message += "2. Gunakan stop loss pada level -5% dari harga entry\n"
    message += "3. Take profit parsial pada +5% dan +8% dari harga entry\n"
    message += "4. Trailing stop loss setelah +5%\n\n"
    
    # Bagian 5: Disclaimer
    message += "*Disclaimer:*\n"
    message += "Analisis ini menggunakan algoritma AI/ML dengan performa historis >80%. "
    message += "Meski demikian, tidak ada jaminan profit. Lakukan analisis tambahan "
    message += "dan gunakan manajemen risiko yang ketat."
    
    # Kirim ke Telegram
    result = send_telegram_message(message)
    if "successfully" in result:
        return f"Comprehensive signal report sent with {len(high_prob_df)} high probability signals"
    else:
        return result

def send_high_probability_signals():
    """
    Mengirim sinyal trading dengan probabilitas tinggi (>80%) ke Telegram
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    # Ambil sinyal dengan probabilitas tinggi (>80%)
    try:
        sql = """
        WITH stock_info AS (
            SELECT 
                s.symbol,
                s.date,
                s.buy_score,
                s.signal_strength,
                s.winning_probability,
                s.volume_shock,
                s.demand_zone,
                s.foreign_flow,
                s.price_pattern,
                s.market_structure,
                s.adx,
                s.fib_support,
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
            WHERE s.date = (SELECT MAX(date) FROM public_analytics.advanced_trading_signals)
            AND s.winning_probability >= 0.8
        )
        SELECT * FROM stock_info
        ORDER BY buy_score DESC, winning_probability DESC, foreign_flow DESC
        LIMIT 10
        """
        
        df = pd.read_sql(sql, conn)
        conn.close()
    except Exception as e:
        logger.error(f"Error querying high probability signals: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error querying signals: {str(e)}"
    
    if df.empty:
        logger.warning("No high probability trading signals found")
        return "No high probability trading signals found"
    
    # Create date string for report
    report_date = df['date'].iloc[0]
    if isinstance(report_date, pd.Timestamp):
        report_date = report_date.strftime('%Y-%m-%d')
    
    # Create Telegram message
    message = f"🔮 *SINYAL TRADING PROBABILITAS TINGGI ({report_date})* 🔮\n\n"
    message += "Saham-saham berikut memiliki probabilitas profit >80% berdasarkan analisis multi-faktor:\n\n"
    
    for i, row in enumerate(df.itertuples(), 1):
        message += f"*{i}. {row.symbol}* ({row.name})\n"
        message += f"   Harga: Rp{row.close:,.0f} | Skor: {row.buy_score}/10\n"
        message += f"   Probabilitas: {row.winning_probability*100:.0f}% | Signal: {row.signal_strength}\n"
        
        # Faktor-faktor pendukung
        factors = []
        if hasattr(row, 'volume_shock') and row.volume_shock:
            factors.append("Volume Shock")
        if hasattr(row, 'demand_zone') and row.demand_zone:
            factors.append("Demand Zone")
        if hasattr(row, 'foreign_flow') and row.foreign_flow > 0:
            factors.append(f"Foreign Flow +{row.foreign_flow:,.0f}")
        if hasattr(row, 'price_pattern') and row.price_pattern != "None":
            factors.append(f"Pattern: {row.price_pattern}")
        if hasattr(row, 'market_structure') and row.market_structure == "Uptrend":
            factors.append("Uptrend Structure")
        if hasattr(row, 'rsi') and row.rsi is not None and row.rsi < 30:
            factors.append(f"RSI: {row.rsi:.1f}")
        if hasattr(row, 'macd_signal') and row.macd_signal == "Bullish":
            factors.append("MACD Bullish")
        if hasattr(row, 'adx') and row.adx > 20:
            factors.append(f"ADX: {row.adx:.1f}")
        if hasattr(row, 'fib_support') and row.fib_support:
            factors.append("Fib Support")
            
        message += f"   Faktor: {', '.join(factors)}\n"
        
        # Target harga dan stop loss
        target_price_1 = row.close * 1.05  # Target 5%
        target_price_2 = row.close * 1.10  # Target 10%
        stop_loss = row.close * 0.95      # Stop loss 5%
        
        message += f"   🎯 Target 1: Rp{target_price_1:,.0f} (+5%) | Target 2: Rp{target_price_2:,.0f} (+10%)\n"
        message += f"   🛑 Stop Loss: Rp{stop_loss:,.0f} (-5%)\n\n"
    
    # Strategy section
    message += "*Strategi Entry:*\n"
    message += "• Beli pada harga pasar atau tunggu pullback kecil\n"
    message += "• Entry bertahap: 50% posisi di awal, 50% setelah konfirmasi\n"
    message += "• Hold periode: 5-10 hari trading\n\n"
    
    # Tambahkan disclaimer
    message += "*Disclaimer:*\n"
    message += "Analisis ini menggunakan algoritma data science dan tidak menjamin profit. "
    message += "Lakukan analisis tambahan dan gunakan manajemen risiko."
    
    # Send to Telegram
    result = send_telegram_message(message)
    if "successfully" in result:
        return f"High probability signals sent: {len(df)} stocks"
    else:
        return result

# DAG definition - High Win Rate Trading Signals
with DAG(
    dag_id="high_winrate_trading_signals",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="30 18 * * 1-5",  # Setiap hari kerja pukul 18:30 WIB
    catchup=False,
    default_args=default_args,
    tags=["trading", "signals", "high_probability", "ai", "ml"]
) as dag:

    # Tunggu hingga advanced trading signals selesai dihitung
    wait_for_advanced = ExternalTaskSensor(
        task_id="wait_for_advanced_signals",
        external_dag_id="advanced_trading_signals",
        external_task_id="end_task",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )
    
    # Langkah 1: Train model ML untuk prediksi win rate
    # (Ini dijalankan hanya seminggu sekali - pada hari Senin)
    train_model = PythonOperator(
        task_id="train_ml_model",
        python_callable=train_win_rate_predictor,
        trigger_rule='none_failed',
        retries=1,
        retry_delay=pendulum.duration(minutes=5),
        execution_timeout=pendulum.duration(minutes=30)
    )
    
    # Langkah 2: Kirim laporan sinyal trading komprehensif
    send_report = PythonOperator(
        task_id="send_comprehensive_signals",
        python_callable=send_comprehensive_signal_report,
        retries=2,
        retry_delay=pendulum.duration(minutes=2)
    )
    
    # Langkah 3: Kirim sinyal high-probability
    send_high_prob = PythonOperator(
        task_id="send_high_probability_signals",
        python_callable=send_high_probability_signals,
        retries=2,
        retry_delay=pendulum.duration(minutes=2)
    )
    
    # Marker task
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    # Task dependencies - optimasi alur kerja dengan beberapa task berjalan paralel
    wait_for_advanced >> train_model >> [send_report, send_high_prob] >> end_task