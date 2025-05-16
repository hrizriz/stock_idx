from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime
import pendulum
import pandas as pd
import logging

# Import utility modules
from utils.database import get_database_connection, get_latest_stock_date, fetch_data
from utils.telegram import send_telegram_message

# Konfigurasi timezone
local_tz = pendulum.timezone("Asia/Jakarta")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

def send_stock_movement_alert():
    """
    Send stock movement report to Telegram
    """
    try:
        # Find latest date in database
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date) FROM public_analytics.daily_stock_metrics")
        latest_date = cursor.fetchone()[0]
        
        # If no data at all, exit function
        if not latest_date:
            logger.warning("No data in database")
            return "No data"
        
        # Format date for query
        date_filter = latest_date.strftime('%Y-%m-%d')
        logger.info(f"Using data for date: {date_filter}")
        
        # Tambahkan threshold untuk alerts
        min_change_percent = 5.0  # Hanya tampilkan saham dengan perubahan > 5%
        
        # Query for stock movements with threshold
        query = {
            "🔼 *Top 10 Gainers:*": f"""
                SELECT symbol, name, percent_change
                FROM public_analytics.daily_stock_metrics
                WHERE date = '{date_filter}'
                  AND percent_change IS NOT NULL
                  AND percent_change > {min_change_percent}
                ORDER BY percent_change DESC
                LIMIT 10;
            """,
            "🔽 *Top 10 Losers:*": f"""
                SELECT symbol, name, percent_change
                FROM public_analytics.daily_stock_metrics
                WHERE date = '{date_filter}'
                  AND percent_change IS NOT NULL
                  AND percent_change < -{min_change_percent}
                ORDER BY percent_change ASC
                LIMIT 10;
            """,
            "💰 *Top 10 by Value:*": f"""
                SELECT symbol, name, value
                FROM public_analytics.daily_stock_metrics
                WHERE date = '{date_filter}'
                  AND value IS NOT NULL
                ORDER BY value DESC
                LIMIT 10;
            """,
            "🔊 *Top 10 by Volume:*": f"""
                SELECT symbol, name, volume
                FROM public_analytics.daily_stock_metrics
                WHERE date = '{date_filter}'
                  AND volume IS NOT NULL
                ORDER BY volume DESC
                LIMIT 10;
            """
        }
        
        # Execute queries and process results
        body = ""
        last_df = None  # Variable to store last DataFrame
        
        for section_title, sql in query.items():
            df = fetch_data(sql)
            last_df = df  # Save last DataFrame
            
            if not df.empty:
                body += f"{section_title}\n"
                for _, row in df.iterrows():
                    if 'percent_change' in row:
                        body += f"• *{row['symbol']}* ({row['name']}): {row['percent_change']:.2f}%\n"
                    elif 'value' in row:
                        body += f"• *{row['symbol']}* ({row['name']}): Rp{row['value']:,.0f}\n"
                    elif 'volume' in row:
                        body += f"• *{row['symbol']}* ({row['name']}): {row['volume']:,} lot\n"
                body += "\n"
        
        conn.close()
        
        # If no data to send
        if body == "":
            logger.warning(f"No stock movement data for date {date_filter}")
            return f"No data for date {date_filter}"
        
        # Send to Telegram
        message = f"🔔 *Summary Saham ({date_filter})* 🔔\n\n{body}"
        
        # Tambahkan failsafe untuk Telegram
        try:
            result = send_telegram_message(message)
            if "successfully" in result:
                return f"Stock movement report sent: {len(last_df) if last_df is not None else 0} stocks"
            else:
                # Coba menyimpan ke log file jika Telegram gagal
                with open("/opt/airflow/logs/alerts.log", "a") as f:
                    f.write(f"=== ALERT {datetime.now()} ===\n{message}\n\n")
                return f"Telegram failed, message saved to log: {result}"
        except Exception as e:
            logger.error(f"Error sending Telegram: {str(e)}")
            # Simpan ke log file sebagai fallback
            with open("/opt/airflow/logs/alerts.log", "a") as f:
                f.write(f"=== ALERT {datetime.now()} ===\n{message}\n\n")
            return f"Telegram error, saved to log: {str(e)}"
    except Exception as e:
        logger.error(f"Error in send_stock_movement_alert: {str(e)}")
        return f"Error: {str(e)}"

def send_news_sentiment_report():
    """
    Send news sentiment report to Telegram
    """
    try:
        conn = get_database_connection()
        
        # Check latest available date in detik_ticker_sentiment table
        date_query = """
        SELECT MAX(date) 
        FROM detik_ticker_sentiment
        WHERE news_count >= 1
        """
        
        latest_date = None
        try:
            cursor = conn.cursor()
            cursor.execute(date_query)
            latest_date = cursor.fetchone()[0]
        except:
            logger.warning("Could not query detik_ticker_sentiment table")
            
        # If no data at all
        if not latest_date:
            logger.warning("No date data in news sentiment table")
            return "No news sentiment data"
        
        logger.info(f"Using sentiment data for date: {latest_date}")
        
        # Query for stocks with highest and lowest sentiment
        sql = f"""
        SELECT 
            ticker as symbol,
            avg_sentiment,
            news_count,
            positive_count,
            negative_count,
            CASE WHEN news_count > 0 THEN (positive_count::float / news_count) * 100 ELSE 0 END as positive_percentage,
            CASE 
                WHEN avg_sentiment > 0.5 THEN 'Strong Buy'
                WHEN avg_sentiment > 0.2 THEN 'Buy'
                WHEN avg_sentiment < -0.5 THEN 'Strong Sell'
                WHEN avg_sentiment < -0.2 THEN 'Sell'
                ELSE 'Hold/No Signal'
            END as trading_signal
        FROM detik_ticker_sentiment
        WHERE date = '{latest_date}'
            AND news_count >= 1
        ORDER BY avg_sentiment DESC
        """
        
        df = pd.read_sql(sql, conn)
        
        if df.empty:
            logger.warning(f"No news sentiment data for date {latest_date}")
            return f"No news sentiment data for date {latest_date}"
        
        # Create Telegram message
        message = f"📰 *LAPORAN SENTIMEN BERITA ({latest_date})* 📰\n\n"
        
        # Add top 5 stocks with positive sentiment
        message += "*Top 5 Saham dengan Sentimen Positif:*\n"
        positive_df = df[df['avg_sentiment'] > 0].head(5)
        
        if not positive_df.empty:
            for i, row in enumerate(positive_df.itertuples(), 1):
                message += f"{i}. *{row.symbol}*: Sentimen {row.avg_sentiment:.2f} ({row.news_count} berita)\n"
                if hasattr(row, 'trading_signal') and row.trading_signal and row.trading_signal != "Hold/No Signal":
                    message += f"   Signal: {row.trading_signal}\n"
        else:
            message += "Tidak ada saham dengan sentimen positif\n"
        
        message += "\n"
        
        # Add top 5 stocks with negative sentiment
        message += "*Top 5 Saham dengan Sentimen Negatif:*\n"
        negative_df = df[df['avg_sentiment'] < 0].sort_values('avg_sentiment').head(5)
        
        if not negative_df.empty:
            for i, row in enumerate(negative_df.itertuples(), 1):
                message += f"{i}. *{row.symbol}*: Sentimen {row.avg_sentiment:.2f} ({row.news_count} berita)\n"
                if hasattr(row, 'trading_signal') and row.trading_signal and row.trading_signal != "Hold/No Signal":
                    message += f"   Signal: {row.trading_signal}\n"
        else:
            message += "Tidak ada saham dengan sentimen negatif\n"
        
        message += "\n"
        
        # Check latest date for news
        news_date_query = """
        SELECT MAX(date(published_at)) 
        FROM detik_news
        """
        
        latest_news_date = None
        try:
            cursor = conn.cursor()
            cursor.execute(news_date_query)
            latest_news_date = cursor.fetchone()[0]
        except:
            logger.warning("Could not query detik_news table")
            
        if not latest_news_date:
            logger.warning("No news data in detik_news table")
        else:
            logger.info(f"Using latest news from date: {latest_news_date}")
            
            # Add some latest news
            news_sql = f"""
            SELECT ticker, title, url 
            FROM detik_news
            WHERE date(published_at) = '{latest_news_date}'
            ORDER BY published_at DESC
            LIMIT 5
            """
            
            news_df = pd.read_sql(news_sql, conn)
            
            if not news_df.empty:
                message += "*Berita Terbaru:*\n\n"
                for i, row in enumerate(news_df.itertuples(), 1):
                    message += f"{i}. *{row.ticker}*: [{row.title}]({row.url})\n\n"
        
        conn.close()
        
        # Send to Telegram
        result = send_telegram_message(message, disable_web_page_preview=True)
        if "successfully" in result:
            return f"News sentiment report sent: {len(df)} stocks"
        else:
            return result
    except Exception as e:
        logger.error(f"Error in send_news_sentiment_report: {str(e)}")
        return f"Error: {str(e)}"

def send_technical_signal_report():
    """
    Send technical signal report to Telegram
    """
    try:
        # Query for stocks with technical signals
        sql = """
        WITH avg_volume AS (
        SELECT 
            symbol,
            AVG(volume) AS avg_10day_volume
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '5 days'
            AND date < CURRENT_DATE - INTERVAL '1 day'
        GROUP BY symbol
        )
        SELECT 
            r.symbol,
            r.date,
            r.rsi,
            r.rsi_signal,
            m.macd_signal,
            v.volume AS volume_today
        FROM public_analytics.technical_indicators_rsi r
        JOIN public_analytics.technical_indicators_macd m 
            ON r.symbol = m.symbol AND r.date = m.date
        JOIN public_analytics.daily_stock_metrics v
            ON r.symbol = v.symbol AND r.date = v.date
        JOIN avg_volume av
            ON v.symbol = av.symbol
        WHERE r.date = CURRENT_DATE - INTERVAL '1 day'
            AND r.rsi_signal = 'Oversold'
            AND m.macd_signal = 'Bullish'
            AND v.volume > av.avg_10day_volume
        ORDER BY v.volume desc
        LIMIT 20;
        """
        
        df = fetch_data(sql)
        
        if df.empty:
            logger.warning("No significant technical signals")
            return "No technical signals"
        
        # Create Telegram message
        message = "📊 *SINYAL TEKNIKAL HARI INI* 📊\n\n"
        
        # Stocks with buy signals (Oversold + Bullish)
        buy_signals = df[(df['rsi_signal'] == 'Oversold') & (df['macd_signal'] == 'Bullish')]
        
        if not buy_signals.empty:
            message += "*Sinyal Beli Kuat:*\n"
            for i, row in enumerate(buy_signals.itertuples(), 1):
                message += f"{i}. *{row.symbol}*: RSI={row.rsi:.2f} (Oversold), MACD=Bullish\n"
            message += "\n"
        
        # Stocks with sell signals (Overbought + Bearish)
        sell_signals = df[(df['rsi_signal'] == 'Overbought') & (df['macd_signal'] == 'Bearish')]
        
        if not sell_signals.empty:
            message += "*Sinyal Jual Kuat:*\n"
            for i, row in enumerate(sell_signals.itertuples(), 1):
                message += f"{i}. *{row.symbol}*: RSI={row.rsi:.2f} (Overbought), MACD=Bearish\n"
            message += "\n"
        
        # Stocks with weak buy signals (only one indicator)
        weak_buy = df[(df['rsi_signal'] == 'Oversold') & (df['macd_signal'] != 'Bullish')] 
        weak_buy = pd.concat([weak_buy, df[(df['rsi_signal'] != 'Oversold') & (df['macd_signal'] == 'Bullish')]])
        
        if not weak_buy.empty:
            message += "*Sinyal Beli Lemah:*\n"
            for i, row in enumerate(weak_buy.itertuples(), 1):
                rsi_info = f"RSI={row.rsi:.2f} ({row.rsi_signal})" if row.rsi_signal != 'Neutral' else ""
                macd_info = f"MACD={row.macd_signal}" if row.macd_signal != 'Neutral' else ""
                signal_info = ", ".join([info for info in [rsi_info, macd_info] if info])
                message += f"{i}. *{row.symbol}*: {signal_info}\n"
        
        # Send to Telegram
        result = send_telegram_message(message)
        if "successfully" in result:
            return f"Technical signal report sent: {len(df)} stocks"
        else:
            return result
    except Exception as e:
        logger.error(f"Error in send_technical_signal_report: {str(e)}")
        return f"Error: {str(e)}"

def send_accumulation_distribution_report():
    """
    Send Accumulation/Distribution (A/D) Line report to Telegram
    """
    try:
        conn = get_database_connection()
        
        # Query to get A/D Line data
        # We create a temporary table first to calculate money flow volume
        cur = conn.cursor()
        
        # Step 1: Delete temporary table if it already exists
        cur.execute("DROP TABLE IF EXISTS temp_mfv;")
        
        # Step 2: Create temporary table to calculate Money Flow Volume
        cur.execute("""
        CREATE TEMPORARY TABLE temp_mfv AS
        SELECT
            symbol,
            date,
            CASE
                WHEN (high - low) = 0 THEN 0
                ELSE ((close - low) - (high - close)) / (high - low) * volume
            END AS money_flow_volume
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '40 day';
        """)
        
        # Query to get data ACCUMULATION (positive A/D Line)
        # Stocks with positive accumulation (money_flow_volume > 0)
        accumulation_sql = """
        SELECT
            t1.symbol,
            t1.date,
            t1.money_flow_volume,
            (
                SELECT SUM(t2.money_flow_volume)
                FROM temp_mfv t2
                WHERE t2.symbol = t1.symbol
                AND t2.date <= t1.date
            ) AS ad_line,
            m.name,
            m.close,
            'accumulation' AS type
        FROM temp_mfv t1
        JOIN public_analytics.daily_stock_metrics m 
            ON t1.symbol = m.symbol AND t1.date = m.date
        WHERE t1.date = (SELECT MAX(date) FROM public_analytics.daily_stock_metrics)
            AND t1.money_flow_volume > 0  -- Only stocks with positive accumulation
        ORDER BY ad_line DESC
        LIMIT 10;
        """
        
        # Query to get data DISTRIBUTION (negative A/D Line)
        # Stocks with distribution (money_flow_volume < 0)
        distribution_sql = """
        SELECT
            t1.symbol,
            t1.date,
            t1.money_flow_volume,
            (
                SELECT SUM(t2.money_flow_volume)
                FROM temp_mfv t2
                WHERE t2.symbol = t1.symbol
                AND t2.date <= t1.date
            ) AS ad_line,
            m.name,
            m.close,
            'distribution' AS type
        FROM temp_mfv t1
        JOIN public_analytics.daily_stock_metrics m 
            ON t1.symbol = m.symbol AND t1.date = m.date
        WHERE t1.date = (SELECT MAX(date) FROM public_analytics.daily_stock_metrics)
            AND t1.money_flow_volume < 0  -- Only stocks with distribution (negative)
        ORDER BY ad_line ASC  -- Sort from most negative
        LIMIT 10;
        """
        
        # Execute query for accumulation
        accumulation_df = pd.read_sql(accumulation_sql, conn)
        
        # Execute query for distribution
        distribution_df = pd.read_sql(distribution_sql, conn)
        
        # Delete temporary table
        cur.execute("DROP TABLE IF EXISTS temp_mfv;")
        conn.commit()
        conn.close()
        
        # Check if there is data
        if accumulation_df.empty and distribution_df.empty:
            logger.warning("No significant Accumulation/Distribution Line data")
            return "No A/D Line data"
        
        # Create Telegram message
        message = "📊 *LAPORAN ACCUMULATION/DISTRIBUTION LINE* 📊\n\n"
        message += "Berdasarkan indikator A/D Line 40 hari terakhir\n\n"
        
        # ACCUMULATION SECTION
        if not accumulation_df.empty:
            message += "📈 *TOP 10 SAHAM DENGAN AKUMULASI TERTINGGI*\n\n"
            
            # Format for accumulation table
            for i, row in enumerate(accumulation_df.itertuples(), 1):
                # Format money_flow_volume and ad_line for better readability
                mfv_formatted = f"{row.money_flow_volume:,.0f}" if abs(row.money_flow_volume) >= 1000 else f"{row.money_flow_volume:.2f}"
                ad_line_formatted = f"{row.ad_line:,.0f}" if abs(row.ad_line) >= 1000 else f"{row.ad_line:.2f}"
                
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Harga: Rp{row.close:,.0f} | A/D Line: {ad_line_formatted}\n"
                message += f"   Money Flow Volume: {mfv_formatted}\n\n"
        
        # DISTRIBUTION SECTION
        if not distribution_df.empty:
            message += "📉 *TOP 10 SAHAM DENGAN DISTRIBUSI TERTINGGI*\n\n"
            
            # Format for distribution table
            for i, row in enumerate(distribution_df.itertuples(), 1):
                # Format money_flow_volume and ad_line for better readability
                mfv_formatted = f"{row.money_flow_volume:,.0f}" if abs(row.money_flow_volume) >= 1000 else f"{row.money_flow_volume:.2f}"
                ad_line_formatted = f"{row.ad_line:,.0f}" if abs(row.ad_line) >= 1000 else f"{row.ad_line:.2f}"
                
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Harga: Rp{row.close:,.0f} | A/D Line: {ad_line_formatted}\n"
                message += f"   Money Flow Volume: {mfv_formatted}\n\n"
        
        # Add explanation about A/D Line indicator
        message += "*Tentang A/D Line:*\n"
        message += "Indikator A/D Line menunjukkan aliran uang ke dalam saham (akumulasi) vs. aliran keluar (distribusi). "
        message += "• Nilai positif & meningkat: Akumulasi oleh investor, potensi kenaikan harga.\n"
        message += "• Nilai negatif & menurun: Distribusi oleh investor, potensi penurunan harga.\n\n"
        message += "Data diambil dari 40 hari terakhir."
        
        # Send to Telegram
        result = send_telegram_message(message)
        if "successfully" in result:
            return f"A/D Line report sent: {len(accumulation_df) + len(distribution_df)} stocks"
        else:
            return result
    except Exception as e:
        logger.error(f"Error in send_accumulation_distribution_report: {str(e)}")
        return f"Error: {str(e)}"

# DAG definition
with DAG(
    dag_id="reporting_and_alerting",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 19 * * 1-5",  # Every weekday at 19:00 Jakarta time
    catchup=False,
    default_args=default_args,
    tags=["reporting", "alerting", "telegram"]
) as dag:
    
    # Wait for ML trading signals DAG to complete
    wait_for_ml = ExternalTaskSensor(
        task_id="wait_for_ml_signals",
        external_dag_id="ml_trading_signals",
        external_task_id="end_task",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )
    
    # Stock movement report
    send_movement = PythonOperator(
        task_id="send_stock_movement_alert",
        python_callable=send_stock_movement_alert
    )
    
    # News sentiment report
    send_sentiment = PythonOperator(
        task_id="send_news_sentiment_report",
        python_callable=send_news_sentiment_report
    )
    
    # Technical signal report
    send_technical = PythonOperator(
        task_id="send_technical_signal_report",
        python_callable=send_technical_signal_report
    )
    
    # Accumulation/Distribution report
    send_ad_report = PythonOperator(
        task_id="send_accumulation_distribution_report",
        python_callable=send_accumulation_distribution_report
    )
    
    # End marker
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    # Define task dependencies
    wait_for_ml >> [send_movement, send_sentiment, send_technical, send_ad_report] >> end_task