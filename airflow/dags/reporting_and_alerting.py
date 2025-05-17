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
        
        # Check if detik_ticker_sentiment table exists
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'detik_ticker_sentiment'
        );
        """
        
        cursor = conn.cursor()
        cursor.execute(check_table_sql)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.warning("Table detik_ticker_sentiment does not exist")
            return "Table detik_ticker_sentiment does not exist. Skipping report."
        
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
        message = f"📰 *LAPORAN SENTIMEN BERITA (Detik) ({latest_date})* 📰\n\n"
        
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
        
        # Check if detik_news table exists
        check_news_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'detik_news'
        );
        """
        
        cursor.execute(check_news_table_sql)
        news_table_exists = cursor.fetchone()[0]
        
        if news_table_exists:
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

def send_newsapi_sentiment_report():
    """
    Send NewsAPI sentiment report to Telegram
    """
    try:
        conn = get_database_connection()
        
        # Check if newsapi_ticker_sentiment table exists
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'newsapi_ticker_sentiment'
        );
        """
        
        cursor = conn.cursor()
        cursor.execute(check_table_sql)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.warning("Table newsapi_ticker_sentiment does not exist")
            return "Table newsapi_ticker_sentiment does not exist. Skipping report."
        
        # Check latest available date in newsapi_ticker_sentiment table
        date_query = """
        SELECT MAX(date) 
        FROM newsapi_ticker_sentiment
        WHERE news_count >= 1
        """
        
        latest_date = None
        try:
            cursor = conn.cursor()
            cursor.execute(date_query)
            latest_date = cursor.fetchone()[0]
        except:
            logger.warning("Could not query newsapi_ticker_sentiment table")
            
        # If no data at all
        if not latest_date:
            logger.warning("No date data in NewsAPI sentiment table")
            return "No NewsAPI sentiment data"
        
        logger.info(f"Using NewsAPI sentiment data for date: {latest_date}")
        
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
        FROM newsapi_ticker_sentiment
        WHERE date = '{latest_date}'
            AND news_count >= 1
        ORDER BY avg_sentiment DESC
        """
        
        df = pd.read_sql(sql, conn)
        
        if df.empty:
            logger.warning(f"No NewsAPI sentiment data for date {latest_date}")
            return f"No NewsAPI sentiment data for date {latest_date}"
        
        # Create Telegram message
        message = f"📰 *LAPORAN SENTIMEN BERITA (NewsAPI) ({latest_date})* 📰\n\n"
        
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
        
        # Check if newsapi_news table exists
        check_news_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'newsapi_news'
        );
        """
        
        cursor.execute(check_news_table_sql)
        news_table_exists = cursor.fetchone()[0]
        
        if news_table_exists:
            # Check latest date for news
            news_date_query = """
            SELECT MAX(date(published_at)) 
            FROM newsapi_news
            """
            
            latest_news_date = None
            try:
                cursor = conn.cursor()
                cursor.execute(news_date_query)
                latest_news_date = cursor.fetchone()[0]
            except:
                logger.warning("Could not query newsapi_news table")
                
            if not latest_news_date:
                logger.warning("No news data in newsapi_news table")
            else:
                logger.info(f"Using latest NewsAPI news from date: {latest_news_date}")
                
                # Add some latest news
                news_sql = f"""
                SELECT ticker, title, url, source_name
                FROM newsapi_news
                WHERE date(published_at) = '{latest_news_date}'
                ORDER BY published_at DESC
                LIMIT 5
                """
                
                news_df = pd.read_sql(news_sql, conn)
                
                if not news_df.empty:
                    message += "*Berita Terbaru (NewsAPI):*\n\n"
                    for i, row in enumerate(news_df.itertuples(), 1):
                        source_info = f" ({row.source_name})" if hasattr(row, 'source_name') and row.source_name else ""
                        message += f"{i}. *{row.ticker}*: [{row.title}]({row.url}){source_info}\n\n"
        
        conn.close()
        
        # Send to Telegram
        result = send_telegram_message(message, disable_web_page_preview=True)
        if "successfully" in result:
            return f"NewsAPI sentiment report sent: {len(df)} stocks"
        else:
            return result
    except Exception as e:
        logger.error(f"Error in send_newsapi_sentiment_report: {str(e)}")
        return f"Error: {str(e)}"

def send_combined_news_sentiment_report():
    """
    Send combined news sentiment report from Detik and NewsAPI to Telegram
    """
    try:
        conn = get_database_connection()
        
        # Check if required tables exist
        check_detik_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'detik_ticker_sentiment'
        );
        """
        
        check_newsapi_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'newsapi_ticker_sentiment'
        );
        """
        
        cursor = conn.cursor()
        cursor.execute(check_detik_table_sql)
        detik_table_exists = cursor.fetchone()[0]
        
        cursor.execute(check_newsapi_table_sql)
        newsapi_table_exists = cursor.fetchone()[0]
        
        if not detik_table_exists and not newsapi_table_exists:
            logger.warning("Neither detik_ticker_sentiment nor newsapi_ticker_sentiment tables exist")
            return "Required sentiment tables do not exist. Skipping combined report."
        
        # Dapatkan tanggal terbaru dari kedua sumber
        latest_detik_date = None
        latest_newsapi_date = None
        
        if detik_table_exists:
            try:
                cursor.execute("SELECT MAX(date) FROM detik_ticker_sentiment WHERE news_count >= 1")
                latest_detik_date = cursor.fetchone()[0]
            except Exception as e:
                logger.warning(f"Error retrieving detik date: {str(e)}")
                
        if newsapi_table_exists:
            try:
                cursor.execute("SELECT MAX(date) FROM newsapi_ticker_sentiment WHERE news_count >= 1")
                latest_newsapi_date = cursor.fetchone()[0]
            except Exception as e:
                logger.warning(f"Error retrieving newsapi date: {str(e)}")
        
        if not latest_detik_date and not latest_newsapi_date:
            logger.warning("No data available in both sentiment tables")
            return "No sentiment data available"
        
        # Buat pesan untuk Telegram
        message = f"📰 *LAPORAN SENTIMEN BERITA GABUNGAN ({datetime.now().strftime('%Y-%m-%d')})* 📰\n\n"
        
        # Tambahkan bagian untuk Detik jika data tersedia
        if latest_detik_date:
            logger.info(f"Using Detik sentiment data for date: {latest_detik_date}")
            
            # Query untuk sentimen Detik
            detik_sql = f"""
            SELECT 
                ticker as symbol,
                avg_sentiment,
                news_count,
                positive_count,
                negative_count,
                'Detik' as source
            FROM detik_ticker_sentiment
            WHERE date = '{latest_detik_date}'
                AND news_count >= 1
            ORDER BY avg_sentiment DESC
            """
            
            detik_df = pd.read_sql(detik_sql, conn)
            
            if not detik_df.empty:
                message += "*🔍 Top 3 Saham Sentimen Positif (Detik):*\n"
                positive_detik = detik_df[detik_df['avg_sentiment'] > 0].head(3)
                
                if not positive_detik.empty:
                    for i, row in enumerate(positive_detik.itertuples(), 1):
                        message += f"{i}. *{row.symbol}*: {row.avg_sentiment:.2f} ({row.news_count} berita)\n"
                else:
                    message += "Tidak ada saham dengan sentimen positif\n"
                
                message += "\n"
        
        # Tambahkan bagian untuk NewsAPI jika data tersedia
        if latest_newsapi_date:
            logger.info(f"Using NewsAPI sentiment data for date: {latest_newsapi_date}")
            
            # Query untuk sentimen NewsAPI
            newsapi_sql = f"""
            SELECT 
                ticker as symbol,
                avg_sentiment,
                news_count,
                positive_count,
                negative_count,
                'NewsAPI' as source
            FROM newsapi_ticker_sentiment
            WHERE date = '{latest_newsapi_date}'
                AND news_count >= 1
            ORDER BY avg_sentiment DESC
            """
            
            newsapi_df = pd.read_sql(newsapi_sql, conn)
            
            if not newsapi_df.empty:
                message += "*🌐 Top 3 Saham Sentimen Positif (NewsAPI):*\n"
                positive_newsapi = newsapi_df[newsapi_df['avg_sentiment'] > 0].head(3)
                
                if not positive_newsapi.empty:
                    for i, row in enumerate(positive_newsapi.itertuples(), 1):
                        message += f"{i}. *{row.symbol}*: {row.avg_sentiment:.2f} ({row.news_count} berita)\n"
                else:
                    message += "Tidak ada saham dengan sentimen positif\n"
                
                message += "\n"
        
        # Gabungkan data jika kedua sumber tersedia
        if latest_detik_date and latest_newsapi_date and detik_table_exists and newsapi_table_exists:
            # Mencoba menggabungkan data dengan error handling
            try:
                # Query untuk gabungan
                combined_sql = f"""
                WITH detik_data AS (
                    SELECT 
                        ticker as symbol,
                        avg_sentiment as detik_sentiment,
                        news_count as detik_count
                    FROM detik_ticker_sentiment
                    WHERE date = '{latest_detik_date}'
                        AND news_count >= 1
                ),
                newsapi_data AS (
                    SELECT 
                        ticker as symbol,
                        avg_sentiment as newsapi_sentiment,
                        news_count as newsapi_count
                    FROM newsapi_ticker_sentiment
                    WHERE date = '{latest_newsapi_date}'
                        AND news_count >= 1
                )
                SELECT 
                    COALESCE(d.symbol, n.symbol) as symbol,
                    d.detik_sentiment,
                    n.newsapi_sentiment,
                    COALESCE(d.detik_count, 0) as detik_count,
                    COALESCE(n.newsapi_count, 0) as newsapi_count,
                    COALESCE(d.detik_sentiment, 0) * 0.5 + COALESCE(n.newsapi_sentiment, 0) * 0.5 as combined_sentiment
                FROM detik_data d
                FULL OUTER JOIN newsapi_data n ON d.symbol = n.symbol
                WHERE d.symbol IS NOT NULL AND n.symbol IS NOT NULL
                ORDER BY combined_sentiment DESC
                """
                
                combined_df = pd.read_sql(combined_sql, conn)
                
                if not combined_df.empty:
                    message += "*📊 Top 5 Saham Sentimen Gabungan:*\n"
                    top_combined = combined_df.head(5)
                    
                    for i, row in enumerate(top_combined.itertuples(), 1):
                        message += f"{i}. *{row.symbol}*: {row.combined_sentiment:.2f} (Detik: {row.detik_sentiment:.2f}, NewsAPI: {row.newsapi_sentiment:.2f})\n"
                    
                    message += "\n"
                    
                    # Tambahkan saham dengan sentimen kontradiktif
                    message += "*⚡ Saham dengan Sentimen Kontradiktif:*\n"
                    
                    contradictory_df = combined_df[
                        ((combined_df['detik_sentiment'] > 0.2) & (combined_df['newsapi_sentiment'] < -0.2)) | 
                        ((combined_df['detik_sentiment'] < -0.2) & (combined_df['newsapi_sentiment'] > 0.2))
                    ]
                    
                    if not contradictory_df.empty:
                        for i, row in enumerate(contradictory_df.head(3).itertuples(), 1):
                            message += f"{i}. *{row.symbol}*: Detik ({row.detik_sentiment:.2f}) vs NewsAPI ({row.newsapi_sentiment:.2f})\n"
                    else:
                        message += "Tidak ditemukan saham dengan sentimen kontradiktif\n"
            except Exception as e:
                logger.warning(f"Error creating combined sentiment: {str(e)}")
                message += "*Catatan: Tidak dapat menggabungkan data sentimen dari kedua sumber.*\n\n"
        
        # Tambahkan berita terbaru
        try:
            # Buat SQL dinamis berdasarkan tabel yang ada
            latest_news_sql = ""
            parts = []
            
            if detik_table_exists:
                check_detik_news_sql = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'detik_news'
                );
                """
                cursor.execute(check_detik_news_sql)
                if cursor.fetchone()[0]:
                    parts.append("""
                    (SELECT 
                        ticker, 
                        title, 
                        url, 
                        published_at, 
                        'Detik' as source
                     FROM detik_news
                     ORDER BY published_at DESC
                     LIMIT 3)
                    """)
            
            if newsapi_table_exists:
                check_newsapi_news_sql = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'newsapi_news'
                );
                """
                cursor.execute(check_newsapi_news_sql)
                if cursor.fetchone()[0]:
                    parts.append("""
                    (SELECT 
                        ticker, 
                        title, 
                        url, 
                        published_at, 
                        'NewsAPI' as source
                     FROM newsapi_news
                     ORDER BY published_at DESC
                     LIMIT 3)
                    """)
            
            if parts:
                latest_news_sql = " UNION ALL ".join(parts) + " ORDER BY published_at DESC LIMIT 5"
                
                latest_news_df = pd.read_sql(latest_news_sql, conn)
                
                if not latest_news_df.empty:
                    message += "\n*📰 Berita Terbaru:*\n\n"
                    for i, row in enumerate(latest_news_df.itertuples(), 1):
                        message += f"{i}. *{row.ticker}* [{row.source}]: [{row.title}]({row.url})\n\n"
        except Exception as e:
            logger.warning(f"Error getting latest news: {str(e)}")
        
        conn.close()
        
        # Kirim ke Telegram
        result = send_telegram_message(message, disable_web_page_preview=True)
        if "successfully" in result:
            return "Combined news sentiment report sent successfully"
        else:
            return result
    except Exception as e:
        logger.error(f"Error in send_combined_news_sentiment_report: {str(e)}")
        return f"Error: {str(e)}"

def send_technical_signal_report():
    """
    Send technical signal report to Telegram
    """
    try:
        # Check if required tables exist
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Check rsi table
        check_rsi_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = 'technical_indicators_rsi'
        );
        """
        
        # Check macd table
        check_macd_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = 'technical_indicators_macd'
        );
        """
        
        cursor.execute(check_rsi_table_sql)
        rsi_table_exists = cursor.fetchone()[0]
        
        cursor.execute(check_macd_table_sql)
        macd_table_exists = cursor.fetchone()[0]
        
        if not rsi_table_exists or not macd_table_exists:
            logger.warning("Required technical tables do not exist")
            return "Required technical tables do not exist. Skipping report."
        
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
        
        # Check if required table exists
        cursor = conn.cursor()
        check_metrics_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = 'daily_stock_metrics'
        );
        """
        
        cursor.execute(check_metrics_table_sql)
        metrics_table_exists = cursor.fetchone()[0]
        
        if not metrics_table_exists:
            logger.warning("Table public_analytics.daily_stock_metrics does not exist")
            return "Required metrics table does not exist. Skipping A/D Line report."
        
        # Query to get A/D Line data
        # We create a temporary table first to calculate money flow volume
        
        # Step 1: Delete temporary table if it already exists
        cur = conn.cursor()
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

def send_bandarmology_report():
    """
    Send Bandarmology analysis report to Telegram
    """
    try:
        conn = get_database_connection()
        
        # Check if required table exists
        cursor = conn.cursor()
        check_summary_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'daily_stock_summary'
        );
        """
        
        cursor.execute(check_summary_table_sql)
        summary_table_exists = cursor.fetchone()[0]
        
        if not summary_table_exists:
            logger.warning("Table public.daily_stock_summary does not exist")
            return "Required summary table does not exist. Skipping Bandarmology report."
        
        # Get latest date from database
        latest_date = get_latest_stock_date()
        if not latest_date:
            logger.warning("No date data available")
            return "No date data available"
        
        date_filter = latest_date.strftime('%Y-%m-%d')
        logger.info(f"Running Bandarmology analysis for date: {date_filter}")
        
        # Execute Bandarmology query
        bandar_sql = """
        WITH volume_analysis AS (
            -- Analisis volume selama 20 hari terakhir
            SELECT 
                symbol,
                date,
                volume,
                ROUND(AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING), 0) AS avg_volume_20d,
                close,
                prev_close,
                ((close - prev_close) / NULLIF(prev_close, 0) * 100) AS percent_change,
                foreign_buy,
                foreign_sell,
                (foreign_buy - foreign_sell) AS foreign_net,
                bid,
                offer,
                bid_volume,
                offer_volume
            FROM public.daily_stock_summary
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
        ),
        accumulation_signals AS (
            -- Identifikasi pola akumulasi
            SELECT 
                v.symbol,
                v.date,
                v.close,
                v.volume,
                v.avg_volume_20d,
                v.percent_change,
                v.foreign_net,
                -- Indikator volume spike (>2x rata-rata)
                CASE WHEN v.volume > 2 * v.avg_volume_20d THEN 1 ELSE 0 END AS volume_spike,
                -- Indikator akumulasi asing
                CASE WHEN v.foreign_net > 0 THEN 1 ELSE 0 END AS foreign_buying,
                -- Indikator kekuatan bid (demand lebih besar dari supply)
                CASE WHEN v.bid_volume > 1.5 * v.offer_volume THEN 1 ELSE 0 END AS strong_bid,
                -- Indikator hidden buying (volume tinggi dengan pergerakan harga terbatas)
                CASE WHEN v.volume > 1.5 * v.avg_volume_20d AND ABS(v.percent_change) < 1.0 THEN 1 ELSE 0 END AS hidden_buying,
                -- Cek apakah ada closing price positif
                CASE WHEN v.close > v.prev_close THEN 1 ELSE 0 END AS positive_close
            FROM volume_analysis v
        ),
        final_scores AS (
            -- Kalkulasi skor final dan tambahkan data historis untuk analisis
            SELECT 
                a.symbol,
                a.date,
                a.close,
                a.percent_change,
                a.volume,
                a.avg_volume_20d,
                a.volume/NULLIF(a.avg_volume_20d, 0) AS volume_ratio,
                a.foreign_net,
                -- Kalkulasi skor bandarmology (semakin tinggi semakin kuat)
                (a.volume_spike + a.foreign_buying + a.strong_bid + a.hidden_buying + a.positive_close) AS bandar_score,
                -- Cek performa harga 1 hari setelah tanggal
                LEAD(a.percent_change, 1) OVER (PARTITION BY a.symbol ORDER BY a.date) AS next_day_change,
                -- Tambahkan data untuk analisis tren jangka pendek
                SUM(a.positive_close) OVER (PARTITION BY a.symbol ORDER BY a.date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS positive_days_last5
            FROM accumulation_signals a
        ),
        bandar_ranking AS (
            -- Final selection dari saham teratas dengan potensi kenaikan
            SELECT 
                symbol,
                date,
                close,
                ROUND(volume_ratio, 2) AS volume_ratio,
                bandar_score,
                positive_days_last5,
                foreign_net,
                CASE
                    WHEN bandar_score >= 4 THEN 'Sangat Kuat'
                    WHEN bandar_score = 3 THEN 'Kuat'
                    WHEN bandar_score = 2 THEN 'Sedang'
                    ELSE 'Lemah'
                END AS signal_strength
            FROM final_scores
            WHERE date = (SELECT MAX(date) FROM public.daily_stock_summary) -- Hanya ambil data hari terakhir
            AND bandar_score >= 2 -- Filter hanya sinyal sedang ke atas
            AND volume_ratio > 1.5 -- Harus ada kenaikan volume signifikan
        )
        -- Join dengan tabel nama untuk hasil lengkap
        SELECT 
            b.symbol,
            d.name,
            b.close,
            b.volume_ratio,
            b.bandar_score,
            b.signal_strength,
            b.positive_days_last5,
            b.foreign_net
        FROM bandar_ranking b
        LEFT JOIN public.daily_stock_summary d 
            ON b.symbol = d.symbol 
            AND d.date = (SELECT MAX(date) FROM public.daily_stock_summary)
        ORDER BY b.bandar_score DESC, b.volume_ratio DESC
        LIMIT 15;
        """
        
        # Execute query
        bandar_df = fetch_data(bandar_sql)
        
        if bandar_df.empty:
            logger.warning("No significant bandarmology patterns found")
            return "No bandarmology patterns found"
        
        # Create Telegram message
        message = "🔍 *ANALISIS BANDARMOLOGY HARI INI* 🔍\n\n"
        message += f"Saham-saham berikut menunjukkan pola akumulasi \"bandar\" ({date_filter}):\n\n"
        
        # Group by signal strength
        strong_signals = bandar_df[bandar_df['signal_strength'] == 'Sangat Kuat']
        medium_signals = bandar_df[bandar_df['signal_strength'] == 'Kuat']
        weak_signals = bandar_df[bandar_df['signal_strength'] == 'Sedang']
        
        # Add strong signals
        if not strong_signals.empty:
            message += "💪 *Sinyal Akumulasi Sangat Kuat:*\n\n"
            for i, row in enumerate(strong_signals.itertuples(), 1):
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Harga: Rp{row.close:,.0f} | Rasio Volume: {row.volume_ratio:.2f}x\n"
                message += f"   Skor Bandar: {row.bandar_score}/5 | Hari Positif: {row.positive_days_last5}/5\n"
                
                # Add foreign flow if available
                if row.foreign_net and row.foreign_net != 0:
                    foreign_net_formatted = f"{row.foreign_net:,.0f}" if abs(row.foreign_net) >= 1000 else f"{row.foreign_net:.2f}"
                    message += f"   Net Asing: {foreign_net_formatted}\n"
                
                message += "\n"
        
        # Add medium signals
        if not medium_signals.empty:
            message += "⚡ *Sinyal Akumulasi Kuat:*\n\n"
            for i, row in enumerate(medium_signals.itertuples(), 1):
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Harga: Rp{row.close:,.0f} | Rasio Volume: {row.volume_ratio:.2f}x\n"
                message += f"   Skor Bandar: {row.bandar_score}/5\n\n"
        
        # Add weak signals (limited to save space)
        if not weak_signals.empty:
            message += "🔎 *Sinyal Akumulasi Sedang:*\n"
            for i, row in enumerate(weak_signals.head(5).itertuples(), 1):
                message += f"{i}. *{row.symbol}* ({row.name}) - Skor: {row.bandar_score}/5\n"
            
            message += "\n"
        
        # Add explanation about Bandarmology
        message += "*Tentang Analisis Bandarmology:*\n"
        message += "Analisis ini mengidentifikasi pola akumulasi \"bandar\" berdasarkan 5 indikator:\n"
        message += "• Volume Spike: Lonjakan volume signifikan (>2x rata-rata)\n"
        message += "• Foreign Buying: Pembelian bersih oleh investor asing\n"
        message += "• Bid Strength: Tekanan beli lebih kuat dari jual\n"
        message += "• Hidden Buying: Volume tinggi dengan pergerakan harga terbatas\n"
        message += "• Positive Close: Penutupan dengan harga positif\n\n"
        message += "*Disclaimer:* Analisis ini bersifat teknikal dan tidak menjamin kenaikan. Lakukan analisis tambahan sebelum mengambil keputusan investasi."
        
        # Send to Telegram
        result = send_telegram_message(message)
        if "successfully" in result:
            return f"Bandarmology report sent: {len(bandar_df)} stocks"
        else:
            return result
    except Exception as e:
        logger.error(f"Error in send_bandarmology_report: {str(e)}")
        return f"Error: {str(e)}"

def send_high_probability_signals():
    """
    Send high probability trading signals report to Telegram
    """
    try:
        conn = get_database_connection()
        
        # Verificar si la tabla existe antes de ejecutar la consulta
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = 'advanced_trading_signals'
        );
        """
        
        cursor = conn.cursor()
        cursor.execute(check_table_sql)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.warning("Table public_analytics.advanced_trading_signals does not exist")
            return "Table advanced_trading_signals does not exist yet. Skipping report."
            
        # Get latest available date
        latest_date = get_latest_stock_date()
        if not latest_date:
            logger.warning("No date data available")
            return "No date data available"
            
        date_filter = latest_date.strftime('%Y-%m-%d')
        
        # Query for high probability signals
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
            WHERE s.date = '{date_filter}'
              AND s.winning_probability >= 0.8
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
            return f"Error querying signals: {str(e)}"
        
        if signals_df.empty:
            logger.warning(f"No high probability signals for date {date_filter}")
            return f"No high probability signals found for date {date_filter}"
        
        # Create Telegram message
        message = f"🎯 *SINYAL TRADING PROBABILITAS TINGGI ({date_filter})* 🎯\n\n"
        message += "Saham-saham berikut menunjukkan pola dengan probabilitas kemenangan tinggi:\n\n"
        
        for i, row in enumerate(signals_df.itertuples(), 1):
            message += f"{i}. *{row.symbol}* ({row.name})\n"
            message += f"   Harga: Rp{row.close:,.0f} | Buy Score: {row.buy_score}/10\n"
            message += f"   Probabilitas: {row.winning_probability:.2%} | RSI: {row.rsi:.1f}\n"
            
            if hasattr(row, 'signal_strength') and row.signal_strength:
                message += f"   Kekuatan Sinyal: {row.signal_strength}\n"
                
            if hasattr(row, 'macd_signal') and row.macd_signal:
                message += f"   MACD: {row.macd_signal}\n"
                
            message += "\n"
        
        # Add disclaimer
        message += "*Disclaimer:* Sinyal ini dihasilkan dari algoritma dan tidak menjamin keberhasilan. Selalu lakukan analisis tambahan sebelum mengambil keputusan investasi."
        
        # Send to Telegram
        result = send_telegram_message(message)
        if "successfully" in result:
            return f"High probability signals report sent: {len(signals_df)} stocks"
        else:
            return result
    except Exception as e:
        logger.error(f"Error in send_high_probability_signals: {str(e)}")
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
    
    wait_for_transformation = ExternalTaskSensor(
        task_id="wait_for_transformation",
        external_dag_id="data_transformation",
        external_task_id="end_task",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )
    
    # Tambahkan wait for newsapi data
    wait_for_newsapi = ExternalTaskSensor(
        task_id="wait_for_newsapi",
        external_dag_id="newsapi_data_ingestion",
        external_task_id="end_task",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        # Tambahkan soft_fail agar DAG bisa tetap berjalan jika newsapi_data_ingestion 
        # belum ada atau belum berjalan
        soft_fail=True
    )

    wait = DummyOperator(
        task_id="wait"
    )
    
    # Stock movement report
    send_movement = PythonOperator(
        task_id="send_stock_movement_alert",
        python_callable=send_stock_movement_alert
    )
    
    # News sentiment report (Detik)
    send_sentiment = PythonOperator(
        task_id="send_news_sentiment_report",
        python_callable=send_news_sentiment_report
    )
    
    # News sentiment report (NewsAPI)
    send_newsapi_sentiment = PythonOperator(
        task_id="send_newsapi_sentiment_report",
        python_callable=send_newsapi_sentiment_report
    )
    
    # Combined news sentiment report
    send_combined_sentiment = PythonOperator(
        task_id="send_combined_news_sentiment_report",
        python_callable=send_combined_news_sentiment_report
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
    
    # Bandarmology report
    send_bandar = PythonOperator(
        task_id="send_bandarmology_report",
        python_callable=send_bandarmology_report
    )
    
    # High probability signals report - new task
    send_signals = PythonOperator(
        task_id="send_high_probability_signals",
        python_callable=send_high_probability_signals
    )

    # End marker
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    # Define task dependencies
    [wait_for_transformation, wait_for_newsapi] >> wait >> [send_movement, send_sentiment, send_newsapi_sentiment, send_technical, send_ad_report, send_bandar, send_signals]
    [send_sentiment, send_newsapi_sentiment] >> send_combined_sentiment
    [send_movement, send_technical, send_ad_report, send_bandar, send_combined_sentiment, send_signals] >> end_task