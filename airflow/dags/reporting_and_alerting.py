from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import pendulum
import json
import os
import requests

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

def get_telegram_credentials():
    """
    Retrieve Telegram bot credentials from Airflow Variables
    """
    telegram_bot_token = Variable.get("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = Variable.get("TELEGRAM_CHAT_ID")
    return telegram_bot_token, telegram_chat_id

def get_telegram_conn():
    """
    Get Telegram connection from Airflow connections
    """
    conn = BaseHook.get_connection("telegram_conn")
    return conn

def get_telegram_url():
    """
    Create Telegram API URL using the bot token
    """
    telegram_bot_token, _ = get_telegram_credentials()
    return f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"

def create_send_message_payload(message, disable_web_page_preview=False):
    """
    Create payload for Telegram API
    """
    _, telegram_chat_id = get_telegram_credentials()
    return {
        "chat_id": telegram_chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": disable_web_page_preview
    }

def send_stock_movement_alert():
    """
    Send stock movement report to Telegram
    """
    # Get connection credentials for PostgreSQL
    postgres_user = os.environ.get('POSTGRES_USER', 'airflow')
    postgres_password = os.environ.get('POSTGRES_PASSWORD', 'airflow')
    postgres_db = os.environ.get('POSTGRES_DB', 'airflow')
    
    conn = psycopg2.connect(
        host="postgres",
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password
    )
    
    # Find latest date in database
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM public_analytics.daily_stock_metrics")
    latest_date = cursor.fetchone()[0]
    
    # If no data at all, exit function
    if not latest_date:
        print("No data in database")
        return "No data"
    
    # Format date for query
    date_filter = latest_date.strftime('%Y-%m-%d')
    print(f"Using data for date: {date_filter}")
    
    # Query for stock movements
    query = {
        "🔼 *Top 10 Gainers:*": f"""
            SELECT symbol, name, percent_change
            FROM public_analytics.daily_stock_metrics
            WHERE date = '{date_filter}'
              AND percent_change IS NOT NULL
            ORDER BY percent_change DESC
            LIMIT 10;
        """,
        "🔽 *Top 10 Losers:*": f"""
            SELECT symbol, name, percent_change
            FROM public_analytics.daily_stock_metrics
            WHERE date = '{date_filter}'
              AND percent_change IS NOT NULL
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
        df = pd.read_sql(sql, conn)
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
    
    # If no data to send
    if body == "":
        print(f"No stock movement data for date {date_filter}")
        return f"No data for date {date_filter}"
    
    # Send to Telegram
    message = f"🔔 *Summary Saham ({date_filter})* 🔔\n\n{body}"
    
    url = get_telegram_url()
    payload = create_send_message_payload(message)
    
    try:
        response = requests.post(url, json=payload)
        print(f"Telegram Status (Movement): {response.status_code}, Response: {response.text}")
        
        if response.status_code == 200:
            return f"Stock movement report sent: {len(last_df) if last_df is not None else 0} stocks"
        else:
            return f"Error sending to Telegram: {response.status_code}, {response.text}"
    except Exception as e:
        print(f"Error exception: {str(e)}")
        return f"Error exception: {str(e)}"

def send_news_sentiment_report():
    """
    Send news sentiment report to Telegram
    """
    # Get connection credentials for PostgreSQL
    postgres_user = os.environ.get('POSTGRES_USER', 'airflow')
    postgres_password = os.environ.get('POSTGRES_PASSWORD', 'airflow')
    postgres_db = os.environ.get('POSTGRES_DB', 'airflow')
    
    conn = psycopg2.connect(
        host="postgres",
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password
    )
    
    # Check latest available date in detik_ticker_sentiment table
    date_query = """
    SELECT MAX(date) 
    FROM detik_ticker_sentiment
    WHERE news_count >= 1
    """
    
    cursor = conn.cursor()
    cursor.execute(date_query)
    latest_date = cursor.fetchone()[0]
    
    # If no data at all
    if not latest_date:
        print("No date data in news sentiment table")
        return "No news sentiment data"
    
    print(f"Using sentiment data for date: {latest_date}")
    
    # Query for stocks with highest and lowest sentiment using latest date
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
        print(f"No news sentiment data for date {latest_date}")
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
    
    cursor.execute(news_date_query)
    latest_news_date = cursor.fetchone()[0]
    
    if not latest_news_date:
        print("No news data in detik_news table")
    else:
        print(f"Using latest news from date: {latest_news_date}")
        
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
        else:
            # If no news for that date, try latest news of any date
            alt_news_sql = """
            SELECT ticker, title, url 
            FROM detik_news
            ORDER BY published_at DESC
            LIMIT 5
            """
            
            alt_news_df = pd.read_sql(alt_news_sql, conn)
            
            if not alt_news_df.empty:
                message += "*Berita Terbaru:*\n\n"
                for i, row in enumerate(alt_news_df.itertuples(), 1):
                    message += f"{i}. *{row.ticker}*: [{row.title}]({row.url})\n\n"
    
    conn.close()
    
    # Send to Telegram
    url = get_telegram_url()
    payload = create_send_message_payload(message, disable_web_page_preview=True)
    
    try:
        response = requests.post(url, json=payload)
        print(f"Telegram Status (Sentiment): {response.status_code}, Response: {response.text}")
        
        if response.status_code == 200:
            return f"News sentiment report sent: {len(df)} stocks"
        else:
            return f"Error sending to Telegram: {response.status_code}, {response.text}"
    except Exception as e:
        print(f"Error exception: {str(e)}")
        return f"Error exception: {str(e)}"

def send_technical_signal_report():
    """
    Send technical signal report to Telegram
    """
    # Get connection credentials for PostgreSQL
    postgres_user = os.environ.get('POSTGRES_USER', 'airflow')
    postgres_password = os.environ.get('POSTGRES_PASSWORD', 'airflow')
    postgres_db = os.environ.get('POSTGRES_DB', 'airflow')
    
    conn = psycopg2.connect(
        host="postgres",
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password
    )
    
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
    
    df = pd.read_sql(sql, conn)
    conn.close()
    
    if df.empty:
        print("No significant technical signals")
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
    url = get_telegram_url()
    payload = create_send_message_payload(message)
    
    try:
        response = requests.post(url, json=payload)
        print(f"Telegram Status (Technical): {response.status_code}, Response: {response.text}")
        
        if response.status_code == 200:
            return f"Technical signal report sent: {len(df)} stocks"
        else:
            return f"Error sending to Telegram: {response.status_code}, {response.text}"
    except Exception as e:
        print(f"Error exception: {str(e)}")
        return f"Error exception: {str(e)}"

def send_accumulation_distribution_report():
    """
    Send Accumulation/Distribution (A/D) Line report to Telegram
    """
    # Get connection credentials for PostgreSQL
    postgres_user = os.environ.get('POSTGRES_USER', 'airflow')
    postgres_password = os.environ.get('POSTGRES_PASSWORD', 'airflow')
    postgres_db = os.environ.get('POSTGRES_DB', 'airflow')
    
    conn = psycopg2.connect(
        host="postgres",
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password
    )
    
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
        print("No significant Accumulation/Distribution Line data")
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
    url = get_telegram_url()
    payload = create_send_message_payload(message)
    
    try:
        response = requests.post(url, json=payload)
        print(f"Telegram Status (A/D Line): {response.status_code}, Response: {response.text}")
        
        if response.status_code == 200:
            return f"A/D Line report sent: {len(accumulation_df) + len(distribution_df)} stocks"
        else:
            return f"Error sending to Telegram: {response.status_code}, {response.text}"
    except Exception as e:
        print(f"Error exception: {str(e)}")
        return f"Error exception: {str(e)}"

# DAG definition
with DAG(
    dag_id="reporting_and_alerting",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["reporting", "alerting", "telegram"]
) as dag:

    # Wait until data transformation is complete
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

    wait_for_technical = ExternalTaskSensor(
        task_id="wait_for_technical",
        external_dag_id="technical_indicators_calculation",
        external_task_id="end_task",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    # wait_2 = DummyOperator(
    #     task_id="wait_2"
    # )

    
    # Send stock movement report
    send_movement_alert = PythonOperator(
        task_id="send_stock_movement_alert",
        python_callable=send_stock_movement_alert
    )
    
    # Send news sentiment report
    send_sentiment_report = PythonOperator(
        task_id="send_news_sentiment_report",
        python_callable=send_news_sentiment_report
    )
    
    # Send technical report
    send_technical_report = PythonOperator(
        task_id="send_technical_signal_report",
        python_callable=send_technical_signal_report
    )
    
    # Send A/D Line report
    send_ad_line_report = PythonOperator(
        task_id="send_accumulation_distribution_report",
        python_callable=send_accumulation_distribution_report
    )
    
    # Marker task
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    # Task dependencies
    wait_for_transformation >> [send_movement_alert, send_sentiment_report, send_technical_report, send_ad_line_report] >> end_task