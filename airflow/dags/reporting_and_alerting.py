from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import requests
import pendulum

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

def send_stock_movement_alert():
    """
    Kirim laporan pergerakan saham ke Telegram
    """
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    
    # Cari tanggal terbaru di database
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM public_analytics.daily_stock_metrics")
    latest_date = cursor.fetchone()[0]
    
    # Jika tidak ada data sama sekali, keluar dari fungsi
    if not latest_date:
        print("Tidak ada data di database")
        return "Tidak ada data"
    
    # Format tanggal untuk query
    date_filter = latest_date.strftime('%Y-%m-%d')
    print(f"Menggunakan data untuk tanggal: {date_filter}")
    
    # Query untuk pergerakan saham
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
    
    # Menjalankan query dan memproses hasilnya
    body = ""
    last_df = None  # Variabel untuk menyimpan DataFrame terakhir
    
    for section_title, sql in query.items():
        df = pd.read_sql(sql, conn)
        last_df = df  # Simpan DataFrame terakhir
        
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
    
    # Jika tidak ada data untuk dikirim
    if body == "":
        print(f"Tidak ada data pergerakan saham untuk tanggal {date_filter}")
        return f"Tidak ada data untuk tanggal {date_filter}"
    
    # Kirim ke Telegram
    telegram_token = "7918924633:AAFyjOZVxilCo2mG1A-G-fm7buo-bJuKGC0"
    chat_id = "213349272"  # Ganti dengan chat ID Anda
    
    message = f"🔔 *Summary Saham ({date_filter})* 🔔\n\n{body}"
    
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=payload)
        print(f"Status Telegram (Pergerakan): {response.status_code}, Response: {response.text}")
        
        if response.status_code == 200:
            return f"Laporan pergerakan saham terkirim: {len(last_df) if last_df is not None else 0} saham"
        else:
            return f"Error mengirim ke Telegram: {response.status_code}, {response.text}"
    except Exception as e:
        print(f"Error exception: {str(e)}")
        return f"Error exception: {str(e)}"

def send_news_sentiment_report():
    """
    Kirim laporan sentimen berita ke Telegram
    """
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    
    # Cek tanggal terbaru yang tersedia di tabel detik_ticker_sentiment
    date_query = """
    SELECT MAX(date) 
    FROM detik_ticker_sentiment
    WHERE news_count >= 1
    """
    
    cursor = conn.cursor()
    cursor.execute(date_query)
    latest_date = cursor.fetchone()[0]
    
    # Jika tidak ada data sama sekali
    if not latest_date:
        print("Tidak ada data tanggal di tabel sentimen berita")
        return "Tidak ada data sentimen berita"
    
    print(f"Menggunakan data sentimen untuk tanggal: {latest_date}")
    
    # Query untuk saham dengan sentimen tertinggi dan terendah menggunakan tanggal terbaru
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
        print(f"Tidak ada data sentimen berita untuk tanggal {latest_date}")
        return f"Tidak ada data sentimen berita untuk tanggal {latest_date}"
    
    # Buat pesan Telegram
    message = f"📰 *LAPORAN SENTIMEN BERITA ({latest_date})* 📰\n\n"
    
    # Tambahkan top 5 saham dengan sentimen positif
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
    
    # Tambahkan top 5 saham dengan sentimen negatif
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
    
    # Cek tanggal terbaru untuk berita
    news_date_query = """
    SELECT MAX(date(published_at)) 
    FROM detik_news
    """
    
    cursor.execute(news_date_query)
    latest_news_date = cursor.fetchone()[0]
    
    if not latest_news_date:
        print("Tidak ada data berita di tabel detik_news")
    else:
        print(f"Menggunakan berita terbaru dari tanggal: {latest_news_date}")
        
        # Tambahkan beberapa berita terbaru
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
            # Jika tidak ada berita untuk tanggal tersebut, coba berita terbaru apapun tanggalnya
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
    
    # Kirim ke Telegram dengan penanganan error
    telegram_token = "7918924633:AAFyjOZVxilCo2mG1A-G-fm7buo-bJuKGC0"
    chat_id = "213349272"  # Ganti dengan chat ID Anda
    
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    
    try:
        response = requests.post(url, json=payload)
        print(f"Status Telegram (Sentimen): {response.status_code}, Response: {response.text}")
        
        if response.status_code == 200:
            return f"Laporan sentimen berita terkirim: {len(df)} saham"
        else:
            return f"Error mengirim ke Telegram: {response.status_code}, {response.text}"
    except Exception as e:
        print(f"Error exception: {str(e)}")
        return f"Error exception: {str(e)}"

def send_technical_signal_report():
    """
    Kirim laporan sinyal teknikal ke Telegram
    """
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    
    # Query untuk saham dengan sinyal teknikal
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
        print("Tidak ada sinyal teknikal signifikan")
        return "Tidak ada sinyal teknikal"
    
    # Buat pesan Telegram
    message = "📊 *SINYAL TEKNIKAL HARI INI* 📊\n\n"
    
    # Saham dengan sinyal beli (Oversold + Bullish)
    buy_signals = df[(df['rsi_signal'] == 'Oversold') & (df['macd_signal'] == 'Bullish')]
    
    if not buy_signals.empty:
        message += "*Sinyal Beli Kuat:*\n"
        for i, row in enumerate(buy_signals.itertuples(), 1):
            message += f"{i}. *{row.symbol}*: RSI={row.rsi:.2f} (Oversold), MACD=Bullish\n"
        message += "\n"
    
    # Saham dengan sinyal jual (Overbought + Bearish)
    sell_signals = df[(df['rsi_signal'] == 'Overbought') & (df['macd_signal'] == 'Bearish')]
    
    if not sell_signals.empty:
        message += "*Sinyal Jual Kuat:*\n"
        for i, row in enumerate(sell_signals.itertuples(), 1):
            message += f"{i}. *{row.symbol}*: RSI={row.rsi:.2f} (Overbought), MACD=Bearish\n"
        message += "\n"
    
    # Saham dengan sinyal beli lemah (hanya satu indikator)
    weak_buy = df[(df['rsi_signal'] == 'Oversold') & (df['macd_signal'] != 'Bullish')] 
    weak_buy = pd.concat([weak_buy, df[(df['rsi_signal'] != 'Oversold') & (df['macd_signal'] == 'Bullish')]])
    
    if not weak_buy.empty:
        message += "*Sinyal Beli Lemah:*\n"
        for i, row in enumerate(weak_buy.itertuples(), 1):
            rsi_info = f"RSI={row.rsi:.2f} ({row.rsi_signal})" if row.rsi_signal != 'Neutral' else ""
            macd_info = f"MACD={row.macd_signal}" if row.macd_signal != 'Neutral' else ""
            signal_info = ", ".join([info for info in [rsi_info, macd_info] if info])
            message += f"{i}. *{row.symbol}*: {signal_info}\n"
    
    # Kirim ke Telegram
    telegram_token = "7918924633:AAFyjOZVxilCo2mG1A-G-fm7buo-bJuKGC0"
    chat_id = "213349272"  # Ganti dengan chat ID Anda
    
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    response = requests.post(url, json=payload)
    print(f"Status Telegram (Teknikal): {response.status_code}")
    
    return f"Laporan sinyal teknikal terkirim: {len(df)} saham"
def send_accumulation_distribution_report():
    """
    Kirim laporan Accumulation/Distribution (A/D) Line ke Telegram
    """
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    
    # Query untuk mendapatkan data A/D Line
    # Kita membuat temporary table terlebih dahulu untuk menghitung nilai money flow volume
    cur = conn.cursor()
    
    # Step 1: Hapus temporary table jika sudah ada
    cur.execute("DROP TABLE IF EXISTS temp_mfv;")
    
    # Step 2: Buat temporary table untuk menghitung Money Flow Volume
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
    
    # Query untuk mendapatkan data AKUMULASI (A/D Line positif)
    # Saham dengan akumulasi positif (money_flow_volume > 0)
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
        AND t1.money_flow_volume > 0  -- Hanya saham dengan akumulasi positif
    ORDER BY ad_line DESC
    LIMIT 10;
    """
    
    # Query untuk mendapatkan data DISTRIBUSI (A/D Line negatif)
    # Saham dengan distribusi (money_flow_volume < 0)
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
        AND t1.money_flow_volume < 0  -- Hanya saham dengan distribusi (negatif)
    ORDER BY ad_line ASC  -- Urutkan dari yang paling negatif
    LIMIT 10;
    """
    
    # Eksekusi query untuk akumulasi
    accumulation_df = pd.read_sql(accumulation_sql, conn)
    
    # Eksekusi query untuk distribusi
    distribution_df = pd.read_sql(distribution_sql, conn)
    
    # Hapus temporary table
    cur.execute("DROP TABLE IF EXISTS temp_mfv;")
    conn.commit()
    conn.close()
    
    # Periksa apakah ada data
    if accumulation_df.empty and distribution_df.empty:
        print("Tidak ada data Accumulation/Distribution Line yang signifikan")
        return "Tidak ada data A/D Line"
    
    # Buat pesan Telegram
    message = "📊 *LAPORAN ACCUMULATION/DISTRIBUTION LINE* 📊\n\n"
    message += "Berdasarkan indikator A/D Line 40 hari terakhir\n\n"
    
    # BAGIAN AKUMULASI
    if not accumulation_df.empty:
        message += "📈 *TOP 10 SAHAM DENGAN AKUMULASI TERTINGGI*\n\n"
        
        # Format untuk tabel akumulasi
        for i, row in enumerate(accumulation_df.itertuples(), 1):
            # Format money_flow_volume dan ad_line agar lebih mudah dibaca
            mfv_formatted = f"{row.money_flow_volume:,.0f}" if abs(row.money_flow_volume) >= 1000 else f"{row.money_flow_volume:.2f}"
            ad_line_formatted = f"{row.ad_line:,.0f}" if abs(row.ad_line) >= 1000 else f"{row.ad_line:.2f}"
            
            message += f"{i}. *{row.symbol}* ({row.name})\n"
            message += f"   Harga: Rp{row.close:,.0f} | A/D Line: {ad_line_formatted}\n"
            message += f"   Money Flow Volume: {mfv_formatted}\n\n"
    
    # BAGIAN DISTRIBUSI
    if not distribution_df.empty:
        message += "📉 *TOP 10 SAHAM DENGAN DISTRIBUSI TERTINGGI*\n\n"
        
        # Format untuk tabel distribusi
        for i, row in enumerate(distribution_df.itertuples(), 1):
            # Format money_flow_volume dan ad_line agar lebih mudah dibaca
            mfv_formatted = f"{row.money_flow_volume:,.0f}" if abs(row.money_flow_volume) >= 1000 else f"{row.money_flow_volume:.2f}"
            ad_line_formatted = f"{row.ad_line:,.0f}" if abs(row.ad_line) >= 1000 else f"{row.ad_line:.2f}"
            
            message += f"{i}. *{row.symbol}* ({row.name})\n"
            message += f"   Harga: Rp{row.close:,.0f} | A/D Line: {ad_line_formatted}\n"
            message += f"   Money Flow Volume: {mfv_formatted}\n\n"
    
    # Tambahkan penjelasan tentang indikator A/D Line
    message += "*Tentang A/D Line:*\n"
    message += "Indikator A/D Line menunjukkan aliran uang ke dalam saham (akumulasi) vs. aliran keluar (distribusi). "
    message += "• Nilai positif & meningkat: Akumulasi oleh investor, potensi kenaikan harga.\n"
    message += "• Nilai negatif & menurun: Distribusi oleh investor, potensi penurunan harga.\n\n"
    message += "Data diambil dari 40 hari terakhir."
    
    # Kirim ke Telegram
    telegram_token = "7918924633:AAFyjOZVxilCo2mG1A-G-fm7buo-bJuKGC0"
    chat_id = "213349272"  # Ganti dengan chat ID Anda
    
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=payload)
        print(f"Status Telegram (A/D Line): {response.status_code}, Response: {response.text}")
        
        if response.status_code == 200:
            return f"Laporan A/D Line terkirim: {len(accumulation_df) + len(distribution_df)} saham"
        else:
            return f"Error mengirim ke Telegram: {response.status_code}, {response.text}"
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

    # Tunggu hingga transformasi data selesai
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
    
    # Kirim laporan pergerakan saham
    send_movement_alert = PythonOperator(
        task_id="send_stock_movement_alert",
        python_callable=send_stock_movement_alert
    )
    
    # Kirim laporan sentimen berita
    send_sentiment_report = PythonOperator(
        task_id="send_news_sentiment_report",
        python_callable=send_news_sentiment_report
    )
    
    # Kirim laporan teknikal
    send_technical_report = PythonOperator(
        task_id="send_technical_signal_report",
        python_callable=send_technical_signal_report
    )
    
    # Kirim laporan A/D Line
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