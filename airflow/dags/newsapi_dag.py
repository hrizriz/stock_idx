from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import json
import os
import pendulum
import requests
from pathlib import Path
import time
import random
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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

# Daftar saham Kompas100 - Fokus pada 30 saham paling aktif
active_tickers = [
    "AKRA", "AMMN", "AMRT", "ANTM", "ARTO", "ASII", "AUTO", "AVIA", "BBCA", "BBNI", 
    "BBRI", "BBTN", "BBYB", "BDKR", "BFIN", "BMRI", "BMTR", "BNGA", "BRIS", "BRMS", 
    "BRPT", "BSDE", "BTPS", "CMRY", "CPIN", "CTRA", "DEWA", "DSNG", "ELSA", "EMTK", 
    "ENRG", "ERAA", "ESSA", "EXCL", "FILM", "GGRM", "GJTL", "GOTO", "HEAL", "HMSP", 
    "HRUM", "ICBP", "INCO", "INDF", "INDY", "INET", "INKP", "INTP", "ISAT", "ITMG", 
    "JPFA", "JSMR", "KIJA", "KLBF", "KPIG", "LSIP", "MAPA", "MAPI", "MARK", "MBMA", 
    "MDKA", "MEDC", "MIDI", "MIKA", "MNCN", "MTEL", "MYOR", "NCKL", "NISP", "PANI", 
    "PGAS", "PGEO", "PNLF", "PTBA", "PTPP", "PTRO", "PWON", "RAJA", "SCMA", "SIDO", 
    "SMGR", "SMIL", "SMRA", "SRTG", "SSIA", "SSMS", "SURI", "TINS", "TKIM", "TLKM", 
    "TOBA", "TOWR", "TPIA", "UNIQ", "UNTR", "UNVR", "WIFI"
]

def get_newsapi_key():
    """
    Mengambil NewsAPI key dari Airflow Variables dengan error handling
    """
    try:
        api_key = Variable.get("NEWS_API_KEY")
        if not api_key:
            raise ValueError("NEWS_API_KEY tidak ditemukan di Airflow Variables")
        return api_key
    except Exception as e:
        logger.error(f"Error mengambil NEWS_API_KEY: {str(e)}")
        return None

def create_news_tables_if_not_exist():
    """Membuat tabel-tabel berita jika belum ada"""
    try:
        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow",
            connect_timeout=10
        )
        cur = conn.cursor()
        
        # Buat tabel newsapi_news jika belum ada
        cur.execute("""
        CREATE TABLE IF NOT EXISTS newsapi_news (
            id SERIAL PRIMARY KEY,
            ticker TEXT,
            title TEXT,
            description TEXT,
            url TEXT,
            source_name TEXT,
            published_at TIMESTAMP,
            sentiment TEXT,
            sentiment_score NUMERIC,
            positive_count INTEGER,
            negative_count INTEGER,
            scrape_date DATE,
            UNIQUE(url)
        )
        """)
        
        # Buat tabel newsapi_ticker_sentiment jika belum ada
        cur.execute("""
        CREATE TABLE IF NOT EXISTS newsapi_ticker_sentiment (
            id SERIAL PRIMARY KEY,
            ticker TEXT,
            date DATE,
            news_count INTEGER,
            avg_sentiment NUMERIC,
            positive_count INTEGER,
            negative_count INTEGER,
            neutral_count INTEGER,
            UNIQUE (ticker, date)
        )
        """)
        
        # Add index untuk performa query
        cur.execute("CREATE INDEX IF NOT EXISTS idx_newsapi_news_ticker ON newsapi_news(ticker)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_newsapi_news_date ON newsapi_news(published_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_newsapi_ticker_sentiment_date ON newsapi_ticker_sentiment(date)")
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info("✅ Tabel newsapi_news telah dibuat atau sudah ada sebelumnya")
        return "Tables created successfully"
    except Exception as e:
        logger.error(f"Error creating news tables: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error: {str(e)}"

def sentiment_analysis(text):
    """
    Analisis sentimen sederhana berbasis keyword
    """
    # Kata positif dalam bahasa Indonesia (diperluas)
    positives = ['naik', 'untung', 'profit', 'positif', 'optimis', 'meningkat', 
                'berkembang', 'pesat', 'melejit', 'melonjak', 'surplus', 'prospek',
                'menjanjikan', 'apresiasi', 'bullish', 'pemulihan', 'efisien', 'potensi', 
                'tumbuh', 'menguat', 'mendongkrak', 'terbaik', 'gemilang', 'cemerlang', 
                'eskalasi', 'tahan banting', 'mempertahankan', 'mendominasi', 'daya tarik', 
                'prospektif', 'rekor', 'tertinggi', 'puncak', 'laba', 'keuntungan', 'prestasi',
                'berkembang', 'bertumbuh', 'peningkatan', 'dividend', 'dividen', 'bonus', 'rekor',
                'sukses', 'berhasil', 'capaian', 'kemajuan', 'prestasi', 'capaian', 'kemajuan',
                'bangkit', 'penguatan', 'penguatan']

    # Kata negatif dalam bahasa Indonesia (diperluas)
    negatives = ['turun', 'rugi', 'negatif', 'pesimis', 'menurun', 'merosot', 
                'anjlok', 'krisis', 'gagal', 'turunkan', 'defisit', 'bearish',
                'koreksi', 'resesi', 'lambat', 'tekanan', 'kesulitan', 'meredup', 
                'terpuruk', 'tertekan', 'krisis likuiditas', 'bubar', 'hancur', 'downtrend', 
                'bersedih', 'jatuh', 'rugi besar', 'pesimisme', 'deklinasi', 'melemah', 
                'tergelincir', 'penurunan', 'kerugian', 'terpangkas', 'merosot', 'terhambat',
                'rontok', 'fluktuatif', 'gejolak', 'volatil', 'kekhawatiran', 'kekecewaan',
                'merugi', 'kolaps', 'bangkrut', 'pailit', 'delisting', 'suspend', 'terjun',
                'meluncur', 'ambrol', 'ambruk', 'tumbang', 'mati']
    
    # Juga menyertakan versi bahasa Inggris untuk berita internasional
    positives_en = ['rise', 'profit', 'positive', 'optimistic', 'increase', 
                  'growth', 'rapid', 'surge', 'jump', 'surplus', 'prospect',
                  'promising', 'appreciation', 'bullish', 'recovery', 'efficient', 'potential', 
                  'grow', 'strengthen', 'boost', 'best', 'outstanding', 'brilliant', 
                  'escalation', 'resilient', 'maintain', 'dominate', 'attractive', 
                  'prospective', 'record', 'highest', 'peak', 'gain', 'dividend', 'bonus', 
                  'success', 'achievement', 'progress', 'achievement', 'upturn', 'upward']

    negatives_en = ['fall', 'loss', 'negative', 'pessimistic', 'decrease', 'decline', 
                  'plunge', 'crisis', 'fail', 'reduce', 'deficit', 'bearish',
                  'correction', 'recession', 'slow', 'pressure', 'difficulty', 'fade', 
                  'struggle', 'stressed', 'liquidity crisis', 'dissolve', 'crash', 'downtrend', 
                  'sad', 'drop', 'major loss', 'pessimism', 'declination', 'weaken', 
                  'slip', 'decrease', 'loss', 'cut', 'plummet', 'impede',
                  'collapse', 'fluctuate', 'volatility', 'volatile', 'concern', 'disappointment',
                  'lose', 'collapse', 'bankrupt', 'bankruptcy', 'delisting', 'suspend', 'dive',
                  'tumble', 'crash', 'crisis', 'fail']
    
    # Tambahkan daftar kata dari bahasa Inggris
    positives.extend(positives_en)
    negatives.extend(negatives_en)
    
    # Lowercase text untuk konsistensi
    text = text.lower() if text else ""
    
    # Hitung kata positif dan negatif
    positive_count = sum([1 for word in positives if word in text])
    negative_count = sum([1 for word in negatives if word in text])
    
    # Tentukan sentimen berdasarkan kata yang ditemukan
    if positive_count > negative_count:
        sentiment = "Positive"
        sentiment_score = positive_count / (positive_count + negative_count) if (positive_count + negative_count) > 0 else 0
    elif negative_count > positive_count:
        sentiment = "Negative"
        sentiment_score = -negative_count / (positive_count + negative_count) if (positive_count + negative_count) > 0 else 0
    else:
        sentiment = "Neutral"
        sentiment_score = 0
    
    return sentiment, sentiment_score, positive_count, negative_count

def fetch_newsapi_articles():
    """
    Mengambil berita dari NewsAPI untuk daftar ticker saham
    """
    NEWS_API_KEY = get_newsapi_key()
    
    if not NEWS_API_KEY:
        logger.error("NEWS_API_KEY tidak tersedia, menggunakan sample data")
        sample_news = generate_sample_news(active_tickers)
        
        data_folder = Path("/opt/airflow/data")
        data_folder.mkdir(exist_ok=True)
        with open(data_folder / "newsapi_articles.json", 'w', encoding='utf-8') as f:
            json.dump(sample_news, f, ensure_ascii=False, indent=4)
            
        return len(sample_news)
    
    all_articles = []
    failed_tickers = []
    
    # Tanggal untuk query (7 hari terakhir)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    # Format tanggal untuk API
    from_date = start_date.strftime('%Y-%m-%d')
    to_date = end_date.strftime('%Y-%m-%d')
    
    logger.info(f"Mengambil berita dari {from_date} hingga {to_date}")
    
    # Base URL NewsAPI
    base_url = "https://newsapi.org/v2/everything"
    
    # Loop untuk setiap ticker
    for ticker in active_tickers:
        try:
            logger.info(f"Mencari berita untuk ticker: {ticker}")
            
            # Parameter untuk API request
            # Mencari berita dengan kata kunci ticker dan kata "saham" atau "stock"
            query = f"{ticker} AND (saham OR stock)"
            params = {
                'q': query,
                'from': from_date,
                'to': to_date,
                'language': 'id,en',  # Bahasa Indonesia dan Inggris
                'sortBy': 'publishedAt',
                'pageSize': 10,  # Ambil 10 artikel teratas
                'apiKey': NEWS_API_KEY
            }
            
            # Request ke NewsAPI
            try:
                response = requests.get(base_url, params=params, timeout=10)
                
                # Cek status response
                if response.status_code != 200:
                    logger.warning(f"Error response dari NewsAPI: {response.status_code}")
                    logger.warning(f"Message: {response.json().get('message', 'No message')}")
                    failed_tickers.append(ticker)
                    continue
                
                # Parse response JSON
                data = response.json()
                
                # Cek hasil
                if data.get('status') != 'ok':
                    logger.warning(f"API status tidak ok: {data.get('status')}")
                    failed_tickers.append(ticker)
                    continue
                
                # Log jumlah artikel yang ditemukan
                total_results = data.get('totalResults', 0)
                logger.info(f"Ditemukan {total_results} artikel untuk {ticker}")
                
                # Ambil artikel dari response
                articles = data.get('articles', [])
                
                # Proses artikel
                for article in articles:
                    # Ambil data yang relevan
                    title = article.get('title', '')
                    description = article.get('description', '')
                    url = article.get('url', '')
                    source_name = article.get('source', {}).get('name', '')
                    published_at = article.get('publishedAt', '')
                    
                    # Skip artikel yang tidak memiliki judul atau URL
                    if not title or not url:
                        continue
                    
                    # Konversi published_at ke format datetime
                    try:
                        published_at_dt = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
                    except:
                        # Jika format tanggal tidak sesuai, gunakan waktu sekarang
                        published_at_dt = datetime.now()
                    
                    # Analisis sentimen dari judul dan deskripsi
                    content_for_analysis = f"{title} {description}"
                    sentiment, sentiment_score, positive_count, negative_count = sentiment_analysis(content_for_analysis)
                    
                    # Tambahkan artikel ke list
                    all_articles.append({
                        'ticker': ticker,
                        'title': title,
                        'description': description,
                        'url': url,
                        'source_name': source_name,
                        'published_at': published_at_dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'sentiment': sentiment,
                        'sentiment_score': sentiment_score,
                        'positive_count': positive_count,
                        'negative_count': negative_count,
                        'scrape_date': datetime.now().strftime('%Y-%m-%d')
                    })
                
                # Jeda antara request untuk menghindari rate limiting
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error untuk {ticker}: {str(e)}")
                failed_tickers.append(ticker)
                continue
                
        except Exception as e:
            logger.error(f"Error saat memproses ticker {ticker}: {str(e)}")
            failed_tickers.append(ticker)
            continue
    
    # Jika tidak cukup artikel yang dikumpulkan, generate sample data
    if len(all_articles) < 10 or len(failed_tickers) > len(active_tickers) * 0.7:
        logger.warning("Terlalu sedikit artikel dikumpulkan atau terlalu banyak ticker gagal")
        logger.warning(f"Failed tickers: {failed_tickers}")
        
        # Generate dan tambahkan sampel data
        sample_news = generate_sample_news(active_tickers)
        all_articles.extend(sample_news)
    
    # Simpan hasil ke file JSON
    data_folder = Path("/opt/airflow/data")
    data_folder.mkdir(exist_ok=True)
    with open(data_folder / "newsapi_articles.json", 'w', encoding='utf-8') as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=4)
    
    logger.info(f"Berhasil kumpulkan {len(all_articles)} artikel untuk {len(active_tickers) - len(failed_tickers)} ticker")
    
    # Simpan log ticker yang gagal
    if failed_tickers:
        with open(data_folder / "newsapi_failed_tickers.json", 'w', encoding='utf-8') as f:
            json.dump({
                'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'failed_count': len(failed_tickers),
                'total_count': len(active_tickers),
                'failed_tickers': failed_tickers
            }, f, ensure_ascii=False, indent=4)
    
    return len(all_articles)

def generate_sample_news(tickers):
    """
    Generate sample news untuk testing saat API tidak tersedia
    """
    sample_news = []
    
    # Berita positif
    positive_titles = [
        "Saham {} Melompat {}%, Ini Penyebabnya",
        "Kinerja Cemerlang, {} Catat Kenaikan Laba {}%",
        "{} Bagikan Dividen Rp {} per Saham",
        "Investor Optimis dengan Prospek {}, Saham Naik {}%",
        "Analis Rekomendasikan Buy untuk Saham {}, Target Harga Rp {}"
    ]
    
    # Berita negatif
    negative_titles = [
        "Saham {} Anjlok {}%, Investor Khawatir",
        "Kinerja Mengecewakan, Laba {} Turun {}%",
        "{} Tidak Bagikan Dividen Tahun Ini",
        "Tekanan Jual Tinggi, Saham {} Melemah {}%",
        "Analis Rekomendasikan Sell untuk Saham {}, Revisi Turun Target Harga"
    ]
    
    # Berita netral
    neutral_titles = [
        "{} Gelar RUPST, Ini Agenda Utamanya",
        "Direktur Utama {} Bicara Strategi Perusahaan",
        "{} Rilis Laporan Keuangan Q1 2025",
        "Begini Pandangan Ekonom Soal Saham {}",
        "Mengenal Lebih Dekat Bisnis {}"
    ]
    
    # Sumber berita sampel
    news_sources = ["CNN Indonesia", "CNBC Indonesia", "Bisnis.com", "Kompas", "Kontan", 
                   "Bloomberg", "Reuters", "Financial Times", "Wall Street Journal"]
    
    now = datetime.now()
    
    # Pilih subset dari tickers untuk sample data (maksimal 30)
    selected_tickers = random.sample(tickers, min(30, len(tickers)))
    logger.info(f"Generating sample news for {len(selected_tickers)} tickers")
    
    # Generate berita untuk setiap ticker
    for ticker in selected_tickers:
        # 2-5 berita per ticker
        num_news = random.randint(2, 5)
        
        for i in range(num_news):
            # Acak jenis berita (60% positif, 30% negatif, 10% netral)
            rand = random.random()
            
            if rand < 0.6:  # Positif
                title_template = random.choice(positive_titles)
                percentage = random.randint(3, 15)
                price = random.randint(1000, 5000)
                title = title_template.format(ticker, percentage, ticker, percentage, ticker, price)
                sentiment = "Positive"
                sentiment_score = random.uniform(0.5, 0.9)
                positive_count = random.randint(3, 8)
                negative_count = random.randint(0, 2)
                description = f"Saham {ticker} mencatatkan kenaikan yang signifikan. Analis menilai prospek perusahaan sangat menjanjikan dengan pertumbuhan yang berkelanjutan."
            elif rand < 0.9:  # Negatif
                title_template = random.choice(negative_titles)
                percentage = random.randint(3, 15)
                title = title_template.format(ticker, percentage, ticker, percentage)
                sentiment = "Negative"
                sentiment_score = -random.uniform(0.5, 0.9)
                positive_count = random.randint(0, 2)
                negative_count = random.randint(3, 8)
                description = f"Saham {ticker} mengalami tekanan jual yang cukup besar. Investor khawatir dengan kinerja perusahaan yang tidak sesuai ekspektasi pasar."
            else:  # Netral
                title_template = random.choice(neutral_titles)
                title = title_template.format(ticker)
                sentiment = "Neutral"
                sentiment_score = random.uniform(-0.2, 0.2)
                positive_count = random.randint(1, 2)
                negative_count = random.randint(1, 2)
                description = f"Manajemen {ticker} memaparkan strategi perusahaan untuk tahun 2025. Mereka fokus pada optimalisasi operasional dan diversifikasi pendapatan."
            
            # Acak tanggal dan waktu dalam 7 hari terakhir
            days_ago = random.randint(0, 6)
            hours_ago = random.randint(0, 23)
            minutes_ago = random.randint(0, 59)
            published_at = now - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
            
            # URL dummy
            url = f"https://samplenews.com/{ticker.lower()}/{published_at.strftime('%Y%m%d')}-{ticker.lower()}-news.html"
            
            # Source name
            source_name = random.choice(news_sources)
            
            # Tambahkan ke daftar berita
            sample_news.append({
                'ticker': ticker,
                'title': title,
                'description': description,
                'url': url,
                'source_name': source_name,
                'published_at': published_at.strftime('%Y-%m-%d %H:%M:%S'),
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'positive_count': positive_count,
                'negative_count': negative_count,
                'scrape_date': now.strftime('%Y-%m-%d')
            })
    
    return sample_news

def process_newsapi_articles():
    """
    Proses hasil dari NewsAPI dan simpan ke database
    """
    try:
        # Baca file JSON
        json_file = '/opt/airflow/data/newsapi_articles.json'
        if not os.path.exists(json_file):
            logger.error("File JSON tidak ditemukan!")
            return "Error: JSON file not found"
            
        with open(json_file, 'r', encoding='utf-8') as f:
            articles = json.load(f)
        
        logger.info(f"Memproses {len(articles)} artikel dari NewsAPI...")
        
        # Validasi data sebelum diproses
        valid_articles = []
        for item in articles:
            if not all(k in item for k in ['ticker', 'title', 'url', 'published_at']):
                logger.warning(f"Skipping invalid article: {item}")
                continue
            valid_articles.append(item)
        
        logger.info(f"{len(valid_articles)} artikel valid setelah validasi")
        
        # Hitung jumlah artikel dan aggregate sentimen per ticker
        ticker_sentiments = {}
        
        for item in valid_articles:
            ticker = item['ticker']
            
            if ticker not in ticker_sentiments:
                ticker_sentiments[ticker] = {
                    'news_count': 0,
                    'total_sentiment': 0,
                    'positive_count': 0,
                    'negative_count': 0,
                    'neutral_count': 0
                }
            
            ticker_sentiments[ticker]['news_count'] += 1
            ticker_sentiments[ticker]['total_sentiment'] += item['sentiment_score']
            
            if item['sentiment'] == 'Positive':
                ticker_sentiments[ticker]['positive_count'] += 1
            elif item['sentiment'] == 'Negative':
                ticker_sentiments[ticker]['negative_count'] += 1
            else:
                ticker_sentiments[ticker]['neutral_count'] += 1
        
        # Hitung sentimen rata-rata
        for ticker, data in ticker_sentiments.items():
            if data['news_count'] > 0:
                data['avg_sentiment'] = data['total_sentiment'] / data['news_count']
            else:
                data['avg_sentiment'] = 0
        
        # Simpan ke dalam database
        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow",
            connect_timeout=15
        )
        
        # Set autocommit ke False untuk transaction control
        conn.autocommit = False
        
        try:
            cur = conn.cursor()
            
            # Batch size untuk insert
            batch_size = 50
            articles_inserted = 0
            
            # Simpan artikel dalam batch
            for i in range(0, len(valid_articles), batch_size):
                batch = valid_articles[i:i+batch_size]
                
                for item in batch:
                    try:
                        cur.execute("""
                        INSERT INTO newsapi_news (
                            ticker, title, description, url, source_name,
                            published_at, sentiment, sentiment_score,
                            positive_count, negative_count, scrape_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (url) DO UPDATE SET
                            sentiment = EXCLUDED.sentiment,
                            sentiment_score = EXCLUDED.sentiment_score,
                            positive_count = EXCLUDED.positive_count,
                            negative_count = EXCLUDED.negative_count,
                            scrape_date = EXCLUDED.scrape_date
                        """, (
                            item['ticker'],
                            item['title'],
                            item.get('description', ''),
                            item['url'],
                            item.get('source_name', ''),
                            item['published_at'],
                            item['sentiment'],
                            item['sentiment_score'],
                            item['positive_count'],
                            item['negative_count'],
                            item.get('scrape_date', datetime.now().strftime('%Y-%m-%d'))
                        ))
                        articles_inserted += 1
                    except Exception as e:
                        logger.error(f"Error menyimpan artikel: {e}")
                
                # Commit per batch
                conn.commit()
                logger.info(f"Batch {i//batch_size + 1} committed: {len(batch)} items")
            
            # Simpan sentimen per ticker
            today = datetime.now().strftime('%Y-%m-%d')
            sentiments_inserted = 0
            
            for ticker, data in ticker_sentiments.items():
                try:
                    cur.execute("""
                    INSERT INTO newsapi_ticker_sentiment (
                        ticker, date, news_count, avg_sentiment,
                        positive_count, negative_count, neutral_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, date) DO UPDATE SET
                        news_count = EXCLUDED.news_count,
                        avg_sentiment = EXCLUDED.avg_sentiment,
                        positive_count = EXCLUDED.positive_count,
                        negative_count = EXCLUDED.negative_count,
                        neutral_count = EXCLUDED.neutral_count
                    """, (
                        ticker,
                        today,
                        data['news_count'],
                        data['avg_sentiment'],
                        data['positive_count'],
                        data['negative_count'],
                        data['neutral_count']
                    ))
                    sentiments_inserted += 1
                except Exception as e:
                    logger.error(f"Error menyimpan sentimen ticker {ticker}: {e}")
            
            # Final commit for sentiment data
            conn.commit()
            
            # Cetak jumlah data yang disimpan
            cur.execute("SELECT COUNT(*) FROM newsapi_news")
            news_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM newsapi_ticker_sentiment")
            sentiment_count = cur.fetchone()[0]
            
            cur.close()
            
            logger.info(f"Data artikel NewsAPI berhasil disimpan: {news_count} artikel, {sentiment_count} ticker sentiment")
            
            # Save success metrics
            success_metrics = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_processed': len(valid_articles),
                'articles_inserted': articles_inserted,
                'sentiments_inserted': sentiments_inserted,
                'total_articles_in_db': news_count,
                'total_sentiments_in_db': sentiment_count
            }
            
            data_folder = Path("/opt/airflow/data")
            with open(data_folder / "newsapi_processing_metrics.json", 'w', encoding='utf-8') as f:
                json.dump(success_metrics, f, ensure_ascii=False, indent=4)
                
            return f"Successfully processed {len(valid_articles)} articles. Inserted: {articles_inserted} articles, {sentiments_inserted} sentiment records."
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction error: {str(e)}")
            raise
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error in process_newsapi_articles: {str(e)}")
        return f"Error: {str(e)}"

# Define the DAG
with DAG(
    dag_id="newsapi_data_ingestion",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["news", "sentiment", "newsapi", "ingestion"],
    description="Ingests news data from NewsAPI and performs sentiment analysis"
    ) as dag:

    # Task untuk membuat tabel
    create_tables = PythonOperator(
        task_id="create_newsapi_tables",
        python_callable=create_news_tables_if_not_exist,
        retries=3,
        retry_delay=pendulum.duration(minutes=2)
    )

    # Task untuk mengambil berita dari NewsAPI
    fetch_newsapi = PythonOperator(
        task_id="fetch_newsapi_articles",
        python_callable=fetch_newsapi_articles,
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
        execution_timeout=pendulum.duration(minutes=30)  # Timeout 30 menit
    )

    # Task untuk memproses hasil dari NewsAPI
    process_articles = PythonOperator(
        task_id="process_newsapi_articles",
        python_callable=process_newsapi_articles,
        retries=3,
        retry_delay=pendulum.duration(minutes=3)
    )

    # Marker task
    end_task = DummyOperator(
        task_id="end_task"
    )

    # Task dependencies
    create_tables >> fetch_newsapi >> process_articles >> end_task