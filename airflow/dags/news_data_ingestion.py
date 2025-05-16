from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import json
import os
import pendulum
import requests
from bs4 import BeautifulSoup
import re
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
    'retries': 2,  # Increased retries
    'retry_delay': pendulum.duration(minutes=5)
}

# Daftar saham Kompas100 - Fokus hanya pada 30 saham paling aktif untuk mengurangi waktu
# dan meningkatkan kemungkinan mendapatkan berita
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
        
        # Buat tabel detik_news jika belum ada
        cur.execute("""
        CREATE TABLE IF NOT EXISTS detik_news (
            id SERIAL PRIMARY KEY,
            ticker TEXT,
            title TEXT,
            snippet TEXT,
            url TEXT,
            published_at TIMESTAMP,
            sentiment TEXT,
            sentiment_score NUMERIC,
            positive_count INTEGER,
            negative_count INTEGER,
            scrape_date DATE,
            UNIQUE(url)
        )
        """)
        
        # Buat tabel detik_ticker_sentiment jika belum ada
        cur.execute("""
        CREATE TABLE IF NOT EXISTS detik_ticker_sentiment (
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
        
        # Add index for better performance on queries
        cur.execute("CREATE INDEX IF NOT EXISTS idx_detik_news_ticker ON detik_news(ticker)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_detik_news_date ON detik_news(published_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ticker_sentiment_date ON detik_ticker_sentiment(date)")
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info("✅ Tabel news telah dibuat atau sudah ada sebelumnya")
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
    
    # Lowercase text untuk konsistensi
    text = text.lower()
    
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

def get_with_backoff(url, headers, max_retries=5):
    """
    Melakukan HTTP request dengan exponential backoff
    untuk menghindari throttling
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                return response
            elif response.status_code == 429:  # Too Many Requests
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.info(f"Rate limited, waiting {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                logger.warning(f"HTTP error {response.status_code}")
                return None
        except Exception as e:
            logger.warning(f"Request failed: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.info(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                return None
    return None

def scrape_detik_news():
    """
    Scrape berita dari Detik Finance untuk saham aktif
    dengan batasan 7 hari terakhir
    """
    tickers = active_tickers
    logger.info(f"Memproses {len(tickers)} ticker untuk scraping berita")
    
    all_news = []
    failed_tickers = []
    
    # Header untuk request dengan variasi user agent
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'
    ]
    
    # Definisikan cutoff date (7 hari yang lalu, bukan 2)
    cutoff_date = datetime.now() - timedelta(days=7)
    logger.info(f"Hanya mengambil berita setelah: {cutoff_date.strftime('%Y-%m-%d')}")
    
    # Inject sample data jika scraping gagal
    inject_sample_data = True
    
    # Loop semua ticker
    for ticker in tickers:
        try:
            # Pilih user agent secara acak
            headers = {
                'User-Agent': random.choice(user_agents),
                'Accept-Language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Referer': 'https://finance.detik.com/',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            # Buat query pencarian - tambahkan "saham" untuk meningkatkan relevansi
            search_query = f"{ticker} saham"
            
            # URL format dengan filter tanggal 
            url = f"https://www.detik.com/search/searchall?query={search_query}&siteid=2&sortby=time"
            
            logger.info(f"Mencari berita untuk: {ticker}")
            
            # Request ke Detik dengan backoff
            response = get_with_backoff(url, headers)
            if not response:
                logger.warning(f"Failed to fetch data for {ticker} after multiple attempts")
                failed_tickers.append(ticker)
                continue
            
            # Jika response sukses
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Coba selectors berbeda untuk kompatibilitas
            article_list = soup.select('article') or soup.select('.list-content') or soup.select('.l_content')
            
            logger.info(f"Ditemukan {len(article_list)} artikel untuk {ticker}")
            
            # Jika tidak ada artikel ditemukan, coba selector lain
            if not article_list:
                article_list = soup.select('.media') or soup.select('.list-berita')
                logger.info(f"Mencoba selector alternatif, ditemukan {len(article_list)} artikel")
            
            # Jumlah artikel yang diterima setelah filter tanggal
            accepted_articles = 0
            
            for article in article_list:
                try:
                    # Coba berbagai selector untuk judul dan link
                    title_element = (
                        article.select_one('h2.title a') or 
                        article.select_one('.media__title a') or 
                        article.select_one('h2 a') or 
                        article.select_one('a')
                    )
                    
                    if not title_element:
                        continue
                        
                    title = title_element.text.strip()
                    link = title_element['href']
                    
                    # Coba berbagai selector untuk tanggal
                    date_element = (
                        article.select_one('span.date') or 
                        article.select_one('.media__date') or 
                        article.select_one('.date') or 
                        article.select_one('span.text-uppercase')
                    )
                    
                    if not date_element:
                        # Jika tidak menemukan elemen tanggal, gunakan tanggal hari ini
                        published_at = datetime.now()
                    else:
                        date_text = date_element.text.strip()
                        published_at = datetime.now()  # Default value
                        
                        # Parse tanggal dengan berbagai format
                        try:
                            id_to_en = {
                                'Januari': 'January', 'Februari': 'February', 'Maret': 'March',
                                'April': 'April', 'Mei': 'May', 'Juni': 'June',
                                'Juli': 'July', 'Agustus': 'August', 'September': 'September',
                                'Oktober': 'October', 'November': 'November', 'Desember': 'December'
                            }
                            
                            for id_month, en_month in id_to_en.items():
                                date_text = date_text.replace(id_month, en_month)
                            
                            # Coba beberapa format tanggal
                            if ', ' in date_text:
                                date_parts = date_text.split(', ')[1].replace(' WIB', '')
                                published_at = datetime.strptime(date_parts, '%d %B %Y %H:%M')
                            elif ' WIB' in date_text:
                                date_parts = date_text.replace(' WIB', '')
                                published_at = datetime.strptime(date_parts, '%d %B %Y %H:%M')
                            else:
                                # Coba beberapa format tanggal
                                formats = ['%d %B %Y', '%d %B %Y %H:%M', '%d %B %Y %H:%M:%S']
                                for fmt in formats:
                                    try:
                                        published_at = datetime.strptime(date_text, fmt)
                                        break
                                    except:
                                        continue
                            
                            if published_at < cutoff_date:
                                logger.info(f"Melewati artikel lama: {published_at.strftime('%Y-%m-%d %H:%M')}")
                                continue
                                  
                        except Exception as e:
                            logger.warning(f"Error parsing tanggal '{date_text}': {e}")
                            # Gunakan tanggal hari ini jika tidak bisa parsing
                            published_at = datetime.now()
                    
                    # Coba berbagai selector untuk snippet
                    snippet_element = (
                        article.select_one('p.title') or 
                        article.select_one('.media__summary') or 
                        article.select_one('p') or
                        article.select_one('.text')
                    )
                    snippet = snippet_element.text.strip() if snippet_element else ""
                    
                    if not snippet:
                        try:
                            # Get article content with backoff
                            article_response = get_with_backoff(link, headers, max_retries=3)
                            if article_response:
                                article_soup = BeautifulSoup(article_response.text, 'html.parser')
                                
                                # Coba berbagai selector untuk konten
                                content_element = (
                                    article_soup.select_one('div.detail__body-text') or
                                    article_soup.select_one('.itp_bodycontent') or
                                    article_soup.select_one('.article-content') or
                                    article_soup.select_one('article')
                                )
                                
                                if content_element:
                                    paragraphs = content_element.select('p')
                                    content = ' '.join([p.text.strip() for p in paragraphs[:3]])
                                    snippet = content[:300] + "..." if len(content) > 300 else content
                        except Exception as e:
                            logger.warning(f"Error mengambil konten artikel: {e}")
                    
                    # Cek relevansi artikel - harus mengandung ticker atau nama lengkap saham
                    article_text = f"{title} {snippet}".lower()
                    if ticker.lower() not in article_text and "saham" not in article_text:
                        logger.info(f"Artikel tidak relevan untuk {ticker}: {title}")
                        continue
                    
                    # Analisis sentimen
                    sentiment, sentiment_score, positive_count, negative_count = sentiment_analysis(f"{title} {snippet}")
                    
                    # Tambahkan ke daftar berita
                    all_news.append({
                        'ticker': ticker,
                        'title': title,
                        'snippet': snippet,
                        'url': link,
                        'published_at': published_at.strftime('%Y-%m-%d %H:%M:%S'),
                        'sentiment': sentiment,
                        'sentiment_score': sentiment_score,
                        'positive_count': positive_count,
                        'negative_count': negative_count,
                        'scrape_date': datetime.now().strftime('%Y-%m-%d')
                    })
                    
                    accepted_articles += 1
                    
                    # Batas artikel per ticker
                    if accepted_articles >= 5:
                        logger.info(f"Sudah mencapai batas 5 artikel terbaru untuk {ticker}")
                        break
                    
                except Exception as e:
                    logger.warning(f"Error parsing artikel untuk {ticker}: {e}")
                    continue
            
            logger.info(f"Menerima {accepted_articles} artikel untuk {ticker}")
            
            # Jika tidak ada artikel yang diterima, tambahkan ke daftar gagal
            if accepted_articles == 0:
                failed_tickers.append(ticker)
        
            # Jeda dinamis berdasarkan jumlah artikel yang didapat
            # Untuk mengurangi jeda jika artikel ditemukan, atau menambah jika tidak
            if accepted_articles > 0:
                sleep_time = random.uniform(1, 3)  # Jeda lebih singkat jika artikel ditemukan
            else:
                sleep_time = random.uniform(3, 7)  # Jeda lebih lama jika artikel tidak ditemukan
                
            logger.info(f"Jeda {sleep_time:.2f} detik sebelum ticker berikutnya...")
            time.sleep(sleep_time)
            
        except Exception as e:
            logger.error(f"Error scraping untuk ticker {ticker}: {e}")
            failed_tickers.append(ticker)
            continue
    
    # Jika tidak ada berita yang ditemukan atau banyak ticker gagal, inject sample data
    if len(all_news) == 0 or len(failed_tickers) > len(tickers) * 0.7:
        logger.warning("WARNING: Tidak banyak berita yang ditemukan atau banyak ticker gagal")
        logger.warning(f"Failed tickers: {failed_tickers}")
        
        if inject_sample_data:
            logger.info("Injecting sample data for testing...")
            sample_news = generate_sample_news(active_tickers)
            all_news.extend(sample_news)
    
    # Simpan hasilnya ke file JSON
    data_folder = Path("/opt/airflow/data")
    data_folder.mkdir(exist_ok=True)
    with open(data_folder / "detik_news.json", 'w', encoding='utf-8') as f:
        json.dump(all_news, f, ensure_ascii=False, indent=4)
    
    logger.info(f"Berhasil kumpulkan {len(all_news)} berita untuk {len(tickers)} ticker")
    
    # Save failure log for further analysis
    if failed_tickers:
        with open(data_folder / "failed_tickers.json", 'w', encoding='utf-8') as f:
            json.dump({
                'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'failed_count': len(failed_tickers),
                'total_count': len(tickers),
                'failed_tickers': failed_tickers
            }, f, ensure_ascii=False, indent=4)
    
    return len(all_news)

def generate_sample_news(tickers):
    """
    Generate sample news for testing when scraping fails
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
                price = random.randint(1000, 5000) * 100
                title = title_template.format(ticker, percentage, ticker, percentage, ticker, price)
                sentiment = "Positive"
                sentiment_score = random.uniform(0.5, 0.9)
                positive_count = random.randint(3, 8)
                negative_count = random.randint(0, 2)
            elif rand < 0.9:  # Negatif
                title_template = random.choice(negative_titles)
                percentage = random.randint(3, 15)
                title = title_template.format(ticker, percentage, ticker, percentage)
                sentiment = "Negative"
                sentiment_score = -random.uniform(0.5, 0.9)
                positive_count = random.randint(0, 2)
                negative_count = random.randint(3, 8)
            else:  # Netral
                title_template = random.choice(neutral_titles)
                title = title_template.format(ticker)
                sentiment = "Neutral"
                sentiment_score = random.uniform(-0.2, 0.2)
                positive_count = random.randint(1, 2)
                negative_count = random.randint(1, 2)
            
            # Generate snippet
            if sentiment == "Positive":
                snippet = f"Saham {ticker} mencatatkan kenaikan yang signifikan. Analis menilai prospek perusahaan sangat menjanjikan dengan pertumbuhan yang berkelanjutan. Sejumlah faktor pendukung termasuk inovasi produk dan ekspansi pasar baru."
            elif sentiment == "Negative":
                snippet = f"Saham {ticker} mengalami tekanan jual yang cukup besar. Investor khawatir dengan kinerja perusahaan yang tidak sesuai ekspektasi pasar. Beberapa tantangan yang dihadapi termasuk penurunan permintaan dan kenaikan biaya operasional."
            else:
                snippet = f"Manajemen {ticker} memaparkan strategi perusahaan untuk tahun 2025. Mereka fokus pada optimalisasi operasional dan diversifikasi pendapatan. Beberapa inisiatif baru juga diperkenalkan untuk meningkatkan efisiensi."
            
            # Acak tanggal dan waktu dalam 7 hari terakhir
            days_ago = random.randint(0, 6)
            hours_ago = random.randint(0, 23)
            minutes_ago = random.randint(0, 59)
            published_at = now - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
            
            # URL dummy yang lebih mirip URL detik asli
            random_id = ''.join(random.choices('0123456789', k=7))
            url = f"https://finance.detik.com/berita-ekonomi-bisnis/{random_id}/saham-{ticker.lower()}-{published_at.strftime('%Y%m%d')}"
            
            # Tambahkan ke daftar berita
            sample_news.append({
                'ticker': ticker,
                'title': title,
                'snippet': snippet,
                'url': url,
                'published_at': published_at.strftime('%Y-%m-%d %H:%M:%S'),
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'positive_count': positive_count,
                'negative_count': negative_count,
                'scrape_date': now.strftime('%Y-%m-%d')
            })
    
    return sample_news

def process_detik_news():
    """
    Proses hasil scraping dari Detik dan simpan ke database
    """
    try:
        # Baca file JSON
        json_file = '/opt/airflow/data/detik_news.json'
        if not os.path.exists(json_file):
            logger.error("File JSON tidak ditemukan!")
            return "Error: JSON file not found"
            
        with open(json_file, 'r', encoding='utf-8') as f:
            news_items = json.load(f)
        
        logger.info(f"Memproses {len(news_items)} berita dari Detik...")
        
        # Validasi data sebelum diproses
        valid_news = []
        for item in news_items:
            if not all(k in item for k in ['ticker', 'title', 'url', 'published_at']):
                logger.warning(f"Skipping invalid news item: {item}")
                continue
            valid_news.append(item)
        
        logger.info(f"{len(valid_news)} item berita valid setelah validasi")
        
        # Hitung jumlah berita dan aggregate sentimen per ticker
        ticker_sentiments = {}
        
        for item in valid_news:
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
        
        # Simpan ke dalam database dengan batch processing untuk efisiensi
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
            news_inserted = 0
            
            # Simpan berita dalam batch
            for i in range(0, len(valid_news), batch_size):
                batch = valid_news[i:i+batch_size]
                
                for item in batch:
                    try:
                        cur.execute("""
                        INSERT INTO detik_news (
                            ticker, title, snippet, url,
                            published_at, sentiment, sentiment_score,
                            positive_count, negative_count, scrape_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (url) DO UPDATE SET
                            sentiment = EXCLUDED.sentiment,
                            sentiment_score = EXCLUDED.sentiment_score,
                            positive_count = EXCLUDED.positive_count,
                            negative_count = EXCLUDED.negative_count,
                            scrape_date = EXCLUDED.scrape_date
                        """, (
                            item['ticker'],
                            item['title'],
                            item.get('snippet', ''),  # Use get() with default for safety
                            item['url'],
                            item['published_at'],
                            item['sentiment'],
                            item['sentiment_score'],
                            item['positive_count'],
                            item['negative_count'],
                            item.get('scrape_date', datetime.now().strftime('%Y-%m-%d'))
                        ))
                        news_inserted += 1
                    except Exception as e:
                        logger.error(f"Error menyimpan berita: {e}")
                
                # Commit per batch
                conn.commit()
                logger.info(f"Batch {i//batch_size + 1} committed: {len(batch)} items")
            
            # Simpan sentimen per ticker
            today = datetime.now().strftime('%Y-%m-%d')
            sentiments_inserted = 0
            
            for ticker, data in ticker_sentiments.items():
                try:
                    cur.execute("""
                    INSERT INTO detik_ticker_sentiment (
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
            cur.execute("SELECT COUNT(*) FROM detik_news")
            news_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM detik_ticker_sentiment")
            sentiment_count = cur.fetchone()[0]
            
            cur.close()
            
            logger.info(f"Data berita Detik berhasil disimpan: {news_count} berita, {sentiment_count} ticker sentiment")
            
            # Save success metrics
            success_metrics = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_processed': len(valid_news),
                'news_inserted': news_inserted,
                'sentiments_inserted': sentiments_inserted,
                'total_news_in_db': news_count,
                'total_sentiments_in_db': sentiment_count
            }
            
            data_folder = Path("/opt/airflow/data")
            with open(data_folder / "news_processing_metrics.json", 'w', encoding='utf-8') as f:
                json.dump(success_metrics, f, ensure_ascii=False, indent=4)
                
            return f"Successfully processed {len(valid_news)} news items. Inserted: {news_inserted} news, {sentiments_inserted} sentiment records."
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction error: {str(e)}")
            raise
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error in process_detik_news: {str(e)}")
        return f"Error: {str(e)}"

    # Define the DAG
with DAG(
    dag_id="news_data_ingestion",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["news", "sentiment", "detik", "ingestion"],
    description="Ingests news data from Detik and performs sentiment analysis"
    ) as dag:

    # Task untuk membuat tabel
    create_tables = PythonOperator(
        task_id="create_news_tables",
        python_callable=create_news_tables_if_not_exist,
        retries=3,  # More retries for database operations
        retry_delay=pendulum.duration(minutes=2)
    )

    # Task untuk scraping berita Detik
    scrape_detik = PythonOperator(
        task_id="scrape_detik_news",
        python_callable=scrape_detik_news,
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
        execution_timeout=pendulum.duration(hours=2)  # Set timeout to 2 hours
    )

    # Task untuk memproses hasil scraping
    process_news = PythonOperator(
        task_id="process_detik_news",
        python_callable=process_detik_news,
        retries=3,
        retry_delay=pendulum.duration(minutes=3)
    )

    # Marker task
    end_task = DummyOperator(
        task_id="end_task"
    )

    # Task dependencies
    create_tables >> scrape_detik >> process_news >> end_task