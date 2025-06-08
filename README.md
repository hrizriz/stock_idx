# 📈 Sistem Analisis Trading Multi-Timeframe
Sistem analisis teknikal dan signal generation yang menggunakan pendekatan multi-timeframe untuk menghasilkan sinyal trading saham dengan probabilitas tinggi. Sistem ini dibangun dengan Apache Airflow untuk orkestrasi, analisis teknikal komprehensif, machine learning, dan notifikasi real-time melalui Telegram.

## 📊 Sumber Data
### IDX (Indonesia Stock Exchange)
Data diambil dari situs resmi Bursa Efek Indonesia (IDX): [Ringkasan Perdagangan Saham](https://www.idx.co.id/id/data-pasar/ringkasan-perdagangan/ringkasan-saham/)
- **Cakupan**: 900+ stocks untuk analisis teknikal
- **Processing**: Data manual download, kemudian automated processing dengan Airflow

### News & Sentiment Data
- **Detik Finance**: Automated scraping untuk sentiment analysis (~100 stocks Kompas100)
- **NewsAPI**: Global news integration untuk sentiment analysis
- **Processing**: Fully automated dengan scheduled DAGs

### Periode Data
- **Mulai**: 3 Januari 2020
- **Sampai**: 16 Mei 2025
- **Frekuensi**: Data harian (hari kerja bursa)

### Struktur Data
Data yang diambil memiliki struktur sebagai berikut:
| Kolom | Deskripsi | Tipe Data |
|-------|-----------|-----------|
| Kode Saham | Kode emiten di bursa | VARCHAR |
| Nama Perusahaan | Nama lengkap perusahaan | VARCHAR |
| Remarks | Catatan khusus terkait saham | VARCHAR |
| Sebelumnya | Harga penutupan hari sebelumnya | NUMERIC |
| Open Price | Harga pembukaan | NUMERIC |
| Tanggal Perdagangan Terakhir | Tanggal terakhir saham diperdagangkan | DATE |
| First Trade | Waktu perdagangan pertama | TIMESTAMP |
| Tertinggi | Harga tertinggi hari ini | NUMERIC |
| Terendah | Harga terendah hari ini | NUMERIC |
| Penutupan | Harga penutupan | NUMERIC |
| Selisih | Perubahan harga dari sebelumnya | NUMERIC |
| Volume | Volume perdagangan | BIGINT |
| Nilai | Nilai perdagangan (Rp) | BIGINT |
| Frekuensi | Jumlah transaksi | INTEGER |
| Index Individual | Indeks individual saham | NUMERIC |
| Offer | Harga penawaran jual terbaik | NUMERIC |
| Offer Volume | Volume penawaran jual | BIGINT |
| Bid | Harga penawaran beli terbaik | NUMERIC |
| Bid Volume | Volume penawaran beli | BIGINT |
| Listed Shares | Jumlah saham tercatat | BIGINT |
| Tradeable Shares | Jumlah saham yang dapat diperdagangkan | BIGINT |
| Weight For Index | Bobot dalam perhitungan indeks | NUMERIC |
| Foreign Sell | Volume penjualan asing | BIGINT |
| Foreign Buy | Volume pembelian asing | BIGINT |
| Non Regular Volume | Volume transaksi non-reguler | BIGINT |
| Non Regular Value | Nilai transaksi non-reguler | BIGINT |
| Non Regular Frequency | Frekuensi transaksi non-reguler | INTEGER |

## 🌟 Fitur Utama
### 1. Analisis Multi-Timeframe
- **Harian (Daily)**: 
  - Analisis untuk trading 1-5 hari
  - Probabilitas minimum: 80%
  - Stop loss: 3%
  - Take profit: 5-10%
- **Mingguan (Weekly)**:
  - Analisis untuk trading 1-3 minggu
  - Probabilitas minimum: 75%
  - Stop loss: 5%
  - Take profit: 8-15%
- **Bulanan (Monthly)**:
  - Analisis untuk trading 1-3 bulan
  - Probabilitas minimum: 70%
  - Stop loss: 8%
  - Take profit: 12-20%

### 2. Indikator Teknikal (15+ indicators)
- RSI (Relative Strength Index)
- MACD dengan parameter yang dioptimalkan per timeframe
- Bollinger Bands (20 periode, 2 standar deviasi)
- Filter volatilitas dan likuiditas yang disesuaikan
- Advanced pattern recognition ("Bandarmology" analysis)
- Rebound prediction algorithms

### 3. Machine Learning & Analytics
- **LSTM Neural Networks**: Price prediction dengan TensorFlow/Keras
- **Sentiment Analysis**: Automated keyword-based sentiment scoring
- **Backtesting Engine**: Systematic performance validation
- **Risk Management**: Position sizing dan probability assessment

### 4. Automated Signal Generation
- **High-probability signals**: Multi-timeframe confluence detection
- **Real-time alerts**: Telegram notifications
- **Performance tracking**: Comprehensive backtesting results
- **Market anomaly detection**: Unusual pattern identification

## 🛠 Teknologi yang Digunakan
- **Apache Airflow**: Orkestrasi pipeline data (9 DAGs)
- **Python**: Bahasa pemrograman utama
- **PostgreSQL**: Database utama
- **Docker & Docker Compose**: Containerization
- **DBT**: Transformasi data
- **TensorFlow/Keras**: Machine learning (LSTM)
- **FastAPI**: REST API dengan JWT authentication
- **Telegram API**: Sistem notifikasi
- **NewsAPI**: Sumber berita eksternal
- **Beautiful Soup**: Web scraping

## 📋 Prasyarat
1. **Sistem Operasi**:
   - Windows 10+ atau Linux
   - WSL2 untuk pengguna Windows
2. **Software**:
   - Docker Desktop
   - Python 3.8+
   - PostgreSQL 14+
3. **API Keys**:
   - Telegram Bot Token
   - NewsAPI Key

## 🚀 Instalasi
1. **Clone Repository**:
   ```bash
   git clone [URL_REPOSITORY]
   cd stock_idx
   ```
2. **Setup Environment**:
   ```bash
   cp .env.template .env
   chmod +x setup-env.sh
   ./setup-env.sh
   ```
3. **Saat menjalankan setup-env.sh, Anda akan diminta memasukkan**:
   - Telegram Bot Token
   - Telegram Chat ID
   - NewsAPI Key
   - Kredensial Admin Airflow (opsional)
4. **Jalankan Services**:
   ```bash
   docker-compose up -d
   ```

## ⚙️ Konfigurasi
### Airflow Variables
Beberapa konfigurasi disimpan sebagai Airflow Variables:
- `TELEGRAM_BOT_TOKEN`: Token untuk bot Telegram
- `TELEGRAM_CHAT_ID`: ID chat Telegram untuk notifikasi
- `NEWS_API_KEY`: API key untuk NewsAPI

### File Konfigurasi
- `.env`: File konfigurasi utama (jangan commit ke git)
- `.env.template`: Template untuk konfigurasi
- `setup-env.sh`: Script untuk setup environment

### Keamanan
- Semua file sensitif (`.env`, API keys) sudah dimasukkan dalam `.gitignore`
- Kredensial disimpan menggunakan Airflow's secure storage
- Fernet key di-generate secara otomatis saat setup

### Parameter Trading
Parameter trading dapat dikonfigurasi di `airflow/dags/unified_trading_signals.py`:
```python
timeframe_params = {
    'DAILY': {
        'lookback_periods': {...},
        'hold_period': 5,
        'volatility_params': {...},
        'min_probability': 0.8
    }
}
```

## 📊 Pipeline Data (9 DAGs)
### 1. Data Ingestion
- **stock_data_ingestion**: Import data saham harian dari Excel files
- **bulk_stock_loader**: Bulk loading data historis
- **news_data_ingestion**: Scraping berita Detik Finance 
- **newsapi_data_ingestion**: NewsAPI integration

### 2. Processing & Transformation
- **data_transformation**: DBT transformations & technical indicators

### 3. Analytics & ML
- **unified_trading_signals**: Multi-timeframe technical analysis
- **ml_trading_signals**: LSTM price predictions
- **rebound_prediction**: Pattern recognition & anomaly detection

### 4. Output & Monitoring
- **reporting_and_alerting**: Telegram notifications & reports

## 📅 Penjadwalan
- **Update Data Saham**: Setiap hari kerja pukul 17:00 WIB
- **Scraping Berita**: Setiap hari @daily
- **Sinyal Trading**:
  - Daily: Setiap hari kerja
  - Weekly: Setiap Jumat
  - Monthly: Setiap awal bulan
- **ML Model Training**: Automated retraining dengan performance monitoring

## 🔍 Monitoring
1. **Airflow UI**:
   ```
   http://localhost:8080
   ```
   - Username: admin (default)
   - Password: admin (default)
2. **FastAPI Documentation**:
   ```
   http://localhost:8000/docs
   ```
   - Interactive API documentation
   - JWT authentication required
3. **Log Files**:
   - Location: `airflow/logs/`
   - Format: Terstruktur dengan timestamp
4. **Database**:
   - PostgreSQL di port 5432
   - Monitoring melalui pgAdmin atau tools lain

## 📈 Performance Metrics
- **Technical Analysis**: 900+ stocks processed daily
- **Sentiment Analysis**: ~100 Kompas100 stocks
- **Signal Generation**: Multi-timeframe confluence detection
- **Backtesting**: Systematic validation dengan win rate tracking
- **Processing Speed**: Complex analysis completed in minutes

## 🤝 Kontribusi
1. Fork repository
2. Buat branch fitur (`git checkout -b fitur-baru`)
3. Commit perubahan (`git commit -am 'Menambah fitur baru'`)
4. Push ke branch (`git push origin fitur-baru`)
5. Buat Pull Request

### Panduan Kontribusi
- Ikuti struktur proyek yang ada
- Dokumentasikan kode dengan baik
- Tambahkan unit test untuk fitur baru
- Update README jika diperlukan

## ⚠️ Disclaimer
Sistem ini menghasilkan sinyal berdasarkan analisis teknikal, machine learning, dan sentimen berita. Sinyal yang dihasilkan BUKAN merupakan rekomendasi investasi dan TIDAK MENJAMIN keuntungan. Selalu lakukan analisis tambahan dan gunakan manajemen risiko yang baik dalam trading.

## 🔧 Troubleshooting
### Masalah Umum
1. **Koneksi Database**:
   - Periksa kredensial di `.env`
   - Pastikan PostgreSQL running
   - Cek port 5432 tidak digunakan
2. **API Issues**:
   - Validasi API keys di Airflow Variables
   - Periksa rate limits
   - Monitor error logs
3. **Docker Issues**:
   - Restart Docker daemon
   - Clear Docker cache
   - Periksa resource limits
4. **Data Processing Issues**:
   - Periksa format Excel files untuk IDX data
   - Validasi timezone settings (Asia/Jakarta)
   - Monitor DAG execution di Airflow UI

### Log dan Debugging
- Airflow logs: `airflow/logs/`
- Docker logs: `docker-compose logs`
- Application logs di PostgreSQL
- Telegram notification logs untuk monitoring alerts
