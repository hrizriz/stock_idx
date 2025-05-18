# 📈 Sistem Trading Otomatis Multi-Timeframe

Sistem analisis teknikal dan trading otomatis yang menggunakan pendekatan multi-timeframe untuk menghasilkan sinyal trading saham dengan probabilitas tinggi. Sistem ini dibangun dengan Apache Airflow untuk orkestrasi, analisis teknikal komprehensif, dan notifikasi real-time melalui Telegram.

## 📊 Sumber Data

### IDX (Indonesia Stock Exchange)
Data diambil dari situs resmi Bursa Efek Indonesia (IDX): [Ringkasan Perdagangan Saham](https://www.idx.co.id/id/data-pasar/ringkasan-perdagangan/ringkasan-saham/)

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

### 2. Indikator Teknikal
- RSI (Relative Strength Index)
- MACD dengan parameter yang dioptimalkan per timeframe
- Bollinger Bands (20 periode, 2 standar deviasi)
- Filter volatilitas dan likuiditas yang disesuaikan
- Indikator advanced custom

### 3. Analisis Sentimen Berita
- Integrasi dengan NewsAPI
- Scraping berita dari Detik Finance
- Analisis sentimen otomatis
- Korelasi sentimen dengan pergerakan harga

## 🛠 Teknologi yang Digunakan

- **Apache Airflow**: Orkestrasi pipeline data
- **Python**: Bahasa pemrograman utama
- **PostgreSQL**: Database utama
- **Docker & Docker Compose**: Containerization
- **DBT**: Transformasi data
- **Telegram API**: Sistem notifikasi
- **NewsAPI**: Sumber berita eksternal
- **Machine Learning**: Analisis sentimen dan prediksi

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

## 📊 Pipeline Data

### 1. Ingest Data
- Import data saham harian
- Scraping berita otomatis
- Validasi dan cleaning data

### 2. Transformasi
- Kalkulasi indikator teknikal
- Analisis sentimen berita
- Agregasi data multi-timeframe

### 3. Analisis
- Generasi sinyal trading
- Backtest otomatis
- Validasi probabilitas

## 📅 Penjadwalan

- **Update Data Saham**: Setiap hari kerja pukul 17:00 WIB
- **Scraping Berita**: Setiap hari @daily
- **Sinyal Trading**:
  - Daily: Setiap hari kerja
  - Weekly: Setiap Jumat
  - Monthly: Setiap awal bulan

## 🔍 Monitoring

1. **Airflow UI**:
   ```
   http://localhost:8080
   ```
   - Username: admin (default)
   - Password: admin (default)

2. **Log Files**:
   - Location: `airflow/logs/`
   - Format: Terstruktur dengan timestamp

3. **Database**:
   - PostgreSQL di port 5432
   - Monitoring melalui pgAdmin atau tools lain

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

Sistem ini menghasilkan sinyal berdasarkan analisis teknikal dan sentimen berita secara otomatis. Sinyal yang dihasilkan BUKAN merupakan rekomendasi investasi dan TIDAK MENJAMIN keuntungan. Selalu lakukan analisis tambahan dan gunakan manajemen risiko yang baik dalam trading.

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

### Log dan Debugging
- Airflow logs: `airflow/logs/`
- Docker logs: `docker-compose logs`
- Application logs di PostgreSQL