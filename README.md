# 📈 Sistem Trading Otomatis Multi-Timeframe

Sistem analisis teknikal dan trading otomatis yang menggunakan pendekatan multi-timeframe untuk menghasilkan sinyal trading saham dengan probabilitas tinggi. Sistem ini dibangun dengan Apache Airflow untuk orkestrasi, analisis teknikal komprehensif, dan notifikasi real-time melalui Telegram.

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
   ./setup-env.sh
   ```

3. **Konfigurasi Environment Variables**:
   ```bash
   cp .env.example .env
   # Edit file .env dengan konfigurasi yang sesuai
   ```

4. **Inisialisasi Airflow**:
   ```bash
   ./init-airflow.sh
   ```

5. **Jalankan Services**:
   ```bash
   docker-compose up -d
   ```

## ⚙️ Konfigurasi

### Konfigurasi Telegram
1. Jalankan script setup environment:
   ```bash
   chmod +x setup-env.sh
   ./setup-env.sh
   ```
2. Masukkan Telegram Bot Token dan Chat ID saat diminta
3. Script akan otomatis menyimpan konfigurasi sebagai Airflow Variables

> **Catatan**: Bot Token dan Chat ID akan disimpan secara aman sebagai Airflow Variables dan dapat diakses melalui Airflow UI jika perlu diubah kemudian.

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
   - Username: admin
   - Password: admin (default)

2. **Log Files**:
   - Location: `airflow/logs/`
   - Format: Terstruktur dengan timestamp

## 🤝 Kontribusi

1. Fork repository
2. Buat branch fitur (`git checkout -b fitur-baru`)
3. Commit perubahan (`git commit -am 'Menambah fitur baru'`)
4. Push ke branch (`git push origin fitur-baru`)
5. Buat Pull Request

## ⚠️ Disclaimer

Sistem ini menghasilkan sinyal berdasarkan analisis teknikal dan sentimen berita secara otomatis. Sinyal yang dihasilkan BUKAN merupakan rekomendasi investasi dan TIDAK MENJAMIN keuntungan. Selalu lakukan analisis tambahan dan gunakan manajemen risiko yang baik dalam trading.
