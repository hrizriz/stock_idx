# 🔄 Airflow DAGs Documentation

Dokumentasi singkat untuk setiap DAG (Directed Acyclic Graph) dalam sistem trading otomatis.

## 📊 Data Ingestion DAGs

### 1. `stock_data_ingestion.py`
- **Schedule**: Setiap hari kerja
- **Fungsi**: Mengimpor data saham harian dari file Excel
- **Dependencies**: None
- **Output**: Tabel `daily_stock_summary`

### 2. `bulk_load_all_excel.py`
- **Schedule**: Manual trigger
- **Fungsi**: Import batch data historis saham
- **Dependencies**: None
- **Output**: Tabel `daily_stock_summary` (historical)

### 3. `news_data_ingestion.py`
- **Schedule**: @daily
- **Fungsi**: Scraping berita dari Detik Finance
- **Dependencies**: None
- **Output**: Tabel `detik_news`, `detik_ticker_sentiment`

### 4. `newsapi_dag.py`
- **Schedule**: @daily
- **Fungsi**: Mengambil berita dari NewsAPI
- **Dependencies**: NewsAPI key
- **Output**: Tabel `newsapi_articles`, `newsapi_sentiment`

## 📈 Trading Signal DAGs

### 1. `unified_trading_signals.py`
- **Schedule**: Setiap hari kerja 17:00 WIB
- **Timeframes**: Daily, Weekly, Monthly
- **Features**:
  - Indikator teknikal (RSI, MACD, BB)
  - Filter volatilitas & likuiditas
  - Backtest otomatis
  - Notifikasi Telegram
- **Dependencies**: `data_transformation`

### 2. `ml_trading_signals.py`
- **Schedule**: Setiap hari kerja
- **Fungsi**: Sinyal trading berbasis machine learning
- **Models**: XGBoost, LSTM
- **Dependencies**: `unified_trading_signals`

### 3. `rebound_prediction.py`
- **Schedule**: Setiap hari kerja
- **Fungsi**: Prediksi rebound saham
- **Features**: Pattern recognition, Support/Resistance
- **Dependencies**: `data_transformation`

## 🔄 Data Processing DAGs

### 1. `data_transformation.py`
- **Schedule**: Setiap hari kerja
- **Fungsi**: Transformasi dan agregasi data
- **Dependencies**: `stock_data_ingestion`
- **Output**: Berbagai tabel analitik

### 2. `reporting_and_alerting.py`
- **Schedule**: Setiap hari kerja
- **Fungsi**: Laporan performa dan alert
- **Dependencies**: Semua DAG trading signals
- **Output**: Laporan Telegram

## 📁 Utils

Folder `utils/` berisi modul pendukung untuk DAGs:
- `database.py`: Koneksi dan operasi database
- `technical_indicators.py`: Perhitungan indikator
- `trading_signals.py`: Logika sinyal trading
- `telegram.py`: Integrasi notifikasi Telegram

## ⚙️ Konfigurasi Default

Semua DAG menggunakan konfigurasi default:
```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=5)
}
```

## 🔍 Monitoring

- Semua DAG menggunakan logging terstruktur
- Error handling dengan retry mechanism
- Notifikasi Telegram untuk kegagalan kritikal
- Timeout protection untuk tasks yang panjang

## 📝 Catatan Penting

1. **Urutan Eksekusi**:
   - Data ingestion → Transformation → Signal Generation → Reporting

2. **Dependencies**:
   - Pastikan `data_transformation` selesai sebelum signal generation
   - Alert system bergantung pada completion semua DAG trading

3. **Maintenance**:
   - Backup database sebelum bulk operations
   - Monitor disk space untuk log files
   - Periksa Airflow Variables secara berkala

4. **Error Handling**:
   - Semua DAG memiliki retry mechanism
   - Critical errors dikirim via Telegram
   - Log tersimpan di `airflow/logs/`

## 🔧 Troubleshooting

1. **Data Ingestion Issues**:
   - Periksa format file Excel
   - Validasi koneksi database
   - Cek permission folder data

2. **Signal Generation Issues**:
   - Periksa ketersediaan data historis
   - Validasi parameter teknikal
   - Monitor memory usage

3. **Notification Issues**:
   - Validasi Telegram token
   - Periksa koneksi internet
   - Cek Airflow Variables