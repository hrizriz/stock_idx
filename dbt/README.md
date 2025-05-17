
## 🎯 Model Data

### Staging Models
- Transformasi awal data mentah
- Materialized sebagai views untuk efisiensi
- Schema: `staging`

### Marts Models
1. **Core**
   - Model bisnis inti
   - Materialized sebagai tables
   - Schema: `core`

2. **Analytics**
   - Model untuk analisis
   - Materialized sebagai tables
   - Schema: `analytics`

## 📈 Transformasi Data Utama

### 1. Daily Stock Performance
- Transformasi data harian saham
- Perhitungan metrik performa
- Indikator teknikal dasar

### 2. Weekly Aggregations
- Agregasi data mingguan
- Perhitungan return mingguan
- Volume trading mingguan

### 3. Monthly Analytics
- Analisis performa bulanan
- Tren jangka panjang
- Metrik volatilitas

### 4. Technical Indicators
- RSI (Relative Strength Index)
- MACD (Moving Average Convergence Divergence)
- Bollinger Bands
- Custom indicators

## 🛠 Penggunaan

### Setup Awal
1. Pastikan PostgreSQL sudah berjalan
2. Konfigurasi koneksi di `profiles.yml`
3. Install dependencies:
   ```bash
   dbt deps
   ```

### Menjalankan Models
1. Jalankan semua models:
   ```bash
   dbt run
   ```

2. Jalankan model spesifik:
   ```bash
   dbt run --models staging.daily_stock_summary
   ```

3. Jalankan tests:
   ```bash
   dbt test
   ```

### Generate Dokumentasi
```bash
dbt docs generate
dbt docs serve
```

## 📊 Testing dan Validasi

### Tests yang Diimplementasi
- Unique constraints
- Not null checks
- Relationship checks
- Custom data quality tests

## 🔄 Lineage dan Dependencies

### Data Sources
- Daily stock summary
- Company information
- News sentiment data
- Technical indicators

### Output Tables
- Staged daily stock data
- Core business metrics
- Advanced analytics models
- Trading signals data

## ⚙️ Konfigurasi

### Database
- Schema staging: Transformasi awal
- Schema core: Model bisnis inti
- Schema analytics: Model analitik

### Materialization Strategy
- Views untuk staging models
- Tables untuk marts models
- Incremental models untuk data historis

## 📝 Konvensi Penamaan

### Models
- staging_*: Model staging
- core_*: Model bisnis inti
- fct_*: Fact tables
- dim_*: Dimension tables

### Tests
- schema.yml: Tests generic
- custom_tests/: Tests spesifik

## 🔍 Monitoring dan Maintenance

### Logging
- Logs tersimpan di folder `logs/`
- Target folder untuk artifacts: `target/`

### Maintenance
- Regular schema updates
- Performance optimization
- Data quality monitoring

## 🤝 Kontribusi

1. Ikuti struktur folder yang ada
2. Dokumentasikan models di `schema.yml`
3. Tambahkan tests yang sesuai
4. Update README jika diperlukan

## ⚠️ Catatan Penting

- Jalankan `dbt clean` sebelum deployment
- Periksa dependencies di `packages.yml`
- Monitor performance materializations
- Backup sebelum perubahan besar

## 📚 Referensi

- [DBT Docs](https://docs.getdbt.com/)
- [Best Practices](https://docs.getdbt.com/guides/best-practices)