import pandas as pd
import psycopg2
from datetime import datetime
from pathlib import Path

root_dir = Path(__file__).resolve().parent.parent
excel_file_path = root_dir / "data" / "Ringkasan Saham-20250505.xlsx"
upload_file_name = excel_file_path.name
tanggal_perdagangan = datetime.strptime("2025-05-05", "%Y-%m-%d").date()

# Load dan bersihkan data
df = pd.read_excel(excel_file_path)
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# Rename kolom penting agar sesuai dengan tabel
column_map = {
    "kode_saham": "symbol",
    "nama_perusahaan": "name",
    "sebelumnya": "prev_close",
    "open_price": "open_price",
    "tertinggi": "high",
    "terendah": "low",
    "penutupan": "close",
    "selisih": "change",
    "volume": "volume",
    "nilai": "value",
    "frekuensi": "frequency",
    "index_individual": "index_individual",
    "weight_for_index": "weight_for_index",
    "foreign_buy": "foreign_buy",
    "foreign_sell": "foreign_sell",
    "offer": "offer",
    "offer_volume": "offer_volume",
    "bid": "bid",
    "bid_volume": "bid_volume",
    "listed_shares": "listed_shares",
    "tradeble_shares": "tradable_shares",
    "non_regular_volume": "non_regular_volume",
    "non_regular_value": "non_regular_value",
    "non_regular_frequency": "non_regular_frequency"
}

df = df.rename(columns=column_map)
df = df[list(column_map.values())]
df["date"] = tanggal_perdagangan
df["upload_file"] = upload_file_name

# Simpan ke PostgreSQL
conn = psycopg2.connect(
    host="localhost",  # Ganti ke "postgres" kalau dijalankan dari container
    dbname="airflow",
    user="airflow",
    password="airflow"
)
cur = conn.cursor()

for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO daily_stock_summary (
            symbol, name, date,
            prev_close, open_price, high, low, close, change,
            volume, value, frequency,
            index_individual, weight_for_index,
            foreign_buy, foreign_sell,
            offer, offer_volume, bid, bid_volume,
            listed_shares, tradable_shares,
            non_regular_volume, non_regular_value, non_regular_frequency,
            upload_file
        ) VALUES (
            %(symbol)s, %(name)s, %(date)s,
            %(prev_close)s, %(open_price)s, %(high)s, %(low)s, %(close)s, %(change)s,
            %(volume)s, %(value)s, %(frequency)s,
            %(index_individual)s, %(weight_for_index)s,
            %(foreign_buy)s, %(foreign_sell)s,
            %(offer)s, %(offer_volume)s, %(bid)s, %(bid_volume)s,
            %(listed_shares)s, %(tradable_shares)s,
            %(non_regular_volume)s, %(non_regular_value)s, %(non_regular_frequency)s,
            %(upload_file)s
        )
        ON CONFLICT (symbol, date) DO NOTHING;
    """, row.to_dict())

conn.commit()
cur.close()
conn.close()

print("✅ Data berhasil disimpan ke daily_stock_summary")
