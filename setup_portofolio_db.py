#!/usr/bin/env python3
"""
Portofolio Database Setup Script
Run this to create the portofolio schema and tables
"""

import psycopg2
import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'database': os.getenv('DB_NAME', 'airflow'),
    'user': os.getenv('DB_USER', 'airflow'),
    'password': os.getenv('DB_PASSWORD', 'airflow'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

def setup_portofolio_database():
    """Setup portofolio database schema and tables"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        with conn.cursor() as cursor:
            print("Creating portofolio schema...")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS portofolio")
            
            print("Creating transaksi table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS portofolio.transaksi (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(50) DEFAULT 'default_user',
                    tanggal_transaksi DATE NOT NULL,
                    tanggal_settlement DATE,
                    simbol VARCHAR(10) NOT NULL,
                    nama_perusahaan VARCHAR(255),
                    jenis_transaksi VARCHAR(10) NOT NULL CHECK (jenis_transaksi IN ('BELI', 'JUAL')),
                    jumlah_saham INTEGER NOT NULL,
                    harga_per_saham DECIMAL(15,2) NOT NULL,
                    total_nilai DECIMAL(20,2) NOT NULL,
                    biaya DECIMAL(10,2) DEFAULT 0,
                    nomor_referensi VARCHAR(50),
                    sumber_file VARCHAR(255),
                    dibuat_pada TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    diupdate_pada TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            print("Creating kepemilikan table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS portofolio.kepemilikan (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(50) DEFAULT 'default_user',
                    simbol VARCHAR(10) NOT NULL,
                    nama_perusahaan VARCHAR(255),
                    total_saham INTEGER NOT NULL DEFAULT 0,
                    harga_rata_rata DECIMAL(15,2) NOT NULL DEFAULT 0,
                    total_modal DECIMAL(20,2) NOT NULL DEFAULT 0,
                    harga_sekarang DECIMAL(15,2),
                    nilai_sekarang DECIMAL(20,2),
                    keuntungan_rugi DECIMAL(20,2),
                    persentase_keuntungan_rugi DECIMAL(8,4),
                    terakhir_diupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, simbol)
                )
            """)
            
            print("Creating indexes...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transaksi_user_simbol ON portofolio.transaksi(user_id, simbol)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transaksi_tanggal ON portofolio.transaksi(tanggal_transaksi)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_kepemilikan_user ON portofolio.kepemilikan(user_id)")
            
        conn.commit()
        conn.close()
        
        print("✅ Portofolio database setup completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error setting up portofolio database: {e}")
        return False

if __name__ == "__main__":
    setup_portofolio_database()
