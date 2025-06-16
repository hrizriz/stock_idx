import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Konfigurasi Database Utama (Stock Index)
DB1_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'stock_idx')  # Database untuk stock index
}

# Konfigurasi Database Kedua (untuk data tambahan)
DB2_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),  # Menggunakan host yang sama
    'port': os.getenv('POSTGRES_PORT', '5432'),      # Menggunakan port yang sama
    'user': os.getenv('POSTGRES_USER', 'postgres'),  # Menggunakan user yang sama
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),  # Menggunakan password yang sama
    'database': os.getenv('POSTGRES_DB', 'stock_idx')  # Menggunakan database yang sama
}

def get_db1_connection():
    """Membuat koneksi ke database stock index"""
    connection_string = f"postgresql://{DB1_CONFIG['user']}:{DB1_CONFIG['password']}@{DB1_CONFIG['host']}:{DB1_CONFIG['port']}/{DB1_CONFIG['database']}"
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    return Session()

def get_db2_connection():
    """Membuat koneksi ke database stock index (untuk data tambahan)"""
    connection_string = f"postgresql://{DB2_CONFIG['user']}:{DB2_CONFIG['password']}@{DB2_CONFIG['host']}:{DB2_CONFIG['port']}/{DB2_CONFIG['database']}"
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    return Session()

# Contoh penggunaan:
# db1_session = get_db1_connection()  # Untuk mengakses database stock_idx
# db2_session = get_db2_connection()  # Untuk mengakses database stock_idx (data tambahan) 