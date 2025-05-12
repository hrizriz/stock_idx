from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import psycopg2
import pendulum

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

def calculate_rsi_indicators():
    """
    Menghitung indikator RSI (Relative Strength Index) untuk semua saham
    """
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    
    # Ambil data harga saham untuk periode yang dibutuhkan
    query = """
    SELECT 
        symbol, 
        date, 
        close
    FROM public_analytics.daily_stock_metrics
    WHERE date >= CURRENT_DATE - INTERVAL '100 days'
    ORDER BY symbol, date
    """
    
    df = pd.read_sql(query, conn)
    
    # Jika tidak ada data sama sekali
    if df.empty:
        print("Tidak ada data harga saham untuk perhitungan RSI")
        return "Tidak ada data"
    
    # Fungsi untuk menghitung RSI
    def calculate_rsi(data, period=14):
        # Hitung perubahan harga
        delta = data.diff()
        
        # Pisahkan kenaikan dan penurunan
        gain = delta.clip(lower=0)
        loss = -1 * delta.clip(upper=0)
        
        # Hitung rata-rata kenaikan dan penurunan
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        # Hitung RS
        rs = avg_gain / avg_loss
        
        # Hitung RSI
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    # Buat DataFrame untuk menyimpan hasil
    results = []
    
    # Hitung RSI untuk setiap saham
    for symbol, group in df.groupby('symbol'):
        if len(group) < 15:  # Minimal 15 hari data untuk perhitungan yang baik
            continue
            
        # Sort berdasarkan tanggal
        group = group.sort_values('date')
        
        # Hitung RSI
        group['rsi'] = calculate_rsi(group['close'])
        
        # Filter hanya data terbaru saja
        latest_data = group.dropna().tail(10)  # Ambil 10 hari terbaru dengan RSI yang valid
        
        # Tentukan sinyal berdasarkan nilai RSI
        for _, row in latest_data.iterrows():
            rsi_signal = "Neutral"
            if row['rsi'] <= 30:
                rsi_signal = "Oversold"
            elif row['rsi'] >= 70:
                rsi_signal = "Overbought"
                
            results.append({
                'symbol': symbol,
                'date': row['date'],
                'rsi': row['rsi'],
                'rsi_signal': rsi_signal
            })
    
    # Konversi hasil ke DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        print("Tidak ada hasil RSI yang valid")
        return "Tidak ada hasil RSI"
    
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public_analytics.technical_indicators_rsi (
        symbol TEXT,
        date DATE,
        rsi NUMERIC,
        rsi_signal TEXT,
        PRIMARY KEY (symbol, date)
    )
    """)
    
    # Alternatif: Jika tabel sudah ada, Anda bisa menambahkan konstrain
    try:
        cursor.execute("""
        ALTER TABLE public_analytics.technical_indicators_rsi 
        ADD CONSTRAINT technical_indicators_rsi_pkey PRIMARY KEY (symbol, date)
        """)
    except Exception as e:
        # Jika PRIMARY KEY sudah ada, ini akan error, jadi kita ignore
        print(f"Info: {str(e)}")
    
    # Hapus data yang sudah ada untuk tanggal yang sama dengan yang akan dimasukkan
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM public_analytics.technical_indicators_rsi WHERE date = '{date}'")
    
    # Masukkan data baru - GUNAKAN INSERT TANPA ON CONFLICT
    for _, row in result_df.iterrows():
        cursor.execute("""
        INSERT INTO public_analytics.technical_indicators_rsi (symbol, date, rsi, rsi_signal)
        VALUES (%s, %s, %s, %s)
        """, (row['symbol'], row['date'], row['rsi'], row['rsi_signal']))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Berhasil menghitung RSI untuk {result_df['symbol'].nunique()} saham pada {len(dates_to_update)} tanggal"

def calculate_macd_indicators():
    """
    Menghitung indikator MACD (Moving Average Convergence Divergence) untuk semua saham
    """
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    
    # Ambil data harga saham untuk periode yang dibutuhkan
    query = """
    SELECT 
        symbol, 
        date, 
        close
    FROM public_analytics.daily_stock_metrics
    WHERE date >= CURRENT_DATE - INTERVAL '150 days'
    ORDER BY symbol, date
    """
    
    df = pd.read_sql(query, conn)
    
    # Jika tidak ada data sama sekali
    if df.empty:
        print("Tidak ada data harga saham untuk perhitungan MACD")
        return "Tidak ada data"
    
    # Fungsi untuk menghitung MACD
    def calculate_macd(data, fast_period=12, slow_period=26, signal_period=9):
        # Hitung EMA
        ema_fast = data.ewm(span=fast_period, adjust=False).mean()
        ema_slow = data.ewm(span=slow_period, adjust=False).mean()
        
        # Hitung MACD line
        macd_line = ema_fast - ema_slow
        
        # Hitung signal line
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        
        # Hitung histogram
        histogram = macd_line - signal_line
        
        return macd_line, signal_line, histogram
    
    # Buat DataFrame untuk menyimpan hasil
    results = []
    
    # Hitung MACD untuk setiap saham
    for symbol, group in df.groupby('symbol'):
        if len(group) < 30:  # Minimal 30 hari data untuk perhitungan yang baik
            continue
            
        # Sort berdasarkan tanggal
        group = group.sort_values('date')
        
        # Hitung MACD
        macd_line, signal_line, histogram = calculate_macd(group['close'])
        
        # Gabungkan hasil
        group['macd_line'] = macd_line
        group['signal_line'] = signal_line
        group['histogram'] = histogram
        
        # Tentukan sinyal berdasarkan MACD
        group['prev_histogram'] = group['histogram'].shift(1)
        
        # Filter hanya data terbaru saja
        latest_data = group.dropna().tail(10)  # Ambil 10 hari terbaru dengan MACD yang valid
        
        for _, row in latest_data.iterrows():
            # Tentukan sinyal MACD
            macd_signal = "Neutral"
            
            if row['histogram'] > 0 and row['prev_histogram'] <= 0:
                macd_signal = "Bullish"  # Bullish crossover
            elif row['histogram'] < 0 and row['prev_histogram'] >= 0:
                macd_signal = "Bearish"  # Bearish crossover
            elif row['histogram'] > 0 and row['histogram'] > row['prev_histogram']:
                macd_signal = "Bullish Momentum"  # Bullish momentum
            elif row['histogram'] < 0 and row['histogram'] < row['prev_histogram']:
                macd_signal = "Bearish Momentum"  # Bearish momentum
            
            results.append({
                'symbol': symbol,
                'date': row['date'],
                'macd_line': row['macd_line'],
                'signal_line': row['signal_line'],
                'histogram': row['histogram'],
                'macd_signal': macd_signal
            })
    
    # Konversi hasil ke DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        print("Tidak ada hasil MACD yang valid")
        return "Tidak ada hasil MACD"
    
    # Buat tabel baru jika belum ada
    cursor = conn.cursor()
    
    # Drop tabel jika sudah ada dan tidak sesuai dengan skema yang dibutuhkan
    try:
        cursor.execute("DROP TABLE IF EXISTS public_analytics.technical_indicators_macd;")
    except Exception as e:
        print(f"Error dropping table: {e}")
    
    # Buat tabel baru dengan semua kolom yang diperlukan
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public_analytics.technical_indicators_macd (
        symbol TEXT,
        date DATE,
        macd_line NUMERIC,
        signal_line NUMERIC,
        histogram NUMERIC,
        macd_signal TEXT,
        PRIMARY KEY (symbol, date)
    )
    """)
    
    # Hapus data yang sudah ada untuk tanggal yang sama dengan yang akan dimasukkan
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM public_analytics.technical_indicators_macd WHERE date = '{date}'")
    
    # Masukkan data baru
    for _, row in result_df.iterrows():
        cursor.execute("""
        INSERT INTO public_analytics.technical_indicators_macd 
        (symbol, date, macd_line, signal_line, histogram, macd_signal)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row['symbol'], row['date'], row['macd_line'], 
            row['signal_line'], row['histogram'], row['macd_signal']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Berhasil menghitung MACD untuk {result_df['symbol'].nunique()} saham pada {len(dates_to_update)} tanggal"

def calculate_bollinger_bands():
    """
    Menghitung Bollinger Bands untuk semua saham
    """
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    
    # Ambil data harga saham untuk periode yang dibutuhkan
    query = """
    SELECT 
        symbol, 
        date, 
        close
    FROM public_analytics.daily_stock_metrics
    WHERE date >= CURRENT_DATE - INTERVAL '50 days'
    ORDER BY symbol, date
    """
    
    df = pd.read_sql(query, conn)
    
    # Jika tidak ada data sama sekali
    if df.empty:
        print("Tidak ada data harga saham untuk perhitungan Bollinger Bands")
        return "Tidak ada data"
    
    # Buat DataFrame untuk menyimpan hasil
    results = []
    
    # Hitung Bollinger Bands untuk setiap saham
    for symbol, group in df.groupby('symbol'):
        if len(group) < 20:  # Minimal 20 hari data untuk perhitungan yang baik
            continue
            
        # Sort berdasarkan tanggal
        group = group.sort_values('date')
        
        # Hitung Bollinger Bands
        period = 20
        std_dev = 2
        
        group['middle_band'] = group['close'].rolling(window=period).mean()
        group['std'] = group['close'].rolling(window=period).std()
        group['upper_band'] = group['middle_band'] + (group['std'] * std_dev)
        group['lower_band'] = group['middle_band'] - (group['std'] * std_dev)
        
        # Hitung %B (posisi harga relatif terhadap bands)
        group['percent_b'] = (group['close'] - group['lower_band']) / (group['upper_band'] - group['lower_band'])
        
        # Filter hanya data terbaru saja
        latest_data = group.dropna().tail(10)  # Ambil 10 hari terbaru
        
        # Tentukan sinyal berdasarkan Bollinger Bands
        for _, row in latest_data.iterrows():
            bb_signal = "Neutral"
            
            if row['close'] > row['upper_band']:
                bb_signal = "Overbought"
            elif row['close'] < row['lower_band']:
                bb_signal = "Oversold"
            elif row['percent_b'] > 0.8:
                bb_signal = "Near Overbought"
            elif row['percent_b'] < 0.2:
                bb_signal = "Near Oversold"
                
            results.append({
                'symbol': symbol,
                'date': row['date'],
                'middle_band': row['middle_band'],
                'upper_band': row['upper_band'],
                'lower_band': row['lower_band'],
                'percent_b': row['percent_b'],
                'bb_signal': bb_signal
            })
    
    # Konversi hasil ke DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        print("Tidak ada hasil Bollinger Bands yang valid")
        return "Tidak ada hasil Bollinger Bands"
    
    # Buat tabel baru jika belum ada
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public_analytics.technical_indicators_bollinger (
        symbol TEXT,
        date DATE,
        middle_band NUMERIC,
        upper_band NUMERIC,
        lower_band NUMERIC,
        percent_b NUMERIC,
        bb_signal TEXT,
        PRIMARY KEY (symbol, date)
    )
    """)
    
    # Hapus data yang sudah ada untuk tanggal yang sama dengan yang akan dimasukkan
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM public_analytics.technical_indicators_bollinger WHERE date = '{date}'")
    
    # Masukkan data baru
    for _, row in result_df.iterrows():
        cursor.execute("""
        INSERT INTO public_analytics.technical_indicators_bollinger 
        (symbol, date, middle_band, upper_band, lower_band, percent_b, bb_signal)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE 
        SET middle_band = EXCLUDED.middle_band, 
            upper_band = EXCLUDED.upper_band, 
            lower_band = EXCLUDED.lower_band, 
            percent_b = EXCLUDED.percent_b, 
            bb_signal = EXCLUDED.bb_signal
        """, (
            row['symbol'], row['date'], row['middle_band'], 
            row['upper_band'], row['lower_band'], 
            row['percent_b'], row['bb_signal']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Berhasil menghitung Bollinger Bands untuk {result_df['symbol'].nunique()} saham pada {len(dates_to_update)} tanggal"

# DAG definition - Technical Indicators
with DAG(
    dag_id="technical_indicators_calculation",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["technical", "indicators", "analysis"]
) as dag:

    # Tunggu hingga transformasi data selesai
    wait_for_transformation = ExternalTaskSensor(
        task_id="wait_for_transformation",
        external_dag_id="data_transformation",
        external_task_id="run_dbt_analytics",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )
    
    # Hitung RSI
    calculate_rsi = PythonOperator(
        task_id="calculate_rsi_indicators",
        python_callable=calculate_rsi_indicators
    )
    
    # Hitung MACD
    calculate_macd = PythonOperator(
        task_id="calculate_macd_indicators",
        python_callable=calculate_macd_indicators
    )
    
    # Hitung Bollinger Bands
    calculate_bb = PythonOperator(
        task_id="calculate_bollinger_bands",
        python_callable=calculate_bollinger_bands
    )
    
    # Marker task
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    # Task dependencies
    wait_for_transformation >> [calculate_rsi, calculate_macd, calculate_bb] >> end_task