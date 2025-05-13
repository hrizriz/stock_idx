
import pandas as pd
import numpy as np
import logging
from .database import get_database_connection, get_latest_stock_date, execute_query, fetch_data, create_table_if_not_exists

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_rsi(data, period=14):
    """
    Menghitung indikator RSI (Relative Strength Index)
    """
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

def calculate_macd(data, fast_period=12, slow_period=26, signal_period=9):
    """
    Menghitung indikator MACD (Moving Average Convergence Divergence)
    """
    # Hitung EMA
    ema_fast = data.ewm(span=fast_period, adjust=False).mean()
    ema_slow = data.ewm(span=slow_period, adjust=False).mean()
    
    # Hitung MACD line
    macd_line = ema_fast - ema_slow
    
    # Hitung signal line
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    
    # Hitung histogram - GANTI MENJADI macd_histogram
    macd_histogram = macd_line - signal_line
    
    return macd_line, signal_line, macd_histogram

def calculate_bollinger_bands(data, period=20, std_dev=2):
    """
    Menghitung Bollinger Bands
    """
    # Hitung moving average
    middle_band = data.rolling(window=period).mean()
    
    # Hitung standard deviation
    std = data.rolling(window=period).std()
    
    # Hitung upper dan lower bands
    upper_band = middle_band + (std * std_dev)
    lower_band = middle_band - (std * std_dev)
    
    # Hitung %B (posisi harga relatif terhadap bands)
    percent_b = (data - lower_band) / (upper_band - lower_band)
    
    return middle_band, upper_band, lower_band, percent_b

def calculate_all_rsi_indicators(lookback_period=100, rsi_period=14, signal_type='DAILY'):
    """
    Menghitung indikator RSI untuk semua saham dan menyimpan ke database
    
    Parameters:
    lookback_period (int): Jumlah hari ke belakang untuk analisis (default: 100 untuk daily)
    rsi_period (int): Periode untuk perhitungan RSI (default: 14)
    signal_type (str): Tipe sinyal - DAILY, WEEKLY, atau MONTHLY
    """
    # Ambil data harga saham dengan periode lookback yang sesuai
    query = f"""
        SELECT 
            symbol, 
            date, 
            close
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '{lookback_period} days'
        ORDER BY symbol, date
    """
    
    df = fetch_data(query)
    
    # Jika tidak ada data
    if df.empty:
        logger.warning(f"Tidak ada data harga saham untuk perhitungan RSI {signal_type}")
        return f"Tidak ada data untuk {signal_type}"
    
    # Nama tabel disesuaikan dengan signal_type
    table_name = f"public_analytics.technical_indicators_rsi_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_rsi"
    
    # Buat tabel jika belum ada
    create_table_if_not_exists(
        table_name,
        f"""
        CREATE TABLE {table_name} (
            symbol TEXT,
            date DATE,
            rsi NUMERIC,
            rsi_signal TEXT,
            PRIMARY KEY (symbol, date)
        )
        """
    )
    
    # Buat DataFrame untuk menyimpan hasil
    results = []
    
    # Hitung RSI untuk setiap saham dengan periode yang diberikan
    for symbol, group in df.groupby('symbol'):
        if len(group) < rsi_period + 1:  # Minimal data yang diperlukan
            continue
            
        # Sort berdasarkan tanggal
        group = group.sort_values('date')
        
        # Hitung RSI dengan periode yang diberikan
        group['rsi'] = calculate_rsi(group['close'], period=rsi_period)
        
        # Filter hanya data terbaru saja
        latest_data = group.dropna().tail(10)  # Ambil 10 hari terbaru dengan RSI yang valid
        
        # Sesuaikan threshold RSI berdasarkan timeframe
        if signal_type == 'WEEKLY':
            oversold_threshold = 35  # Lebih longgar untuk weekly
            overbought_threshold = 65
        elif signal_type == 'MONTHLY':
            oversold_threshold = 40  # Lebih longgar lagi untuk monthly
            overbought_threshold = 60
        else:
            oversold_threshold = 30  # Standar untuk daily
            overbought_threshold = 70
        
        # Tentukan sinyal berdasarkan nilai RSI dan threshold
        for _, row in latest_data.iterrows():
            rsi_signal = "Neutral"
            if row['rsi'] <= oversold_threshold:
                rsi_signal = "Oversold"
            elif row['rsi'] >= overbought_threshold:
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
        logger.warning("Tidak ada hasil RSI yang valid")
        return "Tidak ada hasil RSI"
    
    # Simpan hasil ke database
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Hapus data yang sudah ada untuk tanggal yang akan diupdate
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
    # Masukkan data baru
    for _, row in result_df.iterrows():
        cursor.execute(f"""
        INSERT INTO {table_name} (symbol, date, rsi, rsi_signal)
        VALUES (%s, %s, %s, %s)
        """, (row['symbol'], row['date'], row['rsi'], row['rsi_signal']))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Berhasil menghitung RSI {signal_type} untuk {result_df['symbol'].nunique()} saham pada {len(dates_to_update)} tanggal"

def calculate_all_macd_indicators(lookback_period=150, fast_period=12, slow_period=26, signal_period=9, signal_type='DAILY'):
    """
    Menghitung indikator MACD untuk semua saham dan menyimpan ke database
    
    Parameters:
    lookback_period (int): Jumlah hari ke belakang untuk analisis
    fast_period (int): Periode EMA cepat untuk MACD
    slow_period (int): Periode EMA lambat untuk MACD
    signal_period (int): Periode signal line
    signal_type (str): Tipe sinyal - DAILY, WEEKLY, atau MONTHLY
    """
    # Ambil data dengan lookback period yang sesuai
    df = fetch_data(f"""
        SELECT 
            symbol, 
            date, 
            close
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '{lookback_period} days'
        ORDER BY symbol, date
    """)
    
    # Jika tidak ada data
    if df.empty:
        logger.warning(f"Tidak ada data harga saham untuk perhitungan MACD {signal_type}")
        return f"Tidak ada data {signal_type}"
    
    # Untuk weekly/monthly, gunakan periode MACD yang lebih panjang
    if signal_type == 'WEEKLY':
        min_days_required = 40  # Lebih banyak data untuk weekly
    elif signal_type == 'MONTHLY':
        min_days_required = 60  # Lebih banyak lagi untuk monthly
    else:
        min_days_required = 30  # Default untuk daily
    
    # Nama tabel disesuaikan dengan signal_type
    table_name = f"public_analytics.technical_indicators_macd_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_macd"
    
    # Buat tabel jika belum ada
    create_table_if_not_exists(
        table_name,
        f"""
        CREATE TABLE {table_name} (
            symbol TEXT,
            date DATE,
            macd_line NUMERIC,
            signal_line NUMERIC,
            macd_histogram NUMERIC,
            macd_signal TEXT,
            PRIMARY KEY (symbol, date)
        )
        """
    )
    
    # Buat DataFrame untuk menyimpan hasil
    results = []
    
    # Hitung MACD untuk setiap saham
    for symbol, group in df.groupby('symbol'):
        if len(group) < min_days_required:  # Minimal data yang diperlukan
            continue
            
        # Sort berdasarkan tanggal
        group = group.sort_values('date')
        
        # Hitung MACD dengan parameter yang diberikan
        macd_line, signal_line, macd_histogram = calculate_macd(
            group['close'], 
            fast_period=fast_period, 
            slow_period=slow_period, 
            signal_period=signal_period
        )
        
        # Gabungkan hasil
        group['macd_line'] = macd_line
        group['signal_line'] = signal_line
        group['macd_histogram'] = macd_histogram
        
        # Tentukan sinyal berdasarkan MACD
        group['prev_macd_histogram'] = group['macd_histogram'].shift(1)
        
        # Filter hanya data terbaru saja
        latest_data = group.dropna().tail(10)  # Ambil 10 hari terbaru dengan MACD yang valid
        
        for _, row in latest_data.iterrows():
            # Tentukan sinyal MACD
            macd_signal = "Neutral"
            
            # Sesuaikan sensitivitas crossover berdasarkan timeframe
            if signal_type == 'WEEKLY':
                hist_threshold = 0.03  # Lebih tinggi untuk weekly (mengurangi false signals)
            elif signal_type == 'MONTHLY':
                hist_threshold = 0.05  # Lebih tinggi lagi untuk monthly
            else:
                hist_threshold = 0.0   # Standar untuk daily
            
            # Bullish crossover (histogram berubah dari negatif ke positif)
            if row['macd_histogram'] > hist_threshold and row['prev_macd_histogram'] <= hist_threshold:
                macd_signal = "Bullish"  # Bullish crossover
            # Bearish crossover (histogram berubah dari positif ke negatif)
            elif row['macd_histogram'] < -hist_threshold and row['prev_macd_histogram'] >= -hist_threshold:
                macd_signal = "Bearish"  # Bearish crossover
            # Bullish momentum (histogram positif dan meningkat)
            elif row['macd_histogram'] > 0 and row['macd_histogram'] > row['prev_macd_histogram']:
                macd_signal = "Bullish"  # Bullish momentum
            # Bearish momentum (histogram negatif dan menurun)
            elif row['macd_histogram'] < 0 and row['macd_histogram'] < row['prev_macd_histogram']:
                macd_signal = "Bearish"  # Bearish momentum
            
            results.append({
                'symbol': symbol,
                'date': row['date'],
                'macd_line': row['macd_line'],
                'signal_line': row['signal_line'],
                'macd_histogram': row['macd_histogram'],
                'macd_signal': macd_signal
            })
    
    # Konversi hasil ke DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        logger.warning(f"Tidak ada hasil MACD {signal_type} yang valid")
        return f"Tidak ada hasil MACD {signal_type}"
    
    # Simpan hasil ke database
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Hapus data yang sudah ada untuk tanggal yang akan diupdate
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
    # Masukkan data baru
    for _, row in result_df.iterrows():
        cursor.execute(f"""
        INSERT INTO {table_name} 
        (symbol, date, macd_line, signal_line, macd_histogram, macd_signal)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row['symbol'], row['date'], row['macd_line'], 
            row['signal_line'], row['macd_histogram'], row['macd_signal']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Berhasil menghitung MACD {signal_type} untuk {result_df['symbol'].nunique()} saham pada {len(dates_to_update)} tanggal"

def calculate_all_bollinger_bands(lookback_period=50, band_period=20, std_dev=2, signal_type='DAILY'):
    """
    Menghitung Bollinger Bands untuk semua saham dan menyimpan ke database
    
    Parameters:
    lookback_period (int): Jumlah hari ke belakang untuk analisis
    band_period (int): Periode Moving Average untuk Bollinger Bands
    std_dev (int): Jumlah standar deviasi untuk band
    signal_type (str): Tipe sinyal - DAILY, WEEKLY, atau MONTHLY
    """
    # Ambil data harga saham dengan lookback period yang disesuaikan
    df = fetch_data(f"""
        SELECT 
            symbol, 
            date, 
            close
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '{lookback_period} days'
        ORDER BY symbol, date
    """)
    
    # Jika tidak ada data
    if df.empty:
        logger.warning(f"Tidak ada data harga saham untuk perhitungan Bollinger Bands {signal_type}")
        return f"Tidak ada data {signal_type}"
    
    # Nama tabel disesuaikan dengan signal_type
    table_name = f"public_analytics.technical_indicators_bollinger_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_bollinger"
    
    # Buat tabel jika belum ada
    create_table_if_not_exists(
        table_name,
        f"""
        CREATE TABLE {table_name} (
            symbol TEXT,
            date DATE,
            middle_band NUMERIC,
            upper_band NUMERIC,
            lower_band NUMERIC,
            percent_b NUMERIC,
            bb_signal TEXT,
            PRIMARY KEY (symbol, date)
        )
        """
    )
    
    # Sesuaikan minimum jumlah hari data berdasarkan timeframe
    if signal_type == 'WEEKLY':
        min_days_required = 30  # Lebih banyak data untuk weekly
    elif signal_type == 'MONTHLY':
        min_days_required = 40  # Lebih banyak lagi untuk monthly
    else:
        min_days_required = 20  # Default untuk daily
    
    # Buat DataFrame untuk menyimpan hasil
    results = []
    
    # Hitung Bollinger Bands untuk setiap saham
    for symbol, group in df.groupby('symbol'):
        if len(group) < min_days_required:  # Minimal data yang diperlukan
            continue
            
        # Sort berdasarkan tanggal
        group = group.sort_values('date')
        
        # Hitung Bollinger Bands dengan parameter yang diberikan
        middle_band, upper_band, lower_band, percent_b = calculate_bollinger_bands(
            group['close'], 
            period=band_period, 
            std_dev=std_dev
        )
        
        # Gabungkan hasil
        group['middle_band'] = middle_band
        group['upper_band'] = upper_band
        group['lower_band'] = lower_band
        group['percent_b'] = percent_b
        
        # Filter hanya data terbaru saja
        latest_data = group.dropna().tail(10)  # Ambil 10 hari terbaru
        
        # Sesuaikan threshold berdasarkan timeframe
        if signal_type == 'WEEKLY':
            near_overbought = 0.75  # Lebih longgar untuk weekly
            near_oversold = 0.25
        elif signal_type == 'MONTHLY':
            near_overbought = 0.7   # Lebih longgar lagi untuk monthly
            near_oversold = 0.3
        else:
            near_overbought = 0.8   # Standar untuk daily
            near_oversold = 0.2
        
        # Tentukan sinyal berdasarkan Bollinger Bands
        for _, row in latest_data.iterrows():
            bb_signal = "Neutral"
            
            if row['close'] > row['upper_band']:
                bb_signal = "Overbought"
            elif row['close'] < row['lower_band']:
                bb_signal = "Oversold"
            elif row['percent_b'] > near_overbought:
                bb_signal = "Near Overbought"
            elif row['percent_b'] < near_oversold:
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
        logger.warning(f"Tidak ada hasil Bollinger Bands {signal_type} yang valid")
        return f"Tidak ada hasil Bollinger Bands {signal_type}"
    
    # Simpan hasil ke database
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Hapus data yang sudah ada untuk tanggal yang akan diupdate
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
    # Masukkan data baru
    for _, row in result_df.iterrows():
        cursor.execute(f"""
        INSERT INTO {table_name} 
        (symbol, date, middle_band, upper_band, lower_band, percent_b, bb_signal)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row['symbol'], row['date'], row['middle_band'], 
            row['upper_band'], row['lower_band'], 
            row['percent_b'], row['bb_signal']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Berhasil menghitung Bollinger Bands {signal_type} untuk {result_df['symbol'].nunique()} saham pada {len(dates_to_update)} tanggal"

def check_and_create_bollinger_bands_table():
    """
    Memeriksa apakah tabel Bollinger Bands sudah ada, jika belum buat baru
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Periksa apakah tabel sudah ada
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = 'technical_indicators_bollinger'
        )
        """)
        
        table_exists = cursor.fetchone()[0]
        
        # Jika tabel belum ada, buat baru
        if not table_exists:
            logger.info("Creating technical_indicators_bollinger table as it doesn't exist")
            
            # Pastikan schema public_analytics sudah ada
            cursor.execute("CREATE SCHEMA IF NOT EXISTS public_analytics")
            
            # Buat tabel Bollinger Bands
            cursor.execute("""
            CREATE TABLE public_analytics.technical_indicators_bollinger (
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
            
            # Dapatkan tanggal terakhir
            latest_date = get_latest_stock_date()
            
            # Kalkulasi Bollinger Bands dari data harga dan masukkan ke tabel secara terpisah
            # Tahap 1: Hitung nilai untuk setiap saham
            cursor.execute(f"""
            WITH daily_prices AS (
                SELECT
                    symbol,
                    date,
                    close
                FROM public.daily_stock_summary
                WHERE date >= '{latest_date}'::date - INTERVAL '50 days'
            ),
            -- Hitung SMA 20 dan standard deviation
            sma_std AS (
                SELECT
                    symbol,
                    date,
                    close,
                    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS middle_band,
                    STDDEV(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS std_dev
                FROM daily_prices
            )
            SELECT
                symbol,
                date,
                close,
                middle_band,
                middle_band + (2 * std_dev) AS upper_band,
                middle_band - (2 * std_dev) AS lower_band
            INTO TEMP temp_bollinger
            FROM sma_std
            WHERE middle_band IS NOT NULL AND std_dev IS NOT NULL;
            """)
            
            # Tahap 2: Masukkan data dengan menghitung percent_b dan bb_signal
            cursor.execute("""
            INSERT INTO public_analytics.technical_indicators_bollinger (
                symbol, date, middle_band, upper_band, lower_band, percent_b, bb_signal
            )
            SELECT
                symbol,
                date,
                middle_band,
                upper_band,
                lower_band,
                CASE
                    WHEN (upper_band - lower_band) = 0 THEN 0.5
                    ELSE (close - lower_band) / (upper_band - lower_band)
                END AS percent_b,
                CASE
                    WHEN close > upper_band THEN 'Overbought'
                    WHEN close < lower_band THEN 'Oversold'
                    ELSE 'Neutral'
                END AS bb_signal
            FROM temp_bollinger;
            """)
            
            conn.commit()
            logger.info("Successfully created and populated technical_indicators_bollinger table")
        
        cursor.close()
        conn.close()
        return True
    
    except Exception as e:
        logger.error(f"Error checking/creating Bollinger Bands table: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        return False