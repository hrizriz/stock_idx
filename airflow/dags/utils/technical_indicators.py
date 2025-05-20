import pandas as pd
import numpy as np
import logging
from datetime import datetime
from .database import get_database_connection, get_latest_stock_date, execute_query, fetch_data, create_table_if_not_exists

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_rsi(data, period=14):
    """
    Calculate RSI (Relative Strength Index) indicator with improved handling
    """
    delta = data.diff()
    
    gain = delta.clip(lower=0)
    loss = -1 * delta.clip(upper=0)
    
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    avg_loss = avg_loss.replace(0, 1e-10)  # Avoid divide by zero
    
    rs = avg_gain / avg_loss
    
    rsi = 100 - (100 / (1 + rs))
    
    rsi = rsi.clip(0, 100)
    
    return rsi

def calculate_vwap(data_df, period=20):
    """Calculate VWAP indicator for given data"""
    df = data_df.copy()
    
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    
    df['pv'] = df['typical_price'] * df['volume']
    
    df['cumulative_pv'] = df['pv'].rolling(window=period).sum()
    df['cumulative_volume'] = df['volume'].rolling(window=period).sum()
    
    df['vwap'] = df['cumulative_pv'] / df['cumulative_volume']
    
    return df['vwap']

def calculate_macd(data, fast_period=12, slow_period=26, signal_period=9):
    """
    Calculate MACD (Moving Average Convergence Divergence) indicator
    """
    ema_fast = data.ewm(span=fast_period, adjust=False).mean()
    ema_slow = data.ewm(span=slow_period, adjust=False).mean()
    
    macd_line = ema_fast - ema_slow
    
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    
    macd_histogram = macd_line - signal_line
    
    return macd_line, signal_line, macd_histogram

def calculate_bollinger_bands(data, period=20, std_dev=2):
    """
    Calculate Bollinger Bands
    """
    middle_band = data.rolling(window=period).mean()
    
    std = data.rolling(window=period).std()
    
    upper_band = middle_band + (std * std_dev)
    lower_band = middle_band - (std * std_dev)
    
    percent_b = (data - lower_band) / (upper_band - lower_band)
    
    return middle_band, upper_band, lower_band, percent_b

def calculate_all_rsi_indicators(lookback_period=100, rsi_period=14, signal_type='DAILY'):
    """
    Calculate RSI indicators for all stocks and save to database
    
    Parameters:
    lookback_period (int): Number of days to look back for analysis (default: 100 for daily)
    rsi_period (int): Period for RSI calculation (default: 14)
    signal_type (str): Signal type - DAILY, WEEKLY, or MONTHLY
    """
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
    
    if df.empty:
        logger.warning(f"No stock price data for RSI calculation {signal_type}")
        return f"No data for {signal_type}"
    
    table_name = f"public_analytics.technical_indicators_rsi_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_rsi"
    
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
    
    results = []
    
    for symbol, group in df.groupby('symbol'):
        if len(group) < rsi_period + 1:  # Minimum required data
            continue
            
        group = group.sort_values('date')
        
        group['rsi'] = calculate_rsi(group['close'], period=rsi_period)
        
        latest_data = group.dropna().tail(10)  # Get 10 latest days with valid RSI
        
        if signal_type == 'WEEKLY':
            oversold_threshold = 35  # More relaxed for weekly
            overbought_threshold = 65
        elif signal_type == 'MONTHLY':
            oversold_threshold = 40  # Even more relaxed for monthly
            overbought_threshold = 60
        else:
            oversold_threshold = 30  # Standard for daily
            overbought_threshold = 70
        
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
    
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        logger.warning("No valid RSI results")
        return "No RSI results"
    
    conn = get_database_connection()
    cursor = conn.cursor()
    
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
    for _, row in result_df.iterrows():
        cursor.execute(f"""
        INSERT INTO {table_name} (symbol, date, rsi, rsi_signal)
        VALUES (%s, %s, %s, %s)
        """, (row['symbol'], row['date'], row['rsi'], row['rsi_signal']))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Successfully calculated RSI {signal_type} for {result_df['symbol'].nunique()} stocks on {len(dates_to_update)} dates"

def calculate_all_macd_indicators(lookback_period=150, fast_period=12, slow_period=26, signal_period=9, signal_type='DAILY'):
    """
    Calculate MACD indicators for all stocks and save to database
    
    Parameters:
    lookback_period (int): Number of days to look back for analysis
    fast_period (int): Fast EMA period for MACD
    slow_period (int): Slow EMA period for MACD
    signal_period (int): Signal line period
    signal_type (str): Signal type - DAILY, WEEKLY, or MONTHLY
    """
    df = fetch_data(f"""
        SELECT 
            symbol, 
            date, 
            close
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '{lookback_period} days'
        ORDER BY symbol, date
    """)
    
    if df.empty:
        logger.warning(f"No stock price data for MACD calculation {signal_type}")
        return f"No data {signal_type}"
    
    if signal_type == 'WEEKLY':
        min_days_required = 40  # More data for weekly
    elif signal_type == 'MONTHLY':
        min_days_required = 60  # Even more for monthly
    else:
        min_days_required = 30  # Default for daily
    
    table_name = f"public_analytics.technical_indicators_macd_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_macd"
    
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
    
    results = []
    
    for symbol, group in df.groupby('symbol'):
        if len(group) < min_days_required:  # Minimum required data
            continue
            
        group = group.sort_values('date')
        
        macd_line, signal_line, macd_histogram = calculate_macd(
            group['close'], 
            fast_period=fast_period, 
            slow_period=slow_period, 
            signal_period=signal_period
        )
        
        group['macd_line'] = macd_line
        group['signal_line'] = signal_line
        group['macd_histogram'] = macd_histogram
        
        group['prev_macd_histogram'] = group['macd_histogram'].shift(1)
        
        latest_data = group.dropna().tail(10)  # Get 10 latest days with valid MACD
        
        for _, row in latest_data.iterrows():
            macd_signal = "Neutral"
            
            if signal_type == 'WEEKLY':
                hist_threshold = 0.03  # Higher for weekly (reducing false signals)
            elif signal_type == 'MONTHLY':
                hist_threshold = 0.05  # Even higher for monthly
            else:
                hist_threshold = 0.0   # Standard for daily
            
            if row['macd_histogram'] > hist_threshold and row['prev_macd_histogram'] <= hist_threshold:
                macd_signal = "Bullish"  # Bullish crossover
            elif row['macd_histogram'] < -hist_threshold and row['prev_macd_histogram'] >= -hist_threshold:
                macd_signal = "Bearish"  # Bearish crossover
            elif row['macd_histogram'] > 0 and row['macd_histogram'] > row['prev_macd_histogram']:
                macd_signal = "Bullish"  # Bullish momentum
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
    
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        logger.warning(f"No valid MACD {signal_type} results")
        return f"No MACD {signal_type} results"
    
    conn = get_database_connection()
    cursor = conn.cursor()
    
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
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
    
    return f"Successfully calculated MACD {signal_type} for {result_df['symbol'].nunique()} stocks on {len(dates_to_update)} dates"

def calculate_all_bollinger_bands(lookback_period=50, band_period=20, std_dev=2, signal_type='DAILY'):
    """
    Calculate Bollinger Bands for all stocks and save to database
    
    Parameters:
    lookback_period (int): Number of days to look back for analysis
    band_period (int): Moving Average period for Bollinger Bands
    std_dev (int): Number of standard deviations for band
    signal_type (str): Signal type - DAILY, WEEKLY, or MONTHLY
    """
    df = fetch_data(f"""
        SELECT 
            symbol, 
            date, 
            close
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '{lookback_period} days'
        ORDER BY symbol, date
    """)
    
    if df.empty:
        logger.warning(f"No stock price data for Bollinger Bands calculation {signal_type}")
        return f"No data {signal_type}"
    
    table_name = f"public_analytics.technical_indicators_bollinger_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_bollinger"
    
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
    
    if signal_type == 'WEEKLY':
        min_days_required = 30  # More data for weekly
    elif signal_type == 'MONTHLY':
        min_days_required = 40  # Even more for monthly
    else:
        min_days_required = 20  # Default for daily
    
    results = []
    
    for symbol, group in df.groupby('symbol'):
        if len(group) < min_days_required:  # Minimum required data
            continue
            
        group = group.sort_values('date')
        
        middle_band, upper_band, lower_band, percent_b = calculate_bollinger_bands(
            group['close'], 
            period=band_period, 
            std_dev=std_dev
        )
        
        group['middle_band'] = middle_band
        group['upper_band'] = upper_band
        group['lower_band'] = lower_band
        group['percent_b'] = percent_b
        
        latest_data = group.dropna().tail(10)  # Get 10 latest days
        
        if signal_type == 'WEEKLY':
            near_overbought = 0.75  # More relaxed for weekly
            near_oversold = 0.25
        elif signal_type == 'MONTHLY':
            near_overbought = 0.7   # Even more relaxed for monthly
            near_oversold = 0.3
        else:
            near_overbought = 0.8   # Standard for daily
            near_oversold = 0.2
        
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
    
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        logger.warning(f"No valid Bollinger Bands {signal_type} results")
        return f"No Bollinger Bands {signal_type} results"
    
    conn = get_database_connection()
    cursor = conn.cursor()
    
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
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
    
    return f"Successfully calculated Bollinger Bands {signal_type} for {result_df['symbol'].nunique()} stocks on {len(dates_to_update)} dates"

def check_and_create_bollinger_bands_table():
    """
    Check if Bollinger Bands table exists, if not create new
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = 'technical_indicators_bollinger'
        )
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.info("Creating technical_indicators_bollinger table as it doesn't exist")
            
            cursor.execute("CREATE SCHEMA IF NOT EXISTS public_analytics")
            
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
            
            latest_date = get_latest_stock_date()
            
            cursor.execute(f"""
            WITH daily_prices AS (
                SELECT
                    symbol,
                    date,
                    close
                FROM public.daily_stock_summary
                WHERE date >= '{latest_date}'::date - INTERVAL '50 days'
            ),
            -- Calculate SMA 20 and standard deviation
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
