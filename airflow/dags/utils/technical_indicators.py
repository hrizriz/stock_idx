import pandas as pd
import numpy as np
import logging
from datetime import datetime
from .database import get_database_connection, get_latest_stock_date, execute_query, fetch_data, create_table_if_not_exists

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_rsi(data, period=14):
    """
    Calculate RSI (Relative Strength Index) indicator
    """
    # Calculate price changes
    delta = data.diff()
    
    # Split into gains and losses
    gain = delta.clip(lower=0)
    loss = -1 * delta.clip(upper=0)
    
    # Calculate average gains and losses
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    # Calculate RS
    rs = avg_gain / avg_loss
    
    # Calculate RSI
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def calculate_macd(data, fast_period=12, slow_period=26, signal_period=9):
    """
    Calculate MACD (Moving Average Convergence Divergence) indicator
    """
    # Calculate EMAs
    ema_fast = data.ewm(span=fast_period, adjust=False).mean()
    ema_slow = data.ewm(span=slow_period, adjust=False).mean()
    
    # Calculate MACD line
    macd_line = ema_fast - ema_slow
    
    # Calculate signal line
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    
    # Calculate histogram
    macd_histogram = macd_line - signal_line
    
    return macd_line, signal_line, macd_histogram

def calculate_bollinger_bands(data, period=20, std_dev=2):
    """
    Calculate Bollinger Bands
    """
    # Calculate moving average
    middle_band = data.rolling(window=period).mean()
    
    # Calculate standard deviation
    std = data.rolling(window=period).std()
    
    # Calculate upper and lower bands
    upper_band = middle_band + (std * std_dev)
    lower_band = middle_band - (std * std_dev)
    
    # Calculate %B (price position relative to bands)
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
    # Get stock price data with appropriate lookback period
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
    
    # If no data
    if df.empty:
        logger.warning(f"No stock price data for RSI calculation {signal_type}")
        return f"No data for {signal_type}"
    
    # Table name based on signal_type
    table_name = f"public_analytics.technical_indicators_rsi_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_rsi"
    
    # Create table if it doesn't exist
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
    
    # Create DataFrame to store results
    results = []
    
    # Calculate RSI for each stock with given period
    for symbol, group in df.groupby('symbol'):
        if len(group) < rsi_period + 1:  # Minimum required data
            continue
            
        # Sort by date
        group = group.sort_values('date')
        
        # Calculate RSI with given period
        group['rsi'] = calculate_rsi(group['close'], period=rsi_period)
        
        # Filter only latest data
        latest_data = group.dropna().tail(10)  # Get 10 latest days with valid RSI
        
        # Adjust RSI threshold based on timeframe
        if signal_type == 'WEEKLY':
            oversold_threshold = 35  # More relaxed for weekly
            overbought_threshold = 65
        elif signal_type == 'MONTHLY':
            oversold_threshold = 40  # Even more relaxed for monthly
            overbought_threshold = 60
        else:
            oversold_threshold = 30  # Standard for daily
            overbought_threshold = 70
        
        # Determine signal based on RSI value and threshold
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
    
    # Convert results to DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        logger.warning("No valid RSI results")
        return "No RSI results"
    
    # Save results to database
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Delete existing data for dates to be updated
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
    # Insert new data
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
    # Get data with appropriate lookback period
    df = fetch_data(f"""
        SELECT 
            symbol, 
            date, 
            close
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '{lookback_period} days'
        ORDER BY symbol, date
    """)
    
    # If no data
    if df.empty:
        logger.warning(f"No stock price data for MACD calculation {signal_type}")
        return f"No data {signal_type}"
    
    # For weekly/monthly, use longer MACD period
    if signal_type == 'WEEKLY':
        min_days_required = 40  # More data for weekly
    elif signal_type == 'MONTHLY':
        min_days_required = 60  # Even more for monthly
    else:
        min_days_required = 30  # Default for daily
    
    # Table name based on signal_type
    table_name = f"public_analytics.technical_indicators_macd_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_macd"
    
    # Create table if it doesn't exist
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
    
    # Create DataFrame to store results
    results = []
    
    # Calculate MACD for each stock
    for symbol, group in df.groupby('symbol'):
        if len(group) < min_days_required:  # Minimum required data
            continue
            
        # Sort by date
        group = group.sort_values('date')
        
        # Calculate MACD with given parameters
        macd_line, signal_line, macd_histogram = calculate_macd(
            group['close'], 
            fast_period=fast_period, 
            slow_period=slow_period, 
            signal_period=signal_period
        )
        
        # Combine results
        group['macd_line'] = macd_line
        group['signal_line'] = signal_line
        group['macd_histogram'] = macd_histogram
        
        # Determine signal based on MACD
        group['prev_macd_histogram'] = group['macd_histogram'].shift(1)
        
        # Filter only latest data
        latest_data = group.dropna().tail(10)  # Get 10 latest days with valid MACD
        
        for _, row in latest_data.iterrows():
            # Determine MACD signal
            macd_signal = "Neutral"
            
            # Adjust crossover sensitivity based on timeframe
            if signal_type == 'WEEKLY':
                hist_threshold = 0.03  # Higher for weekly (reducing false signals)
            elif signal_type == 'MONTHLY':
                hist_threshold = 0.05  # Even higher for monthly
            else:
                hist_threshold = 0.0   # Standard for daily
            
            # Bullish crossover (histogram changes from negative to positive)
            if row['macd_histogram'] > hist_threshold and row['prev_macd_histogram'] <= hist_threshold:
                macd_signal = "Bullish"  # Bullish crossover
            # Bearish crossover (histogram changes from positive to negative)
            elif row['macd_histogram'] < -hist_threshold and row['prev_macd_histogram'] >= -hist_threshold:
                macd_signal = "Bearish"  # Bearish crossover
            # Bullish momentum (positive histogram and increasing)
            elif row['macd_histogram'] > 0 and row['macd_histogram'] > row['prev_macd_histogram']:
                macd_signal = "Bullish"  # Bullish momentum
            # Bearish momentum (negative histogram and decreasing)
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
    
    # Convert results to DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        logger.warning(f"No valid MACD {signal_type} results")
        return f"No MACD {signal_type} results"
    
    # Save results to database
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Delete existing data for dates to be updated
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
    # Insert new data
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
    # Get stock price data with adjusted lookback period
    df = fetch_data(f"""
        SELECT 
            symbol, 
            date, 
            close
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '{lookback_period} days'
        ORDER BY symbol, date
    """)
    
    # If no data
    if df.empty:
        logger.warning(f"No stock price data for Bollinger Bands calculation {signal_type}")
        return f"No data {signal_type}"
    
    # Table name based on signal_type
    table_name = f"public_analytics.technical_indicators_bollinger_{signal_type.lower()}" if signal_type != 'DAILY' else "public_analytics.technical_indicators_bollinger"
    
    # Create table if it doesn't exist
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
    
    # Adjust minimum number of days data based on timeframe
    if signal_type == 'WEEKLY':
        min_days_required = 30  # More data for weekly
    elif signal_type == 'MONTHLY':
        min_days_required = 40  # Even more for monthly
    else:
        min_days_required = 20  # Default for daily
    
    # Create DataFrame to store results
    results = []
    
    # Calculate Bollinger Bands for each stock
    for symbol, group in df.groupby('symbol'):
        if len(group) < min_days_required:  # Minimum required data
            continue
            
        # Sort by date
        group = group.sort_values('date')
        
        # Calculate Bollinger Bands with given parameters
        middle_band, upper_band, lower_band, percent_b = calculate_bollinger_bands(
            group['close'], 
            period=band_period, 
            std_dev=std_dev
        )
        
        # Combine results
        group['middle_band'] = middle_band
        group['upper_band'] = upper_band
        group['lower_band'] = lower_band
        group['percent_b'] = percent_b
        
        # Filter only latest data
        latest_data = group.dropna().tail(10)  # Get 10 latest days
        
        # Adjust threshold based on timeframe
        if signal_type == 'WEEKLY':
            near_overbought = 0.75  # More relaxed for weekly
            near_oversold = 0.25
        elif signal_type == 'MONTHLY':
            near_overbought = 0.7   # Even more relaxed for monthly
            near_oversold = 0.3
        else:
            near_overbought = 0.8   # Standard for daily
            near_oversold = 0.2
        
        # Determine signal based on Bollinger Bands
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
    
    # Convert results to DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        logger.warning(f"No valid Bollinger Bands {signal_type} results")
        return f"No Bollinger Bands {signal_type} results"
    
    # Save results to database
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Delete existing data for dates to be updated
    dates_to_update = result_df['date'].unique()
    for date in dates_to_update:
        cursor.execute(f"DELETE FROM {table_name} WHERE date = '{date}'")
    
    # Insert new data
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
        
        # Check if table already exists
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = 'technical_indicators_bollinger'
        )
        """)
        
        table_exists = cursor.fetchone()[0]
        
        # If table doesn't exist, create new
        if not table_exists:
            logger.info("Creating technical_indicators_bollinger table as it doesn't exist")
            
            # Ensure public_analytics schema exists
            cursor.execute("CREATE SCHEMA IF NOT EXISTS public_analytics")
            
            # Create Bollinger Bands table
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
            
            # Get latest date
            latest_date = get_latest_stock_date()
            
            # Calculate Bollinger Bands from price data and insert into table separately
            # Step 1: Calculate values for each stock
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
            
            # Step 2: Insert data with percent_b and bb_signal calculation
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