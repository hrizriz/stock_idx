# utils/data_validation.py
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_stock_data(df, date):
    """
    Validasi data saham untuk menemukan anomali
    
    Parameters:
    df (DataFrame): DataFrame dengan data saham
    date (str): Tanggal data yang diperiksa (YYYY-MM-DD)
    
    Returns:
    dict: Hasil validasi dengan status dan issues yang ditemukan
    """
    issues = []
    
    # Check for missing required columns
    required_columns = ['symbol', 'date', 'open_price', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        issues.append(f"Missing required columns: {missing_columns}")
    
    if not issues:  # Only continue if basic columns exist
        # Check for null values in critical columns
        for col in required_columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                issues.append(f"{null_count} null values in {col}")
        
        # Check for duplicate symbols
        duplicates = df['symbol'].duplicated().sum()
        if duplicates > 0:
            issues.append(f"Found {duplicates} duplicate symbol entries")
        
        # Check price consistency
        price_issues = ((df['close'] > df['high']) | (df['close'] < df['low']) | 
                         (df['open_price'] > df['high']) | (df['open_price'] < df['low']))
        price_issue_count = price_issues.sum()
        if price_issue_count > 0:
            issues.append(f"Found {price_issue_count} price consistency issues (close outside high-low range)")
        
        # Check for extreme price moves
        df['price_change_pct'] = (df['close'] - df['prev_close']) / df['prev_close'] * 100
        extreme_moves = df[(df['price_change_pct'].abs() > 20) & (~df['price_change_pct'].isnull())]
        if not extreme_moves.empty:
            issues.append(f"Found {len(extreme_moves)} stocks with >20% daily move")
        
        # Check for extreme volume changes
        if 'prev_volume' in df.columns:
            df['volume_change_pct'] = (df['volume'] - df['prev_volume']) / df['prev_volume'] * 100
            extreme_volumes = df[(df['volume_change_pct'].abs() > 300) & (~df['volume_change_pct'].isnull())]
            if not extreme_volumes.empty:
                issues.append(f"Found {len(extreme_volumes)} stocks with >300% volume change")
        
        # Check total record count compared to previous day
        expected_count = df.shape[0]
        if expected_count < 700:  # Assume IDX has at least 700 stocks
            issues.append(f"Expected at least 700 stocks, but found only {expected_count}")
    
    return {
        "date": date,
        "status": "Failed" if issues else "Passed",
        "record_count": len(df) if isinstance(df, pd.DataFrame) else 0,
        "issues": issues,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def validate_technical_indicators(df, indicator_type):
    """
    Validasi hasil perhitungan indikator teknikal
    
    Parameters:
    df (DataFrame): DataFrame dengan hasil indikator
    indicator_type (str): Jenis indikator (RSI, MACD, BB)
    
    Returns:
    dict: Hasil validasi dengan status dan issues yang ditemukan
    """
    issues = []
    
    # Validate based on indicator type
    if indicator_type == 'RSI':
        # Check for values outside valid range
        invalid_rsi = ((df['rsi'] < 0) | (df['rsi'] > 100)).sum()
        if invalid_rsi > 0:
            issues.append(f"Found {invalid_rsi} invalid RSI values (outside 0-100 range)")
        
        # Check for proper signal values
        valid_signals = ['Oversold', 'Overbought', 'Neutral']
        invalid_signals = df[~df['rsi_signal'].isin(valid_signals)].shape[0]
        if invalid_signals > 0:
            issues.append(f"Found {invalid_signals} invalid RSI signal values")
    
    elif indicator_type == 'MACD':
        # Check for proper signal values
        valid_signals = ['Bullish', 'Bearish', 'Neutral']
        invalid_signals = df[~df['macd_signal'].isin(valid_signals)].shape[0]
        if invalid_signals > 0:
            issues.append(f"Found {invalid_signals} invalid MACD signal values")
    
    elif indicator_type == 'Bollinger':
        # Check percent_b values (should be mostly between 0 and 1)
        extreme_percent_b = ((df['percent_b'] < -0.5) | (df['percent_b'] > 1.5)).sum()
        if extreme_percent_b > 0:
            issues.append(f"Found {extreme_percent_b} extreme percent_b values (< -0.5 or > 1.5)")
        
        # Check for valid signal values
        valid_signals = ['Overbought', 'Oversold', 'Near Overbought', 'Near Oversold', 'Neutral']
        invalid_signals = df[~df['bb_signal'].isin(valid_signals)].shape[0]
        if invalid_signals > 0:
            issues.append(f"Found {invalid_signals} invalid Bollinger Band signal values")
    
    # Common validations
    # Check for missing symbols
    if 'symbol' in df.columns and df['symbol'].isnull().sum() > 0:
        issues.append(f"Found {df['symbol'].isnull().sum()} rows with missing symbol")
    
    # Check for missing dates
    if 'date' in df.columns and df['date'].isnull().sum() > 0:
        issues.append(f"Found {df['date'].isnull().sum()} rows with missing date")
    
    return {
        "indicator_type": indicator_type,
        "status": "Failed" if issues else "Passed",
        "record_count": len(df) if isinstance(df, pd.DataFrame) else 0,
        "issues": issues,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def validate_signals(df, signal_type):
    """
    Validasi sinyal trading untuk memastikan kualitas
    
    Parameters:
    df (DataFrame): DataFrame dengan sinyal trading
    signal_type (str): Jenis sinyal (DAILY, WEEKLY, MONTHLY)
    
    Returns:
    dict: Hasil validasi dengan status dan issues yang ditemukan
    """
    issues = []
    
    # Check probability values (should be between 0 and 1)
    if 'winning_probability' in df.columns:
        invalid_prob = ((df['winning_probability'] < 0) | (df['winning_probability'] > 1)).sum()
        if invalid_prob > 0:
            issues.append(f"Found {invalid_prob} invalid probability values (outside 0-1 range)")
    
    # Check buy score values (should be integers between 0 and 10)
    if 'buy_score' in df.columns:
        invalid_score = ((df['buy_score'] < 0) | (df['buy_score'] > 10)).sum()
        if invalid_score > 0:
            issues.append(f"Found {invalid_score} invalid buy score values (outside 0-10 range)")
    
    # Check signal type distribution (ensure we have a balanced distribution)
    if 'signal_strength' in df.columns:
        signal_counts = df['signal_strength'].value_counts()
        for signal, count in signal_counts.items():
            percent = count / len(df) * 100
            if percent > 80:  # If more than 80% signals are of same type
                issues.append(f"Signal distribution imbalance: {percent:.1f}% are '{signal}'")
    
    return {
        "signal_type": signal_type,
        "status": "Failed" if issues else "Passed",
        "record_count": len(df) if isinstance(df, pd.DataFrame) else 0,
        "issues": issues,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def log_validation_results(validation_results, log_file=None):
    """Log validation results to file and console"""
    if log_file is None:
        log_file = "/opt/airflow/logs/data_validation.log"
        
    # Log to console
    status = validation_results['status']
    issues_count = len(validation_results.get('issues', []))
    
    if status == 'Passed':
        logger.info(f"✅ Validation PASSED: {validation_results.get('indicator_type', '')} "
                    f"{validation_results.get('signal_type', '')} {validation_results.get('date', '')}")
    else:
        logger.warning(f"❌ Validation FAILED: {validation_results.get('indicator_type', '')} "
                       f"{validation_results.get('signal_type', '')} {validation_results.get('date', '')}")
        for issue in validation_results.get('issues', []):
            logger.warning(f"  - {issue}")
    
    # Log to file
    with open(log_file, 'a') as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {str(validation_results)}\n")
    
    return True