# utils/parameter_optimization.py
import pandas as pd
import numpy as np
from itertools import product
import logging
from datetime import datetime, timedelta
import json

from .database import get_database_connection, fetch_data
from .technical_indicators import calculate_rsi, calculate_macd, calculate_bollinger_bands

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_parameter_combinations(parameter_ranges):
    """
    Generate all combinations of parameters for testing
    
    Parameters:
    parameter_ranges (dict): Dictionary with parameter name as key and list of values to test
    
    Returns:
    list: List of parameter combinations as dictionaries
    """
    keys = parameter_ranges.keys()
    values = parameter_ranges.values()
    
    combinations = []
    for combination in product(*values):
        param_dict = dict(zip(keys, combination))
        combinations.append(param_dict)
    
    return combinations

def evaluate_rsi_parameters(symbol, start_date, end_date, parameter_combinations):
    """
    Evaluate different RSI parameters to find optimal values
    
    Parameters:
    symbol (str): Stock symbol to evaluate
    start_date (str): Start date in YYYY-MM-DD format
    end_date (str): End date in YYYY-MM-DD format
    parameter_combinations (list): List of parameter combinations to test
    
    Returns:
    pd.DataFrame: Results of parameter testing
    """
    # Get stock data
    conn = get_database_connection()
    query = f"""
    SELECT 
        date, 
        close
    FROM public.daily_stock_summary
    WHERE 
        symbol = '{symbol}'
        AND date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date
    """
    
    df = fetch_data(query)
    
    if df.empty:
        logger.warning(f"No data found for {symbol} in the specified date range")
        return pd.DataFrame()
    
    # Calculate forward returns for evaluation
    df['return_1d'] = df['close'].pct_change(1).shift(-1)
    df['return_5d'] = df['close'].pct_change(5).shift(-5)
    
    # Evaluate each parameter combination
    results = []
    
    for params in parameter_combinations:
        period = params['period']
        oversold = params['oversold']
        overbought = params['overbought']
        
        # Calculate RSI
        df['rsi'] = calculate_rsi(df['close'], period=period)
        
        # Generate signals
        df['rsi_signal'] = 'Neutral'
        df.loc[df['rsi'] <= oversold, 'rsi_signal'] = 'Oversold'
        df.loc[df['rsi'] >= overbought, 'rsi_signal'] = 'Overbought'
        
        # Evaluate oversold signals (buy)
        oversold_df = df[df['rsi_signal'] == 'Oversold'].copy()
        if not oversold_df.empty:
            avg_return_1d = oversold_df['return_1d'].mean() * 100
            avg_return_5d = oversold_df['return_5d'].mean() * 100
            win_rate_1d = (oversold_df['return_1d'] > 0).mean() * 100
            win_rate_5d = (oversold_df['return_5d'] > 0).mean() * 100
            signal_count = len(oversold_df)
        else:
            avg_return_1d = 0
            avg_return_5d = 0
            win_rate_1d = 0
            win_rate_5d = 0
            signal_count = 0
        
        # Calculate combined score
        score = (win_rate_5d * 0.5) + (avg_return_5d * 0.5)
        
        # Add to results
        results.append({
            'symbol': symbol,
            'period': period,
            'oversold': oversold,
            'overbought': overbought,
            'avg_return_1d': avg_return_1d,
            'avg_return_5d': avg_return_5d,
            'win_rate_1d': win_rate_1d,
            'win_rate_5d': win_rate_5d,
            'signal_count': signal_count,
            'score': score
        })
    
    # Convert to DataFrame and sort by score
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('score', ascending=False)
    
    return results_df

def evaluate_macd_parameters(symbol, start_date, end_date, parameter_combinations):
    """
    Evaluate different MACD parameters to find optimal values
    
    Parameters:
    symbol (str): Stock symbol to evaluate
    start_date (str): Start date in YYYY-MM-DD format
    end_date (str): End date in YYYY-MM-DD format
    parameter_combinations (list): List of parameter combinations to test
    
    Returns:
    pd.DataFrame: Results of parameter testing
    """
    # Get stock data
    conn = get_database_connection()
    query = f"""
    SELECT 
        date, 
        close
    FROM public.daily_stock_summary
    WHERE 
        symbol = '{symbol}'
        AND date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date
    """
    
    df = fetch_data(query)
    
    if df.empty:
        logger.warning(f"No data found for {symbol} in the specified date range")
        return pd.DataFrame()
    
    # Calculate forward returns for evaluation
    df['return_1d'] = df['close'].pct_change(1).shift(-1)
    df['return_5d'] = df['close'].pct_change(5).shift(-5)
    
    # Evaluate each parameter combination
    results = []
    
    for params in parameter_combinations:
        fast_period = params['fast_period']
        slow_period = params['slow_period']
        signal_period = params['signal_period']
        
        # Calculate MACD
        df['macd'], df['signal'], df['hist'] = calculate_macd(df['close'], 
                                                             fast_period=fast_period, 
                                                             slow_period=slow_period, 
                                                             signal_period=signal_period)
        
        # Generate signals - consider MACD crossing above signal line as bullish
        df['prev_hist'] = df['hist'].shift(1)
        df['macd_signal'] = 'Neutral'
        df.loc[(df['hist'] > 0) & (df['prev_hist'] <= 0), 'macd_signal'] = 'Bullish'
        df.loc[(df['hist'] < 0) & (df['prev_hist'] >= 0), 'macd_signal'] = 'Bearish'
        
        # Evaluate bullish signals (buy)
        bullish_df = df[df['macd_signal'] == 'Bullish'].copy()
        if not bullish_df.empty:
            avg_return_1d = bullish_df['return_1d'].mean() * 100
            avg_return_5d = bullish_df['return_5d'].mean() * 100
            win_rate_1d = (bullish_df['return_1d'] > 0).mean() * 100
            win_rate_5d = (bullish_df['return_5d'] > 0).mean() * 100
            signal_count = len(bullish_df)
        else:
            avg_return_1d = 0
            avg_return_5d = 0
            win_rate_1d = 0
            win_rate_5d = 0
            signal_count = 0
        
        # Calculate combined score
        score = (win_rate_5d * 0.5) + (avg_return_5d * 0.5)
        
        # Add to results
        results.append({
            'symbol': symbol,
            'fast_period': fast_period,
            'slow_period': slow_period,
            'signal_period': signal_period,
            'avg_return_1d': avg_return_1d,
            'avg_return_5d': avg_return_5d,
            'win_rate_1d': win_rate_1d,
            'win_rate_5d': win_rate_5d,
            'signal_count': signal_count,
            'score': score
        })
    
    # Convert to DataFrame and sort by score
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('score', ascending=False)
    
    return results_df

def save_optimal_parameters(indicator, symbol, parameters, performance):
    """
    Save optimal parameters to database for future use
    
    Parameters:
    indicator (str): Indicator name (RSI, MACD, Bollinger)
    symbol (str): Stock symbol
    parameters (dict): Optimal parameter values
    performance (dict): Performance metrics
    
    Returns:
    bool: Success status
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS indicator_parameters (
            id SERIAL PRIMARY KEY,
            indicator TEXT,
            symbol TEXT,
            parameters JSONB,
            performance JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(indicator, symbol)
        )
        """)
        
        # Insert or update parameters
        cursor.execute("""
        INSERT INTO indicator_parameters
            (indicator, symbol, parameters, performance, created_at)
        VALUES
            (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (indicator, symbol)
        DO UPDATE SET
            parameters = EXCLUDED.parameters,
            performance = EXCLUDED.performance,
            created_at = CURRENT_TIMESTAMP
        """, (
            indicator,
            symbol,
            json.dumps(parameters),
            json.dumps(performance)
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        logger.error(f"Error saving optimal parameters: {str(e)}")
        return False

def optimize_indicators_for_symbol(symbol):
    """
    Run optimization for all indicators for a given symbol
    
    Parameters:
    symbol (str): Stock symbol to optimize
    
    Returns:
    dict: Optimization results
    """
    # Define date range for optimization (last 1 year)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    # Define parameter ranges to test
    rsi_parameters = {
        'period': [7, 9, 11, 14, 21],
        'oversold': [20, 25, 30, 35],
        'overbought': [65, 70, 75, 80]
    }
    
    macd_parameters = {
        'fast_period': [8, 10, 12, 16],
        'slow_period': [21, 26, 30, 35],
        'signal_period': [7, 9, 12, 14]
    }
    
    # Generate parameter combinations
    rsi_combinations = generate_parameter_combinations(rsi_parameters)
    macd_combinations = generate_parameter_combinations(macd_parameters)
    
    # Run optimization for RSI
    logger.info(f"Optimizing RSI parameters for {symbol}...")
    rsi_results = evaluate_rsi_parameters(symbol, start_date, end_date, rsi_combinations)
    
    if not rsi_results.empty:
        best_rsi = rsi_results.iloc[0].to_dict()
        
        # Save optimal RSI parameters
        save_optimal_parameters(
            'RSI', 
            symbol, 
            {
                'period': int(best_rsi['period']),
                'oversold': int(best_rsi['oversold']),
                'overbought': int(best_rsi['overbought'])
            },
            {
                'win_rate_5d': float(best_rsi['win_rate_5d']),
                'avg_return_5d': float(best_rsi['avg_return_5d']),
                'signal_count': int(best_rsi['signal_count']),
                'score': float(best_rsi['score'])
            }
        )
    
    # Run optimization for MACD
    logger.info(f"Optimizing MACD parameters for {symbol}...")
    macd_results = evaluate_macd_parameters(symbol, start_date, end_date, macd_combinations)
    
    if not macd_results.empty:
        best_macd = macd_results.iloc[0].to_dict()
        
        # Save optimal MACD parameters
        save_optimal_parameters(
            'MACD', 
            symbol, 
            {
                'fast_period': int(best_macd['fast_period']),
                'slow_period': int(best_macd['slow_period']),
               'signal_period': int(best_macd['signal_period'])
           },
           {
               'win_rate_5d': float(best_macd['win_rate_5d']),
               'avg_return_5d': float(best_macd['avg_return_5d']),
               'signal_count': int(best_macd['signal_count']),
               'score': float(best_macd['score'])
           }
       )
   
   # Return results
    return {
       'symbol': symbol,
       'rsi_optimized': not rsi_results.empty,
       'macd_optimized': not macd_results.empty,
       'best_rsi': rsi_results.iloc[0].to_dict() if not rsi_results.empty else None,
       'best_macd': macd_results.iloc[0].to_dict() if not macd_results.empty else None
   }