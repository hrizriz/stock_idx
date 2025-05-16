import os
import numpy as np
import pandas as pd
import pickle
import logging
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.model_selection import train_test_split
import json
from datetime import datetime
from airflow.models import Variable
from pathlib import Path

from .database import get_database_connection, get_latest_stock_date

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def predict_stock_price(symbol):
    """
    Make predictions using trained LSTM model
    
    Parameters:
    symbol (str): Stock symbol to predict price for
    """
    try:
        # Check if model exists
        model_dir = Path(f"/opt/airflow/data/lstm/{symbol}")
        model_path = model_dir / f"{symbol}_lstm_model.pth"
        
        if not model_path.exists():
            logger.error(f"LSTM model for {symbol} not found")
            return f"LSTM model for {symbol} not found"
        
        # Get latest data
        conn = get_database_connection()
        query = f"""
        SELECT 
            date, 
            close, 
            open_price as open,
            prev_close,
            high,
            low,
            volume, 
            CASE 
                WHEN prev_close IS NOT NULL AND prev_close != 0 
                THEN (close - prev_close) / prev_close * 100
                ELSE NULL
            END as percent_change
        FROM 
            daily_stock_summary
        WHERE 
            symbol = '{symbol}'
        ORDER BY 
            date DESC
        LIMIT 100
        """
        
        df = pd.read_sql(query, conn)
        df = df.sort_values('date')  # Ensure data is in chronological order
        
        # Load scalers
        with open(model_dir / f"{symbol}_scaler.pkl", 'rb') as f:
            scaler = pickle.load(f)
        
        with open(model_dir / f"{symbol}_close_scaler.pkl", 'rb') as f:
            close_scaler = pickle.load(f)
        
        # Prepare features
        features = ['close', 'volume', 'percent_change']
        if len(df) > 14:
            # Add technical features as in training
            df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
            df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
            df['macd_line'] = df['ema12'] - df['ema26']
            
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).fillna(0)
            loss = (-delta.where(delta < 0, 0)).fillna(0)
            avg_gain = gain.rolling(window=14).mean()
            avg_loss = loss.rolling(window=14).mean()
            rs = avg_gain / avg_loss
            df['rsi'] = 100 - (100 / (1 + rs))
            
            features.extend(['macd_line', 'rsi'])
        
        # Handle missing features
        for feature in features:
            if feature not in df.columns:
                df[feature] = 0
        
        # Fill missing values
        df = df.ffill().fillna(0)
        
        # Get last sequence for prediction
        seq_length = 10
        if len(df) < seq_length:
            logger.error(f"Not enough data for {symbol} prediction (need at least {seq_length} days)")
            return f"Not enough data for prediction"
        
        last_sequence = df[features].tail(seq_length).values
        
        # Scale the sequence
        last_sequence_scaled = scaler.transform(last_sequence)
        
        # Convert to tensor
        X_pred = torch.FloatTensor(last_sequence_scaled).unsqueeze(0)
        
        # Load the model
        input_dim = len(features)
        hidden_dim = 32
        
        # Create LSTM model class
        class LSTMModel(nn.Module):
            def __init__(self, input_dim, hidden_dim, output_dim=1, num_layers=2):
                super(LSTMModel, self).__init__()
                self.hidden_dim = hidden_dim
                self.num_layers = num_layers
                
                # LSTM layer
                self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True)
                self.fc = nn.Linear(hidden_dim, output_dim)
                
            def forward(self, x):
                h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_dim).to(x.device)
                c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_dim).to(x.device)
                
                out, _ = self.lstm(x, (h0, c0))
                out = self.fc(out[:, -1, :])
                return out
        
        model = LSTMModel(input_dim, hidden_dim, output_dim=1, num_layers=2)
        model.load_state_dict(torch.load(model_path))
        
        # Make prediction
        model.eval()
        with torch.no_grad():
            pred = model(X_pred)
            pred_np = pred.numpy()
            
            # Inverse transform
            pred_denorm = close_scaler.inverse_transform(pred_np)[0][0]
        
        # Get the next business date
        last_date = df['date'].iloc[-1]
        next_date = pd.to_datetime(last_date) + pd.Timedelta(days=1)
        
        # Skip weekends
        while next_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
            next_date = next_date + pd.Timedelta(days=1)
        
        # Save prediction to database
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_predictions (
            symbol VARCHAR(10),
            prediction_date DATE,
            predicted_close NUMERIC(16,2),
            actual_close NUMERIC(16,2),
            prediction_error NUMERIC(16,2),
            error_percentage NUMERIC(8,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, prediction_date)
        )
        """)
        
        # Convert to standard Python float
        pred_value = float(pred_denorm)
    
        # Save prediction
        cursor.execute("""
        INSERT INTO stock_predictions (symbol, prediction_date, predicted_close)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol, prediction_date) 
        DO UPDATE SET 
            predicted_close = EXCLUDED.predicted_close,
            created_at = CURRENT_TIMESTAMP
        """, (symbol, next_date, pred_value))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return pred_value
    except Exception as e:
        logger.error(f"Error predicting {symbol} price: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error predicting price: {str(e)}"

def update_actual_prices():
    """
    Update actual prices after market close and calculate model performance metrics
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Update actual prices from daily_stock_summary
        cursor.execute("""
        UPDATE stock_predictions sp
        SET 
            actual_close = dsm.close,
            prediction_error = ABS(sp.predicted_close - dsm.close),
            error_percentage = (ABS(sp.predicted_close - dsm.close) / dsm.close) * 100
        FROM daily_stock_summary dsm
        WHERE 
            sp.symbol = dsm.symbol
            AND sp.prediction_date = dsm.date
            AND sp.actual_close IS NULL
        """)
        
        rows_updated = cursor.rowcount
        
        # Calculate performance metrics for existing predictions
        cursor.execute("""
        SELECT 
            symbol,
            SQRT(AVG(POWER(predicted_close - actual_close, 2))) as rmse,
            AVG(ABS(predicted_close - actual_close)) as mae,
            AVG(ABS(predicted_close - actual_close) / actual_close) * 100 as mape,
            COUNT(*) as prediction_count
        FROM stock_predictions
        WHERE actual_close IS NOT NULL
        GROUP BY symbol
        """)
        
        # Get results
        model_scores = cursor.fetchall()
        
        # Save metrics to model_performance_metrics table
        for row in model_scores:
            if len(row) >= 4:  # Ensure row has enough elements
                symbol, rmse, mae, mape, pred_count = row
                cursor.execute("""
                INSERT INTO model_performance_metrics
                    (symbol, model_type, rmse, mae, mape, prediction_count, last_updated)
                VALUES
                    (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (symbol, model_type)
                DO UPDATE SET
                    rmse = EXCLUDED.rmse,
                    mae = EXCLUDED.mae,
                    mape = EXCLUDED.mape,
                    prediction_count = EXCLUDED.prediction_count,
                    last_updated = CURRENT_TIMESTAMP
                """, (symbol, "LSTM_Realtime", rmse, mae, mape, pred_count))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return f"Updated {rows_updated} rows with actual prices and calculated metrics"
    except Exception as e:
        logger.error(f"Error updating actual prices: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error updating actual prices: {str(e)}"