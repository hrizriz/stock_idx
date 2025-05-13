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
import collections

from .database import get_database_connection, get_latest_stock_date

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LSTMModel(nn.Module):
    """
    Model LSTM untuk prediksi pergerakan harga saham
    """
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

def train_win_rate_predictor():
    """
    Melatih model machine learning untuk memprediksi probabilitas win rate
    berdasarkan faktor-faktor teknikal
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    # Ambil data historis untuk training
    try:
        # Query data sinyal historis dengan outcome (win/loss)
        sql = """
        WITH historical_signals AS (
            SELECT 
                s.symbol, 
                s.date, 
                s.buy_score,
                s.volume_shock,
                s.demand_zone,
                s.foreign_flow,
                s.price_pattern,
                s.market_structure,
                s.adx,
                s.fib_support,
                r.rsi,
                CASE WHEN m.macd_signal = 'Bullish' THEN 1 ELSE 0 END as macd_bullish,
                CASE 
                    WHEN ((m2.close - m1.close) / m1.close * 100) > 3 THEN 1  -- Win jika naik >3%
                    ELSE 0
                END AS is_win
            FROM public_analytics.advanced_trading_signals s
            JOIN public.daily_stock_summary m1 
                ON s.symbol = m1.symbol AND s.date = m1.date
            LEFT JOIN public.daily_stock_summary m2
                ON s.symbol = m2.symbol AND m2.date = s.date + INTERVAL '5 days'
            LEFT JOIN public_analytics.technical_indicators_rsi r
                ON s.symbol = r.symbol AND s.date = r.date
            LEFT JOIN public_analytics.technical_indicators_macd m
                ON s.symbol = m.symbol AND s.date = m.date
            WHERE s.date BETWEEN CURRENT_DATE - INTERVAL '365 days' AND CURRENT_DATE - INTERVAL '5 days'
            AND m2.close IS NOT NULL  -- Only include cases where we have outcome data
        )
        SELECT * FROM historical_signals
        """
        
        training_data = pd.read_sql(sql, conn)
        conn.close()
    except Exception as e:
        logger.error(f"Error querying training data: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error querying training data: {str(e)}"
    
    if training_data.empty:
        logger.warning("No data available for model training")
        return "No data available for model training"
    
    # Preprocessing data
    try:
        # Convert categorical columns to binary/numeric
        training_data['price_pattern_binary'] = training_data['price_pattern'].apply(
            lambda x: 1 if x == 'Bullish Engulfing' else 0
        )
        
        training_data['market_structure_binary'] = training_data['market_structure'].apply(
            lambda x: 1 if x == 'Uptrend' else 0
        )
        
        # Handle null/nan values
        numeric_cols = ['buy_score', 'foreign_flow', 'adx', 'rsi']
        for col in numeric_cols:
            training_data[col] = training_data[col].fillna(training_data[col].median())
        
        # Fill remaining binary cols with 0
        binary_cols = ['volume_shock', 'demand_zone', 'fib_support', 
                      'price_pattern_binary', 'market_structure_binary', 'macd_bullish']
        for col in binary_cols:
            training_data[col] = training_data[col].fillna(0)
            
        # Pilih feature dan target
        features = [
            'buy_score', 'volume_shock', 'demand_zone', 'foreign_flow',
            'adx', 'rsi', 'fib_support', 
            'price_pattern_binary', 'market_structure_binary', 'macd_bullish'
        ]
        
        X = training_data[features]
        y = training_data['is_win']
        
        # Train-test split
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        
        # Train Random Forest model
        model = RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42, class_weight='balanced')
        model.fit(X_train, y_train)
        
        # Evaluasi model
        y_pred = model.predict(X_test)
        accuracy = (y_pred == y_test).mean()
        
        # Feature importance
        feature_importance = dict(zip(features, model.feature_importances_))
        
        # Save model to file
        model_dir = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'models')
        os.makedirs(model_dir, exist_ok=True)
        
        model_path = os.path.join(model_dir, 'win_rate_predictor.pkl')
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        
        # Save feature importance as Airflow variable
        Variable.set('model_feature_importance', json.dumps(feature_importance))
        
        return f"Model trained with {accuracy*100:.2f}% accuracy"
    except Exception as e:
        logger.error(f"Error training model: {str(e)}")
        return f"Error training model: {str(e)}"

def train_lstm_model(symbol):
    """
    Train LSTM model for a specific stock symbol
    """
    try:
        conn = get_database_connection()
    except Exception as e:
        return f"Database connection error: {str(e)}"
    
    # Extract stock data
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
        date ASC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Konversi kolom date ke datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Handle missing values
    df = df.fillna(method='ffill')
    
    # Add technical features
    if len(df) > 14:
        # Lag features
        for feat in ['volume', 'percent_change']:
            df[f'{feat}_lag1'] = df[feat].shift(1)
        
        # Price momentum
        df['price_momentum'] = df['close'].pct_change(5)
        
        # Volatility
        df['volatility'] = df['close'].rolling(10).std() / df['close']
        
        # Simple MACD 
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['macd_line'] = df['ema12'] - df['ema26']
        df['signal_line'] = df['macd_line'].ewm(span=9, adjust=False).mean()
        
        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # More technical features if we have enough data
        if len(df) > 20:
            # Bollinger Bands
            df['ma20'] = df['close'].rolling(window=20).mean()
            df['std20'] = df['close'].rolling(window=20).std()
            df['upper_band'] = df['ma20'] + (df['std20'] * 2)
            df['lower_band'] = df['ma20'] - (df['std20'] * 2)
            df['bb_position'] = (df['close'] - df['lower_band']) / (df['upper_band'] - df['lower_band'])
    
    # Handle missing values after adding features
    df = df.ffill().fillna(0)
    
    # Save data to CSV for potential reuse
    data_dir = Path(f"/opt/airflow/data/lstm/{symbol}")
    data_dir.mkdir(exist_ok=True, parents=True)
    csv_path = data_dir / f"{symbol}_data.csv"
    df.to_csv(csv_path, index=False)
    
    # Split data (80% training, 20% testing)
    train_size = int(len(df) * 0.8)
    train_data = df[:train_size]
    test_data = df[train_size:]
    
    # Feature selection
    features = ['close', 'volume', 'percent_change']
    if 'macd_line' in df.columns:
        features.append('macd_line')
    if 'rsi' in df.columns:
        features.append('rsi')
    
    # Normalize data
    scaler = MinMaxScaler(feature_range=(0, 1))
    close_scaler = MinMaxScaler(feature_range=(0, 1))
    close_scaler.fit(df[['close']])
    
    scaled_train = pd.DataFrame(
        scaler.fit_transform(train_data[features]),
        columns=features
    )
    
    # Set sequence length
    seq_length = 10
    
    # Create sequences
    X, y = [], []
    
    for i in range(len(scaled_train) - seq_length):
        X.append(scaled_train.iloc[i:i+seq_length].values)
        y.append(scaled_train.iloc[i+seq_length]['close'])
    
    X = np.array(X)
    y = np.array(y)
    
    # Convert to PyTorch tensors
    X_tensor = torch.FloatTensor(X)
    y_tensor = torch.FloatTensor(y).view(-1, 1)
    
    # Create and train model
    input_dim = len(features)
    hidden_dim = 32
    output_dim = 1
    num_layers = 2
    
    # Enhanced LSTM Model with dropout
    class EnhancedLSTMModel(torch.nn.Module):
        def __init__(self, input_dim, hidden_dim, output_dim=1, num_layers=2, dropout=0.2):
            super(EnhancedLSTMModel, self).__init__()
            self.hidden_dim = hidden_dim
            self.num_layers = num_layers
            
            # LSTM layer with dropout
            self.lstm = torch.nn.LSTM(
                input_dim, 
                hidden_dim, 
                num_layers, 
                batch_first=True, 
                dropout=dropout
            )
            
            # Dropout layer
            self.dropout = torch.nn.Dropout(dropout)
            
            # Fully connected layer
            self.fc = torch.nn.Linear(hidden_dim, output_dim)
            
        def forward(self, x):
            h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_dim).to(x.device)
            c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_dim).to(x.device)
            
            out, _ = self.lstm(x, (h0, c0))
            out = self.dropout(out[:, -1, :])
            out = self.fc(out)
            return out
    
    # Use enhanced model
    model = EnhancedLSTMModel(
        input_dim=input_dim, 
        hidden_dim=hidden_dim, 
        output_dim=output_dim, 
        num_layers=num_layers,
        dropout=0.2
    )
    
    # Loss function and optimizer
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    
    # Learning rate scheduler
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode='min', factor=0.5, patience=5
    )
    
    # Train model with early stopping
    num_epochs = 100
    best_loss = float('inf')
    patience = 10
    patience_counter = 0
    
    for epoch in range(num_epochs):
        model.train()
        outputs = model(X_tensor)
        optimizer.zero_grad()
        
        # Obtain the loss function
        loss = criterion(outputs, y_tensor)
        loss.backward()
        
        optimizer.step()
        
        # Update learning rate
        scheduler.step(loss)
        
        # Early stopping
        if loss.item() < best_loss:
            best_loss = loss.item()
            # Save best model
            torch.save(model.state_dict(), data_dir / f"{symbol}_lstm_model_best.pth")
            patience_counter = 0
        else:
            patience_counter += 1
            if patience_counter >= patience:
                logger.info(f'Early stopping at epoch {epoch+1}')
                break
        
        if (epoch+1) % 10 == 0:
            logger.info(f'Epoch [{epoch+1}/{num_epochs}], Loss: {loss.item():.6f}')
    
    # Load the best model
    model.load_state_dict(torch.load(data_dir / f"{symbol}_lstm_model_best.pth"))
    
    # Save final model
    torch.save(model.state_dict(), data_dir / f"{symbol}_lstm_model.pth")
    
    # Save scalers
    with open(data_dir / f"{symbol}_scaler.pkl", 'wb') as f:
        pickle.dump(scaler, f)
    
    with open(data_dir / f"{symbol}_close_scaler.pkl", 'wb') as f:
        pickle.dump(close_scaler, f)
    
    # Test the model on test data
    try:
        test_features = test_data[features]
        scaled_test = scaler.transform(test_features)
        
        # Create test sequences
        X_test_seq = []
        y_test_actual = []
        
        for i in range(len(scaled_test) - seq_length):
            X_test_seq.append(scaled_test[i:i+seq_length])
            y_test_actual.append(test_data.iloc[i+seq_length]['close'])
        
        X_test_tensor = torch.FloatTensor(np.array(X_test_seq))
        
        # Make predictions
        model.eval()
        with torch.no_grad():
            test_preds = model(X_test_tensor)
            test_preds_np = test_preds.numpy()
            
            # Inverse transform predictions
            # Create dummy array with zeros for all columns except close
            dummy = np.zeros((len(test_preds_np), len(features)))
            dummy[:, 0] = test_preds_np.flatten()  # close is the first column
            
            # Inverse transform predictions (only for close)
            test_preds_denorm = close_scaler.inverse_transform(test_preds_np)
            
            # Calculate metrics
            mae = np.mean(np.abs(test_preds_denorm.flatten() - np.array(y_test_actual)))
            rmse = np.sqrt(np.mean((test_preds_denorm.flatten() - np.array(y_test_actual))**2))
            mape = np.mean(np.abs((np.array(y_test_actual) - test_preds_denorm.flatten()) / np.array(y_test_actual))) * 100
            
            # Save metrics to database
            conn = get_database_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS model_performance_metrics (
                symbol VARCHAR(10),
                model_type VARCHAR(50),
                rmse NUMERIC(16,2),
                mae NUMERIC(16,2),
                mape NUMERIC(16,2),
                prediction_count INTEGER,
                last_updated TIMESTAMP,
                PRIMARY KEY (symbol, model_type)
            )
            """)
            
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
            """, (symbol, "LSTM", rmse, mae, mape, len(y_test_actual)))
            
            conn.commit()
            cursor.close()
            conn.close()
        
        return f"LSTM model for {symbol} trained successfully. RMSE: {rmse:.2f}, MAE: {mae:.2f}, MAPE: {mape:.2f}%"
    except Exception as e:
        logger.error(f"Error testing LSTM model: {str(e)}")
        return f"LSTM model trained but error during testing: {str(e)}"

def predict_stock_price(symbol):
    """
    Make predictions using trained LSTM model
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
        
        pred_denorm = close_scaler.inverse_transform(pred_np)[0][0]

        # Menjadi (tambahkan konversi ke float Python standar):
        pred_value = close_scaler.inverse_transform(pred_np)[0][0]
        # Konversi ke float Python standar
        pred_denorm = float(pred_value)
    
        # Save prediction
        cursor.execute("""
        INSERT INTO stock_predictions (symbol, prediction_date, predicted_close)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol, prediction_date) 
        DO UPDATE SET 
            predicted_close = EXCLUDED.predicted_close,
            created_at = CURRENT_TIMESTAMP
        """, (symbol, next_date, pred_denorm))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return pred_denorm
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