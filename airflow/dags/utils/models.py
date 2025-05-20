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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def normalize_features(df, features, scalers=None):
    """
    Normalize features using MinMaxScaler
    Returns normalized dataframe and scalers
    """
    from sklearn.preprocessing import MinMaxScaler
    
    if scalers is None:
        scalers = {}
        
    normalized_df = df.copy()
    
    for feature in features:
        if feature not in scalers:
            scalers[feature] = MinMaxScaler()
            normalized_df[feature] = scalers[feature].fit_transform(
                df[feature].values.reshape(-1, 1)
            ).flatten()
        else:
            normalized_df[feature] = scalers[feature].transform(
                df[feature].values.reshape(-1, 1)
            ).flatten()
    
    return normalized_df, scalers

def train_win_rate_predictor():
    """
    Train win rate prediction model based on technical indicators and historical data
    
    Returns:
        str: Status message
    """
    try:
        logger.info("Starting win rate predictor training...")
        conn = get_database_connection()
        
        query = """
        WITH signal_results AS (
            SELECT 
                s.symbol,
                s.date as signal_date,
                s.buy_score,
                b.is_win,
                -- Use actual columns from advanced_trading_signals
                s.volume_shock,
                s.demand_zone,
                s.foreign_flow,
                s.adx,
                -- Include relevant backtest columns
                b.percent_change_5d,
                -- Calculate volume ratio from daily stock data
                COALESCE(d.volume / NULLIF(AVG(d.volume) OVER (
                    PARTITION BY d.symbol 
                    ORDER BY d.date 
                    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                ), 0), 1.0) as volume_ratio,
                COALESCE(d.daily_change, 0.0) as daily_change
            FROM public_analytics.advanced_trading_signals s
            LEFT JOIN public_analytics.backtest_results b
                ON s.symbol = b.symbol AND s.date = b.signal_date  -- Correct join condition
            LEFT JOIN (
                SELECT 
                    symbol, 
                    date, 
                    volume,
                    (close - prev_close) / NULLIF(prev_close, 0) * 100 as daily_change
                FROM public.daily_stock_summary
            ) d ON s.symbol = d.symbol AND s.date = d.date
            WHERE b.is_win IS NOT NULL
        )
        SELECT * FROM signal_results
        """
        
        try:
            df = pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Error querying signal data: {str(e)}")
            return f"Error querying signal data: {str(e)}"
        
        if len(df) < 50:
            logger.warning("Not enough data for win rate predictor training")
            return "Not enough data for win rate predictor training"
        
        features = ['buy_score', 'rsi', 'macd_line', 'macd_histogram', 'percent_b', 'volume_ratio', 'daily_change']
        
        df = df.dropna(subset=['is_win'])  # Must have outcome
        df = df.fillna(0)  # Fill missing features with 0
        
        X = df[features]
        y = df['is_win']
        
        from sklearn.model_selection import train_test_split
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        from sklearn.ensemble import RandomForestClassifier
        clf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
        clf.fit(X_train, y_train)
        
        train_accuracy = clf.score(X_train, y_train)
        test_accuracy = clf.score(X_test, y_test)
        
        logger.info(f"Win rate predictor trained. Train accuracy: {train_accuracy:.4f}, Test accuracy: {test_accuracy:.4f}")
        
        import pickle
        import os
        
        model_dir = os.path.join("/opt/airflow/data/models")
        os.makedirs(model_dir, exist_ok=True)
        
        with open(os.path.join(model_dir, "win_rate_predictor.pkl"), 'wb') as f:
            pickle.dump(clf, f)
        
        feature_importance = pd.DataFrame({
            'feature': features,
            'importance': clf.feature_importances_
        }).sort_values('importance', ascending=False)
        
        logger.info("Feature importance:")
        for _, row in feature_importance.iterrows():
            logger.info(f"{row['feature']}: {row['importance']:.4f}")
        
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS model_feature_importance (
            model_name VARCHAR(50),
            feature_name VARCHAR(50),
            importance NUMERIC,
            training_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (model_name, feature_name)
        )
        """)
        
        for _, row in feature_importance.iterrows():
            cursor.execute("""
            INSERT INTO model_feature_importance (model_name, feature_name, importance)
            VALUES (%s, %s, %s)
            ON CONFLICT (model_name, feature_name) 
            DO UPDATE SET 
                importance = EXCLUDED.importance,
                training_date = CURRENT_TIMESTAMP
            """, ("win_rate_predictor", row['feature'], float(row['importance'])))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return f"Win rate predictor trained successfully. Accuracy: {test_accuracy:.4f}"
    except Exception as e:
        logger.error(f"Error training win rate predictor: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error training win rate predictor: {str(e)}"

def train_lstm_model(symbol):
    """
    Train LSTM model for a single stock symbol
    
    Parameters:
    symbol (str): Stock symbol to train model for
    
    Returns:
    str: Status message
    """
    try:
        logger.info(f"Starting LSTM model training for {symbol}")
        conn = get_database_connection()
        
        query = f"""
        SELECT 
            date, 
            close, 
            open_price as open,
            high,
            low,
            volume,
            prev_close,
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
            date
        """
        
        df = pd.read_sql(query, conn)
        
        if len(df) < 200:
            logger.warning(f"Not enough data for {symbol} LSTM training")
            return f"Not enough data for {symbol}"
        
        df = df.fillna(method='ffill').fillna(method='bfill')
        
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['macd_line'] = df['ema12'] - df['ema26']
        
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        avg_loss = avg_loss.replace(0, 1e-10)
        rs = avg_gain / avg_loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        features = ['close', 'volume', 'percent_change', 'macd_line', 'rsi']
        
        df = df.dropna(subset=features)
        
        import os
        from pathlib import Path
        
        model_dir = Path(f"/opt/airflow/data/lstm/{symbol}")
        model_dir.mkdir(parents=True, exist_ok=True)
        
        from sklearn.preprocessing import MinMaxScaler
        import numpy as np
        import torch
        import torch.nn as nn
        
        scaler = MinMaxScaler()
        close_scaler = MinMaxScaler()  # Separate scaler for close price
        
        close_values = df['close'].values.reshape(-1, 1)
        close_scaler.fit(close_values)
        
        scaled_data = scaler.fit_transform(df[features].values)
        
        import pickle
        with open(model_dir / f"{symbol}_scaler.pkl", 'wb') as f:
            pickle.dump(scaler, f)
            
        with open(model_dir / f"{symbol}_close_scaler.pkl", 'wb') as f:
            pickle.dump(close_scaler, f)
        
        seq_length = 10
        X, y = [], []
        
        for i in range(len(scaled_data) - seq_length):
            X.append(scaled_data[i:i+seq_length])
            y.append(scaled_data[i+seq_length, 0])  # Predict close price
            
        X = np.array(X)
        y = np.array(y).reshape(-1, 1)
        
        train_size = int(len(X) * 0.8)
        X_train, X_test = X[:train_size], X[train_size:]
        y_train, y_test = y[:train_size], y[train_size:]
        
        X_train = torch.FloatTensor(X_train)
        y_train = torch.FloatTensor(y_train)
        X_test = torch.FloatTensor(X_test)
        y_test = torch.FloatTensor(y_test)
        
        class LSTMModel(nn.Module):
            def __init__(self, input_dim, hidden_dim, output_dim=1, num_layers=2):
                super(LSTMModel, self).__init__()
                self.hidden_dim = hidden_dim
                self.num_layers = num_layers
                
                self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True)
                self.fc = nn.Linear(hidden_dim, output_dim)
                
            def forward(self, x):
                h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_dim).to(x.device)
                c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_dim).to(x.device)
                
                out, _ = self.lstm(x, (h0, c0))
                out = self.fc(out[:, -1, :])
                return out
        
        input_dim = len(features)
        hidden_dim = 32
        num_layers = 2
        output_dim = 1
        
        model = LSTMModel(input_dim, hidden_dim, output_dim, num_layers)
        
        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
        
        num_epochs = 100
        batch_size = 16
        
        for epoch in range(num_epochs):
            for i in range(0, len(X_train), batch_size):
                batch_X = X_train[i:i+batch_size]
                batch_y = y_train[i:i+batch_size]
                
                outputs = model(batch_X)
                loss = criterion(outputs, batch_y)
                
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
            
            if (epoch+1) % 20 == 0:
                logger.info(f'Epoch [{epoch+1}/{num_epochs}], Loss: {loss.item():.6f}')
        
        model.eval()
        with torch.no_grad():
            test_pred = model(X_test)
            test_loss = criterion(test_pred, y_test).item()
            
            y_test_denorm = close_scaler.inverse_transform(y_test.numpy())
            test_pred_denorm = close_scaler.inverse_transform(test_pred.numpy())
            
            from sklearn.metrics import mean_squared_error, mean_absolute_error
            rmse = np.sqrt(mean_squared_error(y_test_denorm, test_pred_denorm))
            mae = mean_absolute_error(y_test_denorm, test_pred_denorm)
            mape = np.mean(np.abs((y_test_denorm - test_pred_denorm) / np.maximum(y_test_denorm, 1e-10))) * 100
            
            logger.info(f"{symbol} LSTM Evaluation: RMSE={rmse:.2f}, MAE={mae:.2f}, MAPE={mape:.2f}%")
        
        version = save_model_with_version(model, model_dir, symbol)
        
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS model_performance_metrics (
            symbol VARCHAR(10),
            model_type VARCHAR(50),
            rmse NUMERIC,
            mae NUMERIC,
            mape NUMERIC,
            prediction_count INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, model_type)
        )
        """)
        
        cursor.execute("""
        INSERT INTO model_performance_metrics
            (symbol, model_type, rmse, mae, mape, last_updated)
        VALUES
            (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (symbol, model_type)
        DO UPDATE SET
            rmse = EXCLUDED.rmse,
            mae = EXCLUDED.mae,
            mape = EXCLUDED.mape,
            last_updated = CURRENT_TIMESTAMP
        """, (symbol, "LSTM", rmse, mae, mape))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return f"{symbol} LSTM model v{version} trained successfully. RMSE={rmse:.2f}, MAPE={mape:.2f}%"
    except Exception as e:
        logger.error(f"Error training LSTM model for {symbol}: {str(e)}")
        if 'conn' in locals() and conn is not None:
            conn.close()
        return f"Error training LSTM model for {symbol}: {str(e)}"

def predict_stock_price(symbol):
    """
    Make predictions using trained LSTM model
    
    Parameters:
    symbol (str): Stock symbol to predict price for
    """
    try:
        model_dir = Path(f"/opt/airflow/data/lstm/{symbol}")
        model_path = model_dir / f"{symbol}_lstm_model.pth"
        
        if not model_path.exists():
            logger.error(f"LSTM model for {symbol} not found")
            return f"LSTM model for {symbol} not found"
        
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
        
        seq_length = 10  # Sesuaikan dengan model
        if len(df) < seq_length + 5:  # Pastikan ada cukup data plus margin
            logger.error(f"Not enough data for {symbol} prediction (need at least {seq_length+5} days)")
            return f"Not enough data for prediction"
        
        with open(model_dir / f"{symbol}_scaler.pkl", 'rb') as f:
            scaler = pickle.load(f)
        
        with open(model_dir / f"{symbol}_close_scaler.pkl", 'rb') as f:
            close_scaler = pickle.load(f)
        
        features = ['close', 'volume', 'percent_change']
        if len(df) > 14:
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
        
        for feature in features:
            if feature not in df.columns:
                df[feature] = 0
        
        df = df.ffill().fillna(0)
        
        seq_length = 10
        if len(df) < seq_length:
            logger.error(f"Not enough data for {symbol} prediction (need at least {seq_length} days)")
            return f"Not enough data for prediction"
        
        last_sequence = df[features].tail(seq_length).values
        
        last_sequence_scaled = scaler.transform(last_sequence)
        
        X_pred = torch.FloatTensor(last_sequence_scaled).unsqueeze(0)
        
        input_dim = len(features)
        hidden_dim = 32
        
        class LSTMModel(nn.Module):
            def __init__(self, input_dim, hidden_dim, output_dim=1, num_layers=2):
                super(LSTMModel, self).__init__()
                self.hidden_dim = hidden_dim
                self.num_layers = num_layers
                
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
        
        model.eval()
        with torch.no_grad():
            pred = model(X_pred)
            pred_np = pred.numpy()
            
            pred_denorm = close_scaler.inverse_transform(pred_np)[0][0]
        
        last_date = df['date'].iloc[-1]
        next_date = pd.to_datetime(last_date) + pd.Timedelta(days=1)
        
        while next_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
            next_date = next_date + pd.Timedelta(days=1)
        
        cursor = conn.cursor()
        
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
        
        pred_value = float(pred_denorm)
    
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
        
        model_scores = cursor.fetchall()
        
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
    
def save_model_with_version(model, model_dir, symbol, version=None):
    """Save model with versioning"""
    if version is None:
        version = 1
        while (model_dir / f"{symbol}_lstm_model_v{version}.pth").exists():
            version += 1
    
    model_path = model_dir / f"{symbol}_lstm_model_v{version}.pth"
    torch.save(model.state_dict(), model_path)
    
    latest_path = model_dir / f"{symbol}_lstm_model.pth"
    torch.save(model.state_dict(), latest_path)
    
    return version
