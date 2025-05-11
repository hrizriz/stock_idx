from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor 
import psycopg2
import pickle
import collections
from pathlib import Path

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

# Model LSTM dengan PyTorch
class LSTMModel(torch.nn.Module):
    def __init__(self, input_dim, hidden_dim, output_dim=1, num_layers=2):
        super(LSTMModel, self).__init__()
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        
        # LSTM layer
        self.lstm = torch.nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True, dropout=0.2)
        
        # Fully connected layer
        self.fc = torch.nn.Linear(hidden_dim, output_dim)
        
    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_dim).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_dim).to(x.device)
        
        out, _ = self.lstm(x, (h0, c0))
        out = self.fc(out[:, -1, :])
        return out

def create_stock_predictions_table():
    """Membuat tabel stock_predictions jika belum ada"""
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
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
    
    # Buat juga tabel untuk hasil backtesting
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS lstm_backtesting_results (
        model_type VARCHAR(50),
        symbol VARCHAR(10),
        test_start_date DATE,
        test_end_date DATE,
        rmse NUMERIC,
        mae NUMERIC,
        mape NUMERIC,
        test_size INTEGER,
        features_used TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (model_type, symbol, test_start_date, test_end_date)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS lstm_backtesting_predictions (
        model_type VARCHAR(50),
        symbol VARCHAR(10),
        prediction_date DATE,
        predicted_close NUMERIC,
        actual_close NUMERIC,
        prediction_error NUMERIC,
        error_percentage NUMERIC,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (model_type, symbol, prediction_date)
    )
    """)
    
    # Buat tabel untuk metrik model
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
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("✅ Tabel stock_predictions dan tabel terkait dibuat")

def extract_bbca_data(**kwargs):
    """Extract data for BBCA from PostgreSQL"""
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    
    # Perbarui query untuk mengakses tabel asli, bukan tabel hasil dbt
    query = """
    SELECT 
        d.date, 
        d.close, 
        d.open_price,
        d.prev_close,
        d.high,
        d.low,
        d.volume, 
        CASE 
            WHEN d.prev_close IS NOT NULL AND d.prev_close != 0 
            THEN (d.close - d.prev_close) / d.prev_close * 100
            ELSE NULL
        END as percent_change
    FROM 
        daily_stock_summary d
    WHERE 
        d.symbol = 'BBCA'
    ORDER BY 
        d.date ASC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # PERBAIKAN: Konversi kolom date ke datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Handle missing values
    df = df.fillna(method='ffill')

    # Bollinger Bands
    df['ma20'] = df['close'].rolling(window=20).mean()
    df['std20'] = df['close'].rolling(window=20).std()
    df['bb_position'] = (df['close'] - df['ma20'] - 2*df['std20']) / (4 * df['std20'])

    # Add trend features
    df['trend_5d'] = (df['close'] / df['close'].shift(5) - 1) * 100
    df['trend_10d'] = (df['close'] / df['close'].shift(10) - 1) * 100

    # Trading volume trends
    df['volume_change'] = (df['volume'] / df['volume'].rolling(5).mean() - 1) * 100

    # Market regime indicators (volatility regimes)
    df['volatility_regime'] = df['close'].rolling(20).std() / df['close'].rolling(20).mean()
    # Tambah fitur teknikal sederhana jika data cukup
    if len(df) > 14:
        # Tambah fitur lag (fitur dari hari sebelumnya)
        for feat in ['volume', 'percent_change']:
            df[f'{feat}_lag1'] = df[feat].shift(1)
        
        # Momentum harga
        df['price_momentum'] = df['close'].pct_change(5)
        
        # Volatilitas
        df['volatility'] = df['close'].rolling(10).std() / df['close']
        
        # Hitung macd line dan signal line sederhana
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['macd_line'] = df['ema12'] - df['ema26']
        df['signal_line'] = df['macd_line'].ewm(span=9, adjust=False).mean()
        
        # Hitung RSI sederhana
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # Tambahkan lebih banyak fitur teknikal yang berkorelasi dengan harga:
        if len(df) > 20:
            # Bollinger Bands
            df['ma20'] = df['close'].rolling(window=20).mean()
            df['std20'] = df['close'].rolling(window=20).std()
            df['upper_band'] = df['ma20'] + (df['std20'] * 2)
            df['lower_band'] = df['ma20'] - (df['std20'] * 2)
            df['bb_position'] = (df['close'] - df['lower_band']) / (df['upper_band'] - df['lower_band'])
            
            # Stochastic Oscillator
            df['highest_high'] = df['high'].rolling(window=14).max()
            df['lowest_low'] = df['low'].rolling(window=14).min()
            df['%K'] = 100 * ((df['close'] - df['lowest_low']) / (df['highest_high'] - df['lowest_low']))
            df['%D'] = df['%K'].rolling(window=3).mean()
            
            # Average True Range (ATR) - Volatilitas
            df['tr1'] = abs(df['high'] - df['low'])
            df['tr2'] = abs(df['high'] - df['close'].shift())
            df['tr3'] = abs(df['low'] - df['close'].shift())
            df['true_range'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
            df['atr'] = df['true_range'].rolling(window=14).mean()
            
            # On-Balance Volume (OBV)
            df['obv'] = (np.sign(df['close'].diff()) * df['volume']).fillna(0).cumsum()
            
            # Ichimoku Cloud
            df['tenkan_sen'] = (df['high'].rolling(window=9).max() + df['low'].rolling(window=9).min()) / 2
            df['kijun_sen'] = (df['high'].rolling(window=26).max() + df['low'].rolling(window=26).min()) / 2
            
            # Relative Strength Index (RSI) dengan periode berbeda
            for period in [7, 14, 21]:
                delta = df['close'].diff()
                gain = (delta.where(delta > 0, 0)).fillna(0)
                loss = (-delta.where(delta < 0, 0)).fillna(0)
                avg_gain = gain.rolling(window=period).mean()
                avg_loss = loss.rolling(window=period).mean()
                rs = avg_gain / avg_loss
                df[f'rsi_{period}'] = 100 - (100 / (1 + rs))
            
            # Seasonal Features
            df['day_of_week'] = df['date'].dt.dayofweek
            df['month'] = df['date'].dt.month
    else:
        # Default dummy values jika data tidak cukup
        df['macd_line'] = 0
        df['signal_line'] = 0
        df['rsi'] = 50
    
    # Handle missing values setelah penambahan fitur
    df = df.fillna(method='ffill').fillna(0)
    
    # Save data to CSV
    data_dir = Path("/opt/airflow/data/lstm")
    data_dir.mkdir(exist_ok=True, parents=True)
    csv_path = data_dir / "bbca_data.csv"
    df.to_csv(csv_path, index=False)
    
    print(f"✅ Data BBCA diambil: {len(df)} baris")
    return str(csv_path)

def feature_engineering(**kwargs):
    """Feature engineering dan normalisasi data yang lebih canggih"""
    ti = kwargs['ti']
    data_path = ti.xcom_pull(task_ids='extract_bbca_data')
    
    df = pd.read_csv(data_path)
    df['date'] = pd.to_datetime(df['date'])
    
    # Tambahkan variabel makroekonomi (data dummy jika tidak ada API)
    df['interest_rate'] = 5.75  # % (angka contoh)
    df['inflation'] = 2.5       # % (angka contoh)
    df['exchange_rate'] = 15500  # IDR/USD (angka contoh)
    
    # Normalisasi menggunakan QuantileTransformer (robust terhadap outlier)
    from sklearn.preprocessing import QuantileTransformer
    
    # PERBAIKAN: Filter kolom numerik dengan cara yang lebih aman
    numeric_features = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    # Pastikan 'date' tidak ada dalam list sebelum mencoba menghapusnya
    if 'date' in numeric_features:
        numeric_features.remove('date')
    
    # Jika data cukup, lakukan transformasi
    if len(df) > 20 and len(numeric_features) > 0:
        qt = QuantileTransformer(output_distribution='normal')
        df[numeric_features] = qt.fit_transform(df[numeric_features])
    
        # Feature Selection dengan Mutual Information (hilangkan fitur yang tidak relevan)
        from sklearn.feature_selection import mutual_info_regression
        
        # Pastikan 'close' ada dalam numeric_features
        if 'close' in numeric_features:
            X = df[numeric_features].drop('close', axis=1)
            y = df['close']
            
            mi_scores = mutual_info_regression(X, y)
            mi_scores = pd.Series(mi_scores, index=X.columns)
            
            # Pilih fitur dengan MI score tinggi (>= 0.1)
            selected_features = mi_scores[mi_scores >= 0.1].index.tolist()
            
            # Tambahkan target ke selected features
            selected_features.append('close')
            
            # Simpan data dengan fitur yang sudah diseleksi
            df_selected = df[['date'] + selected_features]
        else:
            # Jika tidak ada kolom 'close', gunakan semua fitur
            df_selected = df
    else:
        # Jika data tidak cukup, gunakan dataframe asli
        df_selected = df
    
    # Simpan preprocessing components
    data_dir = Path("/opt/airflow/data/lstm")
    data_dir.mkdir(exist_ok=True, parents=True)
    
    try:
        with open(data_dir / 'feature_transformer.pkl', 'wb') as f:
            pickle.dump({
                'selected_features': df_selected.columns.tolist()
            }, f)
    except Exception as e:
        print(f"Warning: Could not save feature transformer: {e}")
    
    # Simpan data
    processed_path = data_dir / 'bbca_processed.csv'
    df_selected.to_csv(processed_path, index=False)
    
    print(f"✅ Feature engineering selesai: {len(df_selected.columns)} fitur dipilih dari {len(df.columns)} fitur asli")
    return str(processed_path)

def hyperparameter_tuning(**kwargs):
    """Melakukan grid search untuk parameter terbaik"""
    ti = kwargs['ti']
    feature_path = ti.xcom_pull(task_ids='feature_engineering')
    
    # Load data hasil feature engineering
    df = pd.read_csv(feature_path)
    df['date'] = pd.to_datetime(df['date'])
    
    # PERBAIKAN: Definisikan variabel features
    # Ambil semua kolom kecuali 'date' dan 'close'
    all_columns = df.columns.tolist()
    # Hapus 'date' jika ada
    if 'date' in all_columns:
        all_columns.remove('date')
    # Hapus 'close' karena ini adalah target
    if 'close' in all_columns:
        all_columns.remove('close')
    
    # Definisikan features
    features = all_columns
    
    print(f"Features yang digunakan untuk hyperparameter tuning: {features}")
    
    # Cek apakah data cukup
    if len(df) < 100 or len(features) < 3:
        print("⚠️ Data tidak cukup untuk hyperparameter tuning, menggunakan parameter default")
        best_params = {
            'seq_length': 10,
            'hidden_dim': 32,
            'num_layers': 2,
            'learning_rate': 0.01,
            'dropout': 0.2
        }
        
        # Simpan parameter default
        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        
        # Buat tabel jika belum ada
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lstm_best_params (
            symbol VARCHAR(10),
            seq_length INTEGER,
            hidden_dim INTEGER,
            num_layers INTEGER,
            learning_rate NUMERIC,
            dropout NUMERIC,
            best_rmse NUMERIC,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol)
        )
        """)
        
        # Simpan parameter default
        cursor.execute("""
        INSERT INTO lstm_best_params 
            (symbol, seq_length, hidden_dim, num_layers, learning_rate, dropout, best_rmse, updated_at)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (symbol) DO UPDATE SET
            seq_length = EXCLUDED.seq_length,
            hidden_dim = EXCLUDED.hidden_dim,
            num_layers = EXCLUDED.num_layers,
            learning_rate = EXCLUDED.learning_rate,
            dropout = EXCLUDED.dropout,
            updated_at = CURRENT_TIMESTAMP
        """, ('BBCA', 10, 32, 2, 0.01, 0.2, 999.99))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return best_params
    
    # Siapkan data untuk hyperparameter tuning
    target = df['close'].values
    data = df[features].values
    
    # Normalisasi data
    scaler_X = MinMaxScaler()
    scaler_y = MinMaxScaler()
    
    X_scaled = scaler_X.fit_transform(data)
    y_scaled = scaler_y.fit_transform(target.reshape(-1, 1)).flatten()
    
    # Simpan scaler untuk digunakan nanti
    data_dir = Path("/opt/airflow/data/lstm")
    with open(data_dir / "scaler_X.pkl", 'wb') as f:
        pickle.dump(scaler_X, f)
    
    with open(data_dir / "scaler_y.pkl", 'wb') as f:
        pickle.dump(scaler_y, f)
    
    # Lakukan grid search ringan (hanya beberapa kombinasi)
    best_rmse = float('inf')
    best_params = {}
    
    # Definisi grid yang lebih kecil
    param_grid = {
    'seq_length': [5, 10, 15, 20],  # Try shorter and longer sequences
    'hidden_dim': [32, 64, 128],
    'num_layers': [1, 2, 3],
    'learning_rate': [0.005, 0.01, 0.02],
    'dropout': [0.0, 0.2, 0.3]
    }
    
    # Logging
    total_combinations = len(param_grid['seq_length']) * len(param_grid['hidden_dim']) * \
                         len(param_grid['num_layers']) * len(param_grid['learning_rate']) * \
                         len(param_grid['dropout'])
    print(f"Melakukan grid search dengan {total_combinations} kombinasi parameter")
    
    # Split data untuk validasi
    train_idx = int(0.7 * len(X_scaled))
    val_idx = int(0.85 * len(X_scaled))
    
    X_train = X_scaled[:train_idx]
    y_train = y_scaled[:train_idx]
    X_val = X_scaled[train_idx:val_idx]
    y_val = y_scaled[train_idx:val_idx]
    
    combination_idx = 0
    for seq_length in param_grid['seq_length']:
        for hidden_dim in param_grid['hidden_dim']:
            for num_layers in param_grid['num_layers']:
                for learning_rate in param_grid['learning_rate']:
                    for dropout in param_grid['dropout']:
                        combination_idx += 1
                        print(f"Testing combination {combination_idx}/{total_combinations}: "
                              f"seq={seq_length}, hidden={hidden_dim}, layers={num_layers}, "
                              f"lr={learning_rate}, dropout={dropout}")
                        
                        # Buat sequences untuk training
                        X_train_seq, y_train_seq = [], []
                        for i in range(len(X_train) - seq_length):
                            X_train_seq.append(X_train[i:i+seq_length])
                            y_train_seq.append(y_train[i+seq_length])
                        
                        X_train_tensor = torch.FloatTensor(np.array(X_train_seq))
                        y_train_tensor = torch.FloatTensor(np.array(y_train_seq)).view(-1, 1)
                        
                        # Buat sequences untuk validasi
                        X_val_seq, y_val_seq = [], []
                        for i in range(len(X_val) - seq_length):
                            X_val_seq.append(X_val[i:i+seq_length])
                            y_val_seq.append(y_val[i+seq_length])
                        
                        X_val_tensor = torch.FloatTensor(np.array(X_val_seq))
                        y_val_tensor = torch.FloatTensor(np.array(y_val_seq)).view(-1, 1)
                        
                        # Buat dan latih model
                        model = LSTMModel(
                            input_dim=len(features),
                            hidden_dim=hidden_dim,
                            output_dim=1,
                            num_layers=num_layers
                        )
                        
                        criterion = nn.MSELoss()
                        optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
                        
                        # Train dengan early stopping
                        patience = 5
                        patience_counter = 0
                        best_val_loss = float('inf')
                        
                        for epoch in range(50):  # Max 50 epochs
                            model.train()
                            optimizer.zero_grad()
                            outputs = model(X_train_tensor)
                            loss = criterion(outputs, y_train_tensor)
                            loss.backward()
                            optimizer.step()
                            
                            # Validasi
                            model.eval()
                            with torch.no_grad():
                                val_outputs = model(X_val_tensor)
                                val_loss = criterion(val_outputs, y_val_tensor)
                                
                                if val_loss < best_val_loss:
                                    best_val_loss = val_loss
                                    patience_counter = 0
                                else:
                                    patience_counter += 1
                                
                                if patience_counter >= patience:
                                    print(f"Early stopping pada epoch {epoch+1}")
                                    break
                            
                            if (epoch+1) % 10 == 0:
                                print(f"Epoch {epoch+1}, Train Loss: {loss.item():.6f}, Val Loss: {val_loss.item():.6f}")
                        
                        # Calculate RMSE on validation set
                        with torch.no_grad():
                            val_preds = model(X_val_tensor)
                            val_preds_np = val_preds.numpy().flatten()
                            y_val_actual = y_val_seq
                            
                            # Denormalize
                            val_preds_denorm = scaler_y.inverse_transform(val_preds_np.reshape(-1, 1)).flatten()
                            y_val_denorm = scaler_y.inverse_transform(np.array(y_val_actual).reshape(-1, 1)).flatten()
                            
                            # Calculate RMSE
                            val_rmse = np.sqrt(np.mean((val_preds_denorm - y_val_denorm) ** 2))
                            print(f"Validation RMSE: {val_rmse:.2f}")
                            
                            if val_rmse < best_rmse:
                                best_rmse = val_rmse
                                best_params = {
                                    'seq_length': seq_length,
                                    'hidden_dim': hidden_dim,
                                    'num_layers': num_layers,
                                    'learning_rate': learning_rate,
                                    'dropout': dropout
                                }
                                # Save best model
                                torch.save(model.state_dict(), data_dir / "bbca_lstm_model_best.pth")
    
    # Simpan parameter terbaik
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
    # Buat tabel jika belum ada
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS lstm_best_params (
        symbol VARCHAR(10),
        seq_length INTEGER,
        hidden_dim INTEGER,
        num_layers INTEGER,
        learning_rate NUMERIC,
        dropout NUMERIC,
        best_rmse NUMERIC,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol)
    )
    """)
    
    # Simpan parameter terbaik
    cursor.execute("""
    INSERT INTO lstm_best_params 
        (symbol, seq_length, hidden_dim, num_layers, learning_rate, dropout, best_rmse, updated_at)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (symbol) DO UPDATE SET
        seq_length = EXCLUDED.seq_length,
        hidden_dim = EXCLUDED.hidden_dim,
        num_layers = EXCLUDED.num_layers,
        learning_rate = EXCLUDED.learning_rate,
        dropout = EXCLUDED.dropout,
        best_rmse = EXCLUDED.best_rmse,
        updated_at = CURRENT_TIMESTAMP
    """, ('BBCA', best_params['seq_length'], best_params['hidden_dim'], best_params['num_layers'], 
          best_params['learning_rate'], best_params['dropout'], best_rmse))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Parameter terbaik untuk BBCA LSTM: {best_params}, RMSE: {best_rmse:.2f}")
    
    return best_params

def train_bbca_lstm(**kwargs):
    """Train LSTM model for BBCA"""
    ti = kwargs['ti']
    data_path = ti.xcom_pull(task_ids='extract_bbca_data')
    
    # Coba ambil hasil hyperparameter tuning jika ada
    try:
        best_params = ti.xcom_pull(task_ids='hyperparameter_tuning')
        print(f"Menggunakan parameter hasil tuning: {best_params}")
    except:
        print("Hyperparameter tuning tidak tersedia, menggunakan parameter default")
        best_params = {
            'seq_length': 10,
            'hidden_dim': 32,
            'num_layers': 2,
            'learning_rate': 0.01,
            'dropout': 0.2
        }
    
    # Load data
    df = pd.read_csv(data_path)
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Select features
    features = ['close', 'volume', 'percent_change', 'macd_line', 'rsi']
    
    # Add missing features with default value 0
    for feature in features:
        if feature not in df.columns:
            df[feature] = 0
    
    # Filter columns to use
    df_selected = df[['date'] + features].copy()
    
    # Split data (80% training, 20% testing)
    train_size = int(len(df_selected) * 0.8)
    train_data = df_selected[:train_size]
    test_data = df_selected[train_size:]
    
    # Normalize data using MinMaxScaler
    scaler = MinMaxScaler(feature_range=(0, 1))
    
    # Save the scaler for close column
    close_scaler = MinMaxScaler(feature_range=(0, 1))
    close_scaler.fit(df_selected[['close']])
    
    # Normalize all numeric features
    scaled_train = pd.DataFrame(
        scaler.fit_transform(train_data[features]),
        columns=features
    )
    
    # Use sequence length from hyperparameter tuning
    seq_length = best_params.get('seq_length', 60)
    
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
    
    # Create and train model with tuned parameters
    input_dim = len(features)
    hidden_dim = best_params.get('hidden_dim', 32)
    output_dim = 1
    num_layers = best_params.get('num_layers', 2)
    dropout = best_params.get('dropout', 0.2)
    
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
        dropout=dropout
    )
    
    # Loss function and optimizer - PERBAIKAN: Menggunakan torch.optim bukan optim
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=best_params.get('learning_rate', 0.01))
    
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
            # Simpan model terbaik
            torch.save(model.state_dict(), Path("/opt/airflow/data/lstm") / "bbca_lstm_model_best.pth")
            patience_counter = 0
        else:
            patience_counter += 1
            if patience_counter >= patience:
                print(f'Early stopping pada epoch {epoch+1}')
                break
        
        if (epoch+1) % 10 == 0:
            print(f'Epoch [{epoch+1}/{num_epochs}], Loss: {loss.item():.6f}')
    
    # Save model and scalers
    data_dir = Path("/opt/airflow/data/lstm")
    model_path = data_dir / "bbca_lstm_model.pth"
    
    # Gunakan model terbaik
    model.load_state_dict(torch.load(data_dir / "bbca_lstm_model_best.pth"))
    
    # Save final model
    torch.save(model.state_dict(), model_path)
    
    # Save scalers
    scaler_path = data_dir / "bbca_scaler.pkl"
    with open(scaler_path, 'wb') as f:
        pickle.dump(scaler, f)
    
    close_scaler_path = data_dir / "bbca_close_scaler.pkl"
    with open(close_scaler_path, 'wb') as f:
        pickle.dump(close_scaler, f)
    
    print(f"✅ LSTM model untuk BBCA berhasil dilatih dan disimpan")
    return str(model_path)

def predict_bbca_prices(**kwargs):
    """Make predictions using trained models including LSTM, RandomForest and XGBoost"""
    
    # Extract the data path from the previous task
    ti = kwargs['ti']
    data_path = ti.xcom_pull(task_ids='extract_bbca_data')
    
    # Load data
    df = pd.read_csv(data_path)
    df['date'] = pd.to_datetime(df['date'])
    
    # Load scalers
    data_dir = Path("/opt/airflow/data/lstm")
    
    with open(data_dir / "bbca_scaler.pkl", 'rb') as f:
        scaler = pickle.load(f)
    
    with open(data_dir / "bbca_close_scaler.pkl", 'rb') as f:
        close_scaler = pickle.load(f)
    
    # Load the LSTM model
    try:
        state_dict = torch.load(data_dir / "bbca_lstm_model.pth")
        
        # Check if it's a state_dict or full model
        if isinstance(state_dict, (collections.OrderedDict, dict)):
            # Use the exact same architecture as used during training
            input_dim = 5  # Number of features
            hidden_dim = 64  # This should match the dimension used during training (appears to be 64 not 32)
            num_layers = 2  # Based on error, model was trained with 2 layers
            lstm_model = LSTMModel(input_dim, hidden_dim, output_dim=1, num_layers=num_layers)
            lstm_model.load_state_dict(state_dict)
        else:
            lstm_model = state_dict
    except Exception as e:
        # Fallback if LSTM model fails to load
        print(f"Error loading PyTorch LSTM model: {e}")
        lstm_model = None
    
    # Prepare data for prediction
    features = ['close', 'volume', 'percent_change', 'macd_line', 'rsi']
    
    # Add missing features with default value 0
    for feature in features:
        if feature not in df.columns:
            df[feature] = 0
    
    # Get last 60 days for prediction
    last_60_days = df[features].tail(60).values
    last_60_days_scaled = scaler.transform(last_60_days)
    X_test = torch.FloatTensor(last_60_days_scaled).unsqueeze(0)
    
    # Initialize RandomForest if LSTM fails
    from sklearn.ensemble import RandomForestRegressor  # Backup import in case the top-level import fails
    rf_model = RandomForestRegressor(n_estimators=200, random_state=42)
    
    # Prepare data for RandomForest
    rf_X = df[features].tail(100).values
    rf_y = df['close'].tail(100).values
    rf_model.fit(rf_X, rf_y)
    
    # Try loading XGBoost model if available
    try:
        import xgboost as xgb
        xgb_model = xgb.XGBRegressor(n_estimators=100, learning_rate=0.08, gamma=0)
        xgb_model.fit(rf_X, rf_y)
        use_xgb = True
    except ImportError:
        use_xgb = False
    
    # Make LSTM prediction if model is loaded
    if lstm_model:
        lstm_model.eval()
        with torch.no_grad():
            lstm_pred = lstm_model(X_test)
            lstm_pred_close = close_scaler.inverse_transform(lstm_pred.detach().numpy())[0][0]
            lstm_pred_close = float(lstm_pred_close)
    else:
        lstm_pred_close = None
    
    # Make RandomForest prediction
    rf_pred_close = float(rf_model.predict(df[features].tail(1).values)[0])
    
    # Make XGBoost prediction if model is available
    if use_xgb:
        xgb_pred_close = float(xgb_model.predict(df[features].tail(1).values)[0])
    else:
        xgb_pred_close = None
    
    # Ensemble prediction (weighted average)
    if lstm_pred_close is not None:
        if use_xgb:
            predicted_close = (0.6 * lstm_pred_close) + (0.2 * rf_pred_close) + (0.2 * xgb_pred_close)
        else:
            predicted_close = (0.7 * lstm_pred_close) + (0.3 * rf_pred_close)
    else:
        # If LSTM model fails, use RandomForest and XGBoost for prediction
        if use_xgb:
            predicted_close = (0.5 * rf_pred_close) + (0.5 * xgb_pred_close)
        else:
            predicted_close = rf_pred_close
    
    # Get the next business date (skip weekends)
    last_date = df['date'].iloc[-1]
    next_date = last_date + pd.Timedelta(days=1)
    
    while next_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
        next_date = next_date + pd.Timedelta(days=1)
    
    # Save the prediction to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
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
    
    # Save prediction
    cursor.execute("""
    INSERT INTO stock_predictions (symbol, prediction_date, predicted_close)
    VALUES (%s, %s, %s)
    ON CONFLICT (symbol, prediction_date) 
    DO UPDATE SET 
        predicted_close = EXCLUDED.predicted_close,
        created_at = CURRENT_TIMESTAMP
    """, ('BBCA', next_date, predicted_close))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Prediksi BBCA untuk {next_date}: Rp {predicted_close:,.2f}")
    return predicted_close

def update_actual_prices(**kwargs):
    """Update actual prices after market close and calculate model performance metrics"""
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
    # Update actual prices from daily_stock_summary (bukan daily_stock_metrics yang di schema analytics)
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
        AND sp.symbol = 'BBCA'
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
        if len(row) >= 4:  # Pastikan row memiliki cukup elemen
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
            """, (symbol, "LSTM", rmse, mae, mape, pred_count))
    
    conn.commit()
    
    # Log results
    for row in model_scores:
        if len(row) >= 4:  # Pastikan row memiliki cukup elemen
            symbol, rmse, mae, mape, pred_count = row
            print(f"Model LSTM untuk {symbol}: RMSE {rmse:.2f}, MAE {mae:.2f}, MAPE {mape:.2f}% (berdasarkan {pred_count} prediksi)")
    
    cursor.close()
    conn.close()
    
    print(f"✅ Updated {rows_updated} baris dengan harga aktual dan menghitung metrik model")
    return rows_updated

def backtest_lstm_model(**kwargs):
    """
    Melakukan backtesting model LSTM dengan data historis yang realistis
    (tanpa menggunakan kolom close pada hari prediksi)
    """
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    
    # Ambil data historis dengan lebih banyak fitur
    query = """
    SELECT 
        date, 
        close, 
        open_price,  -- Tambah fitur yang tersedia di awal hari
        prev_close,  -- Tersedia di awal hari
        volume,      -- Mungkin menggunakan volume H-1
        high,        -- Akan dihapus saat prediksi
        low,         -- Akan dihapus saat prediksi
        CASE 
            WHEN prev_close IS NOT NULL AND prev_close != 0 
            THEN (close - prev_close) / prev_close * 100
            ELSE NULL
        END as percent_change
    FROM daily_stock_summary
    WHERE symbol = 'BBCA'
    ORDER BY date ASC
    """
    
    df = pd.read_sql(query, conn)
    df['date'] = pd.to_datetime(df['date'])
    
    # Tambah fitur lag (fitur dari hari sebelumnya)
    for feat in ['volume', 'percent_change']:
        df[f'{feat}_lag1'] = df[feat].shift(1)
    
    # Tambah fitur teknikal sederhana
    df['price_momentum'] = df['close'].pct_change(5)  # Momentum 5 hari
    df['volatility'] = df['close'].rolling(10).std() / df['close']  # Volatilitas relatif
    
    # Handle missing values
    df = df.dropna().reset_index(drop=True)
    
    # Exit jika data tidak cukup
    if len(df) < 100:
        print("❌ Data tidak cukup untuk backtesting (minimal 100 baris)")
        return 0
    
    # Split data untuk backtesting (misal 60 hari terakhir)
    test_size = min(60, int(len(df) * 0.2))  # 20% data atau 60 hari, mana yang lebih kecil
    train_data = df[:-test_size].copy()
    test_data = df[-test_size:].copy()
    
    # Pilih fitur untuk model
    # Gunakan hanya fitur yang tersedia di awal hari (sebelum close diketahui)
    features = [
        'prev_close',         # Harga penutupan kemarin
        'open_price',         # Harga pembukaan hari ini
        'volume_lag1',        # Volume perdagangan kemarin
        'percent_change_lag1', # Perubahan harga kemarin
        'price_momentum',     # Momentum harga
        'volatility'          # Volatilitas
    ]
    
    # Normalize data
    scaler = MinMaxScaler(feature_range=(0, 1))
    close_scaler = MinMaxScaler(feature_range=(0, 1))
    
    close_scaler.fit(df[['close']])
    train_features = train_data[features]
    scaled_train_features = scaler.fit_transform(train_features)
    
    # Target adalah harga penutupan
    train_target = train_data['close'].values
    scaled_train_target = close_scaler.transform(train_target.reshape(-1, 1)).flatten()
    
    # Buat sequences untuk training
    seq_length = 10  # Panjang sequence 10 hari
    X, y = [], []
    
    for i in range(len(scaled_train_features) - seq_length):
        X.append(scaled_train_features[i:i+seq_length])
        y.append(scaled_train_target[i+seq_length])
    
    X = np.array(X)
    y = np.array(y)
    
    # Train model
    X_tensor = torch.FloatTensor(X)
    y_tensor = torch.FloatTensor(y).view(-1, 1)
    
    input_dim = len(features)
    hidden_dim = 32
    output_dim = 1
    num_layers = 2
    
    model = LSTMModel(input_dim, hidden_dim, output_dim, num_layers)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    
    num_epochs = 50
    for epoch in range(num_epochs):
        model.train()
        outputs = model(X_tensor)
        optimizer.zero_grad()
        loss = criterion(outputs, y_tensor)
        loss.backward()
        optimizer.step()
        
        if (epoch+1) % 10 == 0:
            print(f'Epoch [{epoch+1}/{num_epochs}], Loss: {loss.item():.6f}')
    
    # Lakukan prediksi untuk test data
    predictions = []
    actual_values = []
    test_features = test_data[features]
    scaled_test_features = scaler.transform(test_features)
    
    # Untuk setiap titik di data test, buat prediksi
    with torch.no_grad():
        model.eval()
        for i in range(len(test_data) - seq_length):
            # Ambil sequence
            test_sequence = scaled_test_features[i:i+seq_length]
            X_test = torch.FloatTensor(test_sequence).unsqueeze(0)
            
            # Prediksi
            pred_scaled = model(X_test)
            
            # Inverse transform
            pred_value = close_scaler.inverse_transform(
                pred_scaled.detach().numpy()
            )[0][0]
            
            # Konversi ke float standar
            if isinstance(pred_value, (np.ndarray, np.float32, np.float64)):
                if hasattr(pred_value, 'item'):
                    pred_value = float(pred_value.item())
                else:
                    pred_value = float(pred_value)
            
            # Simpan prediksi dan nilai aktual
            predictions.append(pred_value)
            actual_values.append(test_data['close'].iloc[i+seq_length])
    
    # Hitung metrics
    errors = np.abs(np.array(predictions) - np.array(actual_values))
    rmse = np.sqrt(np.mean(np.square(errors)))
    mae = np.mean(errors)
    mape = np.mean(errors / np.array(actual_values)) * 100
    
    # Simpan hasil dan metrik ke database
    cursor = conn.cursor()
    
    # Simpan metrik
    cursor.execute("""
    INSERT INTO lstm_backtesting_results 
    (model_type, symbol, test_start_date, test_end_date, rmse, mae, mape, test_size, features_used, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (model_type, symbol, test_start_date, test_end_date) DO UPDATE SET
    rmse = EXCLUDED.rmse,
    mae = EXCLUDED.mae,
    mape = EXCLUDED.mape,
    test_size = EXCLUDED.test_size,
    features_used = EXCLUDED.features_used,
    created_at = CURRENT_TIMESTAMP
    """, (
        'LSTM',
        'BBCA',
        test_data['date'].iloc[seq_length].date(),
        test_data['date'].iloc[-1].date(),
        rmse,
        mae,
        mape,
        len(predictions),
        ','.join(features)
    ))
    
    # Simpan detail prediksi vs aktual
    prediction_dates = test_data['date'].iloc[seq_length:seq_length+len(predictions)]
    
    for pred, actual, date in zip(predictions, actual_values, prediction_dates):
        error = abs(pred - actual)
        error_pct = (error / actual) * 100 if actual != 0 else 0
        
        cursor.execute("""
        INSERT INTO lstm_backtesting_predictions
        (model_type, symbol, prediction_date, predicted_close, actual_close, prediction_error, error_percentage, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (model_type, symbol, prediction_date) DO UPDATE SET
        predicted_close = EXCLUDED.predicted_close,
        actual_close = EXCLUDED.actual_close,
        prediction_error = EXCLUDED.prediction_error,
        error_percentage = EXCLUDED.error_percentage,
        created_at = CURRENT_TIMESTAMP
        """, (
           'LSTM',
           'BBCA',
           date.date(),
           pred,
           actual,
           error,
           error_pct
       ))
   
   # Update model_performance_metrics dengan hasil backtesting
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
    """, ('BBCA', "LSTM_Backtest", rmse, mae, mape, len(predictions)))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Backtesting realistis selesai. RMSE: {rmse:.2f}, MAE: {mae:.2f}, MAPE: {mape:.2f}%")
    
    # Tambahan: log hasil sebagai score
    print(f"Model terbaik untuk prediksi harga BBCA: LSTM dengan RMSE {rmse:.2f}")
    
    return rmse

    # DAG definition
with DAG(
    dag_id="bbca_lstm_prediction",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="30 18 * * 1-5",  # 18:30 setiap hari kerja
    catchup=False,
    default_args=default_args,
    tags=["lstm", "prediction", "bbca"]
) as dag:
    
    # Wait for data transformation to complete
    wait_for_transformation = ExternalTaskSensor(
        task_id="wait_for_transformation",
        external_dag_id="data_transformation",
        external_task_id="end_task",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )
    
    # Create necessary tables
    create_table = PythonOperator(
        task_id="create_stock_predictions_table",
        python_callable=create_stock_predictions_table,
        dag=dag
    )
    
    # Extract data
    extract_data = PythonOperator(
        task_id="extract_bbca_data",
        python_callable=extract_bbca_data
    )

    feature_eng_task = PythonOperator(
    task_id="feature_engineering",
    python_callable=feature_engineering,
    dag=dag
    )

    tuning_task = PythonOperator(
    task_id="hyperparameter_tuning",
    python_callable=hyperparameter_tuning,
    dag=dag
    )

    # Train LSTM model
    train_model = PythonOperator(
        task_id="train_bbca_lstm",
        python_callable=train_bbca_lstm
    )
    
    # Make predictions
    predict_prices = PythonOperator(
        task_id="predict_bbca_prices",
        python_callable=predict_bbca_prices
    )
    
    # Update actual prices (run next day)
    update_actual = PythonOperator(
        task_id="update_actual_prices",
        python_callable=update_actual_prices
    )
    
    # Run backtesting
    backtest_model = PythonOperator(
        task_id="backtest_lstm_model",
        python_callable=backtest_lstm_model
    )
    
    # Run dbt models for LSTM
    run_lstm_dbt = BashOperator(
        task_id='run_lstm_dbt',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --select stg_stock_predictions fct_stock_predictions lstm_performance_metrics stock_prediction_dashboard',
    )
    
    # Define task order
    wait_for_transformation >> create_table >> extract_data
    extract_data >> feature_eng_task >> tuning_task >> train_model
    train_model >> [predict_prices, backtest_model]
    predict_prices >> update_actual >> run_lstm_dbt
    backtest_model >> run_lstm_dbt
                       





# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.sensors.external_task import ExternalTaskSensor
# import pendulum
# import torch
# import torch.nn as nn
# import pandas as pd
# import numpy as np
# from sklearn.preprocessing import MinMaxScaler
# import psycopg2
# import pickle
# from pathlib import Path
# import collections

# local_tz = pendulum.timezone("Asia/Jakarta")

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': pendulum.duration(minutes=5)
# }

# # Model LSTM dengan PyTorch
# class LSTMModel(torch.nn.Module):
#     def __init__(self, input_dim, hidden_dim, output_dim=1, num_layers=2):
#         super(LSTMModel, self).__init__()
#         self.hidden_dim = hidden_dim
#         self.num_layers = num_layers
        
#         # LSTM layer
#         self.lstm = torch.nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True, dropout=0.2)
        
#         # Fully connected layer
#         self.fc = torch.nn.Linear(hidden_dim, output_dim)
        
#     def forward(self, x):
#         h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_dim).to(x.device)
#         c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_dim).to(x.device)
        
#         out, _ = self.lstm(x, (h0, c0))
#         out = self.fc(out[:, -1, :])
#         return out
# def create_stock_predictions_table():
#     """Membuat tabel stock_predictions jika belum ada"""
#     conn = psycopg2.connect(
#         host="postgres",
#         dbname="airflow",
#         user="airflow",
#         password="airflow"
#     )
#     cursor = conn.cursor()
    
#     cursor.execute("""
#     CREATE TABLE IF NOT EXISTS stock_predictions (
#         symbol VARCHAR(10),
#         prediction_date DATE,
#         predicted_close NUMERIC(16,2),
#         actual_close NUMERIC(16,2),
#         prediction_error NUMERIC(16,2),
#         error_percentage NUMERIC(8,2),
#         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#         PRIMARY KEY (symbol, prediction_date)
#     )
#     """)
    
#     conn.commit()
#     cursor.close()
#     conn.close()
    
#     print("✅ Tabel stock_predictions dibuat")

# def extract_bbca_data(**kwargs):
#     """Extract data for BBCA from PostgreSQL"""
#     conn = psycopg2.connect(
#         host="postgres",
#         dbname="airflow",
#         user="airflow",
#         password="airflow"
#     )
    
#     # Perbarui query untuk mengakses tabel asli, bukan tabel hasil dbt
#     query = """
#     SELECT 
#         d.date, 
#         d.close, 
#         d.volume, 
#         CASE 
#             WHEN d.prev_close IS NOT NULL AND d.prev_close != 0 
#             THEN (d.close - d.prev_close) / d.prev_close * 100
#             ELSE NULL
#         END as percent_change,
#         0 as macd_line,  -- Dummy value, replace with actual calculation if available 
#         0 as signal_line, -- Dummy value, replace with actual calculation if available
#         0 as rsi  -- Dummy value, replace with actual calculation if available
#     FROM 
#         daily_stock_summary d
#     WHERE 
#         d.symbol = 'BBCA'
#     ORDER BY 
#         d.date ASC
#     """
    
#     df = pd.read_sql(query, conn)
#     conn.close()
    
#     # Handle missing values
#     df = df.fillna(method='ffill')
    
#     # Save data to CSV
#     data_dir = Path("/opt/airflow/data/lstm")
#     data_dir.mkdir(exist_ok=True, parents=True)
#     csv_path = data_dir / "bbca_data.csv"
#     df.to_csv(csv_path, index=False)
    
#     print(f"✅ Data BBCA diambil: {len(df)} baris")
#     return str(csv_path)

# def train_bbca_lstm(**kwargs):
#     """Train LSTM model for BBCA"""
#     ti = kwargs['ti']
#     data_path = ti.xcom_pull(task_ids='extract_bbca_data')
    
#     # Load data
#     df = pd.read_csv(data_path)
    
#     # Convert date column to datetime
#     df['date'] = pd.to_datetime(df['date'])
    
#     # Select features
#     features = ['close', 'volume', 'percent_change', 'macd_line', 'rsi']
    
#     # Add missing features with default value 0
#     for feature in features:
#         if feature not in df.columns:
#             df[feature] = 0
    
#     # Filter columns to use
#     df_selected = df[['date'] + features].copy()
    
#     # Split data (80% training, 20% testing)
#     train_size = int(len(df_selected) * 0.8)
#     train_data = df_selected[:train_size]
#     test_data = df_selected[train_size:]
    
#     # Normalize data using MinMaxScaler
#     scaler = MinMaxScaler(feature_range=(0, 1))
    
#     # Save the scaler for close column
#     close_scaler = MinMaxScaler(feature_range=(0, 1))
#     close_scaler.fit(df_selected[['close']])
    
#     # Normalize all numeric features
#     scaled_train = pd.DataFrame(
#         scaler.fit_transform(train_data[features]),
#         columns=features
#     )
    
#     # Create sequences (60 days to predict next day)
#     seq_length = 60
#     X, y = [], []
    
#     for i in range(len(scaled_train) - seq_length):
#         X.append(scaled_train.iloc[i:i+seq_length].values)
#         y.append(scaled_train.iloc[i+seq_length]['close'])
    
#     X = np.array(X)
#     y = np.array(y)
    
#     # Convert to PyTorch tensors
#     X_tensor = torch.FloatTensor(X)
#     y_tensor = torch.FloatTensor(y).view(-1, 1)
    
#     # Create and train model
#     input_dim = len(features)  # Number of features
#     hidden_dim = 32  # Hidden dimension
#     output_dim = 1  # Output dimension (close price)
#     num_layers = 2  # Number of LSTM layers
    
#     model = LSTMModel(input_dim, hidden_dim, output_dim, num_layers)
    
#     # Loss function and optimizer
#     criterion = nn.MSELoss()
#     optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    
#     # Train model
#     num_epochs = 100
#     for epoch in range(num_epochs):
#         outputs = model(X_tensor)
#         optimizer.zero_grad()
        
#         # Obtain the loss function
#         loss = criterion(outputs, y_tensor)
#         loss.backward()
        
#         optimizer.step()
        
#         if (epoch+1) % 10 == 0:
#             print(f'Epoch [{epoch+1}/{num_epochs}], Loss: {loss.item():.6f}')
    
#     # Save model
#     data_dir = Path("/opt/airflow/data/lstm")
#     model_path = data_dir / "bbca_lstm_model.pth"
#     torch.save(model.state_dict(), model_path)
    
#     # Save scalers
#     scaler_path = data_dir / "bbca_scaler.pkl"
#     with open(scaler_path, 'wb') as f:
#         pickle.dump(scaler, f)
    
#     close_scaler_path = data_dir / "bbca_close_scaler.pkl"
#     with open(close_scaler_path, 'wb') as f:
#         pickle.dump(close_scaler, f)
    
#     print(f"✅ LSTM model untuk BBCA berhasil dilatih dan disimpan")
#     return str(model_path)

# def predict_bbca_prices(**kwargs):
#     """Make predictions using trained LSTM model"""
#     ti = kwargs['ti']
#     data_path = ti.xcom_pull(task_ids='extract_bbca_data')
#     model_path = ti.xcom_pull(task_ids='train_bbca_lstm')
    
#     # Load data
#     df = pd.read_csv(data_path)
#     df['date'] = pd.to_datetime(df['date'])
    
#     # Load model and scalers
#     data_dir = Path("/opt/airflow/data/lstm")
    
#     # Load scalers
#     with open(data_dir / "bbca_scaler.pkl", 'rb') as f:
#         scaler = pickle.load(f)
    
#     with open(data_dir / "bbca_close_scaler.pkl", 'rb') as f:
#         close_scaler = pickle.load(f)
    
#     # PERBAIKAN LOADING MODEL: Cek apakah model disimpan sebagai state_dict atau model penuh
#     try:
#         # Coba load sebagai model PyTorch penuh
#         model = torch.load(data_dir / "bbca_lstm_model.pth")
        
#         # Cek jika model adalah OrderedDict (state_dict), bukan model penuh
#         if isinstance(model, collections.OrderedDict) or isinstance(model, dict):
#             # Ini adalah state_dict, perlu membuat instance model dulu
#             input_dim = 5  # Jumlah features
#             hidden_dim = 32
#             model_instance = LSTMModel(input_dim, hidden_dim)  # Ganti dengan kelas model yang tepat
#             model_instance.load_state_dict(model)
#             model = model_instance
#     except Exception as e:
#         # Jika terjadi error saat loading model, coba pendekatan lain 
#         print(f"Error loading PyTorch model: {e}")
        
#         # Buat model RandomForest sederhana sebagai fallback
#         from sklearn.ensemble import RandomForestRegressor
#         model = RandomForestRegressor(n_estimators=100, random_state=42)
        
#         # Fit model dengan data dummy (ini hanya fallback)
#         X = df[['volume', 'percent_change', 'macd_line', 'rsi']].tail(60).values
#         y = df['close'].tail(60).values
#         model.fit(X, y)
        
#         # Bypass PyTorch prediction process
#         last_data = df[['volume', 'percent_change', 'macd_line', 'rsi']].tail(1).values
#         predicted_close = float(model.predict(last_data)[0])
        
#         # Skip the rest of PyTorch-specific code
#         last_date = df['date'].iloc[-1]
#         next_date = last_date + pd.Timedelta(days=1)
        
#         # Skip weekends
#         while next_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
#             next_date = next_date + pd.Timedelta(days=1)
        
#         # Save to database...
#         conn = psycopg2.connect(
#             host="postgres",
#             dbname="airflow",
#             user="airflow",
#             password="airflow"
#         )
#         cursor = conn.cursor()
        
#         # Create table if not exists
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS stock_predictions (
#             symbol VARCHAR(10),
#             prediction_date DATE,
#             predicted_close NUMERIC(16,2),
#             actual_close NUMERIC(16,2),
#             prediction_error NUMERIC(16,2),
#             error_percentage NUMERIC(8,2),
#             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#             PRIMARY KEY (symbol, prediction_date)
#         )
#         """)
        
#         # Save prediction
#         cursor.execute("""
#         INSERT INTO stock_predictions (symbol, prediction_date, predicted_close)
#         VALUES (%s, %s, %s)
#         ON CONFLICT (symbol, prediction_date) 
#         DO UPDATE SET 
#             predicted_close = EXCLUDED.predicted_close,
#             created_at = CURRENT_TIMESTAMP
#         """, ('BBCA', next_date, predicted_close))
        
#         conn.commit()
#         cursor.close()
#         conn.close()
        
#         print(f"✅ Prediksi BBCA untuk {next_date} (fallback): Rp {predicted_close:,.2f}")
#         return predicted_close
    
#     # Gunakan model untuk prediksi jika model berhasil dimuat
#     model.eval()  # Set model to evaluation mode
    
#     # Features
#     features = ['close', 'volume', 'percent_change', 'macd_line', 'rsi']
    
#     # Add missing features with default value 0
#     for feature in features:
#         if feature not in df.columns:
#             df[feature] = 0
    
#     # Get last 60 days for prediction
#     last_60_days = df[features].tail(60).values
    
#     # Scale the data
#     last_60_days_scaled = scaler.transform(last_60_days)
    
#     # Prepare data for LSTM (add batch dimension)
#     X_test = torch.FloatTensor(last_60_days_scaled).unsqueeze(0)
    
#     # Make prediction
#     with torch.no_grad():
#         predicted_scaled = model(X_test)
    
#     # Inverse transform
#     predicted_close = close_scaler.inverse_transform(
#         predicted_scaled.detach().numpy()
#     )[0][0]
    
#     # Konversi numpy.float32 ke Python float standar
#     if isinstance(predicted_close, (np.ndarray, np.float32, np.float64)):
#         # Jika berbentuk array, ambil elemen pertama
#         if hasattr(predicted_close, 'shape') and predicted_close.shape:
#             predicted_close = float(predicted_close.item())
#         else:
#             predicted_close = float(predicted_close)
    
#     # Prepare dates for prediction
#     last_date = df['date'].iloc[-1]
#     next_date = last_date + pd.Timedelta(days=1)
    
#     # Skip weekends
#     while next_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
#         next_date = next_date + pd.Timedelta(days=1)
    
#     # Save prediction to database
#     conn = psycopg2.connect(
#         host="postgres",
#         dbname="airflow",
#         user="airflow",
#         password="airflow"
#     )
#     cursor = conn.cursor()
    
#     # Create table if not exists
#     cursor.execute("""
#     CREATE TABLE IF NOT EXISTS stock_predictions (
#         symbol VARCHAR(10),
#         prediction_date DATE,
#         predicted_close NUMERIC(16,2),
#         actual_close NUMERIC(16,2),
#         prediction_error NUMERIC(16,2),
#         error_percentage NUMERIC(8,2),
#         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#         PRIMARY KEY (symbol, prediction_date)
#     )
#     """)
    
#     # Save prediction
#     cursor.execute("""
#     INSERT INTO stock_predictions (symbol, prediction_date, predicted_close)
#     VALUES (%s, %s, %s)
#     ON CONFLICT (symbol, prediction_date) 
#     DO UPDATE SET 
#         predicted_close = EXCLUDED.predicted_close,
#         created_at = CURRENT_TIMESTAMP
#     """, ('BBCA', next_date, predicted_close))
    
#     conn.commit()
#     cursor.close()
#     conn.close()
    
#     print(f"✅ Prediksi BBCA untuk {next_date}: Rp {predicted_close:,.2f}")
#     return predicted_close

# def update_actual_prices(**kwargs):
#     """Update actual prices after market close"""
#     conn = psycopg2.connect(
#         host="postgres",
#         dbname="airflow",
#         user="airflow",
#         password="airflow"
#     )
#     cursor = conn.cursor()
    
#     # Update actual price from daily_stock_metrics
#     cursor.execute("""
#     UPDATE stock_predictions sp
#     SET 
#         actual_close = dsm.close,
#         prediction_error = ABS(sp.predicted_close - dsm.close),
#         error_percentage = (ABS(sp.predicted_close - dsm.close) / dsm.close) * 100
#     FROM public_analytics.daily_stock_metrics dsm
#     WHERE 
#         sp.symbol = dsm.symbol
#         AND sp.prediction_date = dsm.date
#         AND sp.symbol = 'BBCA'
#         AND sp.actual_close IS NULL
#     """)
    
#     rows_updated = cursor.rowcount
#     conn.commit()
#     cursor.close()
#     conn.close()
    
#     print(f"✅ Updated {rows_updated} baris dengan harga aktual")



# # DAG definition
# with DAG(
#     dag_id="bbca_lstm_prediction",
#     start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
#     schedule_interval="30 18 * * 1-5",  # 18:30 setiap hari kerja
#     catchup=False,
#     default_args=default_args,
#     tags=["lstm", "prediction", "bbca"]
# ) as dag:
    
#     # Wait for data transformation to complete
#     wait_for_transformation = ExternalTaskSensor(
#     task_id="wait_for_transformation",
#     external_dag_id="data_transformation",
#     external_task_id="end_task",
#     mode="reschedule",
#     timeout=3600,
#     poke_interval=60,
#     allowed_states=["success"],
#     failed_states=["failed", "skipped"]
#     )

#     create_table = PythonOperator(
#     task_id="create_stock_predictions_table",
#     python_callable=create_stock_predictions_table,
#     dag=dag
#     )
    
#     # Extract data
#     extract_data = PythonOperator(
#         task_id="extract_bbca_data",
#         python_callable=extract_bbca_data
#     )
    
#     # Train LSTM model
#     train_model = PythonOperator(
#         task_id="train_bbca_lstm",
#         python_callable=train_bbca_lstm
#     )
    
#     # Make predictions
#     predict_prices = PythonOperator(
#         task_id="predict_bbca_prices",
#         python_callable=predict_bbca_prices
#     )
    
#     # Update actual prices (run next day)
#     update_actual = PythonOperator(
#         task_id="update_actual_prices",
#         python_callable=update_actual_prices
#     )

#     run_lstm_dbt = BashOperator(
#     task_id='run_lstm_dbt',
#     bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --select stg_stock_predictions fct_stock_predictions lstm_performance_metrics stock_prediction_dashboard',
#     )
    
#     # Define task order
#     wait_for_transformation >> create_table >> extract_data >> train_model >> predict_prices >> update_actual >> run_lstm_dbt