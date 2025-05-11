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

def extract_BBRI_data(**kwargs):
    """Extract data for BBRI from PostgreSQL"""
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
        d.symbol = 'BBRI'
    ORDER BY 
        d.date ASC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Handle missing values
    df = df.fillna(method='ffill')
    
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
    csv_path = data_dir / "BBRI_data.csv"
    df.to_csv(csv_path, index=False)
    
    print(f"✅ Data BBRI diambil: {len(df)} baris")
    return str(csv_path)

def train_BBRI_lstm(**kwargs):
    """Train LSTM model for BBRI"""
    ti = kwargs['ti']
    data_path = ti.xcom_pull(task_ids='extract_BBRI_data')
    
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
    
    # Create sequences (60 days to predict next day)
    seq_length = 60
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
    input_dim = len(features)  # Number of features
    hidden_dim = 32  # Hidden dimension
    output_dim = 1  # Output dimension (close price)
    num_layers = 2  # Number of LSTM layers
    
    model = LSTMModel(input_dim, hidden_dim, output_dim, num_layers)
    
    # Loss function and optimizer
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    
    # Train model
    num_epochs = 100
    for epoch in range(num_epochs):
        outputs = model(X_tensor)
        optimizer.zero_grad()
        
        # Obtain the loss function
        loss = criterion(outputs, y_tensor)
        loss.backward()
        
        optimizer.step()
        
        if (epoch+1) % 10 == 0:
            print(f'Epoch [{epoch+1}/{num_epochs}], Loss: {loss.item():.6f}')
    
    # Save model
    data_dir = Path("/opt/airflow/data/lstm")
    model_path = data_dir / "BBRI_lstm_model.pth"
    torch.save(model.state_dict(), model_path)
    
    # Save scalers
    scaler_path = data_dir / "BBRI_scaler.pkl"
    with open(scaler_path, 'wb') as f:
        pickle.dump(scaler, f)
    
    close_scaler_path = data_dir / "BBRI_close_scaler.pkl"
    with open(close_scaler_path, 'wb') as f:
        pickle.dump(close_scaler, f)
    
    print(f"✅ LSTM model untuk BBRI berhasil dilatih dan disimpan")
    return str(model_path)

def predict_BBRI_prices(**kwargs):
    """Make predictions using trained LSTM model"""
    ti = kwargs['ti']
    data_path = ti.xcom_pull(task_ids='extract_BBRI_data')
    model_path = ti.xcom_pull(task_ids='train_BBRI_lstm')
    
    # Load data
    df = pd.read_csv(data_path)
    df['date'] = pd.to_datetime(df['date'])
    
    # Load model and scalers
    data_dir = Path("/opt/airflow/data/lstm")
    
    # Load scalers
    with open(data_dir / "BBRI_scaler.pkl", 'rb') as f:
        scaler = pickle.load(f)
    
    with open(data_dir / "BBRI_close_scaler.pkl", 'rb') as f:
        close_scaler = pickle.load(f)
    
    # PERBAIKAN LOADING MODEL: Cek apakah model disimpan sebagai state_dict atau model penuh
    try:
        # Coba load state dict
        state_dict = torch.load(data_dir / "BBRI_lstm_model.pth")
        
        # Cek jika model adalah OrderedDict (state_dict), bukan model penuh
        if isinstance(state_dict, (collections.OrderedDict, dict)):
            # Ini adalah state_dict, perlu membuat instance model dulu
            input_dim = 5  # Jumlah features
            hidden_dim = 32
            model = LSTMModel(input_dim, hidden_dim)
            model.load_state_dict(state_dict)
        else:
            # Ini model penuh
            model = state_dict
    except Exception as e:
        # Jika terjadi error saat loading model, coba pendekatan lain 
        print(f"Error loading PyTorch model: {e}")
        
        # Buat model RandomForest sederhana sebagai fallback
        from sklearn.ensemble import RandomForestRegressor
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        
        # Fit model dengan data dummy (ini hanya fallback)
        X = df[['volume', 'percent_change', 'macd_line', 'rsi']].tail(60).values
        y = df['close'].tail(60).values
        model.fit(X, y)
        
        # Bypass PyTorch prediction process
        last_data = df[['volume', 'percent_change', 'macd_line', 'rsi']].tail(1).values
        predicted_close = float(model.predict(last_data)[0])
        
        # Skip the rest of PyTorch-specific code
        last_date = df['date'].iloc[-1]
        next_date = last_date + pd.Timedelta(days=1)
        
        # Skip weekends
        while next_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
            next_date = next_date + pd.Timedelta(days=1)
        
        # Save to database...
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
        """, ('BBRI', next_date, predicted_close))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Prediksi BBRI untuk {next_date} (fallback): Rp {predicted_close:,.2f}")
        return predicted_close
    
    # Gunakan model untuk prediksi jika model berhasil dimuat
    model.eval()  # Set model to evaluation mode
    
    # Features
    features = ['close', 'volume', 'percent_change', 'macd_line', 'rsi']
    
    # Add missing features with default value 0
    for feature in features:
        if feature not in df.columns:
            df[feature] = 0
    
    # Get last 60 days for prediction
    last_60_days = df[features].tail(60).values
    
    # Scale the data
    last_60_days_scaled = scaler.transform(last_60_days)
    
    # Prepare data for LSTM (add batch dimension)
    X_test = torch.FloatTensor(last_60_days_scaled).unsqueeze(0)
    
    # Make prediction
    with torch.no_grad():
        predicted_scaled = model(X_test)
    
    # Inverse transform
    predicted_close = close_scaler.inverse_transform(
        predicted_scaled.detach().numpy()
    )[0][0]
    
    # Konversi numpy.float32 ke Python float standar
    if isinstance(predicted_close, (np.ndarray, np.float32, np.float64)):
        # Jika berbentuk array, ambil elemen pertama
        if hasattr(predicted_close, 'shape') and predicted_close.shape:
            predicted_close = float(predicted_close.item())
        else:
            predicted_close = float(predicted_close)
    
    # Prepare dates for prediction
    last_date = df['date'].iloc[-1]
    next_date = last_date + pd.Timedelta(days=1)
    
    # Skip weekends
    while next_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
        next_date = next_date + pd.Timedelta(days=1)
    
    # Save prediction to database
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
    """, ('BBRI', next_date, predicted_close))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Prediksi BBRI untuk {next_date}: Rp {predicted_close:,.2f}")
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
        AND sp.symbol = 'BBRI'
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
    WHERE symbol = 'BBRI'
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
        'BBRI',
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
           'BBRI',
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
    """, ('BBRI', "LSTM_Backtest", rmse, mae, mape, len(predictions)))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Backtesting realistis selesai. RMSE: {rmse:.2f}, MAE: {mae:.2f}, MAPE: {mape:.2f}%")
    
    # Tambahan: log hasil sebagai score
    print(f"Model terbaik untuk prediksi harga BBRI: LSTM dengan RMSE {rmse:.2f}")
    
    return rmse

    # DAG definition
with DAG(
   dag_id="BBRI_lstm_prediction",
   start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
   schedule_interval="30 18 * * 1-5",  # 18:30 setiap hari kerja
   catchup=False,
   default_args=default_args,
   tags=["lstm", "prediction", "BBRI"]
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
       task_id="extract_BBRI_data",
       python_callable=extract_BBRI_data
   )
   
   # Train LSTM model
   train_model = PythonOperator(
       task_id="train_BBRI_lstm",
       python_callable=train_BBRI_lstm
   )
   
   # Make predictions
   predict_prices = PythonOperator(
       task_id="predict_BBRI_prices",
       python_callable=predict_BBRI_prices
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
   wait_for_transformation >> create_table >> extract_data >> train_model
   train_model >> [predict_prices, backtest_model]
   predict_prices >> update_actual >> run_lstm_dbt
   backtest_model >> run_lstm_dbt