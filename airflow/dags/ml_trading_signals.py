from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime
import pendulum
import logging
import pandas as pd

from utils.database import get_database_connection, get_latest_stock_date
from utils.telegram import send_telegram_message
from utils.models import (
    train_win_rate_predictor,
    train_lstm_model,
    predict_stock_price,
    update_actual_prices
)

local_tz = pendulum.timezone("Asia/Jakarta")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

def get_symbols_to_predict():
    """Get list of symbols to predict from Airflow Variable or use default list"""
    try:
        symbols = Variable.get("prediction_symbols", deserialize_json=True)
        if not symbols:
            symbols = ["BBCA", "BBRI", "ASII", "TLKM", "BMRI"]
    except:
        symbols = ["BBCA", "BBRI", "ASII", "TLKM", "BMRI"]
    
    logger.info(f"Will process predictions for {len(symbols)} symbols: {symbols}")
    return symbols

def train_model_for_symbol(symbol, **kwargs):
    """Train LSTM model for a single symbol"""
    result = train_lstm_model(symbol)
    return result

def predict_price_for_symbol(symbol, **kwargs):
    """Make price prediction for a single symbol"""
    prediction = predict_stock_price(symbol)
    return prediction

def send_ml_predictions_report(**kwargs):
    """Send price predictions report via Telegram"""
    try:
        ti = kwargs['ti']
        symbols = get_symbols_to_predict()
        predictions = {}
        
        for symbol in symbols:
            try:
                pred_result = ti.xcom_pull(task_ids=f'predict_price_{symbol}')
                if isinstance(pred_result, (float, int)) or (isinstance(pred_result, str) and pred_result.replace('.', '', 1).isdigit()):
                    predictions[symbol] = float(pred_result)
                else:
                    logger.warning(f"Non-numeric prediction for {symbol}: {pred_result}")
            except:
                logger.warning(f"Could not get prediction for {symbol}")
        
        if not predictions:
            return "No valid predictions to report"
        
        conn = get_database_connection()
        metrics_df = pd.read_sql("""
            SELECT symbol, model_type, rmse, mae, mape
            FROM model_performance_metrics
            WHERE model_type = 'LSTM' OR model_type = 'LSTM_Realtime'
            ORDER BY symbol, model_type
        """, conn)
        conn.close()
        
        message = "🤖 *ML MODEL STOCK PRICE PREDICTIONS* 🤖\n\n"
        
        for symbol, pred_price in predictions.items():
            message += f"*{symbol}*: Rp{pred_price:,.2f}\n"
            
            symbol_metrics = metrics_df[metrics_df['symbol'] == symbol]
            if not symbol_metrics.empty:
                for _, row in symbol_metrics.iterrows():
                    message += f"  {row['model_type']} metrics: MAPE {row['mape']:.2f}%, MAE {row['mae']:.2f}\n"
            message += "\n"
        
        message += "*Disclaimer:*\n"
        message += "Prediksi ini dihasilkan oleh model machine learning dan hanya untuk referensi. "
        message += "Selalu lakukan analisis tambahan sebelum mengambil keputusan investasi."
        
        send_telegram_message(message)
        return f"Sent predictions report for {len(predictions)} symbols"
        
    except Exception as e:
        logger.error(f"Error sending predictions report: {str(e)}")
        return f"Error sending predictions report: {str(e)}"

with DAG(
    dag_id="ml_trading_signals",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="30 18 * * 1-5",  # Every weekday at 18:30 Jakarta time
    catchup=False,
    default_args=default_args,
    tags=["ml", "prediction", "trading"]
) as dag:
    
    wait_for_signals = ExternalTaskSensor(
        task_id="wait_for_signals",
        external_dag_id="unified_trading_signals",
        external_task_id="send_daily_signals",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )
    
    train_win_rate = PythonOperator(
        task_id="train_win_rate_model",
        python_callable=train_win_rate_predictor,
        execution_timeout=pendulum.duration(minutes=20)
    )
    
    symbols = get_symbols_to_predict()
    
    train_tasks = {}
    for symbol in symbols:
        train_tasks[symbol] = PythonOperator(
            task_id=f'train_lstm_{symbol}',
            python_callable=train_model_for_symbol,
            op_kwargs={'symbol': symbol},
            execution_timeout=pendulum.duration(minutes=45)
        )
    
    predict_tasks = {}
    for symbol in symbols:
        predict_tasks[symbol] = PythonOperator(
            task_id=f'predict_price_{symbol}',
            python_callable=predict_price_for_symbol,
            op_kwargs={'symbol': symbol}
        )
    
    update_prices = PythonOperator(
        task_id="update_actual_prices",
        python_callable=update_actual_prices
    )
    
    send_predictions = PythonOperator(
        task_id="send_ml_predictions_report",
        python_callable=send_ml_predictions_report
    )
    
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    wait_for_signals >> train_win_rate
    
    for symbol in symbols:
        train_win_rate >> train_tasks[symbol] >> predict_tasks[symbol]
    
    prediction_tasks_list = list(predict_tasks.values())
    prediction_tasks_list >> send_predictions
    
    [send_predictions, update_prices] >> end_task
