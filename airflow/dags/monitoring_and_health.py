# monitoring_and_health.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.time_sensor import TimeSensor
import pendulum
import psycopg2
import pandas as pd
import json
import os

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'retries': 0
}

def check_database_health():
    """Periksa kesehatan database dan query performance"""
    try:
        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        
        # Cek ukuran database
        cursor.execute("""
        SELECT pg_size_pretty(pg_database_size('airflow')) as db_size;
        """)
        db_size = cursor.fetchone()[0]
        
        # Cek tabel terbesar
        cursor.execute("""
        SELECT 
            table_name, 
            pg_size_pretty(pg_table_size(table_name)) as table_size
        FROM (
            SELECT ('"' || table_schema || '"."' || table_name || '"') as table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' OR table_schema = 'public_analytics'
        ) t
        ORDER BY pg_table_size(table_name) DESC
        LIMIT 5;
        """)
        largest_tables = cursor.fetchall()
        
        # Cek query lambat
        cursor.execute("""
        SELECT query, calls, total_time, mean_time
        FROM pg_stat_statements
        ORDER BY mean_time DESC
        LIMIT 5;
        """)
        slow_queries = cursor.fetchall()
        
        # Cek DAG status
        cursor.execute("""
        SELECT dag_id, state, count(*)
        FROM dag_run
        WHERE start_date >= NOW() - INTERVAL '24 hours'
        GROUP BY dag_id, state
        """)
        dag_status = cursor.fetchall()
        
        # Format and send alert
        alert_message = f"📊 *DATABASE HEALTH REPORT*\n\n"
        alert_message += f"*Database Size:* {db_size}\n\n"
        
        alert_message += "*Largest Tables:*\n"
        for table in largest_tables:
            alert_message += f"• {table[0]}: {table[1]}\n"
        
        alert_message += "\n*Slowest Queries:*\n"
        for query in slow_queries:
            alert_message += f"• Calls: {query[1]}, Avg Time: {query[3]:.2f}ms\n"
        
        alert_message += "\n*DAG Status (Last 24h):*\n"
        for status in dag_status:
            alert_message += f"• {status[0]}: {status[1]} ({status[2]})\n"
        
        # Check disk space
        disk_usage = os.popen("df -h | grep /opt/airflow").read()
        alert_message += f"\n*Disk Usage:*\n{disk_usage}\n"
        
        # Send alert using telegram
        from utils.telegram import send_telegram_message
        send_telegram_message(alert_message)
        
        conn.close()
        return "Health check completed"
    except Exception as e:
        return f"Error checking database health: {str(e)}"

def check_trading_signal_quality():
    """Monitor kualitas sinyal trading yang dihasilkan"""
    try:
        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow"
        )
        
        # Analyze backtest performance
        query = """
        SELECT 
            DATE_TRUNC('day', signal_date) as day,
            COUNT(*) as signal_count,
            SUM(CASE WHEN is_win THEN 1 ELSE 0 END) as win_count,
            ROUND(AVG(percent_change_5d), 2) as avg_return
        FROM public_analytics.backtest_results
        WHERE signal_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY DATE_TRUNC('day', signal_date)
        ORDER BY day DESC
        """
        
        df = pd.read_sql(query, conn)
        
        if not df.empty:
            # Check for quality degradation
            avg_win_rate = df['win_count'].sum() / df['signal_count'].sum() if df['signal_count'].sum() > 0 else 0
            avg_return = df['avg_return'].mean()
            
            # Compare with previous periods
            previous_query = """
            SELECT 
                AVG(win_rate) as avg_win_rate,
                AVG(avg_return) as avg_return
            FROM (
                SELECT 
                    DATE_TRUNC('day', signal_date) as day,
                    SUM(CASE WHEN is_win THEN 1 ELSE 0 END)::float / COUNT(*) as win_rate,
                    AVG(percent_change_5d) as avg_return
                FROM public_analytics.backtest_results
                WHERE signal_date BETWEEN CURRENT_DATE - INTERVAL '90 days' AND CURRENT_DATE - INTERVAL '30 days'
                GROUP BY DATE_TRUNC('day', signal_date)
            ) t
            """
            
            previous_df = pd.read_sql(previous_query, conn)
            previous_win_rate = previous_df['avg_win_rate'].iloc[0] if not previous_df.empty else 0
            previous_return = previous_df['avg_return'].iloc[0] if not previous_df.empty else 0
            
            # Check if performance degrading
            if avg_win_rate < previous_win_rate * 0.9 or avg_return < previous_return * 0.8:
                alert_message = "⚠️ *SIGNAL QUALITY ALERT* ⚠️\n\n"
                alert_message += f"*Current Win Rate:* {avg_win_rate*100:.1f}% (vs Previous: {previous_win_rate*100:.1f}%)\n"
                alert_message += f"*Current Avg Return:* {avg_return:.2f}% (vs Previous: {previous_return:.2f}%)\n\n"
                
                if avg_win_rate < previous_win_rate * 0.9:
                    alert_message += "Signal win rate has significantly decreased.\n"
                    
                if avg_return < previous_return * 0.8:
                    alert_message += "Average return has significantly decreased.\n"
                    
                alert_message += "\nRecommended actions:\n"
                alert_message += "1. Check recent market conditions\n"
                alert_message += "2. Review signal generation parameters\n"
                alert_message += "3. Consider retraining ML models\n"
                
                # Send alert using telegram
                from utils.telegram import send_telegram_message
                send_telegram_message(alert_message)
        
        conn.close()
        return "Signal quality check completed"
    except Exception as e:
        return f"Error checking signal quality: {str(e)}"

# DAG definition
with DAG(
    dag_id="system_monitoring",
    schedule_interval="0 20 * * *",  # Run daily at 8 PM
    catchup=False,
    default_args=default_args,
    tags=["monitoring", "health"]
) as dag:
    
    health_check = PythonOperator(
        task_id="database_health_check",
        python_callable=check_database_health
    )
    
    signal_quality = PythonOperator(
        task_id="trading_signal_quality_check",
        python_callable=check_trading_signal_quality
    )
    
    health_check >> signal_quality