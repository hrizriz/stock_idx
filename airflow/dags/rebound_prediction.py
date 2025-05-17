from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pendulum
import pandas as pd
import numpy as np
import logging
from utils.database import get_database_connection, fetch_data
from utils.telegram import send_telegram_message

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

def identify_potential_rebounds():
    """Identify stocks showing potential rebound signals based on transaction patterns"""
    try:
        conn = get_database_connection()
        
        # Query for stocks in downtrend but showing accumulation patterns
        rebound_query = """
        WITH price_trends AS (
            -- Calculate recent trend (last 10 days)
            SELECT 
                symbol,
                name,
                date,
                close,
                volume,
                foreign_buy,
                foreign_sell,
                (foreign_buy - foreign_sell) AS foreign_net,
                bid,
                offer,
                bid_volume,
                offer_volume,
                -- Calculate price decline
                100 * (close / LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date) - 1) AS price_change_10d,
                -- Avg price for the last 3 days compared to previous 3 days
                AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_price_3d,
                AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 5 PRECEDING AND 3 PRECEDING) AS avg_price_prev_3d,
                -- Volume trend
                volume / AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS volume_ratio,
                -- Bid-Ask balance
                CASE 
                    WHEN offer_volume > 0 THEN bid_volume::float / offer_volume
                    ELSE NULL 
                END AS bid_ask_ratio
            FROM public.daily_stock_summary
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
        ),
        
        rebound_candidates AS (
            SELECT 
                p.symbol, 
                p.name,
                p.date,
                p.close,
                p.volume,
                p.volume_ratio,
                p.price_change_10d,
                p.foreign_net,
                p.bid_ask_ratio,
                -- Join with RSI data
                r.rsi,
                -- Calculate rebound score components
                -- 1. Price decline score (more negative is better for rebound)
                CASE 
                    WHEN p.price_change_10d <= -20 THEN 5
                    WHEN p.price_change_10d <= -15 THEN 4
                    WHEN p.price_change_10d <= -10 THEN 3
                    WHEN p.price_change_10d <= -5 THEN 2
                    WHEN p.price_change_10d < 0 THEN 1
                    ELSE 0
                END AS price_decline_score,
                
                -- 2. RSI oversold score
                CASE 
                    WHEN r.rsi <= 25 THEN 5
                    WHEN r.rsi <= 30 THEN 4
                    WHEN r.rsi <= 35 THEN 3
                    WHEN r.rsi <= 40 THEN 2
                    ELSE 0
                END AS rsi_score,
                
                -- 3. Volume surge score
                CASE 
                    WHEN p.volume_ratio >= 3 THEN 5
                    WHEN p.volume_ratio >= 2 THEN 4
                    WHEN p.volume_ratio >= 1.5 THEN 3
                    WHEN p.volume_ratio >= 1.2 THEN 2
                    WHEN p.volume_ratio >= 1 THEN 1
                    ELSE 0
                END AS volume_score,
                
                -- 4. Foreign flow score
                CASE 
                    WHEN p.foreign_net > 0 AND p.price_change_10d < 0 THEN 3
                    WHEN p.foreign_net > 0 THEN 1
                    ELSE 0
                END AS foreign_flow_score,
                
                -- 5. Bid-Ask imbalance score (accumulation signal)
                CASE 
                    WHEN p.bid_ask_ratio >= 2 THEN 4
                    WHEN p.bid_ask_ratio >= 1.5 THEN 3
                    WHEN p.bid_ask_ratio >= 1.2 THEN 2
                    WHEN p.bid_ask_ratio >= 1 THEN 1
                    ELSE 0
                END AS bid_ask_score,
                
                -- 6. Price stabilization score (comparing latest 3d avg with previous 3d avg)
                CASE 
                    WHEN p.avg_price_3d >= p.avg_price_prev_3d AND p.price_change_10d < 0 THEN 3
                    WHEN p.avg_price_3d >= 0.98 * p.avg_price_prev_3d AND p.price_change_10d < 0 THEN 2
                    WHEN p.avg_price_3d >= 0.95 * p.avg_price_prev_3d AND p.price_change_10d < 0 THEN 1
                    ELSE 0
                END AS stabilization_score
                
            FROM price_trends p
            LEFT JOIN public_analytics.technical_indicators_rsi r ON p.symbol = r.symbol AND p.date = r.date
            WHERE p.date = (SELECT MAX(date) FROM price_trends)
        )
        
        SELECT 
            symbol,
            name,
            date,
            close,
            volume,
            volume_ratio,
            rsi,
            price_change_10d,
            foreign_net,
            bid_ask_ratio,
            -- Individual scores
            price_decline_score,
            rsi_score,
            volume_score,
            foreign_flow_score, 
            bid_ask_score,
            stabilization_score,
            -- Calculate total rebound score
            (price_decline_score + rsi_score + volume_score + foreign_flow_score + bid_ask_score + stabilization_score) AS rebound_score,
            -- Probability calculation (max score is 25)
            (price_decline_score + rsi_score + volume_score + foreign_flow_score + bid_ask_score + stabilization_score) / 25.0 AS rebound_probability
        FROM rebound_candidates
        WHERE 
            -- Filter for minimum decline
            price_change_10d < -5
            -- Require some accumulation signs
            AND (rsi_score > 0 OR volume_score > 0 OR bid_ask_score > 0)
        ORDER BY rebound_score DESC
        LIMIT 30
        """
        
        rebound_df = pd.read_sql(rebound_query, conn)
        
        # Calculate expected rebound percentage based on historical patterns
        rebound_df['expected_rebound_pct'] = rebound_df.apply(
            lambda x: min(abs(x['price_change_10d']) * 0.4, 15) if x['rebound_score'] >= 15 else
                      min(abs(x['price_change_10d']) * 0.3, 10) if x['rebound_score'] >= 10 else
                      min(abs(x['price_change_10d']) * 0.2, 5),
            axis=1
        )
        
        # Save results to database
        if not rebound_df.empty:
            cursor = conn.cursor()
            
            # Create table if not exists
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS public_analytics.potential_rebounds (
                id SERIAL PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                date DATE,
                close NUMERIC,
                rebound_score INTEGER,
                rebound_probability NUMERIC,
                expected_rebound_pct NUMERIC,
                price_change_10d NUMERIC,
                rsi NUMERIC,
                volume_ratio NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Clear previous data for today
            today = pd.Timestamp.now().strftime('%Y-%m-%d')
            cursor.execute(f"DELETE FROM public_analytics.potential_rebounds WHERE date = '{today}'")
            
            # Insert new data
            for _, row in rebound_df.iterrows():
                cursor.execute("""
                INSERT INTO public_analytics.potential_rebounds
                (symbol, name, date, close, rebound_score, rebound_probability, 
                expected_rebound_pct, price_change_10d, rsi, volume_ratio)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['symbol'], row['name'], row['date'], row['close'], 
                    int(row['rebound_score']), float(row['rebound_probability']),
                    float(row['expected_rebound_pct']), float(row['price_change_10d']),
                    float(row['rsi']) if not pd.isna(row['rsi']) else None,
                    float(row['volume_ratio'])
                ))
            
            conn.commit()
            cursor.close()
        
        conn.close()
        
        return f"Found {len(rebound_df)} potential rebound candidates"
    except Exception as e:
        logging.error(f"Error identifying potential rebounds: {str(e)}")
        return f"Error: {str(e)}"

def send_rebound_signals():
    """Send potential rebound signals to Telegram"""
    try:
        conn = get_database_connection()
        
        query = """
        SELECT 
            symbol, 
            name, 
            close, 
            rebound_score, 
            rebound_probability,
            expected_rebound_pct,
            price_change_10d,
            rsi,
            volume_ratio
        FROM public_analytics.potential_rebounds
        WHERE date = (SELECT MAX(date) FROM public_analytics.potential_rebounds)
        ORDER BY rebound_score DESC
        LIMIT 15
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df.empty:
            return "No rebound candidates found"
        
        # Format message
        message = "🔄 *POTENTIAL REBOUND CANDIDATES* 🔄\n\n"
        message += "The following stocks show significant rebound potential based on transaction patterns:\n\n"
        
        for i, row in enumerate(df.itertuples(), 1):
            # Emoji based on rebound score
            if row.rebound_score >= 18:
                emoji = "⭐⭐⭐"  # Strong signal
            elif row.rebound_score >= 15:
                emoji = "⭐⭐"    # Good signal
            else:
                emoji = "⭐"      # Moderate signal
                
            message += f"{emoji} *{row.symbol}* ({row.name})\n"
            message += f"   Price: Rp{row.close:,.0f} | 10d Change: {row.price_change_10d:.1f}%\n"
            message += f"   Rebound Score: {row.rebound_score}/25 | Probability: {row.rebound_probability*100:.0f}%\n"
            message += f"   Expected Rebound: +{row.expected_rebound_pct:.1f}%\n"
            
            # Add supporting indicators
            indicators = []
            if row.rsi is not None and row.rsi < 40:
                indicators.append(f"RSI: {row.rsi:.1f}")
            if row.volume_ratio > 1.2:
                indicators.append(f"Vol: {row.volume_ratio:.1f}x")
                
            if indicators:
                message += f"   Indicators: {' | '.join(indicators)}\n"
                
            message += "\n"
        
        # Strategy tips
        message += "*Rebound Strategy Tips:*\n"
        message += "• Enter on morning weakness or consolidation\n"
        message += "• Target: +5-15% from entry\n"
        message += "• Stop loss: -3% below entry\n"
        message += "• Best trades: Scores 15+ with high volume\n\n"
        
        message += "*Disclaimer:* Rebound predictions are based on transaction patterns and technical analysis. Always conduct your own research."
        
        # Send to Telegram
        send_telegram_message(message)
        
        return f"Sent {len(df)} rebound signals to Telegram"
    except Exception as e:
        logging.error(f"Error sending rebound signals: {str(e)}")
        return f"Error: {str(e)}"

# Define the DAG
with DAG(
    dag_id="rebound_prediction",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 18 * * 1-5",  # Run weekdays at 6 PM
    catchup=False,
    default_args=default_args,
    tags=["trading", "rebound", "prediction"]
) as dag:
    
    # Identify potential rebounds
    identify_rebounds = PythonOperator(
        task_id="identify_potential_rebounds",
        python_callable=identify_potential_rebounds
    )
    
    # Send signals
    send_signals = PythonOperator(
        task_id="send_rebound_signals",
        python_callable=send_rebound_signals
    )
    
    # End marker
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    # Define task dependencies
    identify_rebounds >> send_signals >> end_task