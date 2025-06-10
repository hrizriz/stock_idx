from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import pendulum
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

def get_db_connection():
    """Utility function untuk koneksi database"""
    return psycopg2.connect(
        host="postgres",
        dbname="airflow", 
        user="airflow",
        password="airflow"
    )

def create_breakout_analysis_tables():
    """
    Buat tabel untuk analisis saham siap breakout
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Tabel untuk analisis accumulation maturity
    cur.execute("""
    CREATE TABLE IF NOT EXISTS accumulation_maturity (
        symbol TEXT,
        analysis_date DATE,
        accumulation_duration_days INTEGER,
        total_volume_accumulated BIGINT,
        avg_daily_volume BIGINT,
        volume_vs_historical NUMERIC,
        foreign_net_accumulated BIGINT,
        price_stability_score NUMERIC,
        accumulation_maturity_signal TEXT,
        PRIMARY KEY (symbol, analysis_date)
    );
    """)
    
    # Tabel untuk breakout readiness signals
    cur.execute("""
    CREATE TABLE IF NOT EXISTS breakout_readiness (
        symbol TEXT,
        analysis_date DATE,
        current_price NUMERIC,
        resistance_level NUMERIC,
        distance_to_resistance_pct NUMERIC,
        volume_surge_indicator NUMERIC,
        foreign_activity_recent BIGINT,
        price_momentum_score NUMERIC,
        breakout_probability NUMERIC,
        breakout_signal TEXT,
        PRIMARY KEY (symbol, analysis_date)
    );
    """)
    
    # Tabel untuk early breakout signals
    cur.execute("""
    CREATE TABLE IF NOT EXISTS early_breakout_signals (
        symbol TEXT,
        analysis_date DATE,
        volume_expansion_today NUMERIC,
        price_action_signal TEXT,
        foreign_buying_surge BOOLEAN,
        support_strength NUMERIC,
        momentum_building BOOLEAN,
        catalysts_detected TEXT,
        early_signal_strength NUMERIC,
        PRIMARY KEY (symbol, analysis_date)
    );
    """)
    
    # Tabel utama untuk breakout candidates
    cur.execute("""
    CREATE TABLE IF NOT EXISTS breakout_candidates (
        symbol TEXT,
        analysis_date DATE,
        current_price NUMERIC,
        target_price NUMERIC,
        resistance_level NUMERIC,
        accumulation_score NUMERIC,
        breakout_readiness_score NUMERIC,
        early_signal_score NUMERIC,
        overall_breakout_score NUMERIC,
        breakout_timeframe TEXT,
        recommendation TEXT,
        risk_reward_ratio NUMERIC,
        stop_loss_level NUMERIC,
        confidence_level TEXT,
        action_plan TEXT,
        PRIMARY KEY (symbol, analysis_date)
    );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info("✅ Tabel analisis breakout berhasil dibuat")

def analyze_accumulation_maturity():
    """
    Menganalisis kematangan fase akumulasi
    """
    conn = get_db_connection()
    
    query = """
    WITH accumulation_data AS (
        SELECT 
            symbol,
            date,
            close,
            volume,
            foreign_buy,
            foreign_sell,
            (foreign_buy - foreign_sell) as foreign_net,
            high,
            low,
            -- Identifikasi periode akumulasi dengan volume tinggi + harga stabil
            CASE 
                WHEN volume > AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 89 PRECEDING AND 30 PRECEDING) * 1.2
                     AND ABS(close - LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date)) / LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date) < 0.05
                THEN 1 ELSE 0
            END as accumulation_day
        FROM daily_stock_summary
        WHERE date >= CURRENT_DATE - INTERVAL '6 months'
    ),
    
    accumulation_periods AS (
        SELECT 
            symbol,
            date,
            close,
            volume,
            foreign_net,
            accumulation_day,
            -- Hitung periode akumulasi berturut-turut
            SUM(accumulation_day) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
            ) as accumulation_days_60,
            -- Volume total yang ter-accumulate
            SUM(CASE WHEN accumulation_day = 1 THEN volume ELSE 0 END) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
            ) as volume_accumulated_60d,
            -- Foreign net selama periode akumulasi
            SUM(CASE WHEN accumulation_day = 1 THEN foreign_net ELSE 0 END) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
            ) as foreign_accumulated_60d
        FROM accumulation_data
    ),
    
    latest_maturity AS (
        SELECT DISTINCT ON (symbol)
            symbol,
            date as analysis_date,
            accumulation_days_60 as accumulation_duration_days,
            volume_accumulated_60d as total_volume_accumulated,
            CASE 
                WHEN accumulation_days_60 > 0 THEN volume_accumulated_60d / accumulation_days_60
                ELSE 0
            END as avg_daily_volume,
            -- Bandingkan dengan volume historical
            CASE 
                WHEN AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 149 PRECEDING AND 90 PRECEDING) > 0
                THEN (volume_accumulated_60d / NULLIF(accumulation_days_60, 0)) / 
                     AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 149 PRECEDING AND 90 PRECEDING)
                ELSE 1
            END as volume_vs_historical,
            foreign_accumulated_60d as foreign_net_accumulated,
            -- Price stability score (rendah = stabil)
            100 - (STDDEV(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) / 
                   AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) * 100) as price_stability_score
        FROM accumulation_periods
        WHERE accumulation_days_60 >= 15  -- Min 15 hari akumulasi dalam 60 hari
        ORDER BY symbol, date DESC
    )
    
    SELECT 
        symbol,
        analysis_date,
        accumulation_duration_days,
        total_volume_accumulated,
        avg_daily_volume::BIGINT,
        volume_vs_historical,
        foreign_net_accumulated,
        price_stability_score,
        CASE
            WHEN accumulation_duration_days >= 30 AND volume_vs_historical >= 1.5 AND price_stability_score >= 85
                THEN 'Mature Accumulation - Ready'
            WHEN accumulation_duration_days >= 20 AND volume_vs_historical >= 1.3 AND price_stability_score >= 80
                THEN 'Advanced Accumulation'
            WHEN accumulation_duration_days >= 15 AND volume_vs_historical >= 1.2
                THEN 'Building Accumulation'
            ELSE 'Early Accumulation'
        END as accumulation_maturity_signal
    FROM latest_maturity
    """
    
    df = pd.read_sql(query, conn)
    
    if not df.empty:
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO accumulation_maturity 
        (symbol, analysis_date, accumulation_duration_days, total_volume_accumulated, 
         avg_daily_volume, volume_vs_historical, foreign_net_accumulated, 
         price_stability_score, accumulation_maturity_signal)
        VALUES (%(symbol)s, %(analysis_date)s, %(accumulation_duration_days)s, %(total_volume_accumulated)s,
                %(avg_daily_volume)s, %(volume_vs_historical)s, %(foreign_net_accumulated)s,
                %(price_stability_score)s, %(accumulation_maturity_signal)s)
        ON CONFLICT (symbol, analysis_date) DO UPDATE SET
            accumulation_duration_days = EXCLUDED.accumulation_duration_days,
            total_volume_accumulated = EXCLUDED.total_volume_accumulated,
            avg_daily_volume = EXCLUDED.avg_daily_volume,
            volume_vs_historical = EXCLUDED.volume_vs_historical,
            foreign_net_accumulated = EXCLUDED.foreign_net_accumulated,
            price_stability_score = EXCLUDED.price_stability_score,
            accumulation_maturity_signal = EXCLUDED.accumulation_maturity_signal;
        """
        
        execute_batch(cur, insert_query, df.to_dict('records'))
        conn.commit()
        cur.close()
        
        mature_count = len(df[df['accumulation_maturity_signal'] == 'Mature Accumulation - Ready'])
        logger.info(f"✅ Accumulation maturity analysis: {len(df)} stocks, {mature_count} mature")
    
    conn.close()

def analyze_breakout_readiness():
    """
    Menganalisis kesiapan breakout berdasarkan posisi harga dan momentum
    """
    conn = get_db_connection()
    
    query = """
    WITH price_analysis AS (
        SELECT 
            symbol,
            date,
            close,
            high,
            low,
            volume,
            foreign_buy,
            foreign_sell,
            (foreign_buy - foreign_sell) as foreign_net,
            -- Resistance level (high tertinggi 60 hari)
            MAX(high) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) as resistance_60d,
            -- Support level (low terendah 60 hari)
            MIN(low) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) as support_60d,
            -- Volume 5 hari terakhir vs 30 hari sebelumnya
            AVG(volume) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) / NULLIF(AVG(volume) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 34 PRECEDING AND 5 PRECEDING
            ), 0) as volume_surge_5d,
            -- Momentum harga (close vs 10 hari lalu)
            (close - LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date)) / 
            NULLIF(LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date), 0) * 100 as price_momentum_10d
        FROM daily_stock_summary
        WHERE date >= CURRENT_DATE - INTERVAL '3 months'
    ),
    
    foreign_analysis AS (
        SELECT 
            symbol,
            date,
            foreign_net,
            -- Foreign buying dalam 3 hari terakhir  
            SUM(CASE WHEN foreign_net > 0 THEN foreign_net ELSE 0 END) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) as foreign_buying_3d
        FROM price_analysis
    ),
    
    latest_readiness AS (
        SELECT DISTINCT ON (p.symbol)
            p.symbol,
            p.date as analysis_date,
            p.close as current_price,
            p.resistance_60d as resistance_level,
            -- Jarak ke resistance (semakin dekat semakin siap)
            (p.resistance_60d - p.close) / NULLIF(p.close, 0) * 100 as distance_to_resistance_pct,
            p.volume_surge_5d as volume_surge_indicator,
            f.foreign_buying_3d as foreign_activity_recent,
            p.price_momentum_10d as price_momentum_score,
            -- Probabilitas breakout
            CASE 
                WHEN (p.resistance_60d - p.close) / NULLIF(p.close, 0) * 100 <= 3 
                     AND p.volume_surge_5d >= 1.3 
                     AND f.foreign_buying_3d > 0
                THEN 85
                WHEN (p.resistance_60d - p.close) / NULLIF(p.close, 0) * 100 <= 5 
                     AND p.volume_surge_5d >= 1.2
                THEN 70
                WHEN (p.resistance_60d - p.close) / NULLIF(p.close, 0) * 100 <= 8 
                     AND p.volume_surge_5d >= 1.1
                THEN 55
                ELSE 30
            END as breakout_probability
        FROM price_analysis p
        JOIN foreign_analysis f ON p.symbol = f.symbol AND p.date = f.date
        ORDER BY p.symbol, p.date DESC
    )
    
    SELECT 
        symbol,
        analysis_date,
        current_price,
        resistance_level,
        distance_to_resistance_pct,
        volume_surge_indicator,
        foreign_activity_recent,
        price_momentum_score,
        breakout_probability,
        CASE
            WHEN breakout_probability >= 80 AND distance_to_resistance_pct <= 3
                THEN 'Imminent Breakout (1-3 days)'
            WHEN breakout_probability >= 70 AND distance_to_resistance_pct <= 5
                THEN 'High Breakout Probability (3-7 days)'
            WHEN breakout_probability >= 55 AND distance_to_resistance_pct <= 8
                THEN 'Moderate Breakout Setup (1-2 weeks)'
            ELSE 'Building Momentum'
        END as breakout_signal
    FROM latest_readiness
    WHERE distance_to_resistance_pct <= 15  -- Focus pada saham yang sudah dekat resistance
    """
    
    df = pd.read_sql(query, conn)
    
    if not df.empty:
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO breakout_readiness 
        (symbol, analysis_date, current_price, resistance_level, distance_to_resistance_pct,
         volume_surge_indicator, foreign_activity_recent, price_momentum_score,
         breakout_probability, breakout_signal)
        VALUES (%(symbol)s, %(analysis_date)s, %(current_price)s, %(resistance_level)s,
                %(distance_to_resistance_pct)s, %(volume_surge_indicator)s, %(foreign_activity_recent)s,
                %(price_momentum_score)s, %(breakout_probability)s, %(breakout_signal)s)
        ON CONFLICT (symbol, analysis_date) DO UPDATE SET
            current_price = EXCLUDED.current_price,
            resistance_level = EXCLUDED.resistance_level,
            distance_to_resistance_pct = EXCLUDED.distance_to_resistance_pct,
            volume_surge_indicator = EXCLUDED.volume_surge_indicator,
            foreign_activity_recent = EXCLUDED.foreign_activity_recent,
            price_momentum_score = EXCLUDED.price_momentum_score,
            breakout_probability = EXCLUDED.breakout_probability,
            breakout_signal = EXCLUDED.breakout_signal;
        """
        
        execute_batch(cur, insert_query, df.to_dict('records'))
        conn.commit()
        cur.close()
        
        imminent_count = len(df[df['breakout_signal'].str.contains('Imminent')])
        logger.info(f"✅ Breakout readiness analysis: {len(df)} stocks, {imminent_count} imminent")
    
    conn.close()

def detect_early_breakout_signals():
    """
    Mendeteksi sinyal-sinyal early breakout untuk hari ini
    """
    conn = get_db_connection()
    
    query = """
    WITH support_levels AS (
        SELECT 
            symbol,
            date,
            low,
            MIN(low) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
            ) as support_level_20d
        FROM daily_stock_summary
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
    ),
    
    today_signals AS (
        SELECT 
            s.symbol,
            s.date,
            s.close,
            s.volume,
            s.foreign_buy,
            s.foreign_sell,
            s.high,
            s.low,
            s.open_price,
            -- Volume expansion hari ini
            s.volume / NULLIF(AVG(s.volume) OVER (
                PARTITION BY s.symbol ORDER BY s.date 
                ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
            ), 0) as volume_expansion_today,
            -- Price action signals
            CASE 
                WHEN s.close > s.open_price AND s.close >= s.high * 0.95 THEN 'Strong Close'
                WHEN s.close > (s.high + s.low) / 2 THEN 'Above Midpoint'
                WHEN s.low > LAG(s.high, 1) OVER (PARTITION BY s.symbol ORDER BY s.date) THEN 'Gap Up'
                ELSE 'Neutral'
            END as price_action_signal,
            -- Foreign buying surge today
            CASE 
                WHEN (s.foreign_buy - s.foreign_sell) > 
                     AVG(s.foreign_buy - s.foreign_sell) OVER (
                         PARTITION BY s.symbol ORDER BY s.date 
                         ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
                     ) * 2
                THEN true ELSE false
            END as foreign_buying_surge,
            -- Support test detection
            CASE 
                WHEN sl.support_level_20d IS NOT NULL AND s.low <= sl.support_level_20d * 1.02 
                THEN 1 ELSE 0
            END as support_test_today
        FROM daily_stock_summary s
        LEFT JOIN support_levels sl ON s.symbol = sl.symbol AND s.date = sl.date
        WHERE s.date >= CURRENT_DATE - INTERVAL '30 days'
    ),
    
    support_strength_calc AS (
        SELECT 
            symbol,
            date,
            volume_expansion_today,
            price_action_signal,
            foreign_buying_surge,
            -- Hitung total support tests dalam 20 hari terakhir
            SUM(support_test_today) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) as support_strength
        FROM today_signals
    ),
    
    latest_signals AS (
        SELECT DISTINCT ON (symbol)
            symbol,
            date as analysis_date,
            volume_expansion_today,
            price_action_signal,
            foreign_buying_surge,
            support_strength,
            -- Momentum building indicator
            CASE 
                WHEN volume_expansion_today >= 1.5 
                     AND price_action_signal IN ('Strong Close', 'Gap Up')
                     AND foreign_buying_surge = true
                THEN true ELSE false
            END as momentum_building,
            -- Katalisis detection (simplistic)
            CASE 
                WHEN volume_expansion_today >= 2.0 THEN 'High Volume Spike'
                WHEN foreign_buying_surge = true THEN 'Foreign Surge'
                WHEN price_action_signal = 'Gap Up' THEN 'Gap Up'
                ELSE 'No Clear Catalyst'
            END as catalysts_detected
        FROM support_strength_calc
        WHERE date = CURRENT_DATE - INTERVAL '1 day'  -- Yesterday's data
        ORDER BY symbol, date DESC
    )
    
    SELECT 
        symbol,
        analysis_date,
        volume_expansion_today,
        price_action_signal,
        foreign_buying_surge,
        support_strength,
        momentum_building,
        catalysts_detected,
        -- Early signal strength score
        (CASE WHEN volume_expansion_today >= 2.0 THEN 30 ELSE volume_expansion_today * 15 END) +
        (CASE price_action_signal 
            WHEN 'Strong Close' THEN 25
            WHEN 'Gap Up' THEN 30
            WHEN 'Above Midpoint' THEN 15
            ELSE 5 END) +
        (CASE WHEN foreign_buying_surge THEN 25 ELSE 0 END) +
        (CASE WHEN momentum_building THEN 20 ELSE 0 END) as early_signal_strength
    FROM latest_signals
    WHERE volume_expansion_today >= 1.2  -- Filter minimal volume expansion
    """
    
    df = pd.read_sql(query, conn)
    
    if not df.empty:
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO early_breakout_signals 
        (symbol, analysis_date, volume_expansion_today, price_action_signal,
         foreign_buying_surge, support_strength, momentum_building, 
         catalysts_detected, early_signal_strength)
        VALUES (%(symbol)s, %(analysis_date)s, %(volume_expansion_today)s, %(price_action_signal)s,
                %(foreign_buying_surge)s, %(support_strength)s, %(momentum_building)s,
                %(catalysts_detected)s, %(early_signal_strength)s)
        ON CONFLICT (symbol, analysis_date) DO UPDATE SET
            volume_expansion_today = EXCLUDED.volume_expansion_today,
            price_action_signal = EXCLUDED.price_action_signal,
            foreign_buying_surge = EXCLUDED.foreign_buying_surge,
            support_strength = EXCLUDED.support_strength,
            momentum_building = EXCLUDED.momentum_building,
            catalysts_detected = EXCLUDED.catalysts_detected,
            early_signal_strength = EXCLUDED.early_signal_strength;
        """
        
        execute_batch(cur, insert_query, df.to_dict('records'))
        conn.commit()
        cur.close()
        
        strong_signals = len(df[df['early_signal_strength'] >= 70])
        logger.info(f"✅ Early breakout signals: {len(df)} stocks, {strong_signals} strong signals")
    
    conn.close()

def calculate_breakout_candidates():
    """
    Menggabungkan semua analisis untuk menentukan kandidat breakout terbaik
    """
    conn = get_db_connection()
    
    query = """
    WITH combined_analysis AS (
        SELECT 
            COALESCE(a.symbol, b.symbol, e.symbol) as symbol,
            COALESCE(a.analysis_date, b.analysis_date, e.analysis_date) as analysis_date,
            b.current_price,
            b.resistance_level,
            
            -- Accumulation scores
            CASE a.accumulation_maturity_signal
                WHEN 'Mature Accumulation - Ready' THEN 90
                WHEN 'Advanced Accumulation' THEN 75
                WHEN 'Building Accumulation' THEN 60
                ELSE 40
            END as accumulation_score,
            
            -- Breakout readiness scores
            COALESCE(b.breakout_probability, 30) as breakout_readiness_score,
            
            -- Early signal scores
            COALESCE(e.early_signal_strength, 20) as early_signal_score,
            
            -- Distance to resistance
            COALESCE(b.distance_to_resistance_pct, 20) as distance_to_resistance_pct,
            
            -- Original signals for context
            a.accumulation_maturity_signal,
            b.breakout_signal,
            e.catalysts_detected
            
        FROM accumulation_maturity a
        FULL OUTER JOIN breakout_readiness b 
            ON a.symbol = b.symbol AND a.analysis_date = b.analysis_date
        FULL OUTER JOIN early_breakout_signals e 
            ON COALESCE(a.symbol, b.symbol) = e.symbol 
            AND COALESCE(a.analysis_date, b.analysis_date) = e.analysis_date
        WHERE COALESCE(a.analysis_date, b.analysis_date, e.analysis_date) = CURRENT_DATE - INTERVAL '1 day'
    ),
    
    final_candidates AS (
        SELECT 
            symbol,
            analysis_date,
            current_price,
            resistance_level,
            accumulation_score,
            breakout_readiness_score,
            early_signal_score,
            distance_to_resistance_pct,
            
            -- Overall breakout score (weighted)
            (accumulation_score * 0.4 + 
             breakout_readiness_score * 0.4 + 
             early_signal_score * 0.2) as overall_breakout_score,
            
            -- Target price (conservative: resistance level)
            resistance_level as target_price,
            
            -- Risk reward calculation
            CASE 
                WHEN current_price > 0 AND resistance_level > current_price
                THEN (resistance_level - current_price) / (current_price * 0.05)  -- 5% stop loss
                ELSE 1
            END as risk_reward_ratio,
            
            -- Stop loss level (5% below current price)
            current_price * 0.95 as stop_loss_level,
            
            accumulation_maturity_signal,
            breakout_signal,
            catalysts_detected
            
        FROM combined_analysis
        WHERE current_price IS NOT NULL
    )
    
    SELECT 
        symbol,
        analysis_date,
        current_price,
        target_price,
        resistance_level,
        accumulation_score,
        breakout_readiness_score,
        early_signal_score,
        overall_breakout_score,
        
        -- Breakout timeframe
        CASE
            WHEN overall_breakout_score >= 85 AND distance_to_resistance_pct <= 3
                THEN '1-3 days'
            WHEN overall_breakout_score >= 75 AND distance_to_resistance_pct <= 5
                THEN '3-7 days'
            WHEN overall_breakout_score >= 65
                THEN '1-2 weeks'
            ELSE '2+ weeks'
        END as breakout_timeframe,
        
        -- Trading recommendation
        CASE
            WHEN overall_breakout_score >= 85 AND risk_reward_ratio >= 2
                THEN 'STRONG BUY - Immediate Entry'
            WHEN overall_breakout_score >= 75 AND risk_reward_ratio >= 1.5
                THEN 'BUY - Good Setup'
            WHEN overall_breakout_score >= 65
                THEN 'WATCH - Prepare Entry'
            ELSE 'MONITOR - Wait for Better Setup'
        END as recommendation,
        
        risk_reward_ratio,
        stop_loss_level,
        
        -- Confidence level
        CASE
            WHEN overall_breakout_score >= 85 THEN 'Very High'
            WHEN overall_breakout_score >= 75 THEN 'High'
            WHEN overall_breakout_score >= 65 THEN 'Medium'
            ELSE 'Low'
        END as confidence_level,
        
        -- Action plan
        CONCAT(
            'Entry: ', ROUND(current_price, 0), 
            ' | Target: ', ROUND(target_price, 0),
            ' | Stop: ', ROUND(stop_loss_level, 0),
            ' | Signals: ', COALESCE(catalysts_detected, 'Standard')
        ) as action_plan
        
    FROM final_candidates
    WHERE overall_breakout_score >= 50  -- Filter candidates dengan score minimal
    ORDER BY overall_breakout_score DESC
    """
    
    df = pd.read_sql(query, conn)
    
    if not df.empty:
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO breakout_candidates 
        (symbol, analysis_date, current_price, target_price, resistance_level,
         accumulation_score, breakout_readiness_score, early_signal_score, overall_breakout_score,
         breakout_timeframe, recommendation, risk_reward_ratio, stop_loss_level,
         confidence_level, action_plan)
        VALUES (%(symbol)s, %(analysis_date)s, %(current_price)s, %(target_price)s, %(resistance_level)s,
                %(accumulation_score)s, %(breakout_readiness_score)s, %(early_signal_score)s, %(overall_breakout_score)s,
                %(breakout_timeframe)s, %(recommendation)s, %(risk_reward_ratio)s, %(stop_loss_level)s,
                %(confidence_level)s, %(action_plan)s)
        ON CONFLICT (symbol, analysis_date) DO UPDATE SET
            current_price = EXCLUDED.current_price,
            target_price = EXCLUDED.target_price,
            resistance_level = EXCLUDED.resistance_level,
            accumulation_score = EXCLUDED.accumulation_score,
            breakout_readiness_score = EXCLUDED.breakout_readiness_score,
            early_signal_score = EXCLUDED.early_signal_score,
            overall_breakout_score = EXCLUDED.overall_breakout_score,
            breakout_timeframe = EXCLUDED.breakout_timeframe,
            recommendation = EXCLUDED.recommendation,
            risk_reward_ratio = EXCLUDED.risk_reward_ratio,
            stop_loss_level = EXCLUDED.stop_loss_level,
            confidence_level = EXCLUDED.confidence_level,
            action_plan = EXCLUDED.action_plan;
        """
        
        execute_batch(cur, insert_query, df.to_dict('records'))
        conn.commit()
        cur.close()
        
        strong_buy = len(df[df['recommendation'].str.contains('STRONG BUY')])
        buy_count = len(df[df['recommendation'].str.contains('BUY')])
        
        logger.info(f"✅ Breakout candidates identified:")
        logger.info(f"   - Total candidates: {len(df)}")
        logger.info(f"   - Strong buy: {strong_buy}")
        logger.info(f"   - Buy signals: {buy_count}")
    
    conn.close()

def generate_breakout_alert(**context):
    """
    Generate daily breakout alert untuk trading
    """
    conn = get_db_connection()
    
    query = """
    SELECT 
        symbol,
        current_price,
        target_price,
        overall_breakout_score,
        breakout_timeframe,
        recommendation,
        risk_reward_ratio,
        confidence_level,
        action_plan
    FROM breakout_candidates 
    WHERE analysis_date = CURRENT_DATE - INTERVAL '1 day'
      AND overall_breakout_score >= 70  -- High probability candidates only
    ORDER BY overall_breakout_score DESC
    LIMIT 10
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    if not df.empty:
        logger.info("🚀 TODAY'S BREAKOUT ALERTS:")
        logger.info("="*80)
        
        for _, row in df.iterrows():
            potential_gain = ((row['target_price'] - row['current_price']) / row['current_price'] * 100) if row['current_price'] > 0 else 0
            
            logger.info(f"📈 {row['symbol']} | Score: {row['overall_breakout_score']:.1f} | Confidence: {row['confidence_level']}")
            logger.info(f"   💰 Current: {row['current_price']:.0f} → Target: {row['target_price']:.0f} ({potential_gain:.1f}%)")
            logger.info(f"   ⏰ Timeframe: {row['breakout_timeframe']} | R/R: {row['risk_reward_ratio']:.1f}")
            logger.info(f"   🎯 {row['recommendation']}")
            logger.info(f"   📋 {row['action_plan']}")
            logger.info("-" * 80)
            
        # Send top 3 untuk immediate action
        top_3 = df.head(3)
        logger.info("🔥 TOP 3 IMMEDIATE ACTION:")
        for _, row in top_3.iterrows():
            logger.info(f"   {row['symbol']}: {row['recommendation']} - {row['breakout_timeframe']}")
            
    else:
        logger.info("📊 No high-probability breakout candidates today. Market in consolidation phase.")

# DAG Definition
with DAG(
    dag_id="accumulation_breakout_detector",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval='0 17 * * 1-5',  # Setiap hari kerja jam 5 sore (setelah market close)
    catchup=False,
    default_args=default_args,
    description="Deteksi saham yang sudah selesai akumulasi dan siap breakout dalam waktu dekat",
    tags=["breakout", "accumulation-complete", "swing-trading", "alert"]
) as dag:

    start_task = DummyOperator(
        task_id="start_breakout_detection"
    )

    create_tables = PythonOperator(
        task_id="create_breakout_tables",
        python_callable=create_breakout_analysis_tables
    )

    maturity_analysis = PythonOperator(
        task_id="analyze_accumulation_maturity",
        python_callable=analyze_accumulation_maturity
    )

    readiness_analysis = PythonOperator(
        task_id="analyze_breakout_readiness",
        python_callable=analyze_breakout_readiness
    )

    early_signals = PythonOperator(
        task_id="detect_early_breakout_signals",
        python_callable=detect_early_breakout_signals
    )

    calculate_candidates = PythonOperator(
        task_id="calculate_breakout_candidates",
        python_callable=calculate_breakout_candidates
    )

    daily_alert = PythonOperator(
        task_id="generate_breakout_alert",
        python_callable=generate_breakout_alert,
        provide_context=True
    )

    end_task = DummyOperator(
        task_id="end_breakout_detection"
    )

    # Task dependencies
    start_task >> create_tables
    create_tables >> [maturity_analysis, readiness_analysis, early_signals]
    [maturity_analysis, readiness_analysis, early_signals] >> calculate_candidates
    calculate_candidates >> daily_alert >> end_task