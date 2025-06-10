from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import pendulum
import logging
from utils.telegram import send_telegram_message

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

def create_relaxed_tables():
    """
    Create tabel untuk relaxed analysis
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS relaxed_breakout_candidates (
        symbol TEXT,
        analysis_date DATE,
        current_price NUMERIC,
        target_price NUMERIC,
        volume_score NUMERIC,
        momentum_score NUMERIC,
        foreign_score NUMERIC,
        technical_score NUMERIC,
        overall_score NUMERIC,
        timeframe_estimate TEXT,
        recommendation TEXT,
        risk_level TEXT,
        notes TEXT,
        PRIMARY KEY (symbol, analysis_date)
    );
    """)
    
    # Table for advanced bandarmology analysis
    cur.execute("""
    CREATE TABLE IF NOT EXISTS advanced_bandarmology_signals (
        symbol TEXT,
        analysis_date DATE,
        composite_score NUMERIC,
        signal_strength TEXT,
        confidence_level TEXT,
        volume_intensity NUMERIC,
        stealth_score NUMERIC,
        foreign_momentum NUMERIC,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, analysis_date)
    );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info("✅ Relaxed tables created")

def relaxed_momentum_analysis():
    """
    Analisis momentum yang lebih relaxed - fokus pada tren positif
    """
    conn = get_db_connection()
    
    query = """
    WITH price_momentum AS (
        SELECT 
            symbol,
            date,
            close,
            volume,
            foreign_buy,
            foreign_sell,
            high,
            low,
            (foreign_buy - foreign_sell) as foreign_net,
            
            -- Price momentum (5 hari, 10 hari)
            (close - LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date)) / 
            NULLIF(LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date), 0) * 100 as price_momentum_5d,
            
            (close - LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date)) / 
            NULLIF(LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date), 0) * 100 as price_momentum_10d,
            
            -- Volume trend (lebih relaxed)
            volume / NULLIF(AVG(volume) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ), 0) as volume_ratio_7d,
            
            -- Foreign flow trend (3 hari)
            SUM(foreign_buy - foreign_sell) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) as foreign_net_3d,
            
            -- Resistance level (20 hari, lebih pendek)
            MAX(high) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) as resistance_20d,
            
            -- Support level  
            MIN(low) OVER (
                PARTITION BY symbol ORDER BY date 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) as support_20d
            
        FROM daily_stock_summary
        WHERE date >= CURRENT_DATE - INTERVAL '2 months'
    ),
    
    latest_momentum AS (
        SELECT DISTINCT ON (symbol)
            symbol,
            date as analysis_date,
            close as current_price,
            resistance_20d,
            support_20d,
            price_momentum_5d,
            price_momentum_10d,
            volume_ratio_7d,
            foreign_net_3d,
            
            -- Distance ke resistance (relaxed threshold)
            (resistance_20d - close) / NULLIF(close, 0) * 100 as distance_to_resistance_pct,
            
            -- Target price estimate
            resistance_20d as target_price,
            
            -- Volume Score (relaxed: 1.1x sudah bagus)
            CASE 
                WHEN volume_ratio_7d >= 1.5 THEN 80
                WHEN volume_ratio_7d >= 1.3 THEN 65
                WHEN volume_ratio_7d >= 1.1 THEN 50
                WHEN volume_ratio_7d >= 1.0 THEN 35
                ELSE 20
            END as volume_score,
            
            -- Momentum Score (price trend)
            CASE 
                WHEN price_momentum_5d > 3 AND price_momentum_10d > 2 THEN 80
                WHEN price_momentum_5d > 1 AND price_momentum_10d > 0 THEN 65
                WHEN price_momentum_5d > 0 OR price_momentum_10d > 0 THEN 50
                WHEN price_momentum_5d > -2 AND price_momentum_10d > -3 THEN 35
                ELSE 20
            END as momentum_score,
            
            -- Foreign Score (relaxed)
            CASE 
                WHEN foreign_net_3d > 0 THEN 70
                WHEN foreign_net_3d >= 0 THEN 50
                ELSE 30
            END as foreign_score,
            
            -- Technical Score (position vs support/resistance)
            CASE 
                WHEN (resistance_20d - close) / NULLIF(close, 0) * 100 <= 3 THEN 80
                WHEN (resistance_20d - close) / NULLIF(close, 0) * 100 <= 7 THEN 65
                WHEN (resistance_20d - close) / NULLIF(close, 0) * 100 <= 12 THEN 50
                WHEN (resistance_20d - close) / NULLIF(close, 0) * 100 <= 20 THEN 35
                ELSE 20
            END as technical_score
            
        FROM price_momentum
        WHERE date >= CURRENT_DATE - INTERVAL '3 days'  -- Recent data
        ORDER BY symbol, date DESC
    )
    
    SELECT 
        symbol,
        analysis_date,
        current_price,
        target_price,
        volume_score,
        momentum_score,
        foreign_score,
        technical_score,
        distance_to_resistance_pct,
        
        -- Overall score (weighted, lebih generous)
        (volume_score * 0.25 + 
         momentum_score * 0.35 + 
         foreign_score * 0.20 + 
         technical_score * 0.20) as overall_score,
         
        -- Timeframe estimate
        CASE
            WHEN (volume_score + momentum_score + technical_score) / 3 >= 70 
                 AND distance_to_resistance_pct <= 5
                THEN '2-5 days'
            WHEN (volume_score + momentum_score + technical_score) / 3 >= 60
                 AND distance_to_resistance_pct <= 10
                THEN '1-2 weeks'
            WHEN (volume_score + momentum_score) / 2 >= 50
                THEN '2-4 weeks'
            ELSE '1+ month'
        END as timeframe_estimate,
        
        -- Recommendation (lebih accessible)
        CASE
            WHEN (volume_score * 0.25 + momentum_score * 0.35 + foreign_score * 0.20 + technical_score * 0.20) >= 65
                THEN 'BUY - Strong Setup'
            WHEN (volume_score * 0.25 + momentum_score * 0.35 + foreign_score * 0.20 + technical_score * 0.20) >= 55
                THEN 'WATCH - Good Potential'
            WHEN (volume_score * 0.25 + momentum_score * 0.35 + foreign_score * 0.20 + technical_score * 0.20) >= 45
                THEN 'MONITOR - Building Momentum'
            ELSE 'WAIT - No Clear Signal'
        END as recommendation,
        
        -- Risk level
        CASE
            WHEN distance_to_resistance_pct <= 5 AND volume_score >= 60 THEN 'Low Risk'
            WHEN distance_to_resistance_pct <= 10 AND momentum_score >= 60 THEN 'Medium Risk'
            WHEN distance_to_resistance_pct <= 15 THEN 'Medium-High Risk'
            ELSE 'High Risk'
        END as risk_level,
        
        -- Notes
        CONCAT(
            'Vol: ', volume_score, '/100 | ',
            'Momentum: ', momentum_score, '/100 | ',
            'Foreign: ', foreign_score, '/100 | ',
            'Technical: ', technical_score, '/100'
        ) as notes
        
    FROM latest_momentum
    WHERE current_price IS NOT NULL
      AND target_price IS NOT NULL
      AND distance_to_resistance_pct <= 25  -- More relaxed distance filter
    ORDER BY overall_score DESC
    """
    
    df = pd.read_sql(query, conn)
    
    if not df.empty:
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO relaxed_breakout_candidates 
        (symbol, analysis_date, current_price, target_price, volume_score, momentum_score,
         foreign_score, technical_score, overall_score, timeframe_estimate, 
         recommendation, risk_level, notes)
        VALUES (%(symbol)s, %(analysis_date)s, %(current_price)s, %(target_price)s, 
                %(volume_score)s, %(momentum_score)s, %(foreign_score)s, %(technical_score)s,
                %(overall_score)s, %(timeframe_estimate)s, %(recommendation)s, 
                %(risk_level)s, %(notes)s)
        ON CONFLICT (symbol, analysis_date) DO UPDATE SET
            current_price = EXCLUDED.current_price,
            target_price = EXCLUDED.target_price,
            volume_score = EXCLUDED.volume_score,
            momentum_score = EXCLUDED.momentum_score,
            foreign_score = EXCLUDED.foreign_score,
            technical_score = EXCLUDED.technical_score,
            overall_score = EXCLUDED.overall_score,
            timeframe_estimate = EXCLUDED.timeframe_estimate,
            recommendation = EXCLUDED.recommendation,
            risk_level = EXCLUDED.risk_level,
            notes = EXCLUDED.notes;
        """
        
        execute_batch(cur, insert_query, df.to_dict('records'))
        conn.commit()
        cur.close()
        
        buy_signals = len(df[df['recommendation'].str.contains('BUY')])
        watch_signals = len(df[df['recommendation'].str.contains('WATCH')])
        
        logger.info(f"✅ Relaxed momentum analysis:")
        logger.info(f"   - Total candidates: {len(df)}")
        logger.info(f"   - Buy signals: {buy_signals}")
        logger.info(f"   - Watch signals: {watch_signals}")
    else:
        logger.info("⚠️ Still no candidates found even with relaxed criteria")
    
    conn.close()

def send_advanced_bandarmology_report():
    """
    Send Advanced Bandarmology analysis report to Telegram
    Fixed version dengan better error handling untuk division by zero
    """
    try:
        conn = get_db_connection()
        
        # Get latest date dengan error handling
        latest_date_query = "SELECT MAX(date) FROM daily_stock_summary"
        latest_date_df = pd.read_sql(latest_date_query, conn)
        
        if latest_date_df.empty or latest_date_df.iloc[0, 0] is None:
            logger.warning("No date data available")
            return "No date data available"
        
        latest_date = latest_date_df.iloc[0, 0]
        date_filter = latest_date.strftime('%Y-%m-%d')
        logger.info(f"Running Advanced Bandarmology analysis for date: {date_filter}")
        
        # Simplified but still advanced query dengan better error handling
        advanced_bandar_sql = f"""
        WITH base_data AS (
            SELECT 
                symbol,
                date,
                close,
                prev_close,
                high,
                low,
                volume,
                value,
                foreign_buy,
                foreign_sell,
                COALESCE(foreign_buy - foreign_sell, 0) AS foreign_net,
                COALESCE(bid, 0) as bid,
                COALESCE(offer, 0) as offer,
                COALESCE(bid_volume, 0) as bid_volume,
                COALESCE(offer_volume, 0) as offer_volume,
                name
            FROM daily_stock_summary
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
              AND volume > 0  -- Ensure no zero volumes
              AND close > 0   -- Ensure no zero prices
              AND prev_close > 0
        ),
        
        volume_metrics AS (
            SELECT 
                symbol,
                date,
                close,
                prev_close,
                high,
                low,
                volume,
                foreign_net,
                bid_volume,
                offer_volume,
                name,
                
                -- Safe volume averages
                COALESCE(AVG(volume) OVER (
                    PARTITION BY symbol ORDER BY date 
                    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ), volume) as vol_avg_20d,
                
                COALESCE(AVG(volume) OVER (
                    PARTITION BY symbol ORDER BY date 
                    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
                ), volume) as vol_avg_10d,
                
                -- Safe foreign flow averages
                COALESCE(AVG(ABS(foreign_net)) OVER (
                    PARTITION BY symbol ORDER BY date 
                    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
                ), ABS(foreign_net) + 1) as foreign_avg_abs_10d
                
            FROM base_data
            WHERE volume > 0
        ),
        
        advanced_scores AS (
            SELECT 
                symbol,
                date,
                close,
                volume,
                foreign_net,
                bid_volume,
                offer_volume,
                name,
                vol_avg_20d,
                
                -- 1. VOLUME INTENSITY SCORE (0-100) - Safe calculation
                CASE 
                    WHEN vol_avg_20d > 0 
                    THEN LEAST(100, GREATEST(0, ((volume / vol_avg_20d) - 1) * 30))
                    ELSE 0
                END AS volume_intensity_score,
                
                -- 2. VOLUME SURGE SCORE (0-100) 
                CASE 
                    WHEN vol_avg_10d > 0 AND volume > vol_avg_10d * 1.5
                    THEN LEAST(100, (volume / vol_avg_10d) * 20)
                    WHEN vol_avg_10d > 0 AND volume > vol_avg_10d * 1.2
                    THEN LEAST(100, (volume / vol_avg_10d) * 15)
                    ELSE 0
                END AS volume_surge_score,
                
                -- 3. FOREIGN MOMENTUM SCORE (0-100) - Safe calculation
                CASE 
                    WHEN foreign_net > 0 AND foreign_avg_abs_10d > 0
                    THEN LEAST(100, (foreign_net / foreign_avg_abs_10d) * 25)
                    WHEN foreign_net > 0
                    THEN 50  -- Default positive score
                    ELSE 0
                END AS foreign_momentum_score,
                
                -- 4. BID-ASK STRENGTH SCORE (0-100) - Safe calculation
                CASE 
                    WHEN offer_volume > 0 AND bid_volume > offer_volume
                    THEN LEAST(100, ((bid_volume / GREATEST(offer_volume, 1)) - 1) * 40)
                    WHEN bid_volume > 0 AND offer_volume = 0
                    THEN 70  -- Strong bid with no offer
                    ELSE 0
                END AS bid_ask_strength_score,
                
                -- 5. STEALTH ACCUMULATION SCORE (0-100)
                CASE 
                    WHEN vol_avg_20d > 0 
                         AND volume > vol_avg_20d * 1.3
                         AND prev_close > 0
                         AND ABS((close - prev_close) / prev_close * 100) < 2.5
                    THEN LEAST(100, (volume / vol_avg_20d) * 25)
                    ELSE 0
                END AS stealth_accumulation_score,
                
                -- 6. BUYING ON WEAKNESS SCORE (0-100)
                CASE 
                    WHEN prev_close > 0 
                         AND close < prev_close 
                         AND vol_avg_10d > 0
                         AND volume > vol_avg_10d * 1.2
                    THEN 75  -- Strong buying on price weakness
                    WHEN prev_close > 0 
                         AND close >= prev_close 
                         AND vol_avg_10d > 0
                         AND volume > vol_avg_10d * 1.5
                    THEN 60  -- Normal strength buying
                    ELSE 0
                END AS buying_weakness_score,
                
                -- Calculate volume ratio safely
                CASE 
                    WHEN vol_avg_20d > 0 THEN volume / vol_avg_20d
                    ELSE 1
                END AS volume_ratio
                
            FROM volume_metrics
        ),
        
        foreign_consistency AS (
            SELECT 
                symbol,
                date,
                close,
                volume,
                foreign_net,
                name,
                volume_intensity_score,
                volume_surge_score,
                foreign_momentum_score,
                bid_ask_strength_score,
                stealth_accumulation_score,
                buying_weakness_score,
                volume_ratio,
                
                -- 7. FOREIGN CONSISTENCY SCORE (0-100)
                (COUNT(CASE WHEN foreign_net > 0 THEN 1 END) OVER (
                    PARTITION BY symbol ORDER BY date 
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                ) * 100.0 / 6) AS foreign_consistency_score
                
            FROM advanced_scores
        ),
        
        final_composite AS (
            SELECT 
                symbol,
                date,
                close,
                volume,
                foreign_net,
                name,
                volume_ratio,
                
                -- Individual component scores
                volume_intensity_score,
                volume_surge_score,
                foreign_momentum_score,
                bid_ask_strength_score,
                stealth_accumulation_score,
                buying_weakness_score,
                foreign_consistency_score,
                
                -- WEIGHTED COMPOSITE SCORE (Total: 100%)
                (volume_intensity_score * 0.20 +      -- 20% - Volume expansion
                 volume_surge_score * 0.15 +          -- 15% - Volume surge  
                 foreign_momentum_score * 0.20 +      -- 20% - Foreign activity
                 bid_ask_strength_score * 0.10 +      -- 10% - Order book
                 stealth_accumulation_score * 0.20 +  -- 20% - Stealth buying
                 buying_weakness_score * 0.10 +       -- 10% - Buying on dips
                 foreign_consistency_score * 0.05     -- 5%  - Consistency
                ) AS composite_bandar_score
                
            FROM foreign_consistency
        ),
        
        final_ranking AS (
            SELECT 
                symbol,
                name,
                close,
                volume_ratio,
                foreign_net,
                ROUND(composite_bandar_score, 1) as composite_bandar_score,
                
                -- Signal strength classification
                CASE
                    WHEN composite_bandar_score >= 70 THEN 'Sangat Kuat (Tier 1)'
                    WHEN composite_bandar_score >= 55 THEN 'Kuat (Tier 2)'
                    WHEN composite_bandar_score >= 40 THEN 'Sedang (Tier 3)'
                    WHEN composite_bandar_score >= 25 THEN 'Lemah (Tier 4)'
                    ELSE 'Sangat Lemah'
                END AS advanced_signal_strength,
                
                -- Confidence level based on score and volume
                CASE
                    WHEN composite_bandar_score >= 65 AND volume_ratio >= 2.0 THEN 'Very High Confidence'
                    WHEN composite_bandar_score >= 55 AND volume_ratio >= 1.5 THEN 'High Confidence'
                    WHEN composite_bandar_score >= 45 AND volume_ratio >= 1.3 THEN 'Medium Confidence'
                    WHEN composite_bandar_score >= 35 THEN 'Low-Medium Confidence'
                    ELSE 'Low Confidence'
                END AS confidence_level,
                
                -- Expected timeframe
                CASE
                    WHEN composite_bandar_score >= 65 THEN '1-3 hari'
                    WHEN composite_bandar_score >= 55 THEN '3-7 hari'
                    WHEN composite_bandar_score >= 40 THEN '1-2 minggu'
                    ELSE '2+ minggu'
                END AS expected_timeframe,
                
                -- Key component scores for analysis
                ROUND(volume_intensity_score, 0) as volume_intensity,
                ROUND(foreign_momentum_score, 0) as foreign_momentum,
                ROUND(stealth_accumulation_score, 0) as stealth_score,
                ROUND(foreign_consistency_score, 0) as foreign_consistency
                
            FROM final_composite
            WHERE date = '{date_filter}'
              AND composite_bandar_score >= 25  -- Filter meaningful signals
              AND volume_ratio >= 1.1          -- Minimum volume requirement
        )
        
        SELECT * FROM final_ranking
        ORDER BY composite_bandar_score DESC, volume_ratio DESC
        LIMIT 20
        """
        
        bandar_df = pd.read_sql(advanced_bandar_sql, conn)
        conn.close()
        
        if bandar_df.empty:
            message = "🔍 *ADVANCED BANDARMOLOGY ANALYSIS* 🔍\n\n"
            message += f"No significant advanced bandarmology patterns detected today ({date_filter}).\n"
            message += "Market showing normal institutional behavior - no stealth accumulation signals.\n\n"
            message += "*Status:* Waiting for clearer accumulation patterns.\n"
            message += "*Recommendation:* Monitor daily for emerging signals."
            
            send_telegram_message(message)
            logger.info("📊 No advanced bandarmology patterns found")
            return "No advanced bandarmology patterns found"
        
        # Build Telegram message
        message = f"🔍 *ADVANCED BANDARMOLOGY ANALYSIS ({date_filter})* 🔍\n\n"
        message += "AI-powered detection of institutional accumulation patterns:\n\n"
        
        # Tier 1 signals (Very Strong)
        tier1_signals = bandar_df[bandar_df['advanced_signal_strength'] == 'Sangat Kuat (Tier 1)']
        if not tier1_signals.empty:
            message += "💎 *TIER 1 - VERY STRONG ACCUMULATION:*\n"
            for _, row in tier1_signals.iterrows():
                message += f"⭐⭐⭐ *{row['symbol']}* ({row['name']})\n"
                message += f"   Price: Rp{row['close']:,.0f} | Score: {row['composite_bandar_score']}/100\n"
                message += f"   Volume Ratio: {row['volume_ratio']:.2f}x | {row['confidence_level']}\n"
                message += f"   Timeframe: {row['expected_timeframe']}\n"
                
                # Show key strength components
                strengths = []
                if row['volume_intensity'] >= 40:
                    strengths.append(f"Vol: {row['volume_intensity']:.0f}")
                if row['foreign_momentum'] >= 40:
                    strengths.append(f"Foreign: {row['foreign_momentum']:.0f}")
                if row['stealth_score'] >= 40:
                    strengths.append(f"Stealth: {row['stealth_score']:.0f}")
                if row['foreign_consistency'] >= 60:
                    strengths.append(f"Consistent: {row['foreign_consistency']:.0f}")
                    
                if strengths:
                    message += f"   Key Strengths: {' | '.join(strengths)}\n"
                    
                message += "\n"
        
        # Tier 2 signals (Strong)
        tier2_signals = bandar_df[bandar_df['advanced_signal_strength'] == 'Kuat (Tier 2)']
        if not tier2_signals.empty:
            message += "🔥 *TIER 2 - STRONG ACCUMULATION:*\n"
            for _, row in tier2_signals.head(4).iterrows():
                message += f"⭐⭐ *{row['symbol']}* | Score: {row['composite_bandar_score']}\n"
                message += f"   Rp{row['close']:,.0f} | Vol: {row['volume_ratio']:.2f}x | {row['expected_timeframe']}\n\n"
        
        # Tier 3 signals (Medium) - show top 3
        tier3_signals = bandar_df[bandar_df['advanced_signal_strength'] == 'Sedang (Tier 3)']
        if not tier3_signals.empty:
            message += f"📊 *TIER 3 - BUILDING MOMENTUM:* {len(tier3_signals)} stocks\n"
            top_tier3 = tier3_signals.head(3)
            for _, row in top_tier3.iterrows():
                message += f"⭐ {row['symbol']} (Score: {row['composite_bandar_score']}) "
            message += "\n\n"
        
        # Market intelligence summary
        avg_score = bandar_df['composite_bandar_score'].mean()
        high_confidence = len(bandar_df[bandar_df['confidence_level'].isin(['Very High Confidence', 'High Confidence'])])
        avg_volume_ratio = bandar_df['volume_ratio'].mean()
        
        message += f"📈 *MARKET INTELLIGENCE:*\n"
        message += f"• Total candidates: {len(bandar_df)}\n"
        message += f"• High confidence signals: {high_confidence}\n"
        message += f"• Average accumulation score: {avg_score:.1f}/100\n"
        message += f"• Average volume expansion: {avg_volume_ratio:.2f}x\n\n"
        
        # Trading recommendations
        if not tier1_signals.empty or not tier2_signals.empty:
            message += "*💡 ACTIONABLE INSIGHTS:*\n"
            if not tier1_signals.empty:
                message += "• Tier 1: Immediate watchlist - consider entry on any dip\n"
            if not tier2_signals.empty:
                message += "• Tier 2: Medium-term watchlist - wait for volume confirmation\n"
            message += "• Best entry timing: Morning weakness or consolidation break\n"
            message += "• Risk management: 5-7% stop loss below entry\n"
            message += "• Position sizing: Higher confidence = larger allocation\n\n"
        
        message += "*🔬 ALGORITHM COMPONENTS:*\n"
        message += "Advanced 7-factor institutional detection system:\n"
        message += "• Volume Intensity & Surge Analysis (35%)\n"
        message += "• Foreign Flow Momentum & Consistency (25%)\n"
        message += "• Stealth Accumulation Detection (20%)\n"
        message += "• Bid-Ask Imbalance & Weakness Buying (20%)\n\n"
        
        message += "*Disclaimer:* AI-powered analysis for educational purposes. Always combine with fundamental research and risk management."
        
        send_telegram_message(message)
        
        # Enhanced logging
        tier1_count = len(tier1_signals)
        tier2_count = len(tier2_signals) 
        tier3_count = len(tier3_signals)
        
        logger.info(f"✅ Advanced Bandarmology Analysis Completed:")
        logger.info(f"   - Date analyzed: {date_filter}")
        logger.info(f"   - Total candidates: {len(bandar_df)}")
        logger.info(f"   - Tier 1 (Very Strong): {tier1_count}")
        logger.info(f"   - Tier 2 (Strong): {tier2_count}")
        logger.info(f"   - Tier 3 (Medium): {tier3_count}")
        logger.info(f"   - Average score: {avg_score:.1f}/100")
        logger.info(f"   - High confidence signals: {high_confidence}")
        
        return f"Advanced bandarmology analysis sent: {len(bandar_df)} candidates (T1:{tier1_count}, T2:{tier2_count}, T3:{tier3_count}, Avg:{avg_score:.1f})"
        
    except Exception as e:
        logger.error(f"Error in send_advanced_bandarmology_report: {str(e)}")
        return f"Error: {str(e)}"

def send_breakout_signals():
    """
    Send breakout signals to Telegram
    """
    try:
        conn = get_db_connection()
        
        query = """
        SELECT 
            symbol,
            current_price,
            target_price,
            overall_score,
            timeframe_estimate,
            recommendation,
            risk_level,
            volume_score,
            momentum_score,
            foreign_score,
            technical_score
        FROM relaxed_breakout_candidates 
        WHERE analysis_date >= CURRENT_DATE - INTERVAL '1 day'
          AND overall_score >= 45  -- Lower threshold for broader coverage
        ORDER BY overall_score DESC
        LIMIT 15
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df.empty:
            message = "🔍 *BREAKOUT ANALYSIS* 📊\n\n"
            message += "No breakout candidates found today.\n"
            message += "Market in consolidation phase - waiting for better setups.\n\n"
            message += "*Recommendation:* Stay patient and wait for clearer signals."
            
            send_telegram_message(message)
            logger.info("📊 No breakout candidates found - sent consolidation message")
            return "No breakout candidates found"
        
        # Separate by recommendation type
        buy_signals = df[df['recommendation'].str.contains('BUY')]
        watch_signals = df[df['recommendation'].str.contains('WATCH')]
        monitor_signals = df[df['recommendation'].str.contains('MONITOR')]
        
        message = "🚀 *BREAKOUT OPPORTUNITIES* 📊\n\n"
        message += f"Found {len(df)} potential breakout candidates:\n\n"
        
        # BUY Signals (highest priority)
        if not buy_signals.empty:
            message += f"🔥 *BUY SIGNALS ({len(buy_signals)})*\n"
            for _, row in buy_signals.head(3).iterrows():
                potential_gain = ((row['target_price'] - row['current_price']) / row['current_price'] * 100)
                
                if row['overall_score'] >= 65:
                    emoji = "⭐⭐⭐"  # Strong signal
                else:
                    emoji = "⭐⭐"    # Good signal
                    
                message += f"{emoji} *{row['symbol']}*\n"
                message += f"   Price: Rp{row['current_price']:,.0f} → Rp{row['target_price']:,.0f} (+{potential_gain:.1f}%)\n"
                message += f"   Score: {row['overall_score']:.1f}/100 | {row['timeframe_estimate']} | {row['risk_level']}\n"
                
                # Show key scores
                scores = []
                if row['volume_score'] >= 60:
                    scores.append(f"Vol: {row['volume_score']:.0f}")
                if row['momentum_score'] >= 60:
                    scores.append(f"Mom: {row['momentum_score']:.0f}")
                if row['foreign_score'] >= 60:
                    scores.append(f"For: {row['foreign_score']:.0f}")
                if row['technical_score'] >= 60:
                    scores.append(f"Tech: {row['technical_score']:.0f}")
                    
                if scores:
                    message += f"   Strengths: {' | '.join(scores)}\n"
                    
                message += "\n"
        
        # WATCH Signals
        if not watch_signals.empty:
            message += f"👀 *WATCH LIST ({len(watch_signals)})*\n"
            for _, row in watch_signals.head(3).iterrows():
                potential_gain = ((row['target_price'] - row['current_price']) / row['current_price'] * 100)
                message += f"⭐ *{row['symbol']}* | Score: {row['overall_score']:.1f}\n"
                message += f"   Rp{row['current_price']:,.0f} → Rp{row['target_price']:,.0f} (+{potential_gain:.1f}%) | {row['timeframe_estimate']}\n\n"
        
        # Show top MONITOR if no BUY/WATCH
        if buy_signals.empty and watch_signals.empty and not monitor_signals.empty:
            message += f"📊 *BUILDING MOMENTUM ({len(monitor_signals)})*\n"
            for _, row in monitor_signals.head(3).iterrows():
                potential_gain = ((row['target_price'] - row['current_price']) / row['current_price'] * 100)
                message += f"📈 *{row['symbol']}* | Score: {row['overall_score']:.1f}\n"
                message += f"   Rp{row['current_price']:,.0f} → Rp{row['target_price']:,.0f} (+{potential_gain:.1f}%) | {row['timeframe_estimate']}\n\n"
        
        # Trading tips
        message += "*Tips Strategi Breakout:*\n"
        message += "• Entry: Wait for volume confirmation atau dip buying\n"
        message += "• BUY signals: Higher conviction, larger position\n"
        message += "• WATCH signals: Wait for better entry atau smaller position\n"
        message += "• Stop loss: 5-7% below entry\n"
        message += "• Target: Sesuai resistance level estimate\n\n"
        
        message += "*Disclaimer:* Breakout predictions based on momentum & volume analysis. Always conduct your own research."
        
        send_telegram_message(message)
        
        # Log summary
        buy_count = len(buy_signals)
        watch_count = len(watch_signals)
        monitor_count = len(monitor_signals)
        
        logger.info(f"✅ Sent breakout signals to Telegram:")
        logger.info(f"   - Total candidates: {len(df)}")
        logger.info(f"   - BUY signals: {buy_count}")
        logger.info(f"   - WATCH signals: {watch_count}")
        logger.info(f"   - MONITOR signals: {monitor_count}")
        
        return f"Sent {len(df)} breakout signals to Telegram (BUY: {buy_count}, WATCH: {watch_count})"
        
    except Exception as e:
        logger.error(f"Error sending breakout signals: {str(e)}")
        return f"Error: {str(e)}"
    """
    Send breakout signals to Telegram
    """
    try:
        conn = get_db_connection()
        
        query = """
        SELECT 
            symbol,
            current_price,
            target_price,
            overall_score,
            timeframe_estimate,
            recommendation,
            risk_level,
            volume_score,
            momentum_score,
            foreign_score,
            technical_score
        FROM relaxed_breakout_candidates 
        WHERE analysis_date >= CURRENT_DATE - INTERVAL '1 day'
          AND overall_score >= 45  -- Lower threshold for broader coverage
        ORDER BY overall_score DESC
        LIMIT 15
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df.empty:
            message = "🔍 *BREAKOUT ANALYSIS* 📊\n\n"
            message += "No breakout candidates found today.\n"
            message += "Market in consolidation phase - waiting for better setups.\n\n"
            message += "*Recommendation:* Stay patient and wait for clearer signals."
            
            send_telegram_message(message)
            logger.info("📊 No breakout candidates found - sent consolidation message")
            return "No breakout candidates found"
        
        # Separate by recommendation type
        buy_signals = df[df['recommendation'].str.contains('BUY')]
        watch_signals = df[df['recommendation'].str.contains('WATCH')]
        monitor_signals = df[df['recommendation'].str.contains('MONITOR')]
        
        message = "🚀 *BREAKOUT OPPORTUNITIES* 📊\n\n"
        message += f"Found {len(df)} potential breakout candidates:\n\n"
        
        # BUY Signals (highest priority)
        if not buy_signals.empty:
            message += f"🔥 *BUY SIGNALS ({len(buy_signals)})*\n"
            for _, row in buy_signals.head(3).iterrows():
                potential_gain = ((row['target_price'] - row['current_price']) / row['current_price'] * 100)
                
                if row['overall_score'] >= 65:
                    emoji = "⭐⭐⭐"  # Strong signal
                else:
                    emoji = "⭐⭐"    # Good signal
                    
                message += f"{emoji} *{row['symbol']}*\n"
                message += f"   Price: Rp{row['current_price']:,.0f} → Rp{row['target_price']:,.0f} (+{potential_gain:.1f}%)\n"
                message += f"   Score: {row['overall_score']:.1f}/100 | {row['timeframe_estimate']} | {row['risk_level']}\n"
                
                # Show key scores
                scores = []
                if row['volume_score'] >= 60:
                    scores.append(f"Vol: {row['volume_score']:.0f}")
                if row['momentum_score'] >= 60:
                    scores.append(f"Mom: {row['momentum_score']:.0f}")
                if row['foreign_score'] >= 60:
                    scores.append(f"For: {row['foreign_score']:.0f}")
                if row['technical_score'] >= 60:
                    scores.append(f"Tech: {row['technical_score']:.0f}")
                    
                if scores:
                    message += f"   Strengths: {' | '.join(scores)}\n"
                    
                message += "\n"
        
        # WATCH Signals
        if not watch_signals.empty:
            message += f"👀 *WATCH LIST ({len(watch_signals)})*\n"
            for _, row in watch_signals.head(3).iterrows():
                potential_gain = ((row['target_price'] - row['current_price']) / row['current_price'] * 100)
                message += f"⭐ *{row['symbol']}* | Score: {row['overall_score']:.1f}\n"
                message += f"   Rp{row['current_price']:,.0f} → Rp{row['target_price']:,.0f} (+{potential_gain:.1f}%) | {row['timeframe_estimate']}\n\n"
        
        # Show top MONITOR if no BUY/WATCH
        if buy_signals.empty and watch_signals.empty and not monitor_signals.empty:
            message += f"📊 *BUILDING MOMENTUM ({len(monitor_signals)})*\n"
            for _, row in monitor_signals.head(3).iterrows():
                potential_gain = ((row['target_price'] - row['current_price']) / row['current_price'] * 100)
                message += f"📈 *{row['symbol']}* | Score: {row['overall_score']:.1f}\n"
                message += f"   Rp{row['current_price']:,.0f} → Rp{row['target_price']:,.0f} (+{potential_gain:.1f}%) | {row['timeframe_estimate']}\n\n"
        
        # Trading tips
        message += "*Tips Strategi Breakout:*\n"
        message += "• Entry: Wait for volume confirmation atau dip buying\n"
        message += "• BUY signals: Higher conviction, larger position\n"
        message += "• WATCH signals: Wait for better entry atau smaller position\n"
        message += "• Stop loss: 5-7% below entry\n"
        message += "• Target: Sesuai resistance level estimate\n\n"
        
        message += "*Disclaimer:* Breakout predictions based on momentum & volume analysis. Always conduct your own research."
        
        send_telegram_message(message)
        
        # Log summary
        buy_count = len(buy_signals)
        watch_count = len(watch_signals)
        monitor_count = len(monitor_signals)
        
        logger.info(f"✅ Sent breakout signals to Telegram:")
        logger.info(f"   - Total candidates: {len(df)}")
        logger.info(f"   - BUY signals: {buy_count}")
        logger.info(f"   - WATCH signals: {watch_count}")
        logger.info(f"   - MONITOR signals: {monitor_count}")
        
        return f"Sent {len(df)} breakout signals to Telegram (BUY: {buy_count}, WATCH: {watch_count})"
        
    except Exception as e:
        logger.error(f"Error sending breakout signals: {str(e)}")
        return f"Error: {str(e)}"

# DAG Definition
with DAG(
    dag_id="relaxed_breakout_detector",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval='0 17 * * 1-5',  # Setiap hari kerja jam 5 sore
    catchup=False,
    default_args=default_args,
    description="Relaxed breakout detection dengan advanced bandarmology analysis",
    tags=["relaxed", "momentum", "accessible", "bandarmology", "advanced"]
) as dag:

    start_task = DummyOperator(
        task_id="start_relaxed_analysis"
    )

    create_tables = PythonOperator(
        task_id="create_relaxed_tables",
        python_callable=create_relaxed_tables
    )

    momentum_analysis = PythonOperator(
        task_id="relaxed_momentum_analysis",
        python_callable=relaxed_momentum_analysis
    )

    send_signals = PythonOperator(
        task_id="send_breakout_signals",
        python_callable=send_breakout_signals
    )

    send_bandarmology = PythonOperator(
        task_id="send_advanced_bandarmology",
        python_callable=send_advanced_bandarmology_report
    )

    end_task = DummyOperator(
        task_id="end_relaxed_analysis"
    )

    # Task dependencies
    start_task >> create_tables >> momentum_analysis >> [send_signals, send_bandarmology] >> end_task