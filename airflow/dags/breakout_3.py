from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
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
    return psycopg2.connect(
        host="postgres",
        dbname="airflow", 
        user="airflow",
        password="airflow"
    )

def create_breakout_lifecycle_tables():
    """Create tables for complete breakout lifecycle tracking"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Table for tracking breakout history
    cur.execute("""
    CREATE TABLE IF NOT EXISTS breakout_history (
        symbol TEXT,
        breakout_date DATE,
        resistance_level NUMERIC,
        breakout_price NUMERIC,
        breakout_volume BIGINT,
        volume_ratio NUMERIC,
        status TEXT DEFAULT 'PENDING',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, breakout_date)
    );
    """)
    
    # Table for validation results
    cur.execute("""
    CREATE TABLE IF NOT EXISTS breakout_validation_results (
        symbol TEXT,
        breakout_date DATE,
        validation_date DATE,
        validation_status TEXT,
        confidence_score NUMERIC,
        price_performance NUMERIC,
        volume_sustainability NUMERIC,
        recommendation TEXT,
        PRIMARY KEY (symbol, breakout_date)
    );
    """)
    
    # Table for retest opportunities
    cur.execute("""
    CREATE TABLE IF NOT EXISTS retest_opportunities (
        symbol TEXT,
        breakout_date DATE,
        retest_date DATE,
        retest_price NUMERIC,
        support_level NUMERIC,
        retest_quality TEXT,
        entry_recommendation TEXT,
        PRIMARY KEY (symbol, breakout_date, retest_date)
    );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info("✅ Breakout lifecycle tables created")

def option_a_post_breakout_validation():
    """
    OPTION A: Post-Breakout Validation
    Validate if recent breakouts are true or false after 2-3 days
    """
    try:
        conn = get_db_connection()
        
        # First, detect and record recent breakouts
        detect_breakouts_query = """
        WITH resistance_levels AS (
            SELECT 
                symbol,
                date,
                high,
                close,
                volume,
                -- Calculate resistance as highest high in last 20 days
                MAX(high) OVER (
                    PARTITION BY symbol ORDER BY date 
                    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) as resistance_20d,
                -- Average volume for comparison
                AVG(volume) OVER (
                    PARTITION BY symbol ORDER BY date 
                    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) as avg_volume_20d
            FROM daily_stock_summary
            WHERE date >= CURRENT_DATE - INTERVAL '10 days'
        ),
        
        breakout_candidates AS (
            SELECT 
                symbol,
                date as breakout_date,
                resistance_20d as resistance_level,
                close as breakout_price,
                volume as breakout_volume,
                volume / NULLIF(avg_volume_20d, 0) as volume_ratio
            FROM resistance_levels
            WHERE close > resistance_20d * 1.02  -- 2% above resistance
              AND volume > avg_volume_20d * 1.5  -- 50% volume increase
              AND date >= CURRENT_DATE - INTERVAL '5 days'
              AND resistance_20d > 0
        )
        
        INSERT INTO breakout_history (symbol, breakout_date, resistance_level, breakout_price, breakout_volume, volume_ratio)
        SELECT * FROM breakout_candidates
        ON CONFLICT (symbol, breakout_date) DO NOTHING
        """
        
        cur = conn.cursor()
        cur.execute(detect_breakouts_query)
        conn.commit()
        
        # Now validate breakouts that are 2-3 days old
        validation_query = """
        WITH breakout_performance AS (
            SELECT 
                bh.symbol,
                bh.breakout_date,
                bh.resistance_level,
                bh.breakout_price,
                bh.volume_ratio,
                
                -- Current performance
                s.close as current_price,
                s.date as current_date,
                
                -- Performance metrics
                (s.close - bh.breakout_price) / bh.breakout_price * 100 as price_performance_pct,
                
                -- Post-breakout highest and lowest
                MAX(s2.high) as highest_post_breakout,
                MIN(s2.low) as lowest_post_breakout,
                
                -- Volume sustainability (average volume post-breakout)
                AVG(s2.volume) as avg_volume_post,
                bh.breakout_volume / NULLIF(AVG(s2.volume), 0) as volume_sustainability_ratio,
                
                -- Price sustainability (how many days closed above breakout)
                COUNT(CASE WHEN s2.close >= bh.breakout_price THEN 1 END) as days_above_breakout,
                COUNT(*) as total_days_observed
                
            FROM breakout_history bh
            JOIN daily_stock_summary s ON bh.symbol = s.symbol
            JOIN daily_stock_summary s2 ON bh.symbol = s2.symbol 
                AND s2.date > bh.breakout_date 
                AND s2.date <= bh.breakout_date + INTERVAL '5 days'
            WHERE bh.breakout_date >= CURRENT_DATE - INTERVAL '7 days'
              AND bh.breakout_date <= CURRENT_DATE - INTERVAL '2 days'  -- 2+ days old
              AND s.date = (SELECT MAX(date) FROM daily_stock_summary)
            GROUP BY bh.symbol, bh.breakout_date, bh.resistance_level, bh.breakout_price, 
                     bh.volume_ratio, s.close, s.date
        ),
        
        validation_scores AS (
            SELECT 
                symbol,
                breakout_date,
                current_price,
                breakout_price,
                price_performance_pct,
                volume_ratio,
                volume_sustainability_ratio,
                days_above_breakout,
                total_days_observed,
                
                -- Validation scoring components
                CASE 
                    WHEN price_performance_pct >= 5 THEN 30
                    WHEN price_performance_pct >= 2 THEN 20  
                    WHEN price_performance_pct >= 0 THEN 10
                    ELSE 0
                END as price_score,
                
                CASE 
                    WHEN volume_sustainability_ratio >= 0.8 THEN 25
                    WHEN volume_sustainability_ratio >= 0.6 THEN 15
                    WHEN volume_sustainability_ratio >= 0.4 THEN 10
                    ELSE 0
                END as volume_score,
                
                CASE 
                    WHEN days_above_breakout = total_days_observed THEN 25
                    WHEN days_above_breakout >= total_days_observed * 0.8 THEN 20
                    WHEN days_above_breakout >= total_days_observed * 0.6 THEN 15
                    ELSE 5
                END as consistency_score,
                
                CASE 
                    WHEN volume_ratio >= 2.0 THEN 20
                    WHEN volume_ratio >= 1.5 THEN 15
                    WHEN volume_ratio >= 1.2 THEN 10
                    ELSE 5
                END as initial_volume_score
                
            FROM breakout_performance
        )
        
        SELECT 
            symbol,
            breakout_date,
            current_price,
            breakout_price,
            price_performance_pct,
            
            -- Total validation score (0-100)
            (price_score + volume_score + consistency_score + initial_volume_score) as validation_score,
            
            -- Validation status
            CASE 
                WHEN (price_score + volume_score + consistency_score + initial_volume_score) >= 70 THEN 'TRUE BREAKOUT - STRONG'
                WHEN (price_score + volume_score + consistency_score + initial_volume_score) >= 50 THEN 'TRUE BREAKOUT - MODERATE' 
                WHEN (price_score + volume_score + consistency_score + initial_volume_score) >= 30 THEN 'WEAK BREAKOUT'
                ELSE 'FALSE BREAKOUT'
            END as validation_status,
            
            -- Recommendation
            CASE 
                WHEN (price_score + volume_score + consistency_score + initial_volume_score) >= 70 THEN 'HOLD/ADD - Strong breakout confirmed'
                WHEN (price_score + volume_score + consistency_score + initial_volume_score) >= 50 THEN 'HOLD - Moderate breakout, monitor closely'
                WHEN (price_score + volume_score + consistency_score + initial_volume_score) >= 30 THEN 'CAUTION - Weak follow-through'
                ELSE 'EXIT - False breakout detected'
            END as recommendation,
            
            days_above_breakout,
            total_days_observed
            
        FROM validation_scores
        ORDER BY validation_score DESC
        """
        
        validation_df = pd.read_sql(validation_query, conn)
        conn.close()
        
        if validation_df.empty:
            message = "🔍 *OPTION A: POST-BREAKOUT VALIDATION* ⚖️\n\n"
            message += "No breakouts to validate at this time.\n"
            message += "Waiting for 2-3 day post-breakout period to assess true vs false breakouts.\n\n"
            message += "*Status:* Monitoring recent breakouts for validation signals."
            
            send_telegram_message(message)
            logger.info("📊 Option A: No breakouts to validate")
            return "Option A: No breakouts to validate"
        
        message = f"🔍 *OPTION A: POST-BREAKOUT VALIDATION* ⚖️\n\n"
        message += f"Validation results for {len(validation_df)} recent breakouts:\n\n"
        
        # True breakouts (Strong)
        true_strong = validation_df[validation_df['validation_status'] == 'TRUE BREAKOUT - STRONG']
        if not true_strong.empty:
            message += "✅ *TRUE BREAKOUTS - STRONG:*\n"
            for _, row in true_strong.iterrows():
                message += f"⭐⭐⭐ *{row['symbol']}*\n"
                message += f"   Breakout: Rp{row['breakout_price']:,.0f} → Now: Rp{row['current_price']:,.0f} ({row['price_performance_pct']:+.1f}%)\n"
                message += f"   Score: {row['validation_score']:.0f}/100 | Days held: {row['days_above_breakout']}/{row['total_days_observed']}\n"
                message += f"   💡 {row['recommendation']}\n\n"
        
        # True breakouts (Moderate)
        true_moderate = validation_df[validation_df['validation_status'] == 'TRUE BREAKOUT - MODERATE']
        if not true_moderate.empty:
            message += "✅ *TRUE BREAKOUTS - MODERATE:*\n"
            for _, row in true_moderate.iterrows():
                message += f"⭐⭐ *{row['symbol']}* | Score: {row['validation_score']:.0f}\n"
                message += f"   Rp{row['breakout_price']:,.0f} → Rp{row['current_price']:,.0f} ({row['price_performance_pct']:+.1f}%)\n"
                message += f"   💡 {row['recommendation']}\n\n"
        
        # Weak breakouts
        weak_breakouts = validation_df[validation_df['validation_status'] == 'WEAK BREAKOUT']
        if not weak_breakouts.empty:
            message += "⚠️ *WEAK BREAKOUTS:*\n"
            for _, row in weak_breakouts.head(3).iterrows():
                message += f"⚠️ *{row['symbol']}* | Score: {row['validation_score']:.0f}\n"
                message += f"   Performance: {row['price_performance_pct']:+.1f}% | {row['recommendation']}\n\n"
        
        # False breakouts
        false_breakouts = validation_df[validation_df['validation_status'] == 'FALSE BREAKOUT']
        if not false_breakouts.empty:
            message += "❌ *FALSE BREAKOUTS DETECTED:*\n"
            for _, row in false_breakouts.iterrows():
                message += f"❌ *{row['symbol']}* | Score: {row['validation_score']:.0f}\n"
                message += f"   Performance: {row['price_performance_pct']:+.1f}% | {row['recommendation']}\n\n"
        
        # Summary stats
        true_count = len(validation_df[validation_df['validation_status'].str.contains('TRUE')])
        false_count = len(false_breakouts)
        avg_performance = validation_df['price_performance_pct'].mean()
        
        message += f"📊 *VALIDATION SUMMARY:*\n"
        message += f"• True breakouts: {true_count}/{len(validation_df)} ({true_count/len(validation_df)*100:.0f}%)\n"
        message += f"• False breakouts: {false_count}/{len(validation_df)} ({false_count/len(validation_df)*100:.0f}%)\n"
        message += f"• Average performance: {avg_performance:+.1f}%\n\n"
        
        message += "*💡 Key Insight:* Post-breakout validation helps distinguish true institutional buying from false signals and bull traps."
        
        send_telegram_message(message)
        
        logger.info(f"✅ Option A completed: {len(validation_df)} breakouts validated")
        return f"Option A: {len(validation_df)} breakouts validated ({true_count} true, {false_count} false)"
        
    except Exception as e:
        logger.error(f"Error in Option A: {str(e)}")
        return f"Option A Error: {str(e)}"

def option_b_retest_opportunities():
    """
    OPTION B: Retest Opportunity Detection
    Detect healthy pullbacks after confirmed breakouts for better entry points
    """
    try:
        conn = get_db_connection()
        
        retest_query = """
        WITH confirmed_breakouts AS (
            -- Get breakouts that have been validated as true
            SELECT 
                bh.symbol,
                bh.breakout_date,
                bh.resistance_level,
                bh.breakout_price,
                s.date as current_date,
                s.close as current_price,
                s.high as current_high,
                s.low as current_low,
                s.volume as current_volume
            FROM breakout_history bh
            JOIN daily_stock_summary s ON bh.symbol = s.symbol
            WHERE bh.breakout_date >= CURRENT_DATE - INTERVAL '15 days'
              AND s.date >= bh.breakout_date + INTERVAL '1 day'
              AND s.date <= CURRENT_DATE
        ),
        
        pullback_analysis AS (
            SELECT 
                symbol,
                breakout_date,
                resistance_level,
                breakout_price,
                current_date,
                current_price,
                current_volume,
                
                -- Calculate pullback metrics
                (current_price - breakout_price) / breakout_price * 100 as current_gain_pct,
                
                -- Support level (old resistance becomes new support)
                resistance_level as new_support_level,
                (current_price - resistance_level) / resistance_level * 100 as distance_to_support_pct,
                
                -- Fibonacci retracement levels
                breakout_price * 0.618 + resistance_level * 0.382 as fib_618_level,
                breakout_price * 0.5 + resistance_level * 0.5 as fib_50_level,
                
                -- Volume behavior (should decrease on pullback)
                AVG(current_volume) OVER (
                    PARTITION BY symbol, breakout_date ORDER BY current_date 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as avg_volume_3d,
                
                -- Days since breakout
                current_date - breakout_date as days_since_breakout
                
            FROM confirmed_breakouts
        ),
        
        retest_opportunities AS (
            SELECT 
                symbol,
                breakout_date,
                current_date as retest_date,
                breakout_price,
                current_price,
                current_gain_pct,
                new_support_level,
                distance_to_support_pct,
                fib_50_level,
                fib_618_level,
                days_since_breakout,
                
                -- Retest quality scoring
                CASE 
                    WHEN current_price >= fib_50_level AND current_price <= breakout_price * 1.05 THEN 25  -- Healthy 50% retracement
                    WHEN current_price >= fib_618_level AND current_price <= fib_50_level THEN 30         -- Golden ratio retest
                    WHEN current_price >= new_support_level AND current_price <= fib_618_level THEN 15   -- Deep but acceptable
                    ELSE 5
                END as retest_position_score,
                
                CASE 
                    WHEN days_since_breakout BETWEEN 2 AND 7 THEN 20   -- Optimal timing
                    WHEN days_since_breakout BETWEEN 8 AND 12 THEN 15  -- Still good
                    WHEN days_since_breakout BETWEEN 1 AND 1 THEN 10   -- Too early
                    ELSE 5
                END as timing_score,
                
                CASE 
                    WHEN current_gain_pct > 0 AND current_gain_pct <= 8 THEN 20  -- Still profitable but pulled back
                    WHEN current_gain_pct > -3 AND current_gain_pct <= 0 THEN 25 -- Slight pullback below breakout
                    WHEN current_gain_pct > -7 AND current_gain_pct <= -3 THEN 15 -- Deeper pullback but acceptable
                    ELSE 5
                END as pullback_depth_score,
                
                CASE 
                    WHEN distance_to_support_pct >= 2 THEN 15  -- Good cushion above support
                    WHEN distance_to_support_pct >= 0 THEN 10  -- At support level
                    ELSE 0  -- Below support (concerning)
                END as support_strength_score
                
            FROM pullback_analysis
            WHERE current_date = (SELECT MAX(date) FROM daily_stock_summary)  -- Only latest data
        ),
        
        final_retest_analysis AS (
            SELECT 
                symbol,
                breakout_date,
                retest_date,
                breakout_price,
                current_price,
                current_gain_pct,
                new_support_level,
                distance_to_support_pct,
                days_since_breakout,
                
                -- Total retest quality score
                (retest_position_score + timing_score + pullback_depth_score + support_strength_score) as retest_quality_score,
                
                -- Classification
                CASE 
                    WHEN (retest_position_score + timing_score + pullback_depth_score + support_strength_score) >= 70 THEN 'EXCELLENT RETEST'
                    WHEN (retest_position_score + timing_score + pullback_depth_score + support_strength_score) >= 55 THEN 'GOOD RETEST'
                    WHEN (retest_position_score + timing_score + pullback_depth_score + support_strength_score) >= 40 THEN 'FAIR RETEST'
                    ELSE 'POOR RETEST'
                END as retest_quality,
                
                -- Entry recommendation
                CASE 
                    WHEN (retest_position_score + timing_score + pullback_depth_score + support_strength_score) >= 70 THEN 'STRONG BUY - Excellent entry opportunity'
                    WHEN (retest_position_score + timing_score + pullback_depth_score + support_strength_score) >= 55 THEN 'BUY - Good risk/reward setup'
                    WHEN (retest_position_score + timing_score + pullback_depth_score + support_strength_score) >= 40 THEN 'CONSIDER - Monitor for improvement'
                    ELSE 'WAIT - Poor retest setup'
                END as entry_recommendation,
                
                -- Risk/reward calculation
                CASE 
                    WHEN distance_to_support_pct >= 0 
                    THEN (breakout_price * 1.1 - current_price) / (current_price - new_support_level)
                    ELSE 1
                END as risk_reward_ratio
                
            FROM retest_opportunities
        )
        
        SELECT * FROM final_retest_analysis
        WHERE retest_quality_score >= 40  -- Filter only meaningful retests
        ORDER BY retest_quality_score DESC
        """
        
        retest_df = pd.read_sql(retest_query, conn)
        conn.close()
        
        if retest_df.empty:
            message = "🎯 *OPTION B: RETEST OPPORTUNITIES* 🔄\n\n"
            message += "No quality retest opportunities detected at this time.\n"
            message += "Waiting for confirmed breakouts to pullback to optimal entry levels.\n\n"
            message += "*Status:* Monitoring post-breakout pullbacks for buying opportunities."
            
            send_telegram_message(message)
            logger.info("📊 Option B: No retest opportunities found")
            return "Option B: No retest opportunities found"
        
        message = f"🎯 *OPTION B: RETEST OPPORTUNITIES* 🔄\n\n"
        message += f"Found {len(retest_df)} retest opportunities after recent breakouts:\n\n"
        
        # Excellent retests
        excellent_retests = retest_df[retest_df['retest_quality'] == 'EXCELLENT RETEST']
        if not excellent_retests.empty:
            message += "🎯 *EXCELLENT RETEST OPPORTUNITIES:*\n"
            for _, row in excellent_retests.iterrows():
                message += f"⭐⭐⭐ *{row['symbol']}*\n"
                message += f"   Entry: Rp{row['current_price']:,.0f} | Support: Rp{row['new_support_level']:,.0f}\n"
                message += f"   Breakout: {row['days_since_breakout']} days ago | Performance: {row['current_gain_pct']:+.1f}%\n"
                message += f"   Quality Score: {row['retest_quality_score']:.0f}/100 | R/R: {row['risk_reward_ratio']:.1f}\n"
                message += f"   💡 {row['entry_recommendation']}\n\n"
        
        # Good retests
        good_retests = retest_df[retest_df['retest_quality'] == 'GOOD RETEST']
        if not good_retests.empty:
            message += "🎯 *GOOD RETEST OPPORTUNITIES:*\n"
            for _, row in good_retests.head(3).iterrows():
                message += f"⭐⭐ *{row['symbol']}* | Score: {row['retest_quality_score']:.0f}\n"
                message += f"   Entry: Rp{row['current_price']:,.0f} | {row['days_since_breakout']} days post-breakout\n"
                message += f"   💡 {row['entry_recommendation']}\n\n"
        
        # Fair retests
        fair_retests = retest_df[retest_df['retest_quality'] == 'FAIR RETEST']
        if not fair_retests.empty:
            message += f"📊 *FAIR RETESTS:* {len(fair_retests)} opportunities\n"
            for _, row in fair_retests.head(2).iterrows():
                message += f"⭐ {row['symbol']} (Score: {row['retest_quality_score']:.0f}) "
            message += "\n\n"
        
        # Summary and strategy
        avg_score = retest_df['retest_quality_score'].mean()
        avg_rr = retest_df['risk_reward_ratio'].mean()
        excellent_count = len(excellent_retests)
        
        message += f"📈 *RETEST SUMMARY:*\n"
        message += f"• Total opportunities: {len(retest_df)}\n"
        message += f"• Excellent setups: {excellent_count}\n"
        message += f"• Average quality score: {avg_score:.1f}/100\n"
        message += f"• Average risk/reward: {avg_rr:.1f}\n\n"
        
        message += "*💡 STRATEGY:*\n"
        message += "• Excellent retests: Enter immediately with tight stops\n"
        message += "• Good retests: Wait for volume confirmation\n"
        message += "• Entry timing: Market open weakness or EOD strength\n"
        message += "• Stop loss: Below previous support level\n\n"
        
        message += "*Key Insight:* Retest entries after confirmed breakouts offer better risk/reward than chasing breakouts."
        
        send_telegram_message(message)
        
        logger.info(f"✅ Option B completed: {len(retest_df)} retest opportunities found")
        return f"Option B: {len(retest_df)} retest opportunities ({excellent_count} excellent)"
        
    except Exception as e:
        logger.error(f"Error in Option B: {str(e)}")
        return f"Option B Error: {str(e)}"

def option_c_complete_lifecycle():
    """
    OPTION C: Complete Breakout Lifecycle Management
    Comprehensive system covering setup → breakout → validation → retest → recovery
    """
    try:
        conn = get_db_connection()
        
        lifecycle_query = """
        WITH lifecycle_stages AS (
            -- Stage 1: Pre-breakout setups (from relaxed_breakout_candidates)
            SELECT 
                'SETUP' as stage,
                symbol,
                CURRENT_DATE as analysis_date,
                current_price as price,
                target_price,
                overall_score as score,
                recommendation,
                'Pre-breakout accumulation detected' as stage_description
            FROM relaxed_breakout_candidates
            WHERE analysis_date >= CURRENT_DATE - INTERVAL '2 days'
              AND overall_score >= 50
            
            UNION ALL
            
            -- Stage 2: Recent breakouts (new)
            SELECT 
                'BREAKOUT' as stage,
                symbol,
                date as analysis_date,
                close as price,
                high as target_price,
                75 as score,  -- Default breakout score
                'BREAKOUT DETECTED' as recommendation,
                'Fresh breakout with volume confirmation' as stage_description
            FROM daily_stock_summary ds1
            WHERE date >= CURRENT_DATE - INTERVAL '3 days'
              AND close > (
                  SELECT MAX(high) 
                  FROM daily_stock_summary ds2 
                  WHERE ds2.symbol = ds1.symbol 
                    AND ds2.date BETWEEN ds1.date - INTERVAL '20 days' AND ds1.date - INTERVAL '1 day'
              ) * 1.02
              AND volume > (
                  SELECT AVG(volume) * 1.5
                  FROM daily_stock_summary ds3
                  WHERE ds3.symbol = ds1.symbol 
                    AND ds3.date BETWEEN ds1.date - INTERVAL '20 days' AND ds1.date - INTERVAL '1 day'
              )
            
            UNION ALL
            
            -- Stage 3: Validated breakouts
            SELECT 
                'VALIDATED' as stage,
                bh.symbol,
                CURRENT_DATE as analysis_date,
                s.close as price,
                bh.breakout_price * 1.15 as target_price,
                85 as score,
                'VALIDATED TRUE BREAKOUT' as recommendation,
                'Breakout confirmed with follow-through' as stage_description
            FROM breakout_history bh
            JOIN daily_stock_summary s ON bh.symbol = s.symbol
            WHERE bh.breakout_date >= CURRENT_DATE - INTERVAL '7 days'
              AND s.date = (SELECT MAX(date) FROM daily_stock_summary)
              AND s.close >= bh.breakout_price * 0.98  -- Still near breakout level
            
            UNION ALL
            
            -- Stage 4: Retest opportunities
            SELECT 
                'RETEST' as stage,
                symbol,
                retest_date as analysis_date,
                current_price as price,
                breakout_price * 1.1 as target_price,
                retest_quality_score as score,
                entry_recommendation as recommendation,
                CONCAT('Retest opportunity - ', retest_quality) as stage_description
            FROM (
                SELECT 
                    bh.symbol,
                    CURRENT_DATE as retest_date,
                    s.close as current_price,
                    bh.breakout_price,
                    70 as retest_quality_score,
                    'RETEST ENTRY' as entry_recommendation,
                    'GOOD RETEST' as retest_quality
                FROM breakout_history bh
                JOIN daily_stock_summary s ON bh.symbol = s.symbol
                WHERE bh.breakout_date >= CURRENT_DATE - INTERVAL '10 days'
                  AND s.date = (SELECT MAX(date) FROM daily_stock_summary)
                  AND s.close BETWEEN bh.resistance_level AND bh.breakout_price * 1.05
            ) retest_subquery
            
            UNION ALL
            
            -- Stage 5: False breakout recovery
            SELECT 
                'RECOVERY' as stage,
                bh.symbol,
                CURRENT_DATE as analysis_date,
                s.close as price,
                bh.resistance_level as target_price,
                40 as score,
                'MONITOR RECOVERY' as recommendation,
                'False breakout - wait for new setup' as stage_description
            FROM breakout_history bh
            JOIN daily_stock_summary s ON bh.symbol = s.symbol
            WHERE bh.breakout_date >= CURRENT_DATE - INTERVAL '10 days'
              AND s.date = (SELECT MAX(date) FROM daily_stock_summary)
              AND s.close < bh.breakout_price * 0.95  -- Below breakout level
        ),
        
        lifecycle_summary AS (
            SELECT 
                stage,
                symbol,
                analysis_date,
                price,
                target_price,
                score,
                recommendation,
                stage_description,
                -- Stage priority for sorting
                CASE stage
                    WHEN 'BREAKOUT' THEN 1
                    WHEN 'RETEST' THEN 2 
                    WHEN 'VALIDATED' THEN 3
                    WHEN 'SETUP' THEN 4
                    WHEN 'RECOVERY' THEN 5
                END as stage_priority,
                -- Action urgency
                CASE stage
                    WHEN 'BREAKOUT' THEN 'IMMEDIATE ACTION'
                    WHEN 'RETEST' THEN 'ENTRY OPPORTUNITY'
                    WHEN 'VALIDATED' THEN 'HOLD/MONITOR'
                    WHEN 'SETUP' THEN 'PREPARE/WATCH'
                    WHEN 'RECOVERY' THEN 'WAIT/RESET'
                END as action_urgency
            FROM lifecycle_stages
        )
        
        SELECT * FROM lifecycle_summary
        ORDER BY stage_priority, score DESC
        """
        
        lifecycle_df = pd.read_sql(lifecycle_query, conn)
        conn.close()
        
        if lifecycle_df.empty:
            message = "🔄 *OPTION C: COMPLETE BREAKOUT LIFECYCLE* 📊\n\n"
            message += "No active breakout lifecycle stages detected at this time.\n"
            message += "Market in quiet phase - monitoring for new setups and opportunities.\n\n"
            message += "*Status:* Standby mode - waiting for actionable signals."
            
            send_telegram_message(message)
            logger.info("📊 Option C: No lifecycle activities found")
            return "Option C: No lifecycle activities found"
        
        message = f"🔄 *OPTION C: COMPLETE BREAKOUT LIFECYCLE* 📊\n\n"
        message += f"Active lifecycle management for {len(lifecycle_df)} stocks across all stages:\n\n"
        
        # Stage 1: Active breakouts (immediate action)
        breakouts = lifecycle_df[lifecycle_df['stage'] == 'BREAKOUT']
        if not breakouts.empty:
            message += "🚨 *STAGE 1: ACTIVE BREAKOUTS (Immediate Action)*\n"
            for _, row in breakouts.head(3).iterrows():
                potential_gain = ((row['target_price'] - row['price']) / row['price'] * 100)
                message += f"🔥 *{row['symbol']}* - {row['action_urgency']}\n"
                message += f"   Price: Rp{row['price']:,.0f} → Target: Rp{row['target_price']:,.0f} (+{potential_gain:.1f}%)\n"
                message += f"   Status: {row['stage_description']}\n\n"
        
        # Stage 2: Retest opportunities
        retests = lifecycle_df[lifecycle_df['stage'] == 'RETEST']
        if not retests.empty:
            message += "🎯 *STAGE 2: RETEST OPPORTUNITIES (Entry Timing)*\n"
            for _, row in retests.head(3).iterrows():
                message += f"⭐⭐ *{row['symbol']}* - {row['action_urgency']}\n"
                message += f"   Entry: Rp{row['price']:,.0f} | Score: {row['score']:.0f}\n"
                message += f"   💡 {row['recommendation']}\n\n"
        
        # Stage 3: Validated breakouts (hold/monitor)
        validated = lifecycle_df[lifecycle_df['stage'] == 'VALIDATED']
        if not validated.empty:
            message += "✅ *STAGE 3: VALIDATED BREAKOUTS (Hold/Monitor)*\n"
            for _, row in validated.head(2).iterrows():
                message += f"✅ *{row['symbol']}* - Confirmed breakout\n"
                message += f"   Current: Rp{row['price']:,.0f} | {row['recommendation']}\n\n"
        
        # Stage 4: Pre-breakout setups (prepare/watch)
        setups = lifecycle_df[lifecycle_df['stage'] == 'SETUP']
        if not setups.empty:
            message += f"📋 *STAGE 4: PRE-BREAKOUT SETUPS:* {len(setups)} candidates\n"
            for _, row in setups.head(2).iterrows():
                message += f"📊 {row['symbol']} (Score: {row['score']:.0f}) "
            message += "\n\n"
        
        # Stage 5: Recovery situations (wait/reset)
        recoveries = lifecycle_df[lifecycle_df['stage'] == 'RECOVERY']
        if not recoveries.empty:
            message += f"🔄 *STAGE 5: FALSE BREAKOUT RECOVERY:* {len(recoveries)} stocks\n"
            message += "Waiting for new accumulation patterns after false signals.\n\n"
        
        # Lifecycle statistics
        immediate_actions = len(breakouts)
        entry_opportunities = len(retests)
        total_active = len(lifecycle_df)
        
        message += f"📈 *LIFECYCLE DASHBOARD:*\n"
        message += f"• Total active stages: {total_active}\n"
        message += f"• Immediate actions: {immediate_actions}\n"
        message += f"• Entry opportunities: {entry_opportunities}\n"
        message += f"• Validated positions: {len(validated)}\n"
        message += f"• Recovery situations: {len(recoveries)}\n\n"
        
        message += "*💡 LIFECYCLE STRATEGY:*\n"
        message += "• Stage 1-2: Active trading opportunities\n"
        message += "• Stage 3: Portfolio monitoring\n"
        message += "• Stage 4: Preparation and research\n"
        message += "• Stage 5: Risk management and reset\n\n"
        
        message += "*Key Insight:* Complete lifecycle management maximizes opportunities while minimizing false signals and improving risk management."
        
        send_telegram_message(message)
        
        logger.info(f"✅ Option C completed: {total_active} stocks in lifecycle")
        return f"Option C: {total_active} active lifecycle stages ({immediate_actions} breakouts, {entry_opportunities} retests)"
        
    except Exception as e:
        logger.error(f"Error in Option C: {str(e)}")
        return f"Option C Error: {str(e)}"

# DAG Definition
with DAG(
    dag_id="complete_breakout_lifecycle_analysis",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval='0 18 * * 1-5',  # Daily at 6 PM
    catchup=False,
    default_args=default_args,
    description="Complete breakout lifecycle analysis with 3 comprehensive approaches",
    tags=["breakout", "lifecycle", "validation", "retest", "complete"]
) as dag:

    start_task = DummyOperator(
        task_id="start_lifecycle_analysis"
    )

    create_tables = PythonOperator(
        task_id="create_lifecycle_tables",
        python_callable=create_breakout_lifecycle_tables
    )

    option_a_task = PythonOperator(
        task_id="option_a_validation",
        python_callable=option_a_post_breakout_validation
    )

    option_b_task = PythonOperator(
        task_id="option_b_retest",
        python_callable=option_b_retest_opportunities
    )

    option_c_task = PythonOperator(
        task_id="option_c_complete",
        python_callable=option_c_complete_lifecycle
    )

    end_task = DummyOperator(
        task_id="end_lifecycle_analysis"
    )

    # Task dependencies - run all options in parallel
    start_task >> create_tables >> [option_a_task, option_b_task, option_c_task] >> end_task