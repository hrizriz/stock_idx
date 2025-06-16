from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple, Optional

from utils.database import get_database_connection, get_latest_stock_date, fetch_data
from utils.telegram import send_telegram_message

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

class IndonesianAccumulationDetector:
    """
    Comprehensive Indonesian Stock Accumulation Detection System
    Based on academic research and Indonesian market-specific characteristics
    """
    
    def __init__(self):
        self.LQ45_MIN_MARKET_CAP = 1_000_000_000_000  # IDR 10 trillion
        self.MIN_DAILY_VOLUME = 15_000_000_000  # IDR 50 billion
        self.OPTIMAL_FOREIGN_OWNERSHIP_MIN = 0.20  # 20%
        self.OPTIMAL_FOREIGN_OWNERSHIP_MAX = 0.90  # 90%
        
    def calculate_accumulation_distribution_line(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enhanced A/D Line calculation with Indonesian foreign flow integration
        """
        df = df.copy()
        
        # Fix: Handle unit conversion for foreign flows
        # Test if foreign flows are in shares vs volume in lots
        foreign_total = df['foreign_buy'] + df['foreign_sell']
        # If foreign total > volume, assume foreign is in shares, convert to lots
        df['foreign_buy_adj'] = np.where(
            foreign_total > df['volume'],
            df['foreign_buy'] / 100.0,  # Convert shares to lots
            df['foreign_buy']
        )
        df['foreign_sell_adj'] = np.where(
            foreign_total > df['volume'],
            df['foreign_sell'] / 100.0,  # Convert shares to lots
            df['foreign_sell']
        )
        
        # Calculate Close Location Value with corrected foreign ratio weighting
        df['foreign_buy_ratio'] = df['foreign_buy_adj'] / (df['foreign_buy_adj'] + df['foreign_sell_adj'] + 1e-9)
        df['clv'] = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'] + 1e-9)
        
        # Enhanced Money Flow Volume with foreign weighting
        df['money_flow_volume'] = df['clv'] * df['volume'] * df['foreign_buy_ratio']
        
        # Cumulative A/D Line
        df['ad_line'] = df.groupby('symbol')['money_flow_volume'].cumsum()
        
        return df
    
    def calculate_chaikin_money_flow(self, df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        """
        Indonesian-adapted Chaikin Money Flow with foreign flow weighting
        """
        df = df.copy()
        
        # Calculate Money Flow Volume with foreign weighting
        df['foreign_weight'] = df['foreign_buy'] / (df['volume'] + 1e-9)
        df['mfv_foreign'] = df['money_flow_volume'] * df['foreign_weight']
        
        # Calculate CMF over rolling period
        df['cmf'] = (
            df.groupby('symbol')['mfv_foreign'].rolling(period, min_periods=1).sum().reset_index(0, drop=True) /
            df.groupby('symbol')['volume'].rolling(period, min_periods=1).sum().reset_index(0, drop=True)
        ).fillna(0)
        
        return df
    
    def calculate_order_flow_imbalance(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Order Flow Imbalance with foreign institutional focus
        """
        df = df.copy()
        
        # Use the adjusted foreign flows from A/D calculation
        if 'foreign_buy_adj' not in df.columns:
            # If not already calculated, do the unit conversion here
            foreign_total = df['foreign_buy'] + df['foreign_sell']
            df['foreign_buy_adj'] = np.where(
                foreign_total > df['volume'],
                df['foreign_buy'] / 100.0,
                df['foreign_buy']
            )
            df['foreign_sell_adj'] = np.where(
                foreign_total > df['volume'],
                df['foreign_sell'] / 100.0,
                df['foreign_sell']
            )
        
        # Daily OFI based on corrected foreign flows
        df['ofi'] = (df['foreign_buy_adj'] - df['foreign_sell_adj']) / (df['volume'] + 1e-9)
        
        # Cumulative OFI for trend detection
        df['cumulative_ofi'] = df.groupby('symbol')['ofi'].cumsum()
        
        return df
    
    def calculate_volume_profile_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Volume profile analysis with institutional activity weighting
        """
        df = df.copy()
        
        # Relative volume ratio
        df['avg_volume_20'] = df.groupby('symbol')['volume'].rolling(20, min_periods=1).mean().reset_index(0, drop=True)
        df['relative_volume'] = df['volume'] / (df['avg_volume_20'] + 1e-9)
        
        # Large block detection (>0.5% of shares outstanding)
        df['large_block_ratio'] = np.where(
            df['volume'] > (df['listed_shares'] * 0.005), 1, 0
        )
        
        # Use adjusted foreign flows if available
        if 'foreign_buy_adj' in df.columns and 'foreign_sell_adj' in df.columns:
            foreign_activity = df['foreign_buy_adj'] + df['foreign_sell_adj']
        else:
            # Do unit conversion here
            foreign_total = df['foreign_buy'] + df['foreign_sell']
            foreign_activity = np.where(
                foreign_total > df['volume'],
                foreign_total / 100.0,  # Convert shares to lots
                foreign_total
            )
        
        # Institutional footprint
        df['institutional_footprint'] = foreign_activity / (df['volume'] + 1e-9)
        
        return df
    
    def calculate_price_action_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Price action patterns for accumulation detection
        """
        df = df.copy()
        
        # ATR for range contraction analysis
        df['true_range'] = np.maximum.reduce([
            df['high'] - df['low'],
            (df['high'] - df['prev_close']).abs(),
            (df['low'] - df['prev_close']).abs()
        ])
        df['atr_14'] = df.groupby('symbol')['true_range'].rolling(14, min_periods=1).mean().reset_index(0, drop=True)
        
        # Range contraction (declining ATR)
        df['atr_slope'] = df.groupby('symbol')['atr_14'].diff(5).fillna(0)
        df['range_contraction'] = np.where(df['atr_slope'] < 0, 1, 0)
        
        # Price tightening
        df['price_tightening'] = (df['high'] - df['low']) / (df['atr_14'] + 1e-9)
        
        # Support level formation
        df = self.calculate_support_strength(df)
        
        return df
    
    def calculate_support_strength(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate support level strength based on test frequency
        """
        df = df.copy()
        support_strengths = []
        
        for symbol in df['symbol'].unique():
            symbol_data = df[df['symbol'] == symbol].copy()
            symbol_data = symbol_data.sort_values('date').reset_index(drop=True)
            
            for i in range(len(symbol_data)):
                if i < 20:  # Need minimum data
                    support_strengths.append(0)
                    continue
                    
                # Get recent 20-day low
                recent_data = symbol_data.iloc[max(0, i-19):i+1]
                recent_low = recent_data['low'].min()
                
                # Count touches within 2% of recent low
                tolerance = recent_low * 0.02
                touches = sum(abs(recent_data['low'] - recent_low) <= tolerance)
                
                # Normalize to 0-1 scale
                strength = min(touches / 5.0, 1.0)
                support_strengths.append(strength)
        
        df['support_strength'] = support_strengths
        return df
    
    def calculate_accumulation_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Comprehensive accumulation scoring system (0-100 scale)
        """
        df = df.copy()
        
        # Ensure we have the corrected foreign_buy_ratio
        if 'foreign_buy_ratio' not in df.columns:
            # Recalculate with unit conversion
            foreign_total = df['foreign_buy'] + df['foreign_sell']
            foreign_buy_adj = np.where(
                foreign_total > df['volume'],
                df['foreign_buy'] / 100.0,
                df['foreign_buy']
            )
            foreign_sell_adj = np.where(
                foreign_total > df['volume'],
                df['foreign_sell'] / 100.0,
                df['foreign_sell']
            )
            df['foreign_buy_ratio'] = foreign_buy_adj / (foreign_buy_adj + foreign_sell_adj + 1e-9)
        
        # Volume Analysis (30% weight)
        volume_score = (
            np.clip(df['relative_volume'] / 3, 0, 1) * 20 +  # Relative volume (20%)
            np.clip(df['ofi'] * 10 + 0.5, 0, 1) * 10  # Foreign flow dominance (10%)
        )
        
        # Price Action (25% weight)
        price_score = (
            df['range_contraction'] * 15 +  # Range contraction (15%)
            np.clip(df['support_strength'], 0, 1) * 10  # Support level integrity (10%)
        )
        
        # Institutional Activity (25% weight)
        institutional_score = (
            np.clip(df['foreign_buy_ratio'] * 2, 0, 1) * 15 +  # Foreign institutional buying (15%)
            df['large_block_ratio'] * 10  # Large block analysis (10%)
        )
        
        # Technical Strength (20% weight)
        # Calculate relative strength within each date
        df['relative_strength'] = df.groupby('date')['percent_change'].rank(pct=True)
        
        technical_score = (
            np.clip(df['relative_strength'], 0, 1) * 15 +  # Relative strength (15%)
            np.clip(df['cmf'] * 10 + 0.5, 0, 1) * 5  # Momentum confirmation (5%)
        )
        
        # Combined score
        df['accumulation_score'] = (
            volume_score + price_score + institutional_score + technical_score
        )
        
        # Add signal strength categories
        df['signal_strength'] = pd.cut(
            df['accumulation_score'],
            bins=[0, 40, 60, 80, 100],
            labels=['No Signal', 'Weak', 'Moderate', 'Strong'],
            include_lowest=True
        )
        
        return df
    
    def screen_accumulation_candidates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Primary screening for accumulation candidates
        """
        # Apply universe filters
        candidates = df[
            (df['value'] >= self.MIN_DAILY_VOLUME) &  # Minimum liquidity
            (df['accumulation_score'] >= 60) &  # Minimum score threshold
            (df['foreign_net'] > 0) &  # Positive foreign net buying
            (df['relative_volume'] > 1.2)  # Above average volume
        ].copy()
        
        return candidates.sort_values('accumulation_score', ascending=False)

def run_accumulation_analysis():
    """
    Main accumulation analysis function
    """
    try:
        conn = get_database_connection()
        latest_date = get_latest_stock_date()
        
        if not latest_date:
            logger.warning("No data available")
            return "No data available"
        
        date_filter = latest_date.strftime('%Y-%m-%d')
        logger.info(f"Running accumulation analysis for date: {date_filter}")
        
        # Get comprehensive stock data for analysis
        analysis_sql = f"""
        WITH base_data AS (
            SELECT 
                s.symbol,
                s.name,
                s.date,
                s.prev_close,
                s.open_price,
                s.high,
                s.low,
                s.close,
                s.volume,
                s.value,
                COALESCE(s.foreign_buy, 0) as foreign_buy,
                COALESCE(s.foreign_sell, 0) as foreign_sell,
                COALESCE(s.foreign_buy, 0) - COALESCE(s.foreign_sell, 0) as foreign_net,
                COALESCE(s.listed_shares, 1000000000) as listed_shares,
                COALESCE(s.tradable_shares, s.listed_shares, 1000000000) as tradable_shares,
                CASE 
                    WHEN s.prev_close > 0 THEN (s.close - s.prev_close) / s.prev_close * 100
                    ELSE 0
                END as percent_change
            FROM public.daily_stock_summary s
            WHERE s.date >= '{(latest_date - timedelta(days=60)).strftime('%Y-%m-%d')}'
                AND s.date <= '{date_filter}'
                AND s.volume > 0
                AND s.value >= 5000000000  -- Minimum 10B IDR daily value
        ),
        market_cap_filter AS (
            SELECT 
                symbol,
                MAX(close * listed_shares) as market_cap
            FROM base_data
            WHERE date = '{date_filter}'
            GROUP BY symbol
            HAVING MAX(close * listed_shares) >= 1000000000000  -- 5T IDR minimum
        )
        SELECT b.*
        FROM base_data b
        JOIN market_cap_filter m ON b.symbol = m.symbol
        ORDER BY b.symbol, b.date
        """
        
        df = pd.read_sql(analysis_sql, conn)
        conn.close()
        
        if df.empty:
            logger.warning("No data retrieved for analysis")
            return "No data available for analysis"
        
        logger.info(f"Retrieved {len(df)} rows for {df['symbol'].nunique()} stocks")
        
        # Validate required columns
        required_columns = ['symbol', 'date', 'close', 'volume', 'foreign_buy', 'foreign_sell', 'high', 'low']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return f"Missing required columns: {missing_columns}"
        
        # Initialize detector and run analysis
        detector = IndonesianAccumulationDetector()
        
        try:
            # Calculate all indicators step by step
            logger.info("Calculating A/D Line...")
            df = detector.calculate_accumulation_distribution_line(df)
            
            logger.info("Calculating Chaikin Money Flow...")
            df = detector.calculate_chaikin_money_flow(df)
            
            logger.info("Calculating Order Flow Imbalance...")
            df = detector.calculate_order_flow_imbalance(df)
            
            logger.info("Calculating Volume Profile Indicators...")
            df = detector.calculate_volume_profile_indicators(df)
            
            logger.info("Calculating Price Action Indicators...")
            df = detector.calculate_price_action_indicators(df)
            
            logger.info("Calculating Accumulation Score...")
            df = detector.calculate_accumulation_score(df)
            
        except Exception as e:
            logger.error(f"Error in indicator calculations: {str(e)}")
            return f"Error in calculations: {str(e)}"
        
        # Screen for latest date candidates
        latest_data = df[df['date'] == latest_date].copy()
        
        if latest_data.empty:
            logger.warning(f"No data for latest date: {latest_date}")
            return f"No data for latest date: {latest_date}"
        
        candidates = detector.screen_accumulation_candidates(latest_data)
        
        if candidates.empty:
            logger.warning("No accumulation candidates found")
            # Store empty results to indicate analysis was run
            store_accumulation_results(pd.DataFrame(), latest_date)
            return "No accumulation candidates found, but analysis completed"
        
        # Store results for reporting
        store_accumulation_results(candidates, latest_date)
        
        logger.info(f"Found {len(candidates)} accumulation candidates")
        return f"Analysis complete: {len(candidates)} candidates identified"
        
    except Exception as e:
        logger.error(f"Error in accumulation analysis: {str(e)}")
        return f"Error: {str(e)}"

def store_accumulation_results(candidates: pd.DataFrame, analysis_date: datetime):
    """
    Store accumulation analysis results to database
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Create schema if not exists
        cursor.execute("CREATE SCHEMA IF NOT EXISTS public_analytics;")
        
        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS public_analytics.accumulation_signals (
            symbol VARCHAR(10),
            analysis_date DATE,
            accumulation_score FLOAT,
            signal_strength VARCHAR(20),
            foreign_net BIGINT,
            relative_volume FLOAT,
            ad_line FLOAT,
            cmf FLOAT,
            ofi FLOAT,
            price_tightening FLOAT,
            support_strength FLOAT,
            close_price FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, analysis_date)
        );
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        
        # Insert results
        for _, row in candidates.iterrows():
            # Use corrected foreign_net if available
            foreign_net_value = row.get('foreign_net_corrected', row.get('foreign_net', 0))
            
            insert_sql = """
            INSERT INTO public_analytics.accumulation_signals 
            (symbol, analysis_date, accumulation_score, signal_strength, foreign_net,
             relative_volume, ad_line, cmf, ofi, price_tightening, support_strength, close_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, analysis_date) 
            DO UPDATE SET
                accumulation_score = EXCLUDED.accumulation_score,
                signal_strength = EXCLUDED.signal_strength,
                foreign_net = EXCLUDED.foreign_net,
                relative_volume = EXCLUDED.relative_volume,
                ad_line = EXCLUDED.ad_line,
                cmf = EXCLUDED.cmf,
                ofi = EXCLUDED.ofi,
                price_tightening = EXCLUDED.price_tightening,
                support_strength = EXCLUDED.support_strength,
                close_price = EXCLUDED.close_price,
                created_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(insert_sql, (
                row['symbol'], analysis_date, row['accumulation_score'],
                str(row['signal_strength']), int(foreign_net_value), row['relative_volume'],
                row['ad_line'], row['cmf'], row['ofi'], row['price_tightening'],
                row['support_strength'], row['close']
            ))
        
        conn.commit()
        conn.close()
        logger.info(f"Stored {len(candidates)} accumulation signals")
        
    except Exception as e:
        logger.error(f"Error storing accumulation results: {str(e)}")
        if conn:
            conn.close()

def send_accumulation_report():
    """
    Send comprehensive accumulation detection report to Telegram
    """
    try:
        conn = get_database_connection()
        latest_date = get_latest_stock_date()
        
        if not latest_date:
            return "No data available"
        
        date_filter = latest_date.strftime('%Y-%m-%d')
        
        # Check if accumulation_signals table exists
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public_analytics' 
            AND table_name = 'accumulation_signals'
        );
        """
        
        cursor = conn.cursor()
        cursor.execute(check_table_sql)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            conn.close()
            return "Accumulation signals table not found. Analysis needs to run first."
        
        # Get top accumulation signals
        signals_sql = f"""
        SELECT 
            a.symbol,
            s.name,
            a.accumulation_score,
            a.signal_strength,
            a.foreign_net,
            a.relative_volume,
            a.close_price,
            a.cmf,
            a.ofi,
            CASE 
                WHEN a.accumulation_score >= 80 THEN '🔥'
                WHEN a.accumulation_score >= 60 THEN '⚡'
                ELSE '📊'
            END as signal_icon
        FROM public_analytics.accumulation_signals a
        JOIN public.daily_stock_summary s 
            ON a.symbol = s.symbol 
            AND s.date = a.analysis_date
        WHERE a.analysis_date = '{date_filter}'
        ORDER BY a.accumulation_score DESC
        LIMIT 15
        """
        
        signals_df = pd.read_sql(signals_sql, conn)
        
        if signals_df.empty:
            # Try to get some basic data if no signals found
            fallback_sql = f"""
            SELECT 
                symbol,
                name,
                close,
                (foreign_buy - foreign_sell) as foreign_net,
                volume
            FROM public.daily_stock_summary
            WHERE date = '{date_filter}'
                AND (foreign_buy - foreign_sell) > 0
                AND volume > 0
            ORDER BY (foreign_buy - foreign_sell) DESC
            LIMIT 10
            """
            
            fallback_df = pd.read_sql(fallback_sql, conn)
            conn.close()
            
            if fallback_df.empty:
                return "No accumulation signals or data found"
            
            # Send basic foreign flow report instead
            message = f"⚠️ *BASIC FOREIGN FLOW REPORT ({date_filter})* ⚠️\n\n"
            message += "Accumulation analysis pending. Showing top foreign net buyers:\n\n"
            
            for i, row in enumerate(fallback_df.itertuples(), 1):
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Harga: Rp{row.close:,.0f} | Net Asing: {row.foreign_net:,.0f}\n\n"
            
            result = send_telegram_message(message)
            if "successfully" in result:
                return f"Basic report sent: {len(fallback_df)} stocks"
            else:
                return result
        
        # Get market overview
        overview_sql = f"""
        SELECT 
            COUNT(*) as total_stocks_analyzed,
            COUNT(CASE WHEN accumulation_score >= 80 THEN 1 END) as strong_signals,
            COUNT(CASE WHEN accumulation_score >= 60 THEN 1 END) as moderate_plus_signals,
            AVG(foreign_net) as avg_foreign_net,
            AVG(relative_volume) as avg_relative_volume
        FROM public_analytics.accumulation_signals
        WHERE analysis_date = '{date_filter}'
        """
        
        overview = pd.read_sql(overview_sql, conn).iloc[0]
        conn.close()
        
        # Build message
        message = f"🔍 *ANALISIS AKUMULASI SISTEMATIS ({date_filter})* 🔍\n\n"
        
        # Market overview
        message += f"📈 *RINGKASAN PASAR:*\n"
        message += f"• Total Saham Dianalisis: {overview['total_stocks_analyzed']}\n"
        message += f"• Sinyal Kuat (≥80): {overview['strong_signals']}\n"
        message += f"• Sinyal Sedang+ (≥60): {overview['moderate_plus_signals']}\n"
        message += f"• Rata-rata Net Asing: {overview['avg_foreign_net']:,.0f}\n"
        message += f"• Rata-rata Volume Relatif: {overview['avg_relative_volume']:.2f}x\n\n"
        
        # Strong signals
        strong_signals = signals_df[signals_df['accumulation_score'] >= 80]
        if not strong_signals.empty:
            message += "🔥 *SINYAL AKUMULASI KUAT (Score ≥80):*\n\n"
            for i, row in enumerate(strong_signals.itertuples(), 1):
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Score: {row.accumulation_score:.1f}/100 | Harga: Rp{row.close_price:,.0f}\n"
                message += f"   Volume: {row.relative_volume:.2f}x | Net Asing: {row.foreign_net:,.0f}\n"
                message += f"   CMF: {row.cmf:.3f} | OFI: {row.ofi:.3f}\n\n"
        
        # Moderate signals
        moderate_signals = signals_df[
            (signals_df['accumulation_score'] >= 60) & 
            (signals_df['accumulation_score'] < 80)
        ]
        if not moderate_signals.empty:
            message += "⚡ *SINYAL AKUMULASI SEDANG (Score 60-80):*\n\n"
            for i, row in enumerate(moderate_signals.head(8).itertuples(), 1):
                message += f"{i}. *{row.symbol}* ({row.name}) - Score: {row.accumulation_score:.1f}\n"
                message += f"   Harga: Rp{row.close_price:,.0f} | Volume: {row.relative_volume:.2f}x\n\n"
        
        # Key insights
        foreign_buyers = signals_df[signals_df['foreign_net'] > 0]['foreign_net'].sum()
        high_volume_count = len(signals_df[signals_df['relative_volume'] > 2])
        
        message += "💡 *INSIGHT KUNCI:*\n"
        message += f"• Total Pembelian Bersih Asing: {foreign_buyers:,.0f} lot\n"
        message += f"• Saham dengan Volume >2x rata-rata: {high_volume_count}\n"
        message += f"• Kekuatan Sinyal Tertinggi: {signals_df['accumulation_score'].max():.1f}/100\n\n"
        
        message += "*📚 Metodologi:*\n"
        message += "Analisis berbasis riset akademik dengan indikator:\n"
        message += "• A/D Line dengan Foreign Flow (25%)\n"
        message += "• Chaikin Money Flow Termodifikasi (20%)\n"
        message += "• Order Flow Imbalance (20%)\n"
        message += "• Volume Profile & Institutional Activity (20%)\n"
        message += "• Price Action & Support Strength (15%)\n\n"
        
        message += "*⚠️ Risk Disclaimer:*\n"
        message += "Analisis ini bersifat sistematis dan tidak menjamin keuntungan. "
        message += "Selalu lakukan analisis tambahan dan pertimbangkan toleransi risiko Anda."
        
        result = send_telegram_message(message)
        if "successfully" in result:
            return f"Accumulation report sent: {len(signals_df)} signals"
        else:
            return result
            
    except Exception as e:
        logger.error(f"Error in send_accumulation_report: {str(e)}")
        return f"Error: {str(e)}"

def send_institutional_flow_analysis():
    """
    Send detailed foreign institutional flow analysis
    """
    try:
        conn = get_database_connection()
        latest_date = get_latest_stock_date()
        
        if not latest_date:
            return "No data available"
        
        date_filter = latest_date.strftime('%Y-%m-%d')
        
        # Foreign flow analysis
        flow_sql = f"""
        WITH flow_analysis AS (
            SELECT 
                symbol,
                name,
                COALESCE(foreign_buy, 0) as foreign_buy,
                COALESCE(foreign_sell, 0) as foreign_sell,
                COALESCE(foreign_buy, 0) - COALESCE(foreign_sell, 0) as foreign_net,
                volume,
                close,
                value,
                -- Fix: Convert foreign flows to same unit as volume (assuming foreign in shares, volume in lots)
                -- Method 1: Assume foreign flows are in shares, convert to lots
                CASE 
                    WHEN volume > 0 THEN (COALESCE(foreign_buy, 0) + COALESCE(foreign_sell, 0)) / 100.0 / volume * 100
                    ELSE 0 
                END as foreign_participation_method1,
                -- Method 2: Direct calculation (assuming same units)
                CASE 
                    WHEN volume > 0 THEN (COALESCE(foreign_buy, 0) + COALESCE(foreign_sell, 0)) / CAST(volume as FLOAT) * 100
                    ELSE 0 
                END as foreign_participation_method2,
                -- Use the more reasonable one (< 100%)
                CASE 
                    WHEN volume > 0 THEN 
                        CASE 
                            WHEN (COALESCE(foreign_buy, 0) + COALESCE(foreign_sell, 0)) / CAST(volume as FLOAT) <= 1.0 
                            THEN (COALESCE(foreign_buy, 0) + COALESCE(foreign_sell, 0)) / CAST(volume as FLOAT) * 100
                            ELSE (COALESCE(foreign_buy, 0) + COALESCE(foreign_sell, 0)) / 100.0 / volume * 100
                        END
                    ELSE 0 
                END as foreign_participation,
                -- Fix: Better buy/sell ratio calculation
                CASE 
                    WHEN COALESCE(foreign_sell, 0) > 0 THEN COALESCE(foreign_buy, 0) / CAST(foreign_sell as FLOAT)
                    WHEN COALESCE(foreign_buy, 0) > 0 THEN 999.99  -- Infinite ratio indicator
                    ELSE 0
                END as buy_sell_ratio
            FROM public.daily_stock_summary
            WHERE date = '{date_filter}'
                AND volume > 0
                AND (foreign_buy + foreign_sell) > 0
        ),
        flow_ranking AS (
            SELECT 
                *,
                RANK() OVER (ORDER BY foreign_net DESC) as net_rank,
                RANK() OVER (ORDER BY foreign_participation DESC) as participation_rank,
                CASE 
                    WHEN foreign_net > 0 AND foreign_participation > 0.3 THEN 'Strong Accumulation'
                    WHEN foreign_net > 0 AND foreign_participation > 0.15 THEN 'Moderate Accumulation'
                    WHEN foreign_net > 0 THEN 'Weak Accumulation'
                    WHEN foreign_net < 0 AND foreign_participation > 0.3 THEN 'Strong Distribution'
                    WHEN foreign_net < 0 AND foreign_participation > 0.15 THEN 'Moderate Distribution'
                    ELSE 'Weak Distribution'
                END as flow_pattern
            FROM flow_analysis
        )
        SELECT * FROM flow_ranking
        WHERE ABS(foreign_net) > 1000  -- Minimum 1000 lot net flow
        ORDER BY foreign_net DESC
        LIMIT 20
        """
        
        flow_df = pd.read_sql(flow_sql, conn)
        conn.close()
        
        if flow_df.empty:
            return "No significant foreign flows found"
        
        # Build message
        message = f"🌐 *ANALISIS ALIRAN INSTITUSI ASING ({date_filter})* 🌐\n\n"
        
        # Top foreign accumulation
        accumulation = flow_df[flow_df['foreign_net'] > 0].head(8)
        if not accumulation.empty:
            message += "📈 *TOP AKUMULASI ASING:*\n\n"
            for i, row in enumerate(accumulation.itertuples(), 1):
                # Cap participation display at reasonable levels
                participation = min(row.foreign_participation, 100.0)
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Net: +{row.foreign_net:,.0f} lot | Partisipasi: {participation:.1f}%\n"
                message += f"   Ratio B/S: {row.buy_sell_ratio:.2f} | Pola: {row.flow_pattern}\n\n"
        
        # Top foreign distribution
        distribution = flow_df[flow_df['foreign_net'] < 0].head(5)
        if not distribution.empty:
            message += "📉 *TOP DISTRIBUSI ASING:*\n\n"
            for i, row in enumerate(distribution.itertuples(), 1):
                # Cap participation display at reasonable levels
                participation = min(row.foreign_participation, 100.0)
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Net: {row.foreign_net:,.0f} lot | Partisipasi: {participation:.1f}%\n"
                message += f"   Ratio B/S: {row.buy_sell_ratio:.2f}\n\n"
        
        # Summary statistics with capped values
        total_foreign_net = flow_df['foreign_net'].sum()
        avg_participation = min(flow_df['foreign_participation'].mean(), 100.0)
        high_participation_count = len(flow_df[flow_df['foreign_participation'] > 30])  # Using 30 instead of 0.3
        
        message += "📊 *RINGKASAN ALIRAN:*\n"
        message += f"• Net Asing Total: {total_foreign_net:,.0f} lot\n"
        message += f"• Rata-rata Partisipasi: {avg_participation:.1f}%\n"
        message += f"• Saham Partisipasi Tinggi (>30%): {high_participation_count}\n"
        message += f"• Saham Akumulasi vs Distribusi: {len(accumulation)} vs {len(distribution)}\n\n"
        
        flow_trend = "Bullish" if total_foreign_net > 0 else "Bearish"
        message += f"🎯 *Sentiment Institusi: {flow_trend}*\n"
        if total_foreign_net > 0:
            message += "Institusi asing menunjukkan pola akumulasi bersih"
        else:
            message += "Institusi asing menunjukkan pola distribusi bersih"
        
        result = send_telegram_message(message)
        if "successfully" in result:
            return f"Institutional flow analysis sent: {len(flow_df)} stocks"
        else:
            return result
            
    except Exception as e:
        logger.error(f"Error in send_institutional_flow_analysis: {str(e)}")
        return f"Error: {str(e)}"

def send_volume_profile_report():
    """
    Send volume profile and smart money analysis report
    """
    try:
        conn = get_database_connection()
        latest_date = get_latest_stock_date()
        
        if not latest_date:
            return "No data available"
        
        date_filter = latest_date.strftime('%Y-%m-%d')
        
        # Volume profile analysis
        volume_sql = f"""
        WITH volume_analysis AS (
            SELECT 
                s.symbol,
                s.name,
                s.date,
                s.volume,
                s.value,
                s.close,
                s.high,
                s.low,
                COALESCE(s.foreign_buy, 0) + COALESCE(s.foreign_sell, 0) as total_foreign_volume,
                s.volume as total_volume,
                -- Calculate 20-day averages
                AVG(s.volume) OVER (
                    PARTITION BY s.symbol 
                    ORDER BY s.date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as avg_volume_20,
                AVG(s.value) OVER (
                    PARTITION BY s.symbol 
                    ORDER BY s.date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as avg_value_20,
                -- Large block detection
                CASE WHEN s.volume > s.listed_shares * 0.005 THEN 1 ELSE 0 END as large_block,
                -- Price impact analysis
                ABS(s.close - s.open_price) / NULLIF(s.open_price, 0) * 100 as price_impact
            FROM public.daily_stock_summary s
            WHERE s.date >= '{(latest_date - timedelta(days=25)).strftime('%Y-%m-%d')}'
                AND s.volume > 0
        ),
        smart_money_signals AS (
            SELECT 
                symbol,
                name,
                date,
                volume,
                value,
                close,
                volume / NULLIF(avg_volume_20, 0) as volume_ratio,
                value / NULLIF(avg_value_20, 0) as value_ratio,
                -- Fix: Smart unit conversion for foreign participation
                CASE 
                    WHEN total_volume > 0 THEN 
                        CASE 
                            WHEN total_foreign_volume / CAST(total_volume as FLOAT) <= 1.0 
                            THEN total_foreign_volume / CAST(total_volume as FLOAT)
                            ELSE total_foreign_volume / 100.0 / total_volume  -- Convert shares to lots
                        END
                    ELSE 0 
                END as foreign_participation,
                large_block,
                price_impact,
                -- Smart money score with corrected foreign participation
                CASE 
                    WHEN volume / NULLIF(avg_volume_20, 0) > 2 
                         AND CASE 
                             WHEN total_volume > 0 THEN 
                                 CASE 
                                     WHEN total_foreign_volume / CAST(total_volume as FLOAT) <= 1.0 
                                     THEN total_foreign_volume / CAST(total_volume as FLOAT)
                                     ELSE total_foreign_volume / 100.0 / total_volume
                                 END
                             ELSE 0 
                         END > 0.2
                         AND price_impact < 2 THEN 3  -- Strong accumulation
                    WHEN volume / NULLIF(avg_volume_20, 0) > 1.5 
                         AND CASE 
                             WHEN total_volume > 0 THEN 
                                 CASE 
                                     WHEN total_foreign_volume / CAST(total_volume as FLOAT) <= 1.0 
                                     THEN total_foreign_volume / CAST(total_volume as FLOAT)
                                     ELSE total_foreign_volume / 100.0 / total_volume
                                 END
                             ELSE 0 
                         END > 0.15 THEN 2  -- Moderate
                    WHEN volume / NULLIF(avg_volume_20, 0) > 1.2 THEN 1  -- Weak
                    ELSE 0
                END as smart_money_score
            FROM volume_analysis
            WHERE date = '{date_filter}'
        )
        SELECT *
        FROM smart_money_signals
        WHERE smart_money_score > 0
        ORDER BY smart_money_score DESC, volume_ratio DESC
        LIMIT 15
        """
        
        volume_df = pd.read_sql(volume_sql, conn)
        conn.close()
        
        if volume_df.empty:
            return "No significant volume patterns found"
        
        # Build message
        message = f"📊 *ANALISIS VOLUME PROFILE & SMART MONEY ({date_filter})* 📊\n\n"
        
        # Strong smart money signals
        strong_signals = volume_df[volume_df['smart_money_score'] >= 3]
        if not strong_signals.empty:
            message += "🧠 *SINYAL SMART MONEY KUAT:*\n\n"
            for i, row in enumerate(strong_signals.itertuples(), 1):
                foreign_pct = min(row.foreign_participation * 100, 100.0)  # Cap at 100%
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Volume: {row.volume_ratio:.2f}x | Value: {row.value_ratio:.2f}x\n"
                message += f"   Partisipasi Asing: {foreign_pct:.1f}% | Impact: {row.price_impact:.2f}%\n"
                if row.large_block:
                    message += f"   🔴 Large Block Detected\n"
                message += "\n"
        
        # Moderate signals
        moderate_signals = volume_df[volume_df['smart_money_score'] == 2]
        if not moderate_signals.empty:
            message += "⚡ *SINYAL SMART MONEY SEDANG:*\n\n"
            for i, row in enumerate(moderate_signals.head(6).itertuples(), 1):
                foreign_pct = min(row.foreign_participation * 100, 100.0)  # Cap at 100%
                message += f"{i}. *{row.symbol}* ({row.name})\n"
                message += f"   Volume: {row.volume_ratio:.2f}x | Asing: {foreign_pct:.1f}%\n\n"
        
        # Volume statistics with capped values
        avg_volume_ratio = volume_df['volume_ratio'].mean()
        high_volume_count = len(volume_df[volume_df['volume_ratio'] > 2])
        large_block_count = volume_df['large_block'].sum()
        avg_foreign_participation = min(volume_df['foreign_participation'].mean() * 100, 100.0)  # Cap at 100%
        
        message += "📈 *STATISTIK VOLUME:*\n"
        message += f"• Rata-rata Rasio Volume: {avg_volume_ratio:.2f}x\n"
        message += f"• Saham Volume Tinggi (>2x): {high_volume_count}\n"
        message += f"• Large Block Terdeteksi: {large_block_count}\n"
        message += f"• Rata-rata Partisipasi Asing: {avg_foreign_participation:.1f}%\n\n"
        
        # Hidden accumulation detection with corrected foreign participation
        hidden_accumulation = volume_df[
            (volume_df['volume_ratio'] > 1.5) & 
            (volume_df['price_impact'] < 1.5) &
            (volume_df['foreign_participation'] > 0.2)  # 20% threshold (already in ratio form)
        ]
        
        if not hidden_accumulation.empty:
            message += "🔍 *POTENSI HIDDEN ACCUMULATION:*\n"
            message += "Volume tinggi dengan dampak harga minimal:\n\n"
            for i, row in enumerate(hidden_accumulation.head(5).itertuples(), 1):
                message += f"{i}. *{row.symbol}*: Vol {row.volume_ratio:.2f}x, Impact {row.price_impact:.2f}%\n"
            message += "\n"
        
        message += "*💡 INTERPRETASI:*\n"
        message += "• Score 3: Accumulation tersembunyi kuat\n"
        message += "• Score 2: Institutional interest sedang\n"
        message += "• Large Block: Transaksi institusional besar\n"
        message += "• Low Price Impact + High Volume = Absorpsi supply\n\n"
        
        message += "*Catatan: Analisis berbasis volume profile akademik dan deteksi smart money patterns.*"
        
        result = send_telegram_message(message)
        if "successfully" in result:
            return f"Volume profile report sent: {len(volume_df)} patterns"
        else:
            return result
            
    except Exception as e:
        logger.error(f"Error in send_volume_profile_report: {str(e)}")
        return f"Error: {str(e)}"

# DAG Definition
with DAG(
    dag_id="akumulasi_detect",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 20 * * 1-5",  # Every weekday at 20:00 Jakarta time
    catchup=False,
    default_args=default_args,
    tags=["accumulation", "indonesian-stocks", "systematic-analysis"]
) as dag:
    
    # Wait for data dependencies
    wait_for_stock_data = ExternalTaskSensor(
        task_id="wait_for_stock_data",
        external_dag_id="idx_data_ingestion",
        external_task_id="end_task",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )
    
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
    
    # Core accumulation analysis
    accumulation_analysis = PythonOperator(
        task_id="run_accumulation_analysis",
        python_callable=run_accumulation_analysis
    )
    
    # Reporting tasks
    send_accumulation_signals = PythonOperator(
        task_id="send_accumulation_report",
        python_callable=send_accumulation_report
    )
    
    send_foreign_flow_analysis = PythonOperator(
        task_id="send_institutional_flow_analysis",
        python_callable=send_institutional_flow_analysis
    )
    
    send_volume_analysis = PythonOperator(
        task_id="send_volume_profile_report",
        python_callable=send_volume_profile_report
    )
    
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    # Task dependencies
    [wait_for_stock_data, wait_for_transformation] >> accumulation_analysis
    accumulation_analysis >> [send_accumulation_signals, send_foreign_flow_analysis, send_volume_analysis]
    [send_accumulation_signals, send_foreign_flow_analysis, send_volume_analysis] >> end_task