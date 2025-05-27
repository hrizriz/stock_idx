"""
Power BI Data Preparation Script
Membuat materialized views dan aggregated tables untuk performa optimal di Power BI
"""

import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import numpy as np

class PowerBIDataPrep:
    def __init__(self, db_config):
        self.db_config = db_config
        
    def get_connection(self):
        return psycopg2.connect(**self.db_config)
    
    def check_table_exists(self, schema, table_name):
        """Check if a table exists in the database"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            );
        """, (schema, table_name))
        
        exists = cur.fetchone()[0]
        cur.close()
        conn.close()
        
        return exists
    
    def create_powerbi_schema(self):
        """Create dedicated schema for Power BI"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS powerbi;
            
            -- Grant permissions
            GRANT USAGE ON SCHEMA powerbi TO airflow;
            GRANT SELECT ON ALL TABLES IN SCHEMA powerbi TO airflow;
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        
    def create_dim_stocks_table(self):
        """Create dimension table for stocks"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            DROP TABLE IF EXISTS powerbi.dim_stocks CASCADE;
            
            CREATE TABLE powerbi.dim_stocks AS
            WITH stock_info AS (
                SELECT DISTINCT
                    symbol,
                    name,
                    CASE 
                        WHEN symbol IN ('BBCA', 'BBRI', 'BMRI', 'BBNI', 'BBTN', 'BRIS', 'BNGA') THEN 'Banking'
                        WHEN symbol IN ('TLKM', 'EXCL', 'ISAT', 'FREN', 'MTEL') THEN 'Telecommunications'
                        WHEN symbol IN ('ASII', 'UNTR', 'AALI', 'AUTO', 'GJTL') THEN 'Automotive & Plantation'
                        WHEN symbol IN ('ICBP', 'INDF', 'UNVR', 'MYOR', 'CPIN') THEN 'Consumer Goods'
                        WHEN symbol IN ('GOTO', 'BUKA', 'EMTK', 'BFIN') THEN 'Technology'
                        WHEN symbol IN ('ADRO', 'INCO', 'ANTM', 'PTBA', 'TINS') THEN 'Mining'
                        WHEN symbol IN ('SMGR', 'INTP', 'WIKA', 'PTPP', 'WSKT') THEN 'Construction'
                        WHEN symbol IN ('PGAS', 'MEDC', 'ENRG', 'ELSA') THEN 'Energy'
                        ELSE 'Others'
                    END as sector,
                    CASE 
                        WHEN symbol LIKE 'BB%' OR symbol LIKE 'BM%' OR symbol LIKE 'BN%' THEN 'Finance'
                        WHEN symbol IN ('TLKM', 'EXCL', 'ISAT') THEN 'Telco'
                        WHEN symbol IN ('GOTO', 'BUKA', 'EMTK') THEN 'Tech'
                        ELSE 'Non-Finance'
                    END as category,
                    listed_shares,
                    tradable_shares
                FROM public.daily_stock_summary
                WHERE date = (SELECT MAX(date) FROM public.daily_stock_summary)
            ),
            latest_metrics AS (
                SELECT 
                    symbol,
                    close as last_price,
                    volume as last_volume,
                    value as last_value,
                    -- Calculate approximate market cap using close price and listed shares
                    close * listed_shares as market_cap_approx
                FROM public.daily_stock_summary
                WHERE date = (SELECT MAX(date) FROM public.daily_stock_summary)
            ),
            volume_stats AS (
                SELECT 
                    symbol,
                    AVG(volume) as avg_volume_30d,
                    AVG(value) as avg_value_30d,
                    COUNT(*) as trading_days_30d
                FROM public.daily_stock_summary
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY symbol
            )
            SELECT 
                s.symbol,
                s.name,
                s.sector,
                s.category,
                s.listed_shares,
                s.tradable_shares,
                m.last_price,
                m.last_volume,
                m.last_value,
                m.market_cap_approx,
                CASE 
                    WHEN m.market_cap_approx > 100e12 THEN 'Large Cap'
                    WHEN m.market_cap_approx > 10e12 THEN 'Mid Cap'
                    WHEN m.market_cap_approx > 1e12 THEN 'Small Cap'
                    ELSE 'Micro Cap'
                END as market_cap_category,
                v.avg_volume_30d,
                v.avg_value_30d,
                v.trading_days_30d,
                CASE 
                    WHEN v.avg_volume_30d > 50000000 THEN 'Very High Liquidity'
                    WHEN v.avg_volume_30d > 10000000 THEN 'High Liquidity'
                    WHEN v.avg_volume_30d > 1000000 THEN 'Medium Liquidity'
                    ELSE 'Low Liquidity'
                END as liquidity_category,
                CURRENT_TIMESTAMP as last_updated
            FROM stock_info s
            LEFT JOIN latest_metrics m ON s.symbol = m.symbol
            LEFT JOIN volume_stats v ON s.symbol = v.symbol;
            
            -- Add primary key
            ALTER TABLE powerbi.dim_stocks ADD PRIMARY KEY (symbol);
            
            -- Create indexes
            CREATE INDEX idx_powerbi_stocks_sector ON powerbi.dim_stocks(sector);
            CREATE INDEX idx_powerbi_stocks_category ON powerbi.dim_stocks(category);
            CREATE INDEX idx_powerbi_stocks_liquidity ON powerbi.dim_stocks(liquidity_category);
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        
    def create_fact_daily_trading(self):
        """Create fact table combining all daily metrics"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            DROP TABLE IF EXISTS powerbi.fact_daily_trading CASCADE;
            
            CREATE TABLE powerbi.fact_daily_trading AS
            SELECT 
                d.date,
                d.symbol,
                d.open_price,
                d.high,
                d.low,
                d.close,
                d.volume,
                d.value,
                d.frequency,
                d.prev_close,
                CASE 
                    WHEN d.prev_close > 0 THEN (d.close - d.prev_close) / d.prev_close * 100 
                    ELSE 0 
                END as percent_change,
                d.foreign_buy,
                d.foreign_sell,
                (d.foreign_buy - d.foreign_sell) as foreign_net,
                d.bid,
                d.offer,
                d.bid_volume,
                d.offer_volume,
                
                -- Technical Indicators (check if tables exist)
                r.rsi,
                r.rsi_signal,
                m.macd_line,
                m.signal_line,
                m.macd_histogram,
                m.macd_signal,
                b.upper_band,
                b.middle_band,
                b.lower_band,
                b.percent_b,
                b.bb_signal,
                
                -- Trading Signals
                s.buy_score,
                s.winning_probability,
                s.signal_strength,
                ARRAY_LENGTH(s.indicators_triggered, 1) as total_indicators,
                
                -- ML Predictions
                p.predicted_close,
                p.actual_close,
                p.error_percentage,
                CASE 
                    WHEN p.predicted_close > d.close THEN (p.predicted_close - d.close) / d.close * 100 
                    ELSE 0 
                END as expected_return,
                
                -- Sentiment
                n.avg_sentiment,
                n.positive_percentage,
                n.negative_percentage,
                n.news_count,
                n.trading_signal as news_signal,
                
                -- Volume analysis
                CASE 
                    WHEN d.volume > 0 THEN 
                        d.volume / NULLIF(AVG(d.volume) OVER (
                            PARTITION BY d.symbol 
                            ORDER BY d.date 
                            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                        ), 0)
                    ELSE 1
                END as volume_ratio,
                
                -- Calculated fields for Power BI
                CASE 
                    WHEN s.buy_score >= 8 THEN 'Strong Buy'
                    WHEN s.buy_score >= 6 THEN 'Buy'
                    WHEN s.buy_score >= 4 THEN 'Hold'
                    WHEN s.buy_score >= 2 THEN 'Sell'
                    ELSE 'Strong Sell'
                END as signal_category,
                
                CASE 
                    WHEN r.rsi < 30 THEN 'Oversold'
                    WHEN r.rsi > 70 THEN 'Overbought'
                    ELSE 'Neutral'
                END as rsi_status,
                
                -- Time dimensions
                EXTRACT(YEAR FROM d.date) as year,
                EXTRACT(QUARTER FROM d.date) as quarter,
                EXTRACT(MONTH FROM d.date) as month,
                EXTRACT(WEEK FROM d.date) as week,
                TO_CHAR(d.date, 'YYYY-MM') as year_month,
                TO_CHAR(d.date, 'Day') as day_name,
                EXTRACT(DOW FROM d.date) as day_of_week
                
            FROM public.daily_stock_summary d
            LEFT JOIN public_analytics.technical_indicators_rsi r 
                ON d.symbol = r.symbol AND d.date = r.date
            LEFT JOIN public_analytics.technical_indicators_macd m 
                ON d.symbol = m.symbol AND d.date = m.date
            LEFT JOIN public_analytics.technical_indicators_bollinger b 
                ON d.symbol = b.symbol AND d.date = b.date
            LEFT JOIN public_analytics.advanced_trading_signals s 
                ON d.symbol = s.symbol AND d.date = s.date
            LEFT JOIN public.stock_predictions p 
                ON d.symbol = p.symbol AND d.date = p.prediction_date
            LEFT JOIN public_analytics.news_sentiment_analysis n 
                ON d.symbol = n.symbol AND d.date = n.date
            WHERE d.date >= CURRENT_DATE - INTERVAL '2 years';
            
            -- Add indexes for performance
            CREATE INDEX idx_powerbi_fact_date ON powerbi.fact_daily_trading(date);
            CREATE INDEX idx_powerbi_fact_symbol ON powerbi.fact_daily_trading(symbol);
            CREATE INDEX idx_powerbi_fact_date_symbol ON powerbi.fact_daily_trading(date, symbol);
            CREATE INDEX idx_powerbi_fact_signal ON powerbi.fact_daily_trading(signal_category);
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        
    def create_aggregated_metrics(self):
        """Create pre-aggregated metrics for better performance"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        # Monthly aggregates
        cur.execute("""
            DROP TABLE IF EXISTS powerbi.monthly_performance CASCADE;
            
            CREATE TABLE powerbi.monthly_performance AS
            WITH monthly_data AS (
                SELECT 
                    DATE_TRUNC('month', date) as month,
                    symbol,
                    date,
                    close,
                    percent_change,
                    volume,
                    value,
                    buy_score,
                    winning_probability,
                    rsi,
                    ROW_NUMBER() OVER (PARTITION BY symbol, DATE_TRUNC('month', date) ORDER BY date) as rn_first,
                    ROW_NUMBER() OVER (PARTITION BY symbol, DATE_TRUNC('month', date) ORDER BY date DESC) as rn_last
                FROM powerbi.fact_daily_trading
            )
            SELECT 
                month,
                symbol,
                COUNT(DISTINCT date) as trading_days,
                AVG(close) as avg_close,
                MIN(close) as month_low,
                MAX(close) as month_high,
                SUM(volume) as total_volume,
                SUM(value) as total_value,
                
                -- Monthly return calculation
                (MAX(CASE WHEN rn_last = 1 THEN close END) - 
                 MAX(CASE WHEN rn_first = 1 THEN close END)) / 
                 NULLIF(MAX(CASE WHEN rn_first = 1 THEN close END), 0) * 100 as monthly_return,
                
                STDDEV(percent_change) as volatility,
                
                -- Signal performance
                AVG(buy_score) as avg_buy_score,
                AVG(winning_probability) as avg_win_probability,
                COUNT(CASE WHEN buy_score >= 8 THEN 1 END) as strong_buy_signals,
                
                -- Technical indicators average
                AVG(rsi) as avg_rsi,
                COUNT(CASE WHEN rsi < 30 THEN 1 END) as oversold_days,
                COUNT(CASE WHEN rsi > 70 THEN 1 END) as overbought_days
                
            FROM monthly_data
            GROUP BY month, symbol;
            
            CREATE INDEX idx_powerbi_monthly_month ON powerbi.monthly_performance(month);
            CREATE INDEX idx_powerbi_monthly_symbol ON powerbi.monthly_performance(symbol);
        """)
        
        # Signal performance summary
        cur.execute("""
            DROP TABLE IF EXISTS powerbi.signal_performance_summary CASCADE;
            
            CREATE TABLE powerbi.signal_performance_summary AS
            WITH backtest_data AS (
                SELECT 
                    buy_score,
                    COUNT(*) as total_signals,
                    SUM(CASE WHEN is_win THEN 1 ELSE 0 END) as winning_signals,
                    AVG(percent_change_5d) as avg_return_5d,
                    STDDEV(percent_change_5d) as return_std_5d,
                    MIN(percent_change_5d) as worst_return,
                    MAX(percent_change_5d) as best_return,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY percent_change_5d) as median_return
                FROM public_analytics.backtest_results
                WHERE signal_date >= CURRENT_DATE - INTERVAL '6 months'
                GROUP BY buy_score
            )
            SELECT 
                buy_score,
                total_signals,
                winning_signals,
                CASE 
                    WHEN total_signals > 0 THEN winning_signals::float / total_signals * 100 
                    ELSE 0 
                END as win_rate,
                avg_return_5d,
                return_std_5d,
                worst_return,
                best_return,
                median_return,
                CASE 
                    WHEN return_std_5d > 0 THEN avg_return_5d / return_std_5d * SQRT(252/5) 
                    ELSE 0 
                END as sharpe_ratio,
                CASE 
                    WHEN avg_return_5d > 0 AND winning_signals::float / NULLIF(total_signals, 0) > 0.6 THEN 'Profitable'
                    WHEN avg_return_5d > 0 THEN 'Marginal'
                    ELSE 'Unprofitable'
                END as strategy_quality
            FROM backtest_data;
            
            CREATE INDEX idx_powerbi_signal_score ON powerbi.signal_performance_summary(buy_score);
        """)
        
        # Top performing stocks summary
        cur.execute("""
            DROP TABLE IF EXISTS powerbi.top_performers CASCADE;
            
            CREATE TABLE powerbi.top_performers AS
            WITH performance_data AS (
                SELECT 
                    symbol,
                    COUNT(CASE WHEN buy_score >= 7 THEN 1 END) as high_score_days,
                    AVG(CASE WHEN buy_score >= 7 THEN winning_probability END) as avg_win_prob_high_score,
                    AVG(percent_change) as avg_daily_return,
                    STDDEV(percent_change) as daily_volatility,
                    SUM(CASE WHEN percent_change > 0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) * 100 as positive_days_pct,
                    AVG(volume) as avg_volume,
                    AVG(value) as avg_value
                FROM powerbi.fact_daily_trading
                WHERE date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY symbol
            )
            SELECT 
                p.*,
                s.name,
                s.sector,
                s.market_cap_category,
                s.liquidity_category,
                CASE 
                    WHEN p.daily_volatility > 0 THEN p.avg_daily_return / p.daily_volatility * SQRT(252) 
                    ELSE 0 
                END as sharpe_ratio,
                RANK() OVER (ORDER BY p.avg_daily_return DESC) as return_rank,
                RANK() OVER (ORDER BY p.high_score_days DESC) as signal_rank
            FROM performance_data p
            JOIN powerbi.dim_stocks s ON p.symbol = s.symbol
            WHERE p.avg_volume > 1000000;  -- Filter for liquid stocks only
            
            CREATE INDEX idx_powerbi_top_symbol ON powerbi.top_performers(symbol);
        """)
        
        # Create calendar table for Power BI
        cur.execute("""
            DROP TABLE IF EXISTS powerbi.dim_calendar CASCADE;
            
            CREATE TABLE powerbi.dim_calendar AS
            WITH calendar AS (
                SELECT 
                    generate_series(
                        '2020-01-01'::date, 
                        CURRENT_DATE + INTERVAL '1 year', 
                        '1 day'::interval
                    )::date as date
            )
            SELECT 
                date,
                EXTRACT(YEAR FROM date) as year,
                EXTRACT(QUARTER FROM date) as quarter,
                EXTRACT(MONTH FROM date) as month,
                TO_CHAR(date, 'Month') as month_name,
                TO_CHAR(date, 'Mon') as month_short,
                EXTRACT(WEEK FROM date) as week,
                EXTRACT(DOW FROM date) as day_of_week,
                TO_CHAR(date, 'Day') as day_name,
                TO_CHAR(date, 'Dy') as day_short,
                CASE 
                    WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN FALSE 
                    ELSE TRUE 
                END as is_weekday,
                CASE 
                    WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE 
                    ELSE FALSE 
                END as is_weekend,
                TO_CHAR(date, 'YYYY-MM') as year_month,
                TO_CHAR(date, 'YYYY-"Q"Q') as year_quarter,
                date - INTERVAL '1 year' as same_day_last_year,
                date - INTERVAL '1 month' as same_day_last_month,
                date - INTERVAL '1 week' as same_day_last_week
            FROM calendar;
            
            ALTER TABLE powerbi.dim_calendar ADD PRIMARY KEY (date);
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        
    def create_realtime_view(self):
        """Create view for real-time data"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            CREATE OR REPLACE VIEW powerbi.v_realtime_trading AS
            WITH latest_data AS (
                SELECT 
                    symbol,
                    MAX(date) as latest_date
                FROM public.daily_stock_summary
                GROUP BY symbol
            )
            SELECT 
                d.symbol,
                d.name,
                d.date,
                d.open_price,
                d.high,
                d.low,
                d.close,
                d.volume,
                d.value,
                d.change,
                CASE 
                    WHEN d.prev_close > 0 THEN (d.close - d.prev_close) / d.prev_close * 100 
                    ELSE 0 
                END as percent_change,
                d.foreign_buy,
                d.foreign_sell,
                (d.foreign_buy - d.foreign_sell) as foreign_net,
                s.buy_score,
                s.winning_probability,
                s.signal_strength,
                r.rsi,
                r.rsi_signal,
                m.macd_signal,
                CASE 
                    WHEN s.buy_score >= 8 AND s.winning_probability >= 0.8 THEN 'HOT'
                    WHEN s.buy_score >= 6 AND s.winning_probability >= 0.7 THEN 'WARM'
                    ELSE 'NORMAL'
                END as opportunity_level,
                CURRENT_TIMESTAMP as last_refresh
            FROM latest_data l
            JOIN public.daily_stock_summary d 
                ON l.symbol = d.symbol AND l.latest_date = d.date
            LEFT JOIN public_analytics.advanced_trading_signals s 
                ON d.symbol = s.symbol AND d.date = s.date
            LEFT JOIN public_analytics.technical_indicators_rsi r 
                ON d.symbol = r.symbol AND d.date = r.date
            LEFT JOIN public_analytics.technical_indicators_macd m 
                ON d.symbol = m.symbol AND d.date = m.date;
                
            -- Create materialized view for better performance
            DROP MATERIALIZED VIEW IF EXISTS powerbi.mv_latest_signals CASCADE;
            
            CREATE MATERIALIZED VIEW powerbi.mv_latest_signals AS
            SELECT 
                s.symbol,
                s.date,
                s.buy_score,
                s.winning_probability,
                s.signal_strength,
                d.name,
                d.close,
                d.volume,
                d.value,
                CASE 
                    WHEN d.prev_close > 0 THEN (d.close - d.prev_close) / d.prev_close * 100 
                    ELSE 0 
                END as percent_change,
                r.rsi,
                m.macd_signal,
                b.bb_signal,
                ns.avg_sentiment,
                ns.trading_signal as news_signal
            FROM public_analytics.advanced_trading_signals s
            JOIN public.daily_stock_summary d ON s.symbol = d.symbol AND s.date = d.date
            LEFT JOIN public_analytics.technical_indicators_rsi r ON s.symbol = r.symbol AND s.date = r.date
            LEFT JOIN public_analytics.technical_indicators_macd m ON s.symbol = m.symbol AND s.date = m.date
            LEFT JOIN public_analytics.technical_indicators_bollinger b ON s.symbol = b.symbol AND s.date = b.date
            LEFT JOIN public_analytics.news_sentiment_analysis ns ON s.symbol = ns.symbol AND s.date = ns.date
            WHERE s.date = (SELECT MAX(date) FROM public_analytics.advanced_trading_signals)
            AND s.buy_score >= 5;
            
            CREATE INDEX idx_mv_latest_signals_symbol ON powerbi.mv_latest_signals(symbol);
            CREATE INDEX idx_mv_latest_signals_score ON powerbi.mv_latest_signals(buy_score);
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        
    def optimize_for_powerbi(self):
        """Run all optimization steps"""
        print("Creating Power BI schema...")
        self.create_powerbi_schema()
        
        # Check required tables exist
        required_tables = [
            ('public', 'daily_stock_summary'),
            ('public_analytics', 'technical_indicators_rsi'),
            ('public_analytics', 'technical_indicators_macd'),
            ('public_analytics', 'advanced_trading_signals'),
            ('public_analytics', 'backtest_results')
        ]
        
        missing_tables = []
        for schema, table in required_tables:
            if not self.check_table_exists(schema, table):
                missing_tables.append(f"{schema}.{table}")
        
        if missing_tables:
            print("\n⚠️  WARNING: The following tables are missing:")
            for table in missing_tables:
                print(f"   - {table}")
            print("\nSome features may not be available. Please run the following DAGs first:")
            print("   1. stock_data_ingestion")
            print("   2. data_transformation")
            print("   3. unified_trading_signals")
            print("\nContinuing with available data...\n")
        
        print("Creating dimension tables...")
        try:
            self.create_dim_stocks_table()
            print("✅ Dimension tables created successfully")
        except Exception as e:
            print(f"❌ Error creating dimension tables: {str(e)}")
        
        print("\nCreating fact tables...")
        try:
            self.create_fact_daily_trading()
            print("✅ Fact tables created successfully")
        except Exception as e:
            print(f"❌ Error creating fact tables: {str(e)}")
        
        print("\nCreating aggregated metrics...")
        try:
            self.create_aggregated_metrics()
            print("✅ Aggregated metrics created successfully")
        except Exception as e:
            print(f"❌ Error creating aggregated metrics: {str(e)}")
        
        print("\nCreating real-time views...")
        try:
            self.create_realtime_view()
            print("✅ Real-time views created successfully")
        except Exception as e:
            print(f"❌ Error creating real-time views: {str(e)}")
        
        print("\nAnalyzing tables for optimization...")
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            
            # Check which tables were successfully created
            tables_to_analyze = [
                'powerbi.fact_daily_trading',
                'powerbi.dim_stocks',
                'powerbi.monthly_performance',
                'powerbi.dim_calendar'
            ]
            
            for table in tables_to_analyze:
                schema, table_name = table.split('.')
                if self.check_table_exists(schema, table_name):
                    cur.execute(f"ANALYZE {table};")
                    print(f"✅ Analyzed {table}")
            
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"❌ Error analyzing tables: {str(e)}")
        
        print("\n✅ Power BI data preparation completed!")
        
    def generate_connection_script(self):
        """Generate M script for Power BI connection"""
        # Check which tables were successfully created
        available_tables = []
        
        tables_to_check = [
            ('powerbi', 'fact_daily_trading', 'Main fact table with all trading data'),
            ('powerbi', 'dim_stocks', 'Stock dimension with sectors and categories'),
            ('powerbi', 'dim_calendar', 'Calendar dimension for time intelligence'),
            ('powerbi', 'monthly_performance', 'Pre-aggregated monthly metrics'),
            ('powerbi', 'signal_performance_summary', 'Trading signal performance metrics'),
            ('powerbi', 'top_performers', 'Top performing stocks summary'),
            ('powerbi', 'mv_latest_signals', 'Latest trading signals (materialized view)'),
            ('powerbi', 'v_realtime_trading', 'Real-time trading data (view)')
        ]
        
        for schema, table, description in tables_to_check:
            if self.check_table_exists(schema, table):
                available_tables.append((schema, table, description))
        
        if not available_tables:
            print("\n❌ No Power BI tables were created. Please check the errors above.")
            return
        
        print("\n📊 Available Power BI Tables/Views:")
        print("=" * 60)
        for schema, table, description in available_tables:
            print(f"✅ {schema}.{table}")
            print(f"   {description}")
        print("=" * 60)
        
        m_script = f"""
// Power Query M Script for PostgreSQL Connection
// Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
// Database: {self.db_config['dbname']}
// Host: {self.db_config['host']}:{self.db_config['port']}

let
    // Connect to PostgreSQL database
    Source = PostgreSQL.Database("{self.db_config['host']}:{self.db_config['port']}", "{self.db_config['dbname']}"),
    
    // Get powerbi schema
    powerbi_schema = Source{{[Schema="powerbi"]}}[Data],
"""
        
        # Add available tables to M script
        for i, (schema, table, description) in enumerate(available_tables):
            if schema == 'powerbi':
                m_script += f"""    
    // {description}
    {table} = Source{{[Schema="{schema}",Item="{table}"]}}[Data]{"," if i < len(available_tables) - 1 else ""}
"""
        
        m_script += """
in
    // Return the main fact table
    fact_daily_trading
"""
        
        with open('powerbi_connection_script.pq', 'w') as f:
            f.write(m_script)
            
        print(f"\n✅ Power Query script saved to: powerbi_connection_script.pq")
        
        # Generate summary report
        summary_report = f"""
# Power BI Integration Summary Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Database Connection
- Host: {self.db_config['host']}:{self.db_config['port']}
- Database: {self.db_config['dbname']}
- Schema: powerbi

## Available Tables

"""
        
        for schema, table, description in available_tables:
            summary_report += f"### {schema}.{table}\n{description}\n\n"
        
        summary_report += """
## Next Steps in Power BI

1. **Open Power BI Desktop**

2. **Get Data > PostgreSQL**
   - Server: localhost:5432
   - Database: airflow
   - Data Connectivity mode: Import (for best performance)
   
3. **Select Tables**
   - Navigate to 'powerbi' schema
   - Select the tables listed above
   
4. **Create Relationships**
   ```
   dim_stocks[symbol] -> fact_daily_trading[symbol] (1:*)
   dim_calendar[date] -> fact_daily_trading[date] (1:*)
   ```

5. **Create Key Measures**
   ```dax
   Current Price = 
   CALCULATE(
       LASTNONBLANK(fact_daily_trading[close], 1),
       FILTER(ALL(dim_calendar), dim_calendar[date] = MAX(fact_daily_trading[date]))
   )
   
   Win Rate % = 
   DIVIDE(
       COUNTROWS(FILTER(signal_performance_summary, [win_rate] > 50)),
       COUNTROWS(signal_performance_summary)
   ) * 100
   ```

6. **Build Visualizations**
   - Price trends with candlestick chart
   - Trading signals heatmap
   - Performance metrics dashboard
   - News sentiment analysis

## Troubleshooting

If some tables are missing:
1. Run the required Airflow DAGs first
2. Check PostgreSQL logs for errors
3. Verify user permissions

## Performance Tips

- Use Import mode for historical data
- Use DirectQuery only for real-time views
- Create aggregated measures in DAX
- Limit date ranges in filters
"""
        
        with open('powerbi_integration_report.md', 'w') as f:
            f.write(summary_report)
            
        print(f"✅ Integration report saved to: powerbi_integration_report.md")

# Integration with Airflow DAG
def create_powerbi_refresh_dag():
    """Create Airflow DAG for Power BI data refresh"""
    dag_content = """
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'powerbi_data_refresh',
    default_args=default_args,
    description='Refresh Power BI optimized tables',
    schedule_interval='0 */2 * * *',  # Every 2 hours
    catchup=False
)

# Task 1: Refresh materialized views
refresh_powerbi_data = PostgresOperator(
    task_id='refresh_powerbi_tables',
    postgres_conn_id='postgres_default',
    sql='''
        REFRESH MATERIALIZED VIEW CONCURRENTLY powerbi.fact_daily_trading;
        REFRESH MATERIALIZED VIEW CONCURRENTLY powerbi.monthly_performance;
    ''',
    dag=dag
)

# Task 2: Trigger Power BI dataset refresh via API
def trigger_powerbi_refresh(**context):
    # Power BI REST API call
    dataset_id = "your_dataset_id"
    workspace_id = "your_workspace_id"
    
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    
    headers = {
        'Authorization': f'Bearer {Variable.get("powerbi_access_token")}',
        'Content-Type': 'application/json'
    }
    
    response = requests.post(url, headers=headers, json={})
    
    if response.status_code == 202:
        print("Power BI refresh triggered successfully")
    else:
        raise Exception(f"Failed to trigger refresh: {response.text}")

trigger_refresh = PythonOperator(
    task_id='trigger_powerbi_refresh',
    python_callable=trigger_powerbi_refresh,
    dag=dag
)

refresh_powerbi_data >> trigger_refresh
"""
    
    with open('dags/powerbi_refresh_dag.py', 'w') as f:
        f.write(dag_content)
    
    print("Airflow DAG created: dags/powerbi_refresh_dag.py")

if __name__ == "__main__":
    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'dbname': 'airflow',
        'user': 'airflow',
        'password': 'airflow'
    }
    
    print("=" * 60)
    print("POWER BI DATA PREPARATION SCRIPT")
    print("=" * 60)
    print(f"Database: {db_config['dbname']}")
    print(f"Host: {db_config['host']}:{db_config['port']}")
    print("=" * 60)
    
    # Initialize and run optimization
    prep = PowerBIDataPrep(db_config)
    
    # Test database connection
    try:
        conn = prep.get_connection()
        print("✅ Database connection successful")
        conn.close()
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        print("\nPlease check:")
        print("1. PostgreSQL is running")
        print("2. Credentials are correct")
        print("3. Database 'airflow' exists")
        exit(1)
    
    # Run optimization
    prep.optimize_for_powerbi()
    
    # Generate connection scripts
    prep.generate_connection_script()
    
    # Create Airflow DAG
    try:
        create_powerbi_refresh_dag()
        print("✅ Airflow DAG created: dags/powerbi_refresh_dag.py")
    except Exception as e:
        print(f"⚠️  Could not create Airflow DAG: {str(e)}")
    
    print("\n" + "=" * 60)
    print("✅ POWER BI INTEGRATION SETUP COMPLETED!")
    print("=" * 60)
    
    print("\n📋 NEXT STEPS:")
    print("\n1. OPEN POWER BI DESKTOP")
    print("   - Click 'Get Data' > 'PostgreSQL database'")
    print(f"   - Server: {db_config['host']}:{db_config['port']}")
    print(f"   - Database: {db_config['dbname']}")
    print("   - Navigate to 'powerbi' schema")
    
    print("\n2. IMPORT KEY TABLES")
    print("   - fact_daily_trading (main fact table)")
    print("   - dim_stocks (stock dimensions)")
    print("   - dim_calendar (date table)")
    print("   - signal_performance_summary (performance metrics)")
    
    print("\n3. CREATE RELATIONSHIPS")
    print("   - dim_stocks[symbol] -> fact_daily_trading[symbol]")
    print("   - dim_calendar[date] -> fact_daily_trading[date]")
    
    print("\n4. BUILD YOUR DASHBOARD")
    print("   - Use provided DAX measures")
    print("   - Create visualizations")
    print("   - Set up refresh schedule")
    
    print("\n📁 GENERATED FILES:")
    print("   - powerbi_connection_script.pq (Power Query M script)")
    print("   - powerbi_integration_report.md (Full documentation)")
    print("   - dags/powerbi_refresh_dag.py (Airflow refresh DAG)")
    
    print("\n⚠️  IMPORTANT:")
    print("   - If some features are missing, run required Airflow DAGs first")
    print("   - Check powerbi_integration_report.md for detailed instructions")
    print("   - For real-time data, use DirectQuery on views only")
    
    print("\n" + "=" * 60)