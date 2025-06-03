class OverviewQueries:
    """SQL queries for overview dashboard"""
    
    @staticmethod
    def get_market_kpis(metrics_table):
        return f"""
        WITH latest_date AS (
            SELECT MAX(date) as max_date FROM {metrics_table}
        ),
        latest_metrics AS (
            SELECT 
                COUNT(DISTINCT symbol) as total_stocks,
                AVG(
                    CASE
                        WHEN prev_close IS NOT NULL AND prev_close > 0 
                            THEN (close - prev_close) / prev_close * 100
                        ELSE NULL
                    END
                ) as avg_change,
                SUM(volume) as total_volume,
                SUM(value) as total_value
            FROM {metrics_table}, latest_date
            WHERE date = latest_date.max_date
        ),
        gainers_losers AS (
            SELECT 
                SUM(CASE WHEN close > prev_close THEN 1 ELSE 0 END) as gainers,
                SUM(CASE WHEN close < prev_close THEN 1 ELSE 0 END) as losers
            FROM {metrics_table}, latest_date
            WHERE date = latest_date.max_date
            AND prev_close IS NOT NULL AND prev_close > 0
        )
        SELECT 
            lm.total_stocks,
            lm.avg_change,
            lm.total_volume,
            lm.total_value,
            gl.gainers,
            gl.losers
        FROM latest_metrics lm
        CROSS JOIN gainers_losers gl
        """
    
    @staticmethod
    def get_top_movers(metrics_table, limit=15):
        return f"""
        WITH latest_date AS (
            SELECT MAX(date) as max_date FROM {metrics_table}
        )
        SELECT 
            symbol,
            name,
            close,
            prev_close,
            CASE
                WHEN prev_close IS NOT NULL AND prev_close > 0 
                    THEN (close - prev_close) / prev_close * 100
                ELSE 0
            END as percent_change,
            volume
        FROM {metrics_table}, latest_date
        WHERE date = latest_date.max_date
        AND prev_close IS NOT NULL AND prev_close > 0
        AND symbol IS NOT NULL
        ORDER BY ABS(
            CASE
                WHEN prev_close IS NOT NULL AND prev_close > 0 
                    THEN (close - prev_close) / prev_close * 100
                ELSE 0
            END
        ) DESC
        LIMIT {limit}
        """

class TechnicalQueries:
    """SQL queries for technical analysis"""
    
    @staticmethod
    def get_available_stocks_with_technical():
        return """
        SELECT DISTINCT r.symbol, d.name
        FROM public_analytics.technical_indicators_rsi r
        JOIN public_analytics.daily_stock_metrics d ON r.symbol = d.symbol AND r.date = d.date
        WHERE r.date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY r.symbol
        """
    
    @staticmethod
    def get_comprehensive_stock_data(symbol, period_days):
        return f"""
        WITH stock_data AS (
            SELECT 
                dm.symbol,
                dm.name,
                dm.date,
                dm.open_price,
                dm.high,
                dm.low,
                dm.close,
                dm.volume,
                dm.percent_change,
                dm.value
            FROM public_analytics.daily_stock_metrics dm
            WHERE dm.symbol = '{symbol}'
            AND dm.date >= CURRENT_DATE - INTERVAL '{period_days} days'
        ),
        rsi_data AS (
            SELECT 
                symbol,
                date,
                rsi,
                rsi_signal
            FROM public_analytics.technical_indicators_rsi
            WHERE symbol = '{symbol}'
            AND date >= CURRENT_DATE - INTERVAL '{period_days} days'
        ),
        macd_data AS (
            SELECT 
                symbol,
                date,
                macd_line,
                signal_line,
                macd_histogram,
                macd_signal
            FROM public_analytics.technical_indicators_macd
            WHERE symbol = '{symbol}'
            AND date >= CURRENT_DATE - INTERVAL '{period_days} days'
        )
        SELECT 
            sd.*,
            r.rsi,
            r.rsi_signal,
            m.macd_line,
            m.signal_line,
            m.macd_histogram,
            m.macd_signal
        FROM stock_data sd
        LEFT JOIN rsi_data r ON sd.symbol = r.symbol AND sd.date = r.date
        LEFT JOIN macd_data m ON sd.symbol = m.symbol AND sd.date = m.date
        ORDER BY sd.date ASC
        """