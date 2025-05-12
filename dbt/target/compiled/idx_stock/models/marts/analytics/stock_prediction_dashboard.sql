

WITH stock_data AS (
    SELECT *
    FROM "airflow"."public_core"."fct_daily_stock_metrics"
),

technical_indicators AS (
    SELECT 
        r.symbol,
        r.date,
        r.rsi,
        r.rsi_signal,
        m.macd_line,
        m.signal_line,
        m.macd_signal,
        m.histogram
    FROM "airflow"."public_analytics"."technical_indicators_rsi" r
    JOIN "airflow"."public_analytics"."technical_indicators_macd" m 
        ON r.symbol = m.symbol AND r.date = m.date
),

news_sentiment AS (
    SELECT *
    FROM "airflow"."public_analytics"."news_sentiment_analysis"
),

lstm_data AS (
    SELECT *
    FROM "airflow"."public_core"."fct_stock_predictions"
),

combined_data AS (
    SELECT
        sd.symbol,
        sd.date,
        sd.close,
        sd.volume,
        sd.percent_change,
        
        -- Technical indicators
        ti.rsi,
        ti.rsi_signal,
        ti.macd_line,
        ti.macd_signal,
        ti.histogram,

        -- Sentiment
        ns.avg_sentiment,
        ns.positive_percentage,
        ns.negative_percentage,
        ns.trading_signal AS news_trading_signal,

        -- LSTM
        ld.predicted_close,
        ld.prediction_direction AS lstm_direction,
        ld.actual_close,
        ld.prediction_error,
        ld.error_percentage,

        -- Derived
        ld.predicted_close - sd.close AS prediction_gap,
        CASE 
            WHEN sd.close IS NOT NULL AND ld.predicted_close IS NOT NULL
            THEN ROUND((ld.predicted_close - sd.close) / sd.close * 100, 2)
            ELSE NULL
        END AS prediction_gap_pct,

        -- Aggregated final signal (example logic)
        CASE
            WHEN ti.macd_signal = 'Bullish' AND ti.rsi_signal = 'Overbought' AND ns.trading_signal = 'Buy' THEN 'Bullish Strong'
            WHEN ti.macd_signal = 'Bearish' AND ti.rsi_signal = 'Oversold' AND ns.trading_signal = 'Sell' THEN 'Bearish Strong'
            WHEN ti.macd_signal = 'Bullish' OR ti.rsi_signal = 'Overbought' OR ns.trading_signal = 'Buy' THEN 'Bullish'
            WHEN ti.macd_signal = 'Bearish' OR ti.rsi_signal = 'Oversold' OR ns.trading_signal = 'Sell' THEN 'Bearish'
            ELSE 'Neutral'
        END AS final_signal

    FROM stock_data sd
    LEFT JOIN technical_indicators ti ON sd.symbol = ti.symbol AND sd.date = ti.date
    LEFT JOIN news_sentiment ns ON sd.symbol = ns.symbol AND sd.date = ns.date
    LEFT JOIN lstm_data ld ON sd.symbol = ld.symbol AND sd.date = ld.prediction_date
    WHERE sd.date >= CURRENT_DATE - INTERVAL '90 day'
)

SELECT * FROM combined_data