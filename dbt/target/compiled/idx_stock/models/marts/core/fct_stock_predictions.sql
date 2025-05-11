

WITH predictions AS (
    SELECT 
        symbol,
        prediction_date,
        predicted_close,
        actual_close,
        prediction_error,
        error_percentage,
        created_at
    FROM "airflow"."public_staging"."stg_stock_predictions"
),

stock_data AS (
    SELECT 
        symbol,
        date,
        close,
        volume,
        percent_change
    FROM "airflow"."public_core"."fct_daily_stock_metrics"
),

lstm_processed AS (
    SELECT
        p.symbol,
        p.prediction_date,
        p.predicted_close,
        p.actual_close,
        p.prediction_error,
        p.error_percentage,
        p.created_at,
        s.close as previous_close,
        s.volume,
        s.percent_change,
        CASE
            WHEN p.predicted_close > s.close THEN 'Bullish'
            WHEN p.predicted_close < s.close THEN 'Bearish'
            ELSE 'Neutral'
        END as prediction_direction,
        CASE
            WHEN p.actual_close IS NOT NULL AND 
                 ((p.predicted_close > s.close AND p.actual_close > s.close) OR
                  (p.predicted_close < s.close AND p.actual_close < s.close))
            THEN true
            WHEN p.actual_close IS NOT NULL
            THEN false
            ELSE NULL
        END as direction_correct
    FROM predictions p
    LEFT JOIN stock_data s 
        ON p.symbol = s.symbol 
        AND s.date = p.prediction_date - INTERVAL '1 day'
)

SELECT * FROM lstm_processed