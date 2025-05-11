
  
    

  create  table "airflow"."public_analytics"."lstm_performance_metrics__dbt_tmp"
  
  
    as
  
  (
    

WITH lstm_predictions AS (
    SELECT * FROM "airflow"."public_core"."fct_stock_predictions"
),

-- Akurasi LSTM per ticker
lstm_accuracy AS (
    SELECT
        symbol,
        COUNT(*) as total_predictions,
        SUM(CASE WHEN actual_close IS NOT NULL THEN 1 ELSE 0 END) as predictions_with_actuals,
        SUM(CASE WHEN direction_correct = true THEN 1 ELSE 0 END) as correct_direction_predictions,
        AVG(error_percentage) as avg_error_percentage,
        CASE
            WHEN SUM(CASE WHEN actual_close IS NOT NULL THEN 1 ELSE 0 END) > 0
            THEN 
                SUM(CASE WHEN direction_correct = true THEN 1 ELSE 0 END)::float / 
                NULLIF(SUM(CASE WHEN actual_close IS NOT NULL THEN 1 ELSE 0 END), 0) * 100
            ELSE 0
        END as direction_accuracy_pct
    FROM lstm_predictions
    WHERE actual_close IS NOT NULL
    GROUP BY symbol
),

-- Latest predictions per symbol
latest_predictions AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        prediction_date,
        predicted_close,
        actual_close,
        error_percentage,
        prediction_direction,
        created_at
    FROM lstm_predictions
    ORDER BY symbol, prediction_date DESC
)

-- Main output
SELECT
    a.symbol,
    a.total_predictions,
    a.predictions_with_actuals,
    a.correct_direction_predictions,
    a.avg_error_percentage,
    a.direction_accuracy_pct,
    lp.prediction_date as latest_prediction_date,
    lp.predicted_close as latest_predicted_close,
    lp.prediction_direction as latest_prediction_direction
FROM lstm_accuracy a
LEFT JOIN latest_predictions lp ON a.symbol = lp.symbol
  );
  