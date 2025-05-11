
  
    

  create  table "airflow"."public_analytics"."technical_indicators_rsi__dbt_tmp"
  
  
    as
  
  (
    

WITH price_diff AS (
    SELECT
        symbol,
        date,
        close,
        close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS price_change
    FROM "airflow"."public_staging"."stg_daily_stock_summary"
),

-- Menghitung gain dan loss
gains_losses AS (
    SELECT
        symbol,
        date,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
    FROM price_diff
    WHERE price_change IS NOT NULL
),

-- Menghitung average gain dan loss dengan periode 14 hari
avg_gains_losses AS (
    SELECT
        symbol,
        date,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM gains_losses
),

-- Menghitung Relative Strength dan RSI
rs_rsi AS (
    SELECT
        symbol,
        date,
        CASE 
            WHEN avg_loss = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0))))
        END AS rsi
    FROM avg_gains_losses
    WHERE avg_gain IS NOT NULL AND avg_loss IS NOT NULL
)

SELECT
    r.symbol,
    r.date,
    s.close,
    r.rsi,
    CASE
        WHEN r.rsi > 70 THEN 'Overbought'
        WHEN r.rsi < 30 THEN 'Oversold'
        ELSE 'Neutral'
    END AS rsi_signal
FROM rs_rsi r
JOIN "airflow"."public_staging"."stg_daily_stock_summary" s ON r.symbol = s.symbol AND r.date = s.date
  );
  