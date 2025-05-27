
  
    

  create  table "airflow"."public_analytics"."stock_performance_monthly__dbt_tmp"
  
  
    as
  
  (
    

WITH daily_metrics AS (
    SELECT 
        symbol,
        date,
        close,
        prev_close,
        volume,
        CASE 
            WHEN prev_close > 0 THEN (close - prev_close) / prev_close 
            ELSE 0
        END as daily_return
    FROM "airflow"."public_staging"."stg_daily_stock_summary"
),

monthly_aggregates AS (
    SELECT
        symbol,
        DATE_TRUNC('month', date) as month_starting,
        MIN(date) as first_date_of_month,
        MAX(date) as last_date_of_month,
        AVG(daily_return) as avg_daily_return,
        SUM(volume) as monthly_volume
    FROM daily_metrics
    GROUP BY symbol, DATE_TRUNC('month', date)
),

latest_month AS (
    SELECT MAX(month_starting) AS latest_month_start FROM monthly_aggregates
),

monthly_prices AS (
    SELECT
        ma.symbol,
        ma.month_starting,
        ma.avg_daily_return,
        ma.monthly_volume,
        open_prices.close as month_open,
        close_prices.close as month_close
    FROM monthly_aggregates ma
    JOIN latest_month lm ON ma.month_starting = lm.latest_month_start
    LEFT JOIN daily_metrics open_prices
        ON ma.symbol = open_prices.symbol AND ma.first_date_of_month = open_prices.date
    LEFT JOIN daily_metrics close_prices
        ON ma.symbol = close_prices.symbol AND ma.last_date_of_month = close_prices.date
)

SELECT DISTINCT ON (mp.symbol)
    mp.symbol,
    c.name,
    mp.month_starting,
    mp.avg_daily_return,
    mp.monthly_volume,
    mp.month_open,
    mp.month_close,
    CASE
        WHEN mp.month_open > 0 THEN (mp.month_close - mp.month_open) / mp.month_open
        ELSE 0
    END as monthly_return
FROM monthly_prices mp
JOIN "airflow"."public_core"."dim_companies" c ON mp.symbol = c.symbol
ORDER BY mp.symbol, mp.month_starting DESC
  );
  