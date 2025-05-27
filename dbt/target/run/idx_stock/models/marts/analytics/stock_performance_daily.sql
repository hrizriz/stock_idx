
  
    

  create  table "airflow"."public_analytics"."stock_performance_daily__dbt_tmp"
  
  
    as
  
  (
    

WITH latest_date AS (
    SELECT MAX(date) as latest_day
    FROM "airflow"."public_staging"."stg_daily_stock_summary"
)

SELECT DISTINCT ON (s.symbol)
    s.symbol,
    c.name,
    s.date as day,
    s.prev_close,
    s.close,
    s.volume,
    s.value,
    CASE 
        WHEN s.prev_close > 0 THEN (s.close - s.prev_close) / s.prev_close
        ELSE 0
    END as daily_return
FROM "airflow"."public_staging"."stg_daily_stock_summary" s
JOIN latest_date l ON s.date = l.latest_day
JOIN "airflow"."public_core"."dim_companies" c ON s.symbol = c.symbol
ORDER BY s.symbol, s.date DESC
  );
  