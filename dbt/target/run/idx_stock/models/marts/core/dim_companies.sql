
  
    

  create  table "airflow"."public_core"."dim_companies__dbt_tmp"
  
  
    as
  
  (
    WITH ranked_companies AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rank
    FROM "airflow"."public_staging"."stg_daily_stock_summary"
)

SELECT 
    symbol,
    name,
    listed_shares,
    tradable_shares,
    date as last_updated_date
FROM ranked_companies
WHERE rank = 1
  );
  