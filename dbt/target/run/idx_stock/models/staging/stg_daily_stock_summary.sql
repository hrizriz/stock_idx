
  create view "airflow"."public_staging"."stg_daily_stock_summary__dbt_tmp"
    
    
  as (
    

SELECT
    symbol,
    name,
    date,
    prev_close,
    open_price,
    high,
    low,
    close,
    change,
    volume,
    value,
    frequency,
    index_individual,
    weight_for_index,
    foreign_buy,
    foreign_sell,
    offer,
    offer_volume,
    bid,
    bid_volume,
    listed_shares,
    tradable_shares,
    non_regular_volume,
    non_regular_value,
    non_regular_frequency,
    upload_file
FROM public.daily_stock_summary
  );