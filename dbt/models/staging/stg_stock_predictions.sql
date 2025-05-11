-- models/staging/stg_stock_predictions.sql
with raw as (
  select 
    symbol,
    prediction_date,
    predicted_close,
    actual_close,
    prediction_error,
    error_percentage,
    created_at
  from {{ source('raw', 'stock_predictions') }}
)

select * from raw