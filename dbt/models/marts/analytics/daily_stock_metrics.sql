{{ config(materialized='table') }}

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
    -- Persentase perubahan
    CASE
        WHEN prev_close IS NOT NULL AND prev_close != 0 
            THEN (close - prev_close) / prev_close * 100
        ELSE NULL
    END AS percent_change,
    
    -- Volatilitas intraday
    CASE
        WHEN low IS NOT NULL AND low != 0
            THEN (high - low) / low * 100
        ELSE NULL
    END AS intraday_volatility,
    
    -- Range harian
    (high - low) AS daily_range,
    
    -- Jarak dari terendah ke tertinggi (%)
    CASE
        WHEN low IS NOT NULL AND low != 0
            THEN (high - low) / low * 100
        ELSE NULL
    END AS low_to_high_percent,
    
    -- Volume metrics
    volume,
    value,
    frequency,
    
    -- Aliran dana asing (net)
    (foreign_buy - foreign_sell) AS foreign_net,
    
    -- Update timestamp
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg_daily_stock_summary') }}