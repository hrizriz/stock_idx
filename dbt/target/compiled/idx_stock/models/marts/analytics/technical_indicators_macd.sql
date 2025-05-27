

WITH daily_prices AS (
    SELECT
        symbol,
        date,
        close
    FROM "airflow"."public_staging"."stg_daily_stock_summary"
),

-- Menghitung EMA 12 dan 26 hari
ema_values AS (
    SELECT
        symbol,
        date,
        close,
        -- Simplified EMA calculation using AVG
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS ema_12,
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) AS ema_26
    FROM daily_prices
),

-- Menghitung MACD Line dan Signal Line
macd_values AS (
    SELECT
        symbol,
        date,
        close,
        (ema_12 - ema_26) AS macd_line,
        AVG(ema_12 - ema_26) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) AS signal_line
    FROM ema_values
    WHERE ema_12 IS NOT NULL AND ema_26 IS NOT NULL
)

SELECT
    m.symbol,
    m.date,
    m.close,
    m.macd_line,
    m.signal_line,
    (m.macd_line - m.signal_line) AS macd_histogram,
    CASE
        WHEN m.macd_line > m.signal_line THEN 'Bullish'
        WHEN m.macd_line < m.signal_line THEN 'Bearish'
        ELSE 'Neutral'
    END AS macd_signal
FROM macd_values m
WHERE m.signal_line IS NOT NULL