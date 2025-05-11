

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

weekly_aggregates AS (
    SELECT
        symbol,
        DATE_TRUNC('week', date) as week_starting,
        MIN(date) as first_date_of_week,
        MAX(date) as last_date_of_week,
        AVG(daily_return) as avg_daily_return,
        SUM(volume) as weekly_volume
    FROM daily_metrics
    GROUP BY symbol, DATE_TRUNC('week', date)
),

latest_week AS (
    SELECT MAX(week_starting) AS latest_week_start FROM weekly_aggregates
),

weekly_prices AS (
    SELECT
        wa.symbol,
        wa.week_starting,
        wa.avg_daily_return,
        wa.weekly_volume,
        open_prices.close as week_open,
        close_prices.close as week_close
    FROM weekly_aggregates wa
    JOIN latest_week lw ON wa.week_starting = lw.latest_week_start
    LEFT JOIN daily_metrics open_prices
        ON wa.symbol = open_prices.symbol AND wa.first_date_of_week = open_prices.date
    LEFT JOIN daily_metrics close_prices
        ON wa.symbol = close_prices.symbol AND wa.last_date_of_week = close_prices.date
)

SELECT DISTINCT ON (wp.symbol)
    wp.symbol,
    c.name,
    wp.week_starting,
    wp.avg_daily_return,
    wp.weekly_volume,
    wp.week_open,
    wp.week_close,
    CASE
        WHEN wp.week_open > 0 THEN (wp.week_close - wp.week_open) / wp.week_open
        ELSE 0
    END as weekly_return
FROM weekly_prices wp
JOIN "airflow"."public_core"."dim_companies" c ON wp.symbol = c.symbol
ORDER BY wp.symbol, wp.week_starting DESC