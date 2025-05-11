
  
    

  create  table "airflow"."public_analytics"."news_sentiment_analysis__dbt_tmp"
  
  
    as
  
  (
    

WITH news_sentiment AS (
    SELECT
        ticker AS symbol,
        date,
        avg_sentiment,
        news_count,
        positive_count,
        negative_count,
        neutral_count,
        -- Hitung positive_percentage
        CASE 
            WHEN news_count > 0 THEN 
                (positive_count::float / NULLIF(news_count, 0)) * 100
            ELSE 0
        END AS positive_percentage,
        -- Hitung negative_percentage
        CASE 
            WHEN news_count > 0 THEN 
                (negative_count::float / NULLIF(news_count, 0)) * 100
            ELSE 0
        END AS negative_percentage,
        -- Flag untuk data yang memiliki jumlah berita yang cukup untuk analisis
        CASE 
            WHEN news_count >= 2 THEN true
            ELSE false
        END AS has_sufficient_data
    FROM public.detik_ticker_sentiment
    WHERE date >= CURRENT_DATE - INTERVAL '90 day'  -- Ambil data 90 hari terakhir untuk cakupan lebih luas
),

stock_data AS (
    -- Ambil data harga saham dari daily_stock_metrics
    SELECT
        symbol,
        date,
        close,
        percent_change
    FROM "airflow"."public_analytics"."daily_stock_metrics"
    WHERE date >= CURRENT_DATE - INTERVAL '90 day'
)

SELECT
    -- Data saham
    COALESCE(s.symbol, d.symbol) AS symbol,  -- Gunakan symbol dari salah satu sumber
    c.name,
    COALESCE(s.date, d.date) AS date,        -- Gunakan tanggal dari salah satu sumber
    s.close,
    s.percent_change,
    
    -- Data sentimen
    d.avg_sentiment,
    d.news_count,
    d.positive_count,
    d.negative_count,
    d.neutral_count,
    d.positive_percentage,
    d.negative_percentage,
    d.has_sufficient_data,
    
    -- Korelasi sentimen-harga: hanya valid jika keduanya ada
    CASE
        WHEN d.has_sufficient_data = true AND s.percent_change IS NOT NULL THEN
            CASE
                WHEN d.avg_sentiment > 0.25 AND s.percent_change > 1 THEN 'Positive Alignment'
                WHEN d.avg_sentiment < -0.25 AND s.percent_change < -1 THEN 'Negative Alignment'
                WHEN d.avg_sentiment > 0.25 AND s.percent_change < -1 THEN 'Sentiment-Price Divergence (Bearish)'
                WHEN d.avg_sentiment < -0.25 AND s.percent_change > 1 THEN 'Sentiment-Price Divergence (Bullish)'
                ELSE 'Neutral/No Strong Signal'
            END
        ELSE 'Insufficient Data'
    END AS sentiment_price_signal,
    
    -- Trading signal berdasarkan sentimen: hanya valid jika data sentimen cukup
    CASE
        WHEN d.has_sufficient_data = true THEN
            CASE
                WHEN d.avg_sentiment > 0.4 AND d.news_count >= 3 THEN 'Strong Buy Signal'
                WHEN d.avg_sentiment > 0.2 AND d.news_count >= 2 THEN 'Buy Signal'
                WHEN d.avg_sentiment < -0.4 AND d.news_count >= 3 THEN 'Strong Sell Signal'
                WHEN d.avg_sentiment < -0.2 AND d.news_count >= 2 THEN 'Sell Signal'
                ELSE 'Hold/No Signal'
            END
        ELSE 'Insufficient Data'
    END AS trading_signal,
    
    -- Metadata
    CURRENT_TIMESTAMP AS generated_at
FROM stock_data s
FULL OUTER JOIN news_sentiment d 
   ON s.symbol = d.symbol AND s.date = d.date  -- FULL OUTER JOIN untuk mendapatkan semua data
LEFT JOIN "airflow"."public_core"."dim_companies" c 
   ON COALESCE(s.symbol, d.symbol) = c.symbol  -- Gunakan symbol dari salah satu tabel
WHERE 
   -- Filter untuk mendapatkan data dalam jendela waktu yang diinginkan
   (s.date >= CURRENT_DATE - INTERVAL '30 day' OR d.date >= CURRENT_DATE - INTERVAL '30 day')
   -- Exclude data yang tidak memiliki salah satu dari dua input utama
   AND (s.symbol IS NOT NULL OR d.symbol IS NOT NULL)
ORDER BY 
   -- Urutkan berdasarkan tanggal (terbaru lebih dulu) dan symbol
   COALESCE(s.date, d.date) DESC, 
   COALESCE(s.symbol, d.symbol)
  );
  