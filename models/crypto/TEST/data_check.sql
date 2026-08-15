SELECT 
    trade_hour,
    crypto_name,
    COUNT(*) AS row_count
FROM {{ ref('fct_crypto_hourly_metrics') }}
GROUP BY trade_hour, crypto_name
HAVING COUNT(*) > 1