with  staging as (
    select * from {{ source("Crypto_project", "historical_crypto_data")}}
),

clean_stg as(
    select
    cast(trade_id as string) as trade_id,
    cast(price as DOUBLE) as price_usd,
    cast(qty as DOUBLE) as trade_quantity,
    cast(quote_qty as DOUBLE) as total_usd_value,

    --  time milliseconds to Timestamp
    TIMESTAMP_MICROS(cast(time AS BIGINT)) AS trade_timestamp,
    cast(is_buyer_maker as boolean) as is_buyer_maker,
    cast(is_best_match as boolean) AS is_best_match
    from staging
)

SELECT * FROM clean_stg
