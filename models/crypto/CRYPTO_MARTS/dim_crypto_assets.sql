with combined_trades as (
    select * from {{ ref('int_crypto_trades_combined') }}
)

select
    crypto_name,
    min(event_timestamp) as first_trade_timestamp,
    max(event_timestamp) as latest_trade_timestamp,
    count(event_id) as total_lifetime_ticks,
    min(price_usd) as all_time_low_price_usd,
    max(price_usd) as all_time_high_price_usd,
    round(avg(price_usd), 2) as lifetime_avg_price_usd,
    count(case when data_source = 'HISTORICAL' then 1 end) as historical_lifetime_ticks,
    count(case when data_source = 'STREAMING' then 1 end) as streaming_lifetime_tick
from combined_trades
group by crypto_name