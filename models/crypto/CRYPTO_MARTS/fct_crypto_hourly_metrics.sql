with combined_trades as (
    select * from {{ ref('int_crypto_trades_combined') }}
),

windowed_ohlc as (
    select
        event_id,
        crypto_name,
        price_usd,
        trade_quantity,
        total_usd_value,
        event_timestamp,
        data_source,
        is_buyer_maker,
        date_trunc('hour', event_timestamp) as trade_hour,

        -- Open Price (First trade of the hour)
        first_value(price_usd) over (
            partition by crypto_name, date_trunc('hour', event_timestamp)
            order by event_timestamp asc
            rows between unbounded preceding and unbounded following
        ) as open_price_usd,

        -- Close Price (Last trade of the hour)
        last_value(price_usd) over (
            partition by crypto_name, date_trunc('hour', event_timestamp)
            order by event_timestamp asc
            rows between unbounded preceding and unbounded following
        ) as close_price_usd

    from combined_trades
),

hourly_aggregates as (
    select
        trade_hour,
        crypto_name,

        -- 1. Financial Indicators (OHLC)
        max(open_price_usd) as open_price_usd,
        max(price_usd) as high_price_usd,
        min(price_usd) as low_price_usd,
        max(close_price_usd) as close_price_usd,
        round(avg(price_usd), 2) as avg_price_usd,

        -- 2. Volume & Quantity Metrics
        round(coalesce(sum(trade_quantity), 0), 6) as total_volume_crypto,
        round(coalesce(sum(total_usd_value), 0), 2) as total_volume_usd,

        -- 3. Activity Counts & Source Breakdown
        count(event_id) as total_ticks_count,
        sum(case when data_source = 'HISTORICAL' then 1 else 0 end) as historical_ticks_count,
        sum(case when data_source = 'STREAMING' then 1 else 0 end) as streaming_ticks_count,

        -- 4. Market Sentiment Metrics
        sum(case when is_buyer_maker = true then 1 else 0 end) as buyer_maker_trades_count,
        sum(case when is_buyer_maker = false then 1 else 0 end) as seller_maker_trades_count

    from windowed_ohlc
    group by trade_hour, crypto_name
)

select
    trade_hour,
    crypto_name,

    -- OHLC Metrics
    open_price_usd,
    high_price_usd,
    low_price_usd,
    close_price_usd,
    avg_price_usd,

    -- Price Volatility & Trend Insights
    {{ round_price('high_price_usd - low_price_usd') }} as hourly_price_spread_usd,
    {{ round_price('close_price_usd - open_price_usd') }} as hourly_price_change_usd,
    {{ calc_price_change_pct('open_price_usd', 'close_price_usd') }} as price_change_pct,

    -- Volumes & Trade Counts
    total_volume_crypto,
    total_volume_usd,
    total_ticks_count,
    historical_ticks_count,
    streaming_ticks_count,
    buyer_maker_trades_count,
    seller_maker_trades_count

from hourly_aggregates
order by trade_hour desc