with stream_data as(
    SELECT * FROM {{ source("Crypto_project", "streaming_crypto_data")}}
),

clean_stg as(
    SELECT
    CAST(SequenceNumber as string) as sequence_number,
    CAST(Offset AS STRING) as stream_offset,
    to_timestamp(EnqueuedTimeUtc, 'M/d/yyyy h:mm:ss a') AS enqueued_timestamp,
    element_at(map_Keys(from_json(trade_json, 'MAP<String, STRING>')), 1) AS crypto_name,
    cast(get_json_object(trade_json, '$.bitcoin.usd') AS DOUBLE) AS price_usd,
    cast(trade_json AS STRING) AS raw_trade_json
    FROM stream_data
)

SELECT * FROM clean_stg