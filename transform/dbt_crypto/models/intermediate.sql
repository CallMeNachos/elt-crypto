{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'datetime',
            'data_type': 'datetime',
            'granularity': 'hour'
        }
    )
}}

SELECT
    DATE_TRUNC('sec', epoch_ms(epoch)) AS datetime,
    prices,
    market_cap,
    total_volume
FROM
    {{ ref("stg_prices") }}
INNER JOIN
    {{ ref("stg_market_cap") }}
USING(epoch)
INNER JOIN
    {{ ref("stg_total_volumes") }}
USING(epoch)