{{ config(materialized='table') }}

SELECT
    epoch,
    price,
    market_cap,
    total_volume
FROM
    {{ ref("prices") }}