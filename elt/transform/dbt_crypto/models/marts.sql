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



WITH main AS (
     SELECT
        DATE_TRUNC('sec', epoch_ms(epoch)) AS datetime,
        DATE_TRUNC('day', epoch_ms(epoch)) AS date,
        price
    FROM
        {{ ref('intermediate') }}
),

lh_cte AS (
    SELECT
        datetime,
        MIN(price) OVER(PARTITION BY date) AS low,
        MAX(price) OVER(PARTITION BY date) AS high,
        price
    FROM
        {{ ref('intermediate') }}
),

open_cte AS (
    SELECT
        datetime,
        ROW_NUMBER() OVER(PARTITION BY date ORDER BY datetime ASC) AS open_rank,
        price AS open
    FROM
        {{ ref('intermediate') }}
    QUALIFY
        open_rank = 1
),

close_cte AS (
    SELECT
        datetime,
        ROW_NUMBER() OVER(PARTITION BY date ORDER BY datetime DESC) AS close_rank,
        price AS close
    FROM
        {{ ref('intermediate') }}
    QUALIFY
        close_rank = 1
)

SELECT
    datetime,
    price,
    low,
    high,
    open,
    close
FROM
    lh_cte
LEFT JOIN
    open_cte
USING(date)
LEFT JOIN
    close_cte
USING(date)