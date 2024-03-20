{{ config(materialized='table') }}

WITH T1 AS (

SELECT
    DATE_TRUNC('sec', epoch) AS datetime,
    MIN(price) OVER(PARTITION BY DATE_TRUNC('day', epoch)) AS low,
    MAX(price) OVER(PARTITION BY DATE_TRUNC('day', epoch)) AS high,
    ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('day', epoch) ORDER BY timestamp asc) = 1 AS open_rank,
    ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('day', epoch) ORDER BY timestamp desc) = 1 AS close_rank,
    price
FROM
    prices

),
T2 AS (
SELECT
    day,
    MIN(timestamp) AS time_open,
    MAX(timestamp) AS time_close,
    MIN(price) AS low,
    MAX(price) AS high
FROM T1
GROUP BY day
)
