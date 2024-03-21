{{ config(materialized='table') }}

SELECT
    DATE_TRUNC('sec', epoch) AS datetime,
    price
FROM
    {{ source("crypto", "prices") }}