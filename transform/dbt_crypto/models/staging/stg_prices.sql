{{ config(materialized='table') }}

SELECT
    DATE_TRUNC('sec', epoch) AS datetime,
FROM
    {{ source("crypto", "prices") }}