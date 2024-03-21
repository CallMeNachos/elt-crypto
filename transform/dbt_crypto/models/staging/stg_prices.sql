
SELECT
    epoch,
    price
FROM
    {{ source("crypto", "raw_prices") }}