
SELECT
    epoch,
    market_cap
FROM
    {{ source("crypto", "raw_market_cap") }}