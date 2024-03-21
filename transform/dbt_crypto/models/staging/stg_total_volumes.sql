
SELECT
    epoch,
    total_volume
FROM
    {{ source("crypto", "raw_total_volumes") }}