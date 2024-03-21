import os
import duckdb
import pandas as pd
import plotly.graph_objects as go

abspath = os.path.dirname(os.path.abspath(__file__))
df = pd.read_csv(abspath + "/transform/dbt_crypto/seeds/raw_prices.csv")

result = duckdb.sql("""
    WITH my_table AS (
        SELECT
            DATE_TRUNC('sec', epoch_ms(epoch)) AS datetime,
            DATE_TRUNC('day', epoch_ms(epoch)) AS date,
            price
        FROM
            df
    ),
    
    lh_cte AS (
        SELECT
            datetime,
            date,
            MIN(price) OVER(PARTITION BY date) AS low,
            MAX(price) OVER(PARTITION BY date) AS high,
            price
        FROM
            my_table
    ),
    
    open_cte AS (
    SELECT
        datetime,
        date,
        ROW_NUMBER() OVER(PARTITION BY date ORDER BY datetime ASC) AS open_rank,
        price AS open
    FROM
        my_table
    QUALIFY
        open_rank = 1
    ),
    
    close_cte AS (
    SELECT
        datetime,
        date,
        ROW_NUMBER() OVER(PARTITION BY date ORDER BY datetime DESC) AS close_rank,
        price AS close
    FROM
        my_table
    QUALIFY
        close_rank = 1
    )
    
    SELECT
        lh_cte.datetime,
        lh_cte.date,
        price,
        cast(low AS integer) as low,
        cast(high AS integer) as high,
        cast(open AS integer) as open,
        cast(close AS integer) as close
    FROM
        lh_cte
    LEFT JOIN
        open_cte
    ON 
        lh_cte.date = open_cte.date
    LEFT JOIN
        close_cte
    ON 
        lh_cte.date = close_cte.date
    """).df()

fig = go.Figure(data=[go.Candlestick(x=result['datetime'],
                open=result['open'],
                high=result['high'],
                low=result['low'],
                close=result['close'])])


if __name__ == "__main__":
    fig.show()