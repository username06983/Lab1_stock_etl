{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    SYMBOL,
    DT,
    CLOSE,
    OPEN,
    HIGH,
    LOW,
    VOLUME,
    
    -- Previous day's close
    LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DT) AS PREV_CLOSE,
    
    -- Daily price change (absolute)
    ROUND(
        CLOSE - LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DT),
        2
    ) AS DAILY_CHANGE,
    
    -- Daily price change (percentage)
    ROUND(
        ((CLOSE - LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DT)) / 
         NULLIF(LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DT), 0)) * 100,
        2
    ) AS DAILY_CHANGE_PCT,
    
    -- Price direction
    CASE 
        WHEN CLOSE > LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DT) THEN 'UP'
        WHEN CLOSE < LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DT) THEN 'DOWN'
        ELSE 'FLAT'
    END AS DIRECTION,
    
    -- 7-day cumulative change
    ROUND(
        CLOSE - LAG(CLOSE, 7) OVER (PARTITION BY SYMBOL ORDER BY DT),
        2
    ) AS CHANGE_7D

FROM {{ ref('stg_prices') }}
ORDER BY SYMBOL, DT