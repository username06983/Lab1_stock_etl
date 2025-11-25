{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    SYMBOL,
    DT,
    CLOSE,
    VOLUME,
    OPEN,
    HIGH,
    LOW,
    
    -- 7-day moving average
    ROUND(
        AVG(CLOSE) OVER (
            PARTITION BY SYMBOL 
            ORDER BY DT 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS MA_7,
    
    -- 30-day moving average
    ROUND(
        AVG(CLOSE) OVER (
            PARTITION BY SYMBOL 
            ORDER BY DT 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS MA_30,
    
    -- Trend signal
    CASE 
        WHEN CLOSE > AVG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DT ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
        THEN 'ABOVE_MA'
        ELSE 'BELOW_MA'
    END AS TREND_SIGNAL
    
FROM {{ ref('stg_prices') }}
ORDER BY SYMBOL, DT