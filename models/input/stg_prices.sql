{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT 
    SYMBOL,
    DATE AS DT,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME
FROM {{ source('stocks', 'stock_price') }}