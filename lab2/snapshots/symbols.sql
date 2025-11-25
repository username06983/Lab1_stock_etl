{% snapshot snapshot_stock_prices %}

{{
  config(
    target_schema='snapshot',
    unique_key='symbol || dt',
    strategy='check',
    check_cols=['close', 'volume'],
    invalidate_hard_deletes=True
  )
}}

SELECT
    symbol,
    dt,
    open,
    high,
    low,
    close,
    volume,
    current_timestamp() AS updated_at
FROM {{ ref('stg_prices') }}

{% endsnapshot %}

