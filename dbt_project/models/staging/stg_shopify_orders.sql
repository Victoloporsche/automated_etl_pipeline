{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE) AS order_date,
    COALESCE(CAST(total_price_eur AS FLOAT64), 0.0) AS total_price_eur,
    COALESCE(CAST(discount_code AS STRING), 'None') AS discount_code,
    COALESCE(CAST(status AS STRING), 'None') AS status
FROM {{ source('order_ad_data', 'clean_shopify_orders') }}
WHERE total_price_eur > 0

