{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE) AS order_date,
    total_price_eur,
    discount_code,
    status
FROM {{ source('order_ad_data', 'clean_shopify_orders') }}
WHERE total_price_eur > 0

