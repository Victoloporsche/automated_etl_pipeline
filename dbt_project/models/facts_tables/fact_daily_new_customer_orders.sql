{{ config(
    materialized='incremental',
    on_schema_change='sync'
) }}

WITH stg_shopify AS (
    SELECT *
    FROM {{ ref('stg_shopify_orders') }}
),

first_orders AS (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM stg_shopify
    WHERE status NOT IN ('canceled')  -- Exclude canceled orders
    GROUP BY customer_id
)

SELECT
    s.order_date,
    COUNT(DISTINCT s.customer_id) AS new_customers_count,
    ROUND(SUM(s.total_price_eur), 2) AS total_revenue_eur  -- Round to 2 decimal places
FROM stg_shopify s
JOIN first_orders f
    ON s.customer_id = f.customer_id
    AND s.order_date = f.first_order_date
WHERE s.status NOT IN ('canceled')  -- Ensure valid orders
{% if is_incremental() %}
    AND s.order_date > (SELECT MAX(order_date) FROM {{ this }})  -- Incremental logic
{% endif %}
GROUP BY s.order_date
ORDER BY s.order_date
