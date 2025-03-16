

  create or replace view `atomic-sled-453015-g3`.`order_ad_data`.`fact_customer_dimension`
  OPTIONS()
  as WITH stg_shopify AS (
    SELECT *
    FROM `atomic-sled-453015-g3`.`order_ad_data`.`stg_shopify_orders`  -- Reference to the Shopify staging table
),

first_orders AS (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM stg_shopify
    WHERE status NOT IN ('canceled')  -- Exclude canceled orders
    GROUP BY customer_id
),

customer_data AS (
    SELECT
        s.customer_id,
        f.first_order_date,
        COUNT(DISTINCT s.order_id) AS total_orders,
        ROUND(SUM(s.total_price_eur), 2) AS total_spend_eur,
        COUNT(DISTINCT s.discount_code) AS distinct_discounts_used
    FROM stg_shopify s
    JOIN first_orders f
        ON s.customer_id = f.customer_id
    WHERE s.status NOT IN ('canceled')  -- Only include valid orders
    GROUP BY s.customer_id, f.first_order_date
)

SELECT
    customer_id,
    first_order_date,
    total_orders,
    total_spend_eur,
    distinct_discounts_used
FROM customer_data
ORDER BY customer_id;

