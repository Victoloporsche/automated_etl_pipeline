WITH stg_shopify AS (
    SELECT *
    FROM `atomic-sled-453015-g3`.`order_ad_data`.`stg_shopify_orders`  -- Reference to the Shopify staging table
),

first_orders AS (
    -- Identify the first order for each customer
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM stg_shopify
    WHERE status NOT IN ('canceled')  -- Exclude canceled orders
    GROUP BY customer_id
),

discount_performance AS (
    -- Calculate performance metrics for each discount code
    SELECT
        s.order_date,
        s.discount_code,
        COUNT(DISTINCT s.customer_id) AS new_customers_using_discount,
        ROUND(SUM(s.total_price_eur), 2) AS total_revenue_from_discount
    FROM stg_shopify s
    JOIN first_orders f
        ON s.customer_id = f.customer_id
        AND s.order_date = f.first_order_date  -- Only consider the first order
    WHERE s.status NOT IN ('canceled')  -- Exclude canceled orders
    GROUP BY s.order_date, s.discount_code
)

SELECT
    d.order_date,
    d.discount_code,
    d.new_customers_using_discount,
    d.total_revenue_from_discount
FROM discount_performance d
ORDER BY d.order_date, d.discount_code