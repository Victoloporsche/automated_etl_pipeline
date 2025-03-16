

SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    SUM(total_amount_eur) AS lifetime_value
FROM `atomic-sled-453015-g3`.`order_ad_data`.`fact_daily_new_customer_orders`
GROUP BY customer_id