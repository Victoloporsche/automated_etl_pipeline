

SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE) AS order_date,
    total_price_eur,
    discount_code,
    status
FROM `atomic-sled-453015-g3`.`order_ad_data`.`clean_shopify_orders`  -- ✅ Matches `sources.yml`
WHERE total_price_eur > 0