
  
    

    create or replace table `atomic-sled-453015-g3`.`order_ad_data`.`fact_discount_performance`
      
    
    

    OPTIONS()
    as (
      

SELECT
    discount_code,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_amount_eur) AS total_revenue
FROM `atomic-sled-453015-g3`.`order_ad_data`.`fact_daily_new_customer_orders`
WHERE discount_code IS NOT NULL
GROUP BY discount_code
ORDER BY total_revenue DESC
    );
  