WITH stg_shopify AS (
    SELECT *
    FROM `atomic-sled-453015-g3`.`order_ad_data`.`stg_shopify_orders`  -- Shopify staging table
),

stg_facebook AS (
    SELECT *
    FROM `atomic-sled-453015-g3`.`order_ad_data`.`stg_facebook_ads`  -- Facebook Ads staging table
),

-- Aggregate Shopify data: Total revenue and total orders per day
shopify_aggregated AS (
    SELECT
        order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(total_price_eur), 2) AS total_revenue_eur,
        COUNT(DISTINCT customer_id) AS new_customers_count
    FROM stg_shopify
    WHERE status NOT IN ('canceled')  -- Exclude canceled orders
    GROUP BY order_date
),

-- Aggregate Facebook Ads data: Impressions, clicks, and spend per campaign per day
facebook_aggregated AS (
    SELECT
        ad_date,
        campaign_id,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        ROUND(SUM(spend_eur), 2) AS total_spend_eur
    FROM stg_facebook
    GROUP BY ad_date, campaign_id
)

-- Combine Shopify and Facebook data for daily performance monitoring
SELECT
    fa.ad_date AS date,
    fa.campaign_id,
    fa.total_impressions,
    fa.total_clicks,
    fa.total_spend_eur,
    COALESCE(sa.total_orders, 0) AS total_orders,
    COALESCE(sa.total_revenue_eur, 0.00) AS total_revenue_eur,
    COALESCE(sa.new_customers_count, 0) AS new_customers_count
FROM facebook_aggregated fa
LEFT JOIN shopify_aggregated sa
    ON fa.ad_date = sa.order_date  -- Match Facebook ad date with Shopify order date
ORDER BY fa.ad_date, fa.campaign_id