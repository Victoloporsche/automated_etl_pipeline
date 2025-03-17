{{ config(materialized='view') }}

SELECT
    campaign_id,
    CAST(date_start AS DATE) AS ad_date,
    COALESCE(CAST(impressions as INTEGER), 0) AS impressions,
    COALESCE(CAST(clicks as INTEGER), 0) AS clicks,
    COALESCE(CAST(spend_eur as FLOAT64), 0.0) AS spend_eur
FROM {{ source('order_ad_data', 'clean_facebook_ads') }}
WHERE spend_eur IS NOT NULL