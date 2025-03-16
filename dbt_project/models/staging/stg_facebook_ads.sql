{{ config(materialized='view') }}

SELECT
    campaign_id,
    CAST(date_start AS DATE) AS ad_date,
    impressions,
    clicks,
    spend_eur
FROM {{ source('order_ad_data', 'clean_facebook_ads') }}  -- ✅ Matches `sources.yml`
WHERE spend_eur IS NOT NULL
