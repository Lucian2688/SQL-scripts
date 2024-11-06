-- Combine Facebook and Google Ads data into a unified dataset
WITH facebook_and_google_utm AS (
   SELECT
      fabd.ad_date,
      fabd.url_parameters,
      COALESCE(LOWER(SUBSTRING(fabd.url_parameters, 'utm_campaign=([^#!&]+)')), ' ') AS utm_campaign,
      fa.adset_name,
      fc.campaign_name,
      COALESCE(fabd.spend, 0) AS spend,
      COALESCE(fabd.impressions, 0) AS impressions,
      COALESCE(fabd.reach, 0) AS reach,
      COALESCE(fabd.clicks, 0) AS clicks,
      COALESCE(fabd.leads, 0) AS leads,
      COALESCE(fabd.value, 0) AS value
   FROM facebook_ads_basic_daily fabd
   JOIN facebook_adset fa ON fa.adset_id = fabd.adset_id
   JOIN facebook_campaign fc ON fc.campaign_id = fabd.campaign_id
   UNION
   SELECT
      gabd.ad_date,
      gabd.url_parameters,
      COALESCE(LOWER(SUBSTRING(gabd.url_parameters, 'utm_campaign=([^#!&]+)')), ' ') AS utm_campaign,
      gabd.adset_name,
      gabd.campaign_name,
      COALESCE(gabd.spend, 0) AS spend,
      COALESCE(gabd.impressions, 0) AS impressions,
      COALESCE(gabd.reach, 0) AS reach,
      COALESCE(gabd.clicks, 0) AS clicks,
      COALESCE(gabd.leads, 0) AS leads,
      COALESCE(gabd.value, 0) AS value
   FROM google_ads_basic_daily gabd
),
-- Monthly calculations for each UTM campaign
F_abd_G_ads_month AS (
   SELECT
      DATE_TRUNC('month', ad_date) AS month_ad_date,
      LOWER(
         CASE
            WHEN utm_campaign = 'nan' THEN 'NULL'
            ELSE utm_campaign
         END
      ) AS utm_campaign,
      SUM(spend::numeric) AS total_spend,
      SUM(impressions::numeric) AS total_impressions,
      SUM(reach::numeric) AS total_reach,
      SUM(clicks::numeric) AS total_clicks,
      SUM(leads::numeric) AS total_leads,
      SUM(value::numeric) AS total_value,
      -- Calculate cost per click (CPC)
      CASE
         WHEN SUM(clicks::numeric) = 0 THEN 0
         ELSE ROUND(SUM(spend::numeric) / SUM(clicks::numeric), 2)
      END AS cpc,
      -- Calculate cost per thousand impressions (CPM)
      CASE
         WHEN SUM(impressions::numeric) = 0 THEN 0
         ELSE ROUND((SUM(spend::numeric) / SUM(impressions::numeric)) * 1000, 2)
      END AS cpm,
      -- Calculate click-through rate (CTR)
      CASE
         WHEN SUM(impressions::numeric) = 0 THEN 0
         ELSE ROUND((SUM(clicks::numeric) / SUM(impressions::numeric)) * 100, 2)
      END AS ctr,
      -- Calculate return on marketing investment (ROMI)
      CASE
         WHEN SUM(spend::numeric) = 0 THEN 0
         ELSE ROUND((SUM(value::numeric) - SUM(spend::numeric)) / SUM(spend::numeric) * 100, 2)
      END AS romi
   FROM facebook_and_google_utm
   GROUP BY month_ad_date, utm_campaign
)
-- Final select, organized by month and specific UTM campaign
SELECT
   DISTINCT DATE_TRUNC('month', month_ad_date)::date AS month_ad_date,
   utm_campaign,
   SUM(total_spend) AS total_spend,
   SUM(total_impressions) AS total_impressions,
   SUM(total_clicks) AS total_clicks,
   SUM(total_value) AS total_value,
   SUM(cpc) AS total_cpc,
   SUM(ctr) AS total_ctr,
   ROUND(
      COALESCE(((SUM(ctr) - LAG(SUM(ctr)) OVER (PARTITION BY utm_campaign ORDER BY month_ad_date DESC)) /
      NULLIF(LAG(SUM(ctr)) OVER (PARTITION BY utm_campaign ORDER BY month_ad_date DESC), 0)) * 100, 0)::numeric, 2
   ) AS total_ctr_dif,
   SUM(cpm) AS total_cpm,
   ROUND(
      COALESCE(((SUM(cpm) - LAG(SUM(cpm)) OVER (PARTITION BY utm_campaign ORDER BY month_ad_date DESC)) /
      NULLIF(LAG(SUM(cpm)) OVER (PARTITION BY utm_campaign ORDER BY month_ad_date DESC), 0)) * 100, 0)::numeric, 2
   ) AS total_cpm_dif,
   SUM(romi) AS total_romi,
   ROUND(
      COALESCE(((SUM(romi) - LAG(SUM(romi)) OVER (PARTITION BY utm_campaign ORDER BY month_ad_date DESC)) /
      NULLIF(LAG(SUM(romi)) OVER (PARTITION BY month_ad_date DESC), 0)) * 100, 0)::numeric, 2
   ) AS total_romi_dif
FROM F_abd_G_ads_month
GROUP BY month_ad_date, utm_campaign
HAVING utm_campaign = 'you_might_like'
ORDER BY month_ad_date DESC;