CREATE TEMP FUNCTION
  udf_mode_last(list ANY TYPE) AS ((
    SELECT
      _value
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS
    _offset
    GROUP BY
      _value
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1 ));

WITH
  summary_addon_version AS (
  SELECT
    *,
    udf_mode_last(ARRAY(
    SELECT
      element.version
    FROM
      UNNEST(active_addons.list)
    WHERE
      element.addon_id = 'followonsearch@mozilla.com')) AS addon_version
  FROM
    main_summary_v4
  ),
  augmented AS (
  SELECT
    s.*,
    sc.element.source AS source,
    sc.element.engine AS engine,
    sc.element.count AS count,
    CASE
      WHEN (sc.element.source IN ('searchbar', 'urlbar', 'abouthome', 'newtab', 'contextmenu', 'system', 'activitystream', 'webextension', 'alias') OR sc.element.source IS NULL) THEN 'sap'
      WHEN STARTS_WITH(sc.element.source, 'in-content:sap:')
    OR STARTS_WITH(sc.element.source, 'sap:') THEN 'tagged-sap'
      WHEN STARTS_WITH(sc.element.source, 'in-content:sap-follow-on:') OR STARTS_WITH(sc.element.source,'follow-on:') THEN 'tagged-follow-on'
      WHEN STARTS_WITH(sc.element.source, 'in-content:organic:') THEN 'organic'
      WHEN STARTS_WITH(sc.element.source, 'ad-click:') THEN 'ad-click'
      WHEN STARTS_WITH(sc.element.source, 'search-with-ads:') THEN 'search-with-ads'
      ELSE 'unknown'
    END AS type

  FROM
    summary_addon_version AS s,
    UNNEST(search_counts.list) AS sc
  WHERE
    submission_date_s3 = @submission_date
  UNION ALL
  SELECT
    s.*,
    "ad-click:" as source,
    ac.key as engine,
    ac.value as count,
    "ad-click" as type
  FROM
    summary_addon_version as s,
    UNNEST(s.scalar_parent_browser_search_ad_clicks.key_value) as ac
  WHERE
    s.scalar_parent_browser_search_ad_clicks IS NOT NULL
  UNION ALL
  SELECT
    s.*,
    "search-with-ads:" as source,
    ac.key as engine,
    ac.value as count,
    "search-with-ads" as type
  FROM
    summary_addon_version as s,
    UNNEST(s.scalar_parent_browser_search_with_ads.key_value) as ac
  WHERE
    s.scalar_parent_browser_search_with_ads IS NOT NULL
  ),
  aggregated AS (
  SELECT
    submission_date_s3 AS submission_date,
    addon_version,
    app_version,
    country,
    distribution_id,
    locale,
    search_cohort,
    engine,
    source,
    SUM(count) AS count,
    default_search_engine,
    type
  FROM
    augmented
  WHERE
    count < 10000
    AND engine IS NOT NULL
  GROUP BY
    submission_date_s3,
    addon_version,
    app_version,
    country,
    distribution_id,
    engine,
    locale,
    search_cohort,
    source,
    default_search_engine,
    type)
SELECT
  submission_date,
  addon_version,
  app_version,
  country,
  distribution_id,
  engine,
  locale,
  search_cohort,
  source,
  default_search_engine,
  NULLIF(SUM(IF(type = 'organic', count, 0)), 0) AS organic,
  NULLIF(SUM(IF(type = 'tagged-sap', count, 0)), 0) AS tagged_sap,
  NULLIF(SUM(IF(type = 'tagged-follow-on', count, 0)), 0) AS tagged_follow_on,
  NULLIF(SUM(IF(type = 'sap', count, 0)), 0) AS sap,
  NULLIF(SUM(IF(type = 'ad-click', count, 0)), 0) AS ad_click,
  NULLIF(SUM(IF(type = 'search-with-ads', count, 0)), 0) AS search_with_ads,
  NULLIF(SUM(IF(type = 'unknown', count, 0)), 0) AS unknown
FROM
  aggregated
GROUP BY
  submission_date,
  addon_version,
  app_version,
  country,
  distribution_id,
  locale,
  search_cohort,
  engine,
  source,
  default_search_engine
