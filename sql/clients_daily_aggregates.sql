#standardSQL
SELECT
  submission_date_s3,
  client_id,
  os,
  app_version,
  app_build_id,
  channel,
  ARRAY<STRUCT<metric STRING, agg_type STRING, value FLOAT64, min_bucket INT64, max_bucket INT64, num_buckets INT64>> [
    ('scalar_parent_browser_feeds_livebookmark_count', 'max', max(CAST(scalar_parent_browser_feeds_livebookmark_count AS INT64)), 0, 1000, 50),
    ('scalar_parent_browser_feeds_livebookmark_count', 'avg', avg(CAST(scalar_parent_browser_feeds_livebookmark_count AS INT64)), 0, 1000, 50),
    ('scalar_parent_browser_feeds_livebookmark_count', 'min', min(CAST(scalar_parent_browser_feeds_livebookmark_count AS INT64)), 0, 1000, 50),
    ('scalar_parent_browser_feeds_livebookmark_count', 'sum', sum(CAST(scalar_parent_browser_feeds_livebookmark_count AS INT64)), 0, 1000, 50),
    ('scalar_content_browser_usage_plugin_instantiated', 'max', max(CAST(scalar_content_browser_usage_plugin_instantiated AS INT64)), 0, 1000, 50),
    ('scalar_content_browser_usage_plugin_instantiated', 'avg', avg(CAST(scalar_content_browser_usage_plugin_instantiated AS INT64)), 0, 1000, 50),
    ('scalar_content_browser_usage_plugin_instantiated', 'min', min(CAST(scalar_content_browser_usage_plugin_instantiated AS INT64)), 0, 1000, 50),
    ('scalar_parent_browser_session_restore_number_of_win', 'max', max(CAST(scalar_parent_browser_session_restore_number_of_win AS INT64)), 0, 1000, 50),
    ('scalar_parent_browser_session_restore_number_of_win', 'avg', avg(CAST(scalar_parent_browser_session_restore_number_of_win AS INT64)), 0, 1000, 50),
    ('scalar_parent_browser_session_restore_number_of_win', 'min', min(CAST(scalar_parent_browser_session_restore_number_of_win AS INT64)), 0, 1000, 50)
  ] AS scalar_aggregates
FROM telemetry.main_summary_v4
WHERE submission_date_s3 = '{{ params.submission_date_s3 }}'
GROUP BY
  submission_date_s3,
  client_id,
  os,
  app_version,
  app_build_id,
  channel