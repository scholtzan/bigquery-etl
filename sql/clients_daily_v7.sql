CREATE TEMP FUNCTION
  udf_active_addons_mode_last(nested_list ANY TYPE) AS (STRUCT(ARRAY(
      SELECT
        STRUCT(udf_json_mode_last(ARRAY_AGG(element)) AS element)
      FROM
        UNNEST(nested_list),
        UNNEST(list)
      GROUP BY
        element.addon_id) AS key_value));
  --
CREATE TEMP FUNCTION
  udf_geo_struct(country STRING,
    city STRING,
    geo_subdivision1 STRING,
    geo_subdivision2 STRING) AS ( IF(country != '??',
      STRUCT(country,
        NULLIF(city,
          '??') AS city,
        NULLIF(geo_subdivision1,
          '??') AS geo_subdivision1,
        NULLIF(geo_subdivision2,
          '??') AS geo_subdivision2),
      NULL));
  --
CREATE TEMP FUNCTION
  udf_json_mode_last(list ANY TYPE) AS ((
    SELECT
      ANY_VALUE(_value)
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS _offset
    GROUP BY
      TO_JSON_STRING(_value)
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1));
  --
CREATE TEMP FUNCTION
  udf_map_mode_last(maps ANY TYPE) AS (STRUCT(ARRAY(
      SELECT
        AS STRUCT --
        key,
        udf_mode_last(ARRAY_AGG(value)) AS value
      FROM
        UNNEST(maps),
        UNNEST(key_value)
      GROUP BY
        key) AS key_value));
  --
CREATE TEMP FUNCTION
  udf_map_sum(maps ANY TYPE) AS (STRUCT(ARRAY(
      SELECT
        AS STRUCT key,
        SUM(value) AS value
      FROM
        UNNEST(maps),
        UNNEST(key_value)
      GROUP BY
        key) AS key_value));
  --
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
  --
CREATE TEMP FUNCTION
  udf_null_if_empty_list(list ANY TYPE) AS ( IF(ARRAY_LENGTH(list.list) > 0,
      list,
      NULL) );
  --
CREATE TEMP FUNCTION
  udf_search_counts_sum(search_counts ARRAY<STRUCT<list ARRAY<STRUCT<element STRUCT<engine STRING,
    source STRING,
    count INT64>>>>>) AS ((
    SELECT
      AS STRUCT --
      SUM(element.count) AS search_count_all,
      SUM(IF(element.source = "abouthome",
          element.count,
          0)) AS search_count_abouthome,
      SUM(IF(element.source = "contextmenu",
          element.count,
          0)) AS search_count_contextmenu,
      SUM(IF(element.source = "newtab",
          element.count,
          0)) AS search_count_newtab,
      SUM(IF(element.source = "searchbar",
          element.count,
          0)) AS search_count_searchbar,
      SUM(IF(element.source = "system",
          element.count,
          0)) AS search_count_system,
      SUM(IF(element.source = "urlbar",
          element.count,
          0)) AS search_count_urlbar
    FROM
      UNNEST(search_counts),
      UNNEST(list)
    WHERE
      element.source IN ("abouthome",
        "contextmenu",
        "newtab",
        "searchbar",
        "system",
        "urlbar")));
  --
WITH
  -- normalize client_id and rank by document_id
  numbered_duplicates AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY client_id, submission_date_s3, document_id ORDER BY `timestamp` ASC) AS _n,
    * REPLACE(LOWER(client_id) AS client_id)
  FROM
    main_summary_v4
  WHERE
    submission_date_s3 = @submission_date
    AND client_id IS NOT NULL ),
  -- Deduplicating on document_id is necessary to get valid SUM values.
  deduplicated AS (
  SELECT
    * EXCEPT (_n)
  FROM
    numbered_duplicates
  WHERE
    _n = 1 ),
  -- Aggregate by client_id using windows
  windowed AS (
  SELECT
    ROW_NUMBER() OVER w1_unframed AS _n,
    client_id,
    SUM(aborts_content) OVER w1 AS aborts_content_sum,
    SUM(aborts_gmplugin) OVER w1 AS aborts_gmplugin_sum,
    SUM(aborts_plugin) OVER w1 AS aborts_plugin_sum,
    AVG(active_addons_count) OVER w1 AS active_addons_count_mean,
    udf_active_addons_mode_last(ARRAY_AGG(active_addons) OVER w1) AS active_addons,
    CAST(NULL AS STRING) AS active_experiment_branch, -- deprecated
    CAST(NULL AS STRING) AS active_experiment_id, -- deprecated
    SUM(active_ticks/(3600/5)) OVER w1 AS active_hours_sum,
    udf_mode_last(ARRAY_AGG(addon_compatibility_check_enabled) OVER w1) AS addon_compatibility_check_enabled,
    udf_mode_last(ARRAY_AGG(app_build_id) OVER w1) AS app_build_id,
    udf_mode_last(ARRAY_AGG(app_display_version) OVER w1) AS app_display_version,
    udf_mode_last(ARRAY_AGG(app_name) OVER w1) AS app_name,
    udf_mode_last(ARRAY_AGG(app_version) OVER w1) AS app_version,
    udf_json_mode_last(ARRAY_AGG(attribution) OVER w1) AS attribution,
    udf_mode_last(ARRAY_AGG(blocklist_enabled) OVER w1) AS blocklist_enabled,
    udf_mode_last(ARRAY_AGG(channel) OVER w1) AS channel,
    AVG(client_clock_skew) OVER w1 AS client_clock_skew_mean,
    AVG(client_submission_latency) OVER w1 AS client_submission_latency_mean,
    udf_mode_last(ARRAY_AGG(cpu_cores) OVER w1) AS cpu_cores,
    udf_mode_last(ARRAY_AGG(cpu_count) OVER w1) AS cpu_count,
    udf_mode_last(ARRAY_AGG(cpu_family) OVER w1) AS cpu_family,
    udf_mode_last(ARRAY_AGG(cpu_l2_cache_kb) OVER w1) AS cpu_l2_cache_kb,
    udf_mode_last(ARRAY_AGG(cpu_l3_cache_kb) OVER w1) AS cpu_l3_cache_kb,
    udf_mode_last(ARRAY_AGG(cpu_model) OVER w1) AS cpu_model,
    udf_mode_last(ARRAY_AGG(cpu_speed_mhz) OVER w1) AS cpu_speed_mhz,
    udf_mode_last(ARRAY_AGG(cpu_stepping) OVER w1) AS cpu_stepping,
    udf_mode_last(ARRAY_AGG(cpu_vendor) OVER w1) AS cpu_vendor,
    SUM(crashes_detected_content) OVER w1 AS crashes_detected_content_sum,
    SUM(crashes_detected_gmplugin) OVER w1 AS crashes_detected_gmplugin_sum,
    SUM(crashes_detected_plugin) OVER w1 AS crashes_detected_plugin_sum,
    SUM(crash_submit_attempt_content) OVER w1 AS crash_submit_attempt_content_sum,
    SUM(crash_submit_attempt_main) OVER w1 AS crash_submit_attempt_main_sum,
    SUM(crash_submit_attempt_plugin) OVER w1 AS crash_submit_attempt_plugin_sum,
    SUM(crash_submit_success_content) OVER w1 AS crash_submit_success_content_sum,
    SUM(crash_submit_success_main) OVER w1 AS crash_submit_success_main_sum,
    SUM(crash_submit_success_plugin) OVER w1 AS crash_submit_success_plugin_sum,
    udf_mode_last(ARRAY_AGG(default_search_engine) OVER w1) AS default_search_engine,
    udf_mode_last(ARRAY_AGG(default_search_engine_data_load_path) OVER w1) AS default_search_engine_data_load_path,
    udf_mode_last(ARRAY_AGG(default_search_engine_data_name) OVER w1) AS default_search_engine_data_name,
    udf_mode_last(ARRAY_AGG(default_search_engine_data_origin) OVER w1) AS default_search_engine_data_origin,
    udf_mode_last(ARRAY_AGG(default_search_engine_data_submission_url) OVER w1) AS default_search_engine_data_submission_url,
    SUM(devtools_toolbox_opened_count) OVER w1 AS devtools_toolbox_opened_count_sum,
    udf_mode_last(ARRAY_AGG(distribution_id) OVER w1) AS distribution_id,
    udf_mode_last(ARRAY_AGG(e10s_enabled) OVER w1) AS e10s_enabled,
    udf_mode_last(ARRAY_AGG(env_build_arch) OVER w1) AS env_build_arch,
    udf_mode_last(ARRAY_AGG(env_build_id) OVER w1) AS env_build_id,
    udf_mode_last(ARRAY_AGG(env_build_version) OVER w1) AS env_build_version,
    udf_json_mode_last(ARRAY_AGG(udf_null_if_empty_list(environment_settings_intl_accept_languages)) OVER w1) AS environment_settings_intl_accept_languages,
    udf_json_mode_last(ARRAY_AGG(udf_null_if_empty_list(environment_settings_intl_app_locales)) OVER w1) AS environment_settings_intl_app_locales,
    udf_json_mode_last(ARRAY_AGG(udf_null_if_empty_list(environment_settings_intl_available_locales)) OVER w1) AS environment_settings_intl_available_locales,
    udf_json_mode_last(ARRAY_AGG(udf_null_if_empty_list(environment_settings_intl_requested_locales)) OVER w1) AS environment_settings_intl_requested_locales,
    udf_json_mode_last(ARRAY_AGG(udf_null_if_empty_list(environment_settings_intl_system_locales)) OVER w1) AS environment_settings_intl_system_locales,
    udf_json_mode_last(ARRAY_AGG(udf_null_if_empty_list(environment_settings_intl_regional_prefs_locales)) OVER w1) AS environment_settings_intl_regional_prefs_locales,
    udf_map_mode_last(ARRAY_AGG(experiments) OVER w1) AS experiments,
    AVG(first_paint) OVER w1 AS first_paint_mean,
    udf_mode_last(ARRAY_AGG(flash_version) OVER w1) AS flash_version,
    udf_json_mode_last(ARRAY_AGG(udf_geo_struct(country, city, geo_subdivision1, geo_subdivision2)) OVER w1).*,
    udf_mode_last(ARRAY_AGG(gfx_features_advanced_layers_status) OVER w1) AS gfx_features_advanced_layers_status,
    udf_mode_last(ARRAY_AGG(gfx_features_d2d_status) OVER w1) AS gfx_features_d2d_status,
    udf_mode_last(ARRAY_AGG(gfx_features_d3d11_status) OVER w1) AS gfx_features_d3d11_status,
    udf_mode_last(ARRAY_AGG(gfx_features_gpu_process_status) OVER w1) AS gfx_features_gpu_process_status,
    SUM(histogram_parent_devtools_aboutdebugging_opened_count) OVER w1 AS histogram_parent_devtools_aboutdebugging_opened_count_sum,
    SUM(histogram_parent_devtools_animationinspector_opened_count) OVER w1 AS histogram_parent_devtools_animationinspector_opened_count_sum,
    SUM(histogram_parent_devtools_browserconsole_opened_count) OVER w1 AS histogram_parent_devtools_browserconsole_opened_count_sum,
    SUM(histogram_parent_devtools_canvasdebugger_opened_count) OVER w1 AS histogram_parent_devtools_canvasdebugger_opened_count_sum,
    SUM(histogram_parent_devtools_computedview_opened_count) OVER w1 AS histogram_parent_devtools_computedview_opened_count_sum,
    SUM(histogram_parent_devtools_custom_opened_count) OVER w1 AS histogram_parent_devtools_custom_opened_count_sum,
    NULL AS histogram_parent_devtools_developertoolbar_opened_count_sum, -- deprecated
    SUM(histogram_parent_devtools_dom_opened_count) OVER w1 AS histogram_parent_devtools_dom_opened_count_sum,
    SUM(histogram_parent_devtools_eyedropper_opened_count) OVER w1 AS histogram_parent_devtools_eyedropper_opened_count_sum,
    SUM(histogram_parent_devtools_fontinspector_opened_count) OVER w1 AS histogram_parent_devtools_fontinspector_opened_count_sum,
    SUM(histogram_parent_devtools_inspector_opened_count) OVER w1 AS histogram_parent_devtools_inspector_opened_count_sum,
    SUM(histogram_parent_devtools_jsbrowserdebugger_opened_count) OVER w1 AS histogram_parent_devtools_jsbrowserdebugger_opened_count_sum,
    SUM(histogram_parent_devtools_jsdebugger_opened_count) OVER w1 AS histogram_parent_devtools_jsdebugger_opened_count_sum,
    SUM(histogram_parent_devtools_jsprofiler_opened_count) OVER w1 AS histogram_parent_devtools_jsprofiler_opened_count_sum,
    SUM(histogram_parent_devtools_layoutview_opened_count) OVER w1 AS histogram_parent_devtools_layoutview_opened_count_sum,
    SUM(histogram_parent_devtools_memory_opened_count) OVER w1 AS histogram_parent_devtools_memory_opened_count_sum,
    SUM(histogram_parent_devtools_menu_eyedropper_opened_count) OVER w1 AS histogram_parent_devtools_menu_eyedropper_opened_count_sum,
    SUM(histogram_parent_devtools_netmonitor_opened_count) OVER w1 AS histogram_parent_devtools_netmonitor_opened_count_sum,
    SUM(histogram_parent_devtools_options_opened_count) OVER w1 AS histogram_parent_devtools_options_opened_count_sum,
    SUM(histogram_parent_devtools_paintflashing_opened_count) OVER w1 AS histogram_parent_devtools_paintflashing_opened_count_sum,
    SUM(histogram_parent_devtools_picker_eyedropper_opened_count) OVER w1 AS histogram_parent_devtools_picker_eyedropper_opened_count_sum,
    SUM(histogram_parent_devtools_responsive_opened_count) OVER w1 AS histogram_parent_devtools_responsive_opened_count_sum,
    SUM(histogram_parent_devtools_ruleview_opened_count) OVER w1 AS histogram_parent_devtools_ruleview_opened_count_sum,
    SUM(histogram_parent_devtools_scratchpad_opened_count) OVER w1 AS histogram_parent_devtools_scratchpad_opened_count_sum,
    SUM(histogram_parent_devtools_scratchpad_window_opened_count) OVER w1 AS histogram_parent_devtools_scratchpad_window_opened_count_sum,
    SUM(histogram_parent_devtools_shadereditor_opened_count) OVER w1 AS histogram_parent_devtools_shadereditor_opened_count_sum,
    SUM(histogram_parent_devtools_storage_opened_count) OVER w1 AS histogram_parent_devtools_storage_opened_count_sum,
    SUM(histogram_parent_devtools_styleeditor_opened_count) OVER w1 AS histogram_parent_devtools_styleeditor_opened_count_sum,
    SUM(histogram_parent_devtools_webaudioeditor_opened_count) OVER w1 AS histogram_parent_devtools_webaudioeditor_opened_count_sum,
    SUM(histogram_parent_devtools_webconsole_opened_count) OVER w1 AS histogram_parent_devtools_webconsole_opened_count_sum,
    SUM(histogram_parent_devtools_webide_opened_count) OVER w1 AS histogram_parent_devtools_webide_opened_count_sum,
    udf_mode_last(ARRAY_AGG(install_year) OVER w1) AS install_year,
    udf_mode_last(ARRAY_AGG(is_default_browser) OVER w1) AS is_default_browser,
    udf_mode_last(ARRAY_AGG(is_wow64) OVER w1) AS is_wow64,
    udf_mode_last(ARRAY_AGG(locale) OVER w1) AS locale,
    udf_mode_last(ARRAY_AGG(memory_mb) OVER w1) AS memory_mb,
    udf_mode_last(ARRAY_AGG(normalized_channel) OVER w1) AS normalized_channel,
    udf_mode_last(ARRAY_AGG(normalized_os_version) OVER w1) AS normalized_os_version,
    udf_mode_last(ARRAY_AGG(os) OVER w1) AS os,
    udf_mode_last(ARRAY_AGG(os_service_pack_major) OVER w1) AS os_service_pack_major,
    udf_mode_last(ARRAY_AGG(os_service_pack_minor) OVER w1) AS os_service_pack_minor,
    udf_mode_last(ARRAY_AGG(os_version) OVER w1) AS os_version,
    COUNT(*) OVER w1 AS pings_aggregated_by_this_row,
    AVG(places_bookmarks_count) OVER w1 AS places_bookmarks_count_mean,
    AVG(places_pages_count) OVER w1 AS places_pages_count_mean,
    SUM(plugin_hangs) OVER w1 AS plugin_hangs_sum,
    SUM(plugins_infobar_allow) OVER w1 AS plugins_infobar_allow_sum,
    SUM(plugins_infobar_block) OVER w1 AS plugins_infobar_block_sum,
    SUM(plugins_infobar_shown) OVER w1 AS plugins_infobar_shown_sum,
    SUM(plugins_notification_shown) OVER w1 AS plugins_notification_shown_sum,
    udf_mode_last(ARRAY_AGG(previous_build_id) OVER w1) AS previous_build_id,
    UNIX_DATE(DATE(SAFE.TIMESTAMP(subsession_start_date))) - profile_creation_date AS profile_age_in_days,
    SAFE.DATE_FROM_UNIX_DATE(profile_creation_date) AS profile_creation_date,
    SUM(push_api_notify) OVER w1 AS push_api_notify_sum,
    udf_mode_last(ARRAY_AGG(sample_id) OVER w1) AS sample_id,
    udf_mode_last(ARRAY_AGG(sandbox_effective_content_process_level) OVER w1) AS sandbox_effective_content_process_level,
    SUM(scalar_parent_webrtc_nicer_stun_retransmits + scalar_content_webrtc_nicer_stun_retransmits) OVER w1 AS scalar_combined_webrtc_nicer_stun_retransmits_sum,
    SUM(scalar_parent_webrtc_nicer_turn_401s + scalar_content_webrtc_nicer_turn_401s) OVER w1 AS scalar_combined_webrtc_nicer_turn_401s_sum,
    SUM(scalar_parent_webrtc_nicer_turn_403s + scalar_content_webrtc_nicer_turn_403s) OVER w1 AS scalar_combined_webrtc_nicer_turn_403s_sum,
    SUM(scalar_parent_webrtc_nicer_turn_438s + scalar_content_webrtc_nicer_turn_438s) OVER w1 AS scalar_combined_webrtc_nicer_turn_438s_sum,
    SUM(scalar_content_navigator_storage_estimate_count) OVER w1 AS scalar_content_navigator_storage_estimate_count_sum,
    SUM(scalar_content_navigator_storage_persist_count) OVER w1 AS scalar_content_navigator_storage_persist_count_sum,
    udf_mode_last(ARRAY_AGG(scalar_parent_aushelper_websense_reg_version) OVER w1) AS scalar_parent_aushelper_websense_reg_version,
    MAX(scalar_parent_browser_engagement_max_concurrent_tab_count) OVER w1 AS scalar_parent_browser_engagement_max_concurrent_tab_count_max,
    MAX(scalar_parent_browser_engagement_max_concurrent_window_count) OVER w1 AS scalar_parent_browser_engagement_max_concurrent_window_count_max,
    SUM(scalar_parent_browser_engagement_tab_open_event_count) OVER w1 AS scalar_parent_browser_engagement_tab_open_event_count_sum,
    SUM(scalar_parent_browser_engagement_total_uri_count) OVER w1 AS scalar_parent_browser_engagement_total_uri_count_sum,
    SUM(scalar_parent_browser_engagement_unfiltered_uri_count) OVER w1 AS scalar_parent_browser_engagement_unfiltered_uri_count_sum,
    MAX(scalar_parent_browser_engagement_unique_domains_count) OVER w1 AS scalar_parent_browser_engagement_unique_domains_count_max,
    AVG(scalar_parent_browser_engagement_unique_domains_count) OVER w1 AS scalar_parent_browser_engagement_unique_domains_count_mean,
    SUM(scalar_parent_browser_engagement_window_open_event_count) OVER w1 AS scalar_parent_browser_engagement_window_open_event_count_sum,
    SUM(scalar_parent_devtools_accessibility_node_inspected_count) OVER w1 AS scalar_parent_devtools_accessibility_node_inspected_count_sum,
    SUM(scalar_parent_devtools_accessibility_opened_count) OVER w1 AS scalar_parent_devtools_accessibility_opened_count_sum,
    SUM(scalar_parent_devtools_accessibility_picker_used_count) OVER w1 AS scalar_parent_devtools_accessibility_picker_used_count_sum,
    udf_map_sum(ARRAY_AGG(scalar_parent_devtools_accessibility_select_accessible_for_node) OVER w1) AS scalar_parent_devtools_accessibility_select_accessible_for_node_sum,
    SUM(scalar_parent_devtools_accessibility_service_enabled_count) OVER w1 AS scalar_parent_devtools_accessibility_service_enabled_count_sum,
    SUM(scalar_parent_devtools_copy_full_css_selector_opened) OVER w1 AS scalar_parent_devtools_copy_full_css_selector_opened_sum,
    SUM(scalar_parent_devtools_copy_unique_css_selector_opened) OVER w1 AS scalar_parent_devtools_copy_unique_css_selector_opened_sum,
    SUM(scalar_parent_devtools_toolbar_eyedropper_opened) OVER w1 AS scalar_parent_devtools_toolbar_eyedropper_opened_sum,
    NULL AS scalar_parent_dom_contentprocess_troubled_due_to_memory_sum, -- deprecated
    SUM(scalar_parent_navigator_storage_estimate_count) OVER w1 AS scalar_parent_navigator_storage_estimate_count_sum,
    SUM(scalar_parent_navigator_storage_persist_count) OVER w1 AS scalar_parent_navigator_storage_persist_count_sum,
    SUM(scalar_parent_storage_sync_api_usage_extensions_using) OVER w1 AS scalar_parent_storage_sync_api_usage_extensions_using_sum,
    udf_mode_last(ARRAY_AGG(search_cohort) OVER w1) AS search_cohort,
    udf_search_counts_sum(ARRAY_AGG(search_counts) OVER w1).*,
    AVG(session_restored) OVER w1 AS session_restored_mean,
    COUNTIF(subsession_counter = 1) OVER w1 AS sessions_started_on_this_day,
    SUM(shutdown_kill) OVER w1 AS shutdown_kill_sum,
    SUM(subsession_length/NUMERIC '3600') OVER w1 AS subsession_hours_sum,
    SUM(ssl_handshake_result_failure) OVER w1 AS ssl_handshake_result_failure_sum,
    SUM(ssl_handshake_result_success) OVER w1 AS ssl_handshake_result_success_sum,
    udf_mode_last(ARRAY_AGG(sync_configured) OVER w1) AS sync_configured,
    AVG(sync_count_desktop) OVER w1 AS sync_count_desktop_mean,
    AVG(sync_count_mobile) OVER w1 AS sync_count_mobile_mean,
    SUM(sync_count_desktop) OVER w1 AS sync_count_desktop_sum,
    SUM(sync_count_mobile) OVER w1 AS sync_count_mobile_sum,
    udf_mode_last(ARRAY_AGG(telemetry_enabled) OVER w1) AS telemetry_enabled,
    udf_mode_last(ARRAY_AGG(timezone_offset) OVER w1) AS timezone_offset,
    CAST(NULL AS NUMERIC) AS total_hours_sum,
    udf_mode_last(ARRAY_AGG(update_auto_download) OVER w1) AS update_auto_download,
    udf_mode_last(ARRAY_AGG(update_channel) OVER w1) AS update_channel,
    udf_mode_last(ARRAY_AGG(update_enabled) OVER w1) AS update_enabled,
    udf_mode_last(ARRAY_AGG(vendor) OVER w1) AS vendor,
    SUM(web_notification_shown) OVER w1 AS web_notification_shown_sum,
    udf_mode_last(ARRAY_AGG(windows_build_number) OVER w1) AS windows_build_number,
    udf_mode_last(ARRAY_AGG(windows_ubr) OVER w1) AS windows_ubr
  FROM
    deduplicated
  WINDOW
    -- Aggregations require a framed window
    w1 AS (
    PARTITION BY
      client_id,
      submission_date_s3
    ORDER BY
      `timestamp` ASC ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING),
    -- ROW_NUMBER does not work on a framed window
    w1_unframed AS (
    PARTITION BY
      client_id,
      submission_date_s3
    ORDER BY
      `timestamp` ASC) )
SELECT
  @submission_date AS submission_date,
  CURRENT_DATETIME() AS generated_time,
  * EXCEPT(_n)
FROM
  windowed
WHERE
  _n = 1
