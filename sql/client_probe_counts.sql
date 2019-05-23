CREATE TEMP FUNCTION bucket (val FLOAT64, min_bucket INT64, max_bucket INT64, num_buckets INT64)
RETURNS FLOAT64 AS (
  -- Bucket `value` into a histogram with min_bucket, max_bucket and num_buckets
  (
    SELECT
      max(bucket)
    FROM
      unnest(GENERATE_ARRAY(min_bucket, max_bucket, (max_bucket - min_bucket) / num_buckets)) AS bucket
    WHERE
      val > bucket
  )
);

CREATE TEMP FUNCTION buckets_to_map (buckets ARRAY<FLOAT64>)
RETURNS STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>> AS (
  -- Given an array of values, transform them into a histogram MAP
  -- with the number of each key in the `buckets` array
  (
    SELECT
      STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>(
        ARRAY_AGG(STRUCT<key FLOAT64, value FLOAT64>(bucket, 1.0))
      )
    FROM
      UNNEST(buckets) AS bucket
  )
);

CREATE TEMP FUNCTION dedupe_map_sum (map STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>)
RETURNS STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>> AS (
  -- Given a MAP with duplicate keys, de-duplicates by summing the values of duplicate keys
  (
    WITH summed_counts AS (
      SELECT
        STRUCT<key FLOAT64, value FLOAT64>(e.key, SUM(e.value)) AS record
      FROM
        UNNEST(map.key_value) AS e
      GROUP BY
        e.key
    )
    
    SELECT
      STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>(
        ARRAY_AGG(record)
      )
    FROM
      summed_counts
  )
);

CREATE TEMP FUNCTION fill_buckets(input_map STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>, min_bucket INT64, max_bucket INT64, num_buckets INT64)
RETURNS STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>> AS (
  -- Given a MAP `input_map`, fill in any missing keys with value `0.0`
  (
    WITH total_counts AS (
      SELECT
        key,
        COALESCE(e.value, 0.0) AS value
      FROM
        UNNEST(GENERATE_ARRAY(min_bucket, max_bucket, (max_bucket - min_bucket) / num_buckets)) as key
      LEFT JOIN
        UNNEST(input_map.key_value) AS e ON key = e.key
    )
    
    SELECT
      STRUCT<key_value ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>(
        ARRAY_AGG(STRUCT<key FLOAT64, value FLOAT64>(key, value))
      )
    FROM
      total_counts
  )
);

WITH clients_aggregates AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    aggregate.metric,
    aggregate.agg_type,
    aggregate.min_bucket,
    aggregate.max_bucket,
    aggregate.num_buckets,
    CASE agg_type
      WHEN 'max' THEN max(value)
      WHEN 'min' THEN min(value)
      WHEN 'avg' THEN avg(value)
      WHEN 'sum' THEN sum(value)
    END AS agg_value
  FROM
    analysis.clients_daily_aggregates
  CROSS JOIN
    UNNEST(scalar_aggregates) AS aggregate
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

-- Each client gets assigned a bucket
bucketed_scalars AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    agg_type,
    min_bucket,
    max_bucket,
    num_buckets,
    bucket(agg_value, min_bucket, max_bucket, num_buckets) AS bucket
  FROM
    clients_aggregates
)

SELECT
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  agg_type,
  fill_buckets(
    dedupe_map_sum(buckets_to_map(ARRAY_AGG(bucket))),
    ANY_VALUE(min_bucket), ANY_VALUE(max_bucket), ANY_VALUE(num_buckets)
  ) AS aggregates
FROM
  bucketed_scalars
GROUP BY 1, 2, 3, 4, 5, 6
