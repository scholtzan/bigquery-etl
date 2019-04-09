CREATE TEMP FUNCTION
  udf_map_mode_last(maps ANY TYPE) AS (STRUCT(ARRAY(
      SELECT
        AS STRUCT --
        _key_value_pair.key,
        udf_mode_last(ARRAY_AGG(_key_value_pair.value)) AS value
      FROM
        UNNEST(maps) AS _map,
        UNNEST(_map.key_value) AS _key_value_pair
      GROUP BY
        _key_value_pair.key) AS key_value));
