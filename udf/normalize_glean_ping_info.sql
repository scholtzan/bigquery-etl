/*

Accepts a glean ping_info struct as input and returns a modified struct that
includes a few parsed or normalized variants of the input fields.

*/

CREATE OR REPLACE FUNCTION
  udf.normalize_glean_ping_info(ping_info ANY TYPE) AS ((
    SELECT
      AS STRUCT
        ping_info.*,
        SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time) AS parsed_start_time,
        SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.end_time) AS parsed_end_time));

-- Tests

SELECT
  assert_equals(
    TIMESTAMP '2019-12-01 09:22:00',
    udf.normalize_glean_ping_info(
      STRUCT('2019-12-01T20:22+11:00' AS start_time,
             '2019-12-01T21:24+11:00' AS end_time)).parsed_start_time),
  assert_equals(
    TIMESTAMP '2019-12-01 10:24:00',
    udf.normalize_glean_ping_info(
      STRUCT('2019-12-01T20:22+11:00' AS start_time,
             '2019-12-01T21:24+11:00' AS end_time)).parsed_end_time),
  assert_null(
    udf.normalize_glean_ping_info(
      STRUCT('2019-12-01T20:22+11:00' AS start_time,
             '2019-12-01T21:24:00+11:00' AS end_time)).parsed_end_time);
