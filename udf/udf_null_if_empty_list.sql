CREATE TEMP FUNCTION
  udf_null_if_empty_list(val ANY TYPE) AS ( IF(ARRAY_LENGTH(val.list) > 0,
      val,
      NULL) );

/*

This function accepts STRUCT<list ARRAY<ANY TYPE>> and returns NULL if struct
field `list` is empty.

This function nests `list` within a struct for two reasons. The first is that
ARRAY<ANY TYPE> becomes nested as STRUCT<list ARRAY<STRUCT<element ANY TYPE>>>
when parquet is loaded into BigQuery, such as with main_summary_v4 and
clients_daily_v6, and this behavior simplifies preserving that schema. The
second is in order to support dropping empty lists when passed to udf aggregate
functions via ARRAY_AGG, which cannot accept an ARRAY input directly because
BigQuery doesn't allow ARRAY<ARRAY<ANY TYPE>>.

*/
