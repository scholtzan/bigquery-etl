#!/usr/bin/env python3
"""clients_daily_scalar_aggregates query generator."""
import os
import sys
import json
import textwrap
import urllib.request


PROBE_INFO_SERVICE = (
    "https://probeinfo.telemetry.mozilla.org/firefox/all/main/all_probes"
)


def generate_sql(probes):
    """Create a SQL query for the clients_daily_scalar_aggregates dataset."""
    probe_structs = []
    for probe in probes:
        probe_structs.append(
            (
                "('{metric}', 'max', max(CAST({metric} AS INT64))"
                "OVER w1, 0, 1000, 50)"
            ).format(metric=probe)
        )
        probe_structs.append(
            (
                "('{metric}', 'avg', avg(CAST({metric} AS INT64))"
                "OVER w1, 0, 1000, 50)"
            ).format(metric=probe)
        )
        probe_structs.append(
            (
                "('{metric}', 'min', min(CAST({metric} AS INT64))"
                "OVER w1, 0, 1000, 50)"
            ).format(metric=probe)
        )
        probe_structs.append(
            (
                "('{metric}', 'sum', sum(CAST({metric} AS INT64))"
                "OVER w1, 0, 1000, 50)"
            ).format(metric=probe)
        )

    probes_string = ",\n\t\t\t".join(probe_structs)

    return textwrap.dedent(
        """-- Query generated by: sql/clients_daily_scalar_aggregates.sql.py
        WITH
            -- normalize client_id and rank by document_id
            numbered_duplicates AS (
                SELECT
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            client_id,
                            submission_date_s3,
                            document_id
                        ORDER BY `timestamp`
                        ASC
                    ) AS _n,
                    * REPLACE(LOWER(client_id) AS client_id)
                FROM main_summary_v4
                WHERE submission_date_s3 = @submission_date
                AND client_id IS NOT NULL
            ),


            -- Deduplicating on document_id is necessary to get valid SUM values.
            deduplicated AS (
                SELECT * EXCEPT (_n)
                FROM numbered_duplicates
                WHERE _n = 1
            ),

            -- Aggregate by client_id using windows
            windowed AS (
                SELECT
                    ROW_NUMBER() OVER w1_unframed AS _n,
                    submission_date_s3 as submission_date,
                    client_id,
                    os,
                    app_version,
                    app_build_id,
                    channel,
                    ARRAY<STRUCT<
                        metric STRING,
                        agg_type STRING,
                        value FLOAT64,
                        min_bucket INT64,
                        max_bucket INT64,
                        num_buckets INT64
                    >> [
                        {probes}
                    ] AS scalar_aggregates
                FROM deduplicated
                WINDOW
                    -- Aggregations require a framed window
                    w1 AS (
                        PARTITION BY
                            client_id,
                            submission_date_s3
                        ORDER BY `timestamp` ASC ROWS BETWEEN UNBOUNDED PRECEDING
                        AND UNBOUNDED FOLLOWING
                    ),

                    -- ROW_NUMBER does not work on a framed window
                    w1_unframed AS (
                        PARTITION BY
                            client_id,
                            submission_date_s3
                        ORDER BY `timestamp` ASC
                    )
            )

        SELECT
            * EXCEPT(_n)
        FROM
            windowed
        WHERE
            _n = 1
    """.format(
            probes=probes_string
        )
    )


def get_scalar_probes():
    """Find all scalar probes in main summary.

    Note: that non-integer scalar probes are not included.
    """
    main_summary_scalars = set()
    main_summary_schema = json.loads(
        os.popen("bq show --schema telemetry.main_summary_v4").read()
    )
    for field in main_summary_schema:
        if field["name"].startswith("scalar_parent") and field["type"] == "INTEGER":
            main_summary_scalars.add(field["name"])

    # Find the intersection between relevant scalar probes
    # and those that exist in main summary
    with urllib.request.urlopen(PROBE_INFO_SERVICE) as url:
        data = json.loads(url.read().decode())
        scalar_probes = set(
            [
                x.replace("scalar/", "scalar_parent_").replace(".", "_")
                for x in data.keys()
                if x.startswith("scalar/")
            ]
        )
        return scalar_probes.intersection(main_summary_scalars)


def main(argv, out=print):
    """Print a clients_daily_scalar_aggregates query to stdout."""
    scalar_probes = get_scalar_probes()
    out(generate_sql(scalar_probes))


if __name__ == "__main__":
    main(sys.argv)
