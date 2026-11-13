"""Net job creation ("saldo") from the ingested Parquet, with DuckDB.

The Ministry of Labor publishes two numbers for each month:

* **first release**: the balance of the MOV file of that month (movements declared on time);
* **adjusted**: MOV + FOR (declared late) - EXC (removed later), summed over all later releases.

Both come out of the same partitions, so the size of the later corrections is visible.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from brazil_ingest.caged.pipeline import DATASET

GROUPS = {"reference_month", "state_code", "region", "cnae_section"}


def net_jobs(root: Path, by: list[str] | None = None) -> list[tuple[object, ...]]:
    """Rows of ``(*by, first_release, adjusted, revision)`` ordered by ``by``.

    ``revision`` = adjusted - first_release.
    """
    by = by or ["reference_month"]
    unknown = [c for c in by if c not in GROUPS]
    if unknown:
        raise ValueError(f"cannot group by {unknown}; use {sorted(GROUPS)}")
    keys = ", ".join(by)
    pattern = str(root / DATASET / "*" / "*" / "*" / "*.parquet").replace("'", "''")
    sql = f"""
        WITH rows AS (
            SELECT * FROM read_parquet('{pattern}', hive_partitioning = true, union_by_name = true)
        ),
        first_release AS (
            SELECT {keys}, SUM(balance) AS v FROM rows
            WHERE kind = 'mov'
              AND reference_month = CAST(year AS INTEGER) * 100 + CAST(month AS INTEGER)
            GROUP BY ALL
        ),
        adjusted AS (
            SELECT {keys},
                   SUM(CASE WHEN kind = 'exc' THEN -balance ELSE balance END) AS v
            FROM rows GROUP BY ALL
        )
        SELECT {keys}, f.v AS first_release, a.v AS adjusted, a.v - f.v AS revision
        FROM first_release f JOIN adjusted a USING ({keys})
        ORDER BY {keys}
    """
    with duckdb.connect() as con:
        return con.execute(sql).fetchall()
