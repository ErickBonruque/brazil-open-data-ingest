"""Build the README tables and chart for one ingested CNPJ release.

    uv run python scripts/cnpj_report.py data/ 2026-10

Writes ``results/cnpj_*.csv`` and ``docs/img/cnpj_openings.png``.
"""

from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parent.parent


def main(root: Path, release: str) -> None:
    base = root / "cnpj"
    markers = [json.loads(p.read_text()) for p in base.glob(f"*/release={release}/*/_SUCCESS.json")]
    rows: Counter[str] = Counter()
    invalid_dates: Counter[str] = Counter()
    for m in markers:
        rows[m["table"]] += m["rows"]
        for col, n in m["invalid_dates"].items():
            invalid_dates[f"{m['table']}.{col}"] += n
    print(f"{len(markers)} partitions")
    for table, n in rows.most_common():
        print(f"  {table:16} {n:>12,} rows")
    print("alphanumeric CNPJ:", sum(m["alphanumeric_cnpj"] for m in markers))
    print("invalid check digits:", sum(m["invalid_cnpj"] for m in markers))
    print("invalid dates:", dict(invalid_dates))

    def parquet(name: str) -> str:
        path = str(base / f"table={name}" / f"release={release}" / "*" / "*.parquet")
        return f"read_parquet('{path}')"

    out = REPO / "results"
    out.mkdir(exist_ok=True)
    with duckdb.connect() as con:
        status = con.execute(
            f"SELECT registration_status, COUNT(*) FROM {parquet('establishments')} GROUP BY 1 "
            "ORDER BY 1"
        ).fetchall()
        print("status:", status)
        by_year = con.execute(
            f"""
            SELECT year(activity_start_date) AS year,
                   COUNT(*) FILTER (WHERE registration_status = 2) AS active,
                   COUNT(*) FILTER (WHERE registration_status <> 2) AS not_active
            FROM {parquet("establishments")}
            WHERE activity_start_date >= DATE '2000-01-01'
              AND activity_start_date < DATE '{release}-01'
            GROUP BY 1 ORDER BY 1
            """
        ).fetchall()
        mei = con.execute(
            f"""
            SELECT COUNT(*) FILTER (WHERE s.mei_option = 'S') AS mei, COUNT(*) AS total
            FROM {parquet("establishments")} e
            LEFT JOIN {parquet("simples")} s USING (cnpj_base)
            WHERE e.registration_status = 2 AND e.head_office = 1
            """
        ).fetchone()
        print("active head offices that are MEI:", mei)

    with (out / "cnpj_openings_by_year.csv").open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["year", "still_active", "not_active"])
        writer.writerows(by_year)

    years = [int(r[0]) for r in by_year]
    active = [int(r[1]) / 1e6 for r in by_year]
    closed = [int(r[2]) / 1e6 for r in by_year]
    fig, ax = plt.subplots(figsize=(11, 4.2), dpi=120)
    ax.bar(years, active, color="#3b6ea5", label="still active")
    ax.bar(years, closed, bottom=active, color="#c9c9c9", label="closed, suspended or unfit")
    ax.set_ylabel("establishments opened (millions)")
    ax.set_title(f"Brazil: establishments by year of opening and status in {release} (CNPJ)")
    ax.legend(frameon=False)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    img = REPO / "docs" / "img"
    img.mkdir(parents=True, exist_ok=True)
    fig.savefig(img / "cnpj_openings.png")


if __name__ == "__main__":
    main(Path(sys.argv[1]), sys.argv[2])
