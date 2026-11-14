"""Build the README tables and chart from an ingested output folder.

    uv run python scripts/make_report.py data/

Writes ``results/net_jobs_by_month.csv``, ``results/net_jobs_by_year.csv`` and
``docs/img/net_jobs.png``.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from brazil_ingest.caged.metrics import net_jobs
from brazil_ingest.caged.pipeline import DATASET

REPO = Path(__file__).resolve().parent.parent


def volume(root: Path) -> list[tuple[object, ...]]:
    pattern = str(root / DATASET / "*" / "*" / "*" / "*.parquet")
    with duckdb.connect() as con:
        return con.execute(
            "SELECT kind, COUNT(*) FROM read_parquet(?, hive_partitioning = true) "
            "GROUP BY kind ORDER BY kind",
            [pattern],
        ).fetchall()


def main(root: Path) -> None:
    monthly = net_jobs(root)
    out = REPO / "results"
    out.mkdir(exist_ok=True)
    with (out / "net_jobs_by_month.csv").open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["reference_month", "first_release", "adjusted", "revision"])
        writer.writerows(monthly)

    years: dict[int, list[int]] = {}
    for ref, first, adjusted, _ in monthly:
        acc = years.setdefault(int(str(ref)[:4]), [0, 0])
        acc[0] += int(str(first))
        acc[1] += int(str(adjusted))
    with (out / "net_jobs_by_year.csv").open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["year", "first_release", "adjusted", "revision"])
        for year, (first, adjusted) in sorted(years.items()):
            writer.writerow([year, first, adjusted, adjusted - first])
            print(
                f"{year}  first {first:>+11,}  adjusted {adjusted:>+11,}  "
                f"revision {adjusted - first:>+9,}"
            )

    for kind, rows in volume(root):
        print(f"{kind}: {rows:,} rows")

    labels = [f"{str(r[0])[:4]}-{str(r[0])[4:]}" for r in monthly]
    x = range(len(labels))
    fig, ax = plt.subplots(figsize=(11, 4.2), dpi=120)
    ax.bar(x, [int(str(r[2])) / 1000 for r in monthly], color="#3b6ea5", label="adjusted")
    ax.plot(
        x, [int(str(r[1])) / 1000 for r in monthly], color="#d1495b", lw=1.4, label="first release"
    )
    ax.axhline(0, color="#444", lw=0.6)
    ticks = [i for i, label in enumerate(labels) if label.endswith("-01")]
    ax.set_xticks(ticks, [labels[i][:4] for i in ticks])
    ax.set_ylabel("net formal jobs (thousands)")
    ax.set_title("Brazil: net formal job creation by month (Novo CAGED)")
    ax.legend(frameon=False)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    img = REPO / "docs" / "img"
    img.mkdir(parents=True, exist_ok=True)
    fig.savefig(img / "net_jobs.png")


if __name__ == "__main__":
    main(Path(sys.argv[1] if len(sys.argv) > 1 else "data"))
