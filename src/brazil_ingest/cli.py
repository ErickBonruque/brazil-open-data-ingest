"""Command line: ``brazil-ingest caged {months,ingest,backfill,net-jobs}``."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from brazil_ingest.caged.convert import Filters
from brazil_ingest.caged.layout import Kind
from brazil_ingest.caged.metrics import net_jobs
from brazil_ingest.caged.pipeline import (
    FilterMismatchError,
    PartitionResult,
    done_months,
    ingest_months,
)
from brazil_ingest.caged.sources import FtpSource, LocalSource, Source
from brazil_ingest.months import backfill_plan, parse_range
from brazil_ingest.storage import open_store


def _codes(text: str) -> tuple[int, ...]:
    try:
        return tuple(int(x) for x in text.split(",") if x.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected comma-separated IBGE codes: {text}") from exc


def _kinds(text: str) -> frozenset[Kind]:
    try:
        return frozenset(Kind(x.strip().lower()) for x in text.split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("kinds are mov, for, exc") from exc


def _source(args: argparse.Namespace) -> Source:
    if args.source_dir:
        return LocalSource(Path(args.source_dir))
    return FtpSource()


def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--dest", required=True, help="output folder or s3://bucket/prefix")
    p.add_argument("--endpoint-url", help="S3-compatible endpoint (MinIO)")
    p.add_argument("--source-dir", help="read archives from this folder instead of the FTP")
    p.add_argument("--state", type=_codes, default=(), help="keep only these state codes (41)")
    p.add_argument(
        "--municipality", type=_codes, default=(), help="keep only these municipality codes"
    )
    p.add_argument("--kinds", type=_kinds, default=frozenset(Kind), help="mov,for,exc")
    p.add_argument("--force", action="store_true", help="rewrite partitions that already exist")
    p.add_argument("--workdir", help="folder for temporary files (default: system temp)")


def _print_results(results: list[PartitionResult]) -> None:
    for r in results:
        print(
            f"{r.month}  {r.kind.value}  {r.status:7}  {r.rows:>10,} rows  "
            f"balance {r.balance:>+9,}  {r.seconds:6.1f}s"
        )
    written = [r for r in results if r.status == "written"]
    print(f"{len(written)} written, {len(results) - len(written)} skipped")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="brazil-ingest", description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true")
    datasets = parser.add_subparsers(dest="dataset", required=True)
    caged = datasets.add_parser("caged", help="Novo CAGED (formal employment)")
    sub = caged.add_subparsers(dest="command", required=True)

    p = sub.add_parser("months", help="list the months published on the source")
    p.add_argument("--source-dir")

    p = sub.add_parser("ingest", help="ingest one month or a range (2025-01..2025-06)")
    p.add_argument("months", type=parse_range)
    _add_common(p)

    p = sub.add_parser("backfill", help="ingest every published month not done yet")
    p.add_argument("--lookback", type=int, default=12, help="months to look back (default 12)")
    _add_common(p)

    p = sub.add_parser("net-jobs", help="net job creation from a local output folder")
    p.add_argument("--dest", required=True)
    p.add_argument(
        "--by",
        default="reference_month",
        help="comma-separated: reference_month,state_code,region,cnae_section",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        return _run(args)
    except FilterMismatchError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


def _run(args: argparse.Namespace) -> int:
    if args.command == "months":
        for month in _source(args).list_months():
            print(month)
        return 0

    if args.command == "net-jobs":
        by = [c.strip() for c in args.by.split(",")]
        print("\t".join([*by, "first_release", "adjusted", "revision"]))
        for row in net_jobs(Path(args.dest), by):
            print("\t".join(str(v) for v in row))
        return 0

    source = _source(args)
    store = open_store(args.dest, endpoint_url=args.endpoint_url)
    filters = Filters(args.state, args.municipality)
    workdir = Path(args.workdir) if args.workdir else None

    if args.command == "backfill":
        available = source.list_months()
        window = backfill_plan(available, [], today=date.today(), lookback=args.lookback)
        months = [m for m in window if m not in set(done_months(store, source, window))]
        if not months:
            print("nothing to do")
            return 0
    else:
        months = args.months

    results = ingest_months(
        source,
        store,
        months,
        kinds=args.kinds,
        filters=filters,
        force=args.force,
        workdir=workdir,
    )
    _print_results(results)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
