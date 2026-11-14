"""Command line: ``brazil-ingest caged ...`` and ``brazil-ingest cnpj ...``."""

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
from brazil_ingest.cnpj import sources as cnpj_sources
from brazil_ingest.cnpj.layout import Table
from brazil_ingest.cnpj.pipeline import FileResult, ingest_release
from brazil_ingest.months import Month, backfill_plan, parse_range
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


def _tables(text: str) -> tuple[Table, ...]:
    try:
        return tuple(Table(x.strip().lower()) for x in text.split(","))
    except ValueError as exc:
        names = ", ".join(t.value for t in Table)
        raise argparse.ArgumentTypeError(f"tables are: {names}") from exc


def _cnpj_source(args: argparse.Namespace) -> cnpj_sources.Source:
    if args.source_dir:
        return cnpj_sources.LocalSource(Path(args.source_dir))
    return cnpj_sources.WebDavSource(args.share_url)


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
    p.add_argument(
        "--memory-limit", help="DuckDB memory limit, e.g. 512MB (lower peak, a bit slower)"
    )


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

    cnpj = datasets.add_parser("cnpj", help="CNPJ open data (company registry)")
    sub = cnpj.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("releases", "list the releases on the source"),
        ("ingest", "ingest one release (YYYY-MM or 'latest')"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--source-dir", help="read ZIPs from <dir>/<YYYY-MM>/ instead of the server")
        p.add_argument("--share-url", default=cnpj_sources.SHARE_URL, help="WebDAV share URL")
    p.add_argument("release", help="YYYY-MM or latest")
    p.add_argument("--dest", required=True, help="output folder or s3://bucket/prefix")
    p.add_argument("--endpoint-url", help="S3-compatible endpoint (MinIO)")
    p.add_argument("--tables", type=_tables, default=tuple(Table), help="default: all tables")
    p.add_argument(
        "--keep-personal",
        action="store_true",
        help="keep addresses, contacts, partner names and documents (dropped by default)",
    )
    p.add_argument("--force", action="store_true")
    p.add_argument("--workdir")
    p.add_argument("--memory-limit")
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
    if args.dataset == "cnpj":
        return _run_cnpj(args)
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
        memory_limit=args.memory_limit,
    )
    _print_results(results)
    return 0


def _print_cnpj(results: list[FileResult]) -> None:
    for r in results:
        notes = []
        if r.marker.get("invalid_dates"):
            notes.append(f"invalid dates {r.marker['invalid_dates']}")
        if r.marker.get("invalid_cnpj"):
            notes.append(f"invalid check digits {r.marker['invalid_cnpj']:,}")
        if r.marker.get("alphanumeric_cnpj"):
            notes.append(f"alphanumeric {r.marker['alphanumeric_cnpj']:,}")
        print(
            f"{r.table.value:15} {r.chunk}  {r.status:7} {r.rows:>12,} rows  {r.seconds:6.1f}s  "
            + "  ".join(notes)
        )
    written = [r for r in results if r.status == "written"]
    print(f"{len(written)} written, {len(results) - len(written)} skipped")


def _run_cnpj(args: argparse.Namespace) -> int:
    source = _cnpj_source(args)
    if args.command == "releases":
        for release in source.list_releases():
            print(release)
        return 0
    release = source.list_releases()[-1] if args.release == "latest" else Month.parse(args.release)
    store = open_store(args.dest, endpoint_url=args.endpoint_url)
    results = ingest_release(
        source,
        store,
        release,
        tables=args.tables,
        keep_personal=args.keep_personal,
        force=args.force,
        workdir=Path(args.workdir) if args.workdir else None,
        memory_limit=args.memory_limit,
    )
    print(f"release {release}")
    _print_cnpj(results)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
