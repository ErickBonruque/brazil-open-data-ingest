"""Ingest Novo CAGED months into a store, once per partition.

Output layout (Hive style, readable by DuckDB, Spark, Athena and Glue)::

    caged/kind=mov/year=2025/month=12/part-0.parquet
    caged/kind=mov/year=2025/month=12/_SUCCESS.json

``year`` and ``month`` are the **release** month (the folder on the server). FOR and EXC files
contain movements of earlier months; the ``reference_month`` column keeps that month.

The ``_SUCCESS.json`` marker is written last. A partition with a marker is complete and is
skipped on the next run, so the pipeline can run on a schedule and be stopped at any point.
"""

from __future__ import annotations

import hashlib
import json
import logging
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from brazil_ingest import __version__
from brazil_ingest.caged.convert import Filters, convert
from brazil_ingest.caged.extract import extract_text
from brazil_ingest.caged.layout import Kind, parse_file_name
from brazil_ingest.caged.sources import Source
from brazil_ingest.months import Month
from brazil_ingest.storage import Store

log = logging.getLogger(__name__)

DATASET = "caged"
MARKER = "_SUCCESS.json"
PART = "part-0.parquet"


class FilterMismatchError(RuntimeError):
    """A partition already exists with other filters. Mixing them would corrupt the dataset."""


@dataclass(frozen=True)
class PartitionResult:
    kind: Kind
    month: Month
    status: str  # "written" | "skipped"
    rows: int
    balance: int
    seconds: float


def partition_prefix(kind: Kind, month: Month) -> str:
    return f"{DATASET}/kind={kind.value}/year={month.year}/month={month.month:02d}"


def read_marker(store: Store, kind: Kind, month: Month) -> dict[str, Any] | None:
    key = f"{partition_prefix(kind, month)}/{MARKER}"
    if not store.exists(key):
        return None
    data: dict[str, Any] = json.loads(store.read_bytes(key))
    return data


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def ingest_file(
    source: Source,
    store: Store,
    month: Month,
    name: str,
    *,
    filters: Filters | None = None,
    force: bool = False,
    workdir: Path | None = None,
) -> PartitionResult:
    """Download, extract, convert and publish one archive. Skips it if already done."""
    filters = filters or Filters()
    kind, compact = parse_file_name(name)
    if compact != month.compact:
        raise ValueError(f"{name} does not belong to {month}")
    started = time.perf_counter()
    marker = read_marker(store, kind, month)
    if marker is not None and not force:
        if marker.get("filters") != filters.as_dict():
            raise FilterMismatchError(
                f"{partition_prefix(kind, month)} was written with filters {marker.get('filters')}"
                f", not {filters.as_dict()}. Use another destination or --force."
            )
        return PartitionResult(kind, month, "skipped", marker["rows"], marker["balance"], 0.0)

    with tempfile.TemporaryDirectory(prefix="caged-", dir=workdir) as tmp:
        tmp_dir = Path(tmp)
        archive = source.fetch(month, name, tmp_dir / "download")
        text = extract_text(archive, tmp_dir / "extract")
        result = convert(text, kind, tmp_dir / PART, filters)
        prefix = partition_prefix(kind, month)
        store.put_file(result.parquet, f"{prefix}/{PART}")
        marker_data = {
            "dataset": DATASET,
            "kind": kind.value,
            "release_month": str(month),
            "source_file": name,
            "source_bytes": archive.stat().st_size,
            "source_sha256": _sha256(archive),
            "encoding": result.encoding,
            "filters": filters.as_dict(),
            "rows": result.output.rows,
            "balance": result.output.balance,
            "scientific_notation_values": result.scientific_values,
            "columns": result.columns,
            "written_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "tool_version": __version__,
        }
        store.put_bytes(json.dumps(marker_data, indent=2).encode(), f"{prefix}/{MARKER}")
    seconds = time.perf_counter() - started
    log.info(
        "%s %s: %d rows, balance %+d (%.1fs)",
        kind.value,
        month,
        result.output.rows,
        result.output.balance,
        seconds,
    )
    return PartitionResult(
        kind, month, "written", result.output.rows, result.output.balance, seconds
    )


def ingest_months(
    source: Source,
    store: Store,
    months: list[Month],
    *,
    kinds: frozenset[Kind] = frozenset(Kind),
    filters: Filters | None = None,
    force: bool = False,
    workdir: Path | None = None,
) -> list[PartitionResult]:
    results: list[PartitionResult] = []
    for month in months:
        files = source.list_files(month)
        if not files:
            log.warning("%s: nothing published on the source", month)
            continue
        for name in sorted(files):
            kind, _ = parse_file_name(name)
            if kind in kinds:
                results.append(
                    ingest_file(
                        source,
                        store,
                        month,
                        name,
                        filters=filters,
                        force=force,
                        workdir=workdir,
                    )
                )
    return results


def done_months(store: Store, source: Source, months: list[Month]) -> list[Month]:
    """Months where every file published on the source has a marker in the store."""
    done: list[Month] = []
    for month in months:
        files = source.list_files(month)
        if files and all(
            read_marker(store, parse_file_name(name)[0], month) is not None for name in files
        ):
            done.append(month)
    return done
