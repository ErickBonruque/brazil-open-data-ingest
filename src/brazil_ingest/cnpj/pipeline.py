"""Ingest a CNPJ release into a store, one partition per source ZIP.

Output layout (Hive style)::

    cnpj/table=establishments/release=2026-10/chunk=3/part-0.parquet
    cnpj/table=establishments/release=2026-10/chunk=3/_SUCCESS.json

Each release is a full snapshot of the registry, so ``release`` is part of the path and old
releases stay readable next to new ones.
"""

from __future__ import annotations

import logging
import tempfile
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from brazil_ingest.cnpj.convert import convert, extract_member
from brazil_ingest.cnpj.layout import LayoutError, Table, parse_zip_name
from brazil_ingest.cnpj.sources import Source
from brazil_ingest.months import Month
from brazil_ingest.partitions import FilterMismatchError, publish, read_marker, sha256_file
from brazil_ingest.storage import Store

log = logging.getLogger(__name__)

DATASET = "cnpj"


@dataclass(frozen=True)
class FileResult:
    table: Table
    chunk: int
    status: str  # "written" | "skipped"
    rows: int
    seconds: float
    marker: dict[str, Any]


def partition_prefix(table: Table, release: Month, chunk: int) -> str:
    return f"{DATASET}/table={table.value}/release={release}/chunk={chunk}"


def ingest_file(
    source: Source,
    store: Store,
    release: Month,
    name: str,
    *,
    keep_personal: bool = False,
    force: bool = False,
    workdir: Path | None = None,
    memory_limit: str | None = None,
) -> FileResult:
    table, chunk = parse_zip_name(name)
    prefix = partition_prefix(table, release, chunk)
    options = {"keep_personal": keep_personal}
    marker = read_marker(store, prefix)
    if marker is not None and not force:
        if marker.get("options") != options:
            raise FilterMismatchError(
                f"{prefix} was written with {marker.get('options')}, not {options}. "
                "Use another destination or --force."
            )
        return FileResult(table, chunk, "skipped", int(marker["rows"]), 0.0, marker)

    started = time.perf_counter()
    if workdir is not None:
        workdir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="cnpj-", dir=workdir) as tmp:
        tmp_dir = Path(tmp)
        archive = source.fetch(release, name, tmp_dir / "download")
        text = extract_member(archive, tmp_dir / "extract")
        result = convert(
            text,
            table,
            tmp_dir / "out.parquet",
            keep_personal=keep_personal,
            memory_limit=memory_limit,
            delete_source=True,
        )
        marker = {
            "dataset": DATASET,
            "table": table.value,
            "release": str(release),
            "chunk": chunk,
            "source_file": name,
            "source_bytes": archive.stat().st_size,
            "source_sha256": sha256_file(archive),
            "encoding": result.encoding,
            "options": options,
            "rows": result.rows,
            "invalid_dates": result.invalid_dates,
            "invalid_cnpj": result.invalid_cnpj,
            "alphanumeric_cnpj": result.alphanumeric_cnpj,
            "control_characters": result.control_characters,
            "columns": result.columns,
        }
        publish(store, prefix, result.parquet, marker)
    seconds = time.perf_counter() - started
    log.info("%s %s chunk %d: %d rows (%.1fs)", table.value, release, chunk, result.rows, seconds)
    return FileResult(table, chunk, "written", result.rows, seconds, marker)


def ingest_release(
    source: Source,
    store: Store,
    release: Month,
    *,
    tables: Iterable[Table] = tuple(Table),
    keep_personal: bool = False,
    force: bool = False,
    workdir: Path | None = None,
    memory_limit: str | None = None,
) -> list[FileResult]:
    wanted = set(tables)
    files = source.list_files(release)
    if not files:
        raise FileNotFoundError(f"release {release} not found on the source")
    # Small files first: a broken run still leaves the lookup tables done.
    seen: dict[tuple[Table, int], str] = {}
    for name in files:
        key = parse_zip_name(name)
        if key in seen:
            raise LayoutError(f"{seen[key]} and {name} map to the same partition")
        seen[key] = name
    names = sorted((n for n in files if parse_zip_name(n)[0] in wanted), key=lambda n: files[n])
    return [
        ingest_file(
            source,
            store,
            release,
            name,
            keep_personal=keep_personal,
            force=force,
            workdir=workdir,
            memory_limit=memory_limit,
        )
        for name in names
    ]
