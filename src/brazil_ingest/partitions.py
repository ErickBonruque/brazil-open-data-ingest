"""Partition publishing shared by all datasets.

A partition is a folder with one ``part-0.parquet`` file and a ``_SUCCESS.json`` marker. The
marker is written **after** the data, so a partition with a marker is complete. Pipelines skip
partitions that have a marker, which makes every run safe to repeat or to stop at any point.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from brazil_ingest import __version__
from brazil_ingest.storage import Store

MARKER = "_SUCCESS.json"
PART = "part-0.parquet"

_MEMORY_RE = re.compile(r"^\d+(\.\d+)?\s*(KB|MB|GB|KiB|MiB|GiB)$", re.IGNORECASE)


class FilterMismatchError(RuntimeError):
    """A partition already exists with other options. Mixing them would corrupt the dataset."""


def read_marker(store: Store, prefix: str) -> dict[str, Any] | None:
    key = f"{prefix}/{MARKER}"
    if not store.exists(key):
        return None
    data: dict[str, Any] = json.loads(store.read_bytes(key))
    return data


def publish(store: Store, prefix: str, parquet: Path, marker: dict[str, Any]) -> None:
    """Upload the data file, then the marker (never the other way around)."""
    store.put_file(parquet, f"{prefix}/{PART}")
    body = {
        **marker,
        "written_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "tool_version": __version__,
    }
    store.put_bytes(json.dumps(body, indent=2, ensure_ascii=False).encode(), f"{prefix}/{MARKER}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def check_memory_limit(value: str) -> str:
    """``"512MB"``. Validated because it goes into SQL as text."""
    if not _MEMORY_RE.match(value.strip()):
        raise ValueError(f"memory limit must look like 512MB or 2GB, got {value!r}")
    return value.strip()


def duck_connect(
    *, threads: int | None = None, memory_limit: str | None = None
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    if memory_limit:
        con.execute(f"SET memory_limit = '{check_memory_limit(memory_limit)}'")
    return con


def sql_str(value: str) -> str:
    """A SQL string literal."""
    return "'" + value.replace("'", "''") + "'"
