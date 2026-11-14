"""AWS Lambda entry point: a scheduled backfill into S3.

Configuration comes from the event; environment variables are the defaults:

* ``dest`` / ``CAGED_DEST``: ``s3://bucket/prefix`` (required).
* ``lookback`` / ``CAGED_LOOKBACK``: months to look back (default 12).
* ``state_codes`` / ``CAGED_STATES``: IBGE state codes, comma-separated.
* ``municipality_codes`` / ``CAGED_MUNICIPALITIES``: IBGE municipality codes, comma-separated.
* ``memory_limit`` / ``CAGED_MEMORY_LIMIT``: DuckDB memory limit (default 512MB).

Temporary files go to the system temp folder (``/tmp`` on Lambda; size it with ephemeral
storage of at least 2 GB).

A run that finds nothing new returns ``{"written": 0}``. With an EventBridge schedule (for
example daily in the last week of the month) the function picks up each release when the
Ministry publishes it, and catches up after a late release or a failed run.
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

from brazil_ingest.caged.convert import Filters
from brazil_ingest.caged.pipeline import done_months, ingest_months
from brazil_ingest.caged.sources import FtpSource, Source
from brazil_ingest.months import backfill_plan
from brazil_ingest.storage import Store, open_store

log = logging.getLogger(__name__)


def _codes(value: Any) -> tuple[int, ...]:
    if value is None or value == "":
        return ()
    if isinstance(value, str):
        value = value.split(",")
    return tuple(int(v) for v in value)


def run(
    event: dict[str, Any],
    *,
    source: Source | None = None,
    store: Store | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    dest = event.get("dest") or os.environ.get("CAGED_DEST")
    if not dest and store is None:
        raise ValueError("missing destination: set event['dest'] or CAGED_DEST")
    lookback = int(event.get("lookback") or os.environ.get("CAGED_LOOKBACK") or 12)
    filters = Filters(
        _codes(event.get("state_codes", os.environ.get("CAGED_STATES"))),
        _codes(event.get("municipality_codes", os.environ.get("CAGED_MUNICIPALITIES"))),
    )
    source = source or FtpSource()
    store = store or open_store(str(dest))
    window = backfill_plan(source.list_months(), [], today=today or date.today(), lookback=lookback)
    done = set(done_months(store, source, window))
    todo = [m for m in window if m not in done]
    memory_limit = str(event.get("memory_limit") or os.environ.get("CAGED_MEMORY_LIMIT") or "512MB")
    results = ingest_months(source, store, todo, filters=filters, memory_limit=memory_limit)
    written = [r for r in results if r.status == "written"]
    summary = {
        "months": [str(m) for m in todo],
        "written": len(written),
        "rows": sum(r.rows for r in written),
    }
    log.info("summary %s", summary)
    return summary


def handler(event: dict[str, Any], context: object) -> dict[str, Any]:  # pragma: no cover
    logging.getLogger().setLevel(logging.INFO)
    return run(event or {})
