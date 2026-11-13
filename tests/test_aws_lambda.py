from datetime import date
from pathlib import Path

import pytest

from brazil_ingest.aws_lambda import run
from brazil_ingest.caged.sources import LocalSource
from brazil_ingest.storage import LocalStore
from tests.conftest import ArchiveFactory, row


def test_lambda_run_backfills_then_does_nothing(
    archives: tuple[Path, ArchiveFactory], tmp_path: Path
) -> None:
    root, make = archives
    make("MOV", "202501", [row(202501, 41, 411990, 1), row(202501, 35, 355030, 1)])
    make("MOV", "202412", [row(202412, 41, 411990, -1)])
    source = LocalSource(root)
    store = LocalStore(tmp_path / "lake")
    event = {"lookback": 2, "state_codes": "41"}

    first = run(event, source=source, store=store, today=date(2025, 2, 3))
    assert first == {"months": ["2024-12", "2025-01"], "written": 2, "rows": 2}

    second = run(event, source=source, store=store, today=date(2025, 2, 3))
    assert second == {"months": [], "written": 0, "rows": 0}


def test_lambda_needs_a_destination(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CAGED_DEST", raising=False)
    with pytest.raises(ValueError, match="CAGED_DEST"):
        run({})
