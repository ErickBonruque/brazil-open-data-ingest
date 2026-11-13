import json
from datetime import date
from pathlib import Path

import pytest

from brazil_ingest import cli
from brazil_ingest.caged.convert import Filters
from brazil_ingest.caged.layout import Kind, LayoutError
from brazil_ingest.caged.metrics import net_jobs
from brazil_ingest.caged.pipeline import (
    FilterMismatchError,
    done_months,
    ingest_months,
    read_marker,
)
from brazil_ingest.caged.sources import LocalSource
from brazil_ingest.months import Month
from brazil_ingest.storage import LocalStore
from tests.conftest import ArchiveFactory, row


@pytest.fixture
def server(archives: tuple[Path, ArchiveFactory]) -> Path:
    root, make = archives
    # January: 3 hires, 1 separation -> first release +2
    make(
        "MOV",
        "202501",
        [
            row(202501, 41, 411990, 1),
            row(202501, 41, 411990, 1),
            row(202501, 35, 355030, 1),
            row(202501, 35, 355030, -1),
        ],
    )
    # February: its own movements, plus a late January hire and a removed January hire
    make("MOV", "202502", [row(202502, 41, 411990, -1)])
    make("FOR", "202502", [row(202501, 41, 411990, 1, declared=202502)])
    make("EXC", "202502", [row(202501, 35, 355030, 1, exc=True, declared=202501)])
    return root


def test_ingest_writes_partitions_and_markers(server: Path, tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "lake")
    results = ingest_months(LocalSource(server), store, [Month(2025, 1), Month(2025, 2)])
    assert [(r.kind, r.status, r.rows) for r in results] == [
        (Kind.MOV, "written", 4),
        (Kind.EXC, "written", 1),
        (Kind.FOR, "written", 1),
        (Kind.MOV, "written", 1),
    ]
    part = tmp_path / "lake/caged/kind=mov/year=2025/month=01/part-0.parquet"
    assert part.is_file()
    marker = read_marker(store, Kind.MOV, Month(2025, 1))
    assert marker is not None
    assert marker["rows"] == 4 and marker["balance"] == 2
    assert len(marker["source_sha256"]) == 64


def test_second_run_skips_everything(server: Path, tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "lake")
    source = LocalSource(server)
    ingest_months(source, store, [Month(2025, 1)])
    again = ingest_months(source, store, [Month(2025, 1)])
    assert [r.status for r in again] == ["skipped"]
    forced = ingest_months(source, store, [Month(2025, 1)], force=True)
    assert [r.status for r in forced] == ["written"]


def test_other_filters_on_same_destination_fail(server: Path, tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "lake")
    source = LocalSource(server)
    ingest_months(source, store, [Month(2025, 1)])
    with pytest.raises(FilterMismatchError):
        ingest_months(source, store, [Month(2025, 1)], filters=Filters(state_codes=(41,)))


def test_failed_file_leaves_no_marker(
    archives: tuple[Path, ArchiveFactory], tmp_path: Path
) -> None:
    root, make = archives
    make("MOV", "202503", [row(202503, 41, 411990, 1)])
    bad = root / "2025" / "202503" / "CAGEDFOR202503.7z"
    bad.write_bytes(b"not a 7z file")
    store = LocalStore(tmp_path / "lake")
    with pytest.raises(Exception):  # noqa: B017 - any error from the broken archive
        ingest_months(LocalSource(root), store, [Month(2025, 3)])
    assert read_marker(store, Kind.FOR, Month(2025, 3)) is None
    assert done_months(store, LocalSource(root), [Month(2025, 3)]) == []


def test_done_months(server: Path, tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "lake")
    source = LocalSource(server)
    ingest_months(source, store, [Month(2025, 2)], kinds=frozenset({Kind.MOV}))
    assert done_months(store, source, [Month(2025, 1), Month(2025, 2)]) == []
    ingest_months(source, store, [Month(2025, 1), Month(2025, 2)])
    assert done_months(store, source, [Month(2025, 1), Month(2025, 2)]) == [
        Month(2025, 1),
        Month(2025, 2),
    ]


def test_net_jobs_first_release_and_adjusted(server: Path, tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    ingest_months(LocalSource(server), LocalStore(lake), [Month(2025, 1), Month(2025, 2)])
    # Jan: first release +2; +1 declared late; -1 removed -> adjusted +2, revision 0
    # Feb: -1, no corrections
    assert net_jobs(lake) == [(202501, 2, 2, 0), (202502, -1, -1, 0)]
    by_state = net_jobs(lake, ["reference_month", "state_code"])
    assert (202501, 41, 2, 3, 1) in by_state  # late hire raises Paraná
    assert (202501, 35, 0, -1, -1) in by_state  # removed hire lowers São Paulo
    with pytest.raises(ValueError):
        net_jobs(lake, ["salary"])


def test_cli_ingest_and_net_jobs(
    server: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    lake = tmp_path / "lake"
    base = ["caged", "ingest", "2025-01..2025-02", "--source-dir", str(server), "--dest", str(lake)]
    assert cli.main(base) == 0
    assert "4 written, 0 skipped" in capsys.readouterr().out
    assert cli.main([*base, "--state", "41"]) == 2
    assert "Use another destination" in capsys.readouterr().err
    assert cli.main(["caged", "net-jobs", "--dest", str(lake)]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == "reference_month\tfirst_release\tadjusted\trevision"
    assert out[1] == "202501\t2\t2\t0"


def test_cli_backfill(
    server: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2025, 3, 15)

    monkeypatch.setattr(cli, "date", FixedDate)
    lake = tmp_path / "lake"
    args = ["caged", "backfill", "--source-dir", str(server), "--dest", str(lake)]
    assert cli.main(args) == 0
    assert "4 written" in capsys.readouterr().out
    assert cli.main(args) == 0
    assert "nothing to do" in capsys.readouterr().out
    marker = json.loads((lake / "caged/kind=for/year=2025/month=02/_SUCCESS.json").read_text())
    assert marker["filters"] == {"state_codes": [], "municipality_codes": []}


def test_cli_lists_months(server: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["caged", "months", "--source-dir", str(server)]) == 0
    assert capsys.readouterr().out.split() == ["2025-01", "2025-02"]


def test_archive_with_two_files_is_rejected(tmp_path: Path) -> None:
    import py7zr

    from brazil_ingest.caged.extract import extract_text

    for name in ("a.txt", "b.txt"):
        (tmp_path / name).write_text("x")
    archive = tmp_path / "CAGEDMOV202501.7z"
    with py7zr.SevenZipFile(archive, "w") as zf:
        zf.write(tmp_path / "a.txt", arcname="a.txt")
        zf.write(tmp_path / "b.txt", arcname="b.txt")
    with pytest.raises(LayoutError, match="expected 1"):
        extract_text(archive, tmp_path / "out")
