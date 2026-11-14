from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from brazil_ingest.caged import convert as convert_module
from brazil_ingest.caged.convert import (
    ConservationError,
    Filters,
    Totals,
    convert,
    detect_encoding,
)
from brazil_ingest.caged.layout import Kind, LayoutError
from tests.conftest import HEADER_EXC, HEADER_MOV, row, write_text


def _read(path: Path) -> list[tuple[object, ...]]:
    with duckdb.connect() as con:
        return con.execute(
            "SELECT state_code, municipality_code, balance, salary, contract_hours "
            "FROM read_parquet(?) ORDER BY municipality_code",
            [str(path)],
        ).fetchall()


def test_convert_types_and_totals(tmp_path: Path) -> None:
    src = write_text(
        tmp_path / "CAGEDMOV202501.txt",
        HEADER_MOV,
        [row(202501, 41, 411990, 1, "2500,50"), row(202501, 35, 355030, -1, "1412,00")],
    )
    result = convert(src, Kind.MOV, tmp_path / "out.parquet")
    assert result.source == result.output == Totals(rows=2, balance=0)
    assert result.encoding == "utf-8"
    rows = _read(result.parquet)
    assert rows == [
        (35, 355030, -1, Decimal("1412.00"), Decimal("44.00")),
        (41, 411990, 1, Decimal("2500.50"), Decimal("44.00")),
    ]


def test_filters_by_state_and_municipality(tmp_path: Path) -> None:
    src = write_text(
        tmp_path / "CAGEDMOV202501.txt",
        HEADER_MOV,
        [
            row(202501, 41, 411990, 1),
            row(202501, 41, 410690, 1),
            row(202501, 35, 355030, -1),
        ],
    )
    by_state = convert(src, Kind.MOV, tmp_path / "a.parquet", Filters(state_codes=(41,)))
    assert by_state.output == Totals(2, 2)
    by_city = convert(src, Kind.MOV, tmp_path / "b.parquet", Filters(municipality_codes=(355030,)))
    assert by_city.output == Totals(1, -1)


def test_exc_file_keeps_exclusion_columns(tmp_path: Path) -> None:
    src = write_text(
        tmp_path / "CAGEDEXC202502.txt",
        HEADER_EXC,
        [row(202501, 41, 411990, 1, exc=True, declared=202502)],
    )
    result = convert(src, Kind.EXC, tmp_path / "exc.parquet")
    assert "excluded_month" in result.columns and "exclusion_flag" in result.columns


def test_latin1_file_is_detected(tmp_path: Path) -> None:
    src = write_text(
        tmp_path / "CAGEDMOV202501.txt",
        HEADER_MOV,
        [row(202501, 41, 411990, 1)],
        encoding="latin-1",
    )
    assert detect_encoding(src) == "latin-1"
    assert convert(src, Kind.MOV, tmp_path / "out.parquet").output == Totals(1, 1)


def test_conservation_error_leaves_no_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src = write_text(tmp_path / "CAGEDMOV202501.txt", HEADER_MOV, [row(202501, 41, 411990, 1)])
    monkeypatch.setattr(convert_module, "source_totals", lambda *a, **k: (Totals(2, 2), 0))
    out = tmp_path / "out.parquet"
    with pytest.raises(ConservationError, match="2 rows"):
        convert(src, Kind.MOV, out)
    assert not out.exists()
    assert not out.with_suffix(".parquet.tmp").exists()


def test_short_line_is_a_layout_error(tmp_path: Path) -> None:
    good = row(202501, 41, 411990, 1)
    bad = good.rsplit(";", 1)[0]
    src = write_text(tmp_path / "CAGEDMOV202501.txt", HEADER_MOV, [good, bad])
    with pytest.raises((LayoutError, duckdb.Error)):
        convert(src, Kind.MOV, tmp_path / "out.parquet")


def test_bad_number_fails_loudly(tmp_path: Path) -> None:
    line = row(202501, 41, 411990, 1).replace(";25;", ";vinte;", 1)
    src = write_text(tmp_path / "CAGEDMOV202501.txt", HEADER_MOV, [line])
    with pytest.raises(duckdb.ConversionException):
        convert(src, Kind.MOV, tmp_path / "out.parquet")


def test_scientific_notation_is_parsed_and_counted(tmp_path: Path) -> None:
    line = row(202502, 52, 520450, 1, "1,00e+12")
    src = write_text(tmp_path / "CAGEDMOV202502.txt", HEADER_MOV, [line, row(202502, 41, 1, 1)])
    result = convert(src, Kind.MOV, tmp_path / "out.parquet")
    assert result.scientific_values == 2  # salary and fixed salary of the first line
    with duckdb.connect() as con:
        top = con.execute(
            "SELECT max(salary), max(fixed_salary) FROM read_parquet(?)", [str(result.parquet)]
        ).fetchone()
    assert top == (Decimal("1000000000000.00"), Decimal("1000000000000.00"))


def test_memory_limit_is_applied_and_validated(tmp_path: Path) -> None:
    src = write_text(tmp_path / "CAGEDMOV202501.txt", HEADER_MOV, [row(202501, 41, 411990, 1)])
    ok = convert(src, Kind.MOV, tmp_path / "a.parquet", memory_limit="256MB")
    assert ok.output == Totals(1, 1)
    with pytest.raises(ValueError, match="memory limit"):
        convert(src, Kind.MOV, tmp_path / "b.parquet", memory_limit="1GB'; DROP")
