import pytest

from brazil_ingest.caged.layout import (
    Kind,
    LayoutError,
    normalize_header,
    output_columns,
    parse_file_name,
    resolve_header,
)
from tests.conftest import HEADER_EXC, HEADER_MOV


def test_normalize_header() -> None:
    assert normalize_header("saldoMovimentação ") == "saldomovimentacao"
    assert normalize_header("﻿competênciamov") == "competenciamov"
    assert normalize_header("raçacor") == "racacor"


def test_parse_file_name() -> None:
    assert parse_file_name("CAGEDMOV202512.7z") == (Kind.MOV, "202512")
    assert parse_file_name("cagedexc202101.txt") == (Kind.EXC, "202101")
    with pytest.raises(LayoutError):
        parse_file_name("Leia-me.txt")


def test_real_headers_resolve() -> None:
    mov = resolve_header(HEADER_MOV.split(";"), Kind.MOV)
    exc = resolve_header(HEADER_EXC.split(";"), Kind.EXC)
    assert all(c is not None for c in mov) and len(mov) == 28
    assert all(c is not None for c in exc) and len(exc) == 30
    assert [c.name for c in output_columns(Kind.MOV)] == [c.name for c in mov if c]


def test_columns_found_by_name_not_position() -> None:
    cells = HEADER_MOV.split(";")
    moved = [cells[6], *cells[:6], *cells[7:], "colunanova"]
    resolved = resolve_header(moved, Kind.MOV)
    assert resolved[0] is not None and resolved[0].name == "balance"
    assert resolved[-1] is None  # unknown column is dropped, not an error


def test_missing_column_is_an_error() -> None:
    cells = [c for c in HEADER_MOV.split(";") if c != "salário"]
    with pytest.raises(LayoutError, match="salario"):
        resolve_header(cells, Kind.MOV)


def test_exc_needs_exclusion_columns() -> None:
    with pytest.raises(LayoutError, match="competenciaexc"):
        resolve_header(HEADER_MOV.split(";"), Kind.EXC)


def test_duplicate_column_is_an_error() -> None:
    with pytest.raises(LayoutError, match="twice"):
        resolve_header([*HEADER_MOV.split(";"), "uf"], Kind.MOV)
