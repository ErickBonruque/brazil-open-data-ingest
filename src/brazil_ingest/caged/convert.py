"""Text file -> typed Parquet, with an independent conservation check."""

from __future__ import annotations

import codecs
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from brazil_ingest.caged.layout import (
    COLUMNS,
    Column,
    Kind,
    LayoutError,
    output_columns,
    resolve_header,
)

_DECIMAL_PREFIX = "DECIMAL"
_DECIMAL_NAMES = frozenset(c.name for c in COLUMNS if c.sql_type.startswith(_DECIMAL_PREFIX))


class ConservationError(RuntimeError):
    """The Parquet output does not keep the row count or the balance of the source file."""


@dataclass(frozen=True)
class Filters:
    """Optional row filters by IBGE code. Empty means "all of Brazil"."""

    state_codes: tuple[int, ...] = ()
    municipality_codes: tuple[int, ...] = ()

    def keeps(self, state: int, municipality: int) -> bool:
        if self.state_codes and state not in self.state_codes:
            return False
        return not (self.municipality_codes and municipality not in self.municipality_codes)

    def as_dict(self) -> dict[str, list[int]]:
        return {
            "state_codes": sorted(self.state_codes),
            "municipality_codes": sorted(self.municipality_codes),
        }


@dataclass(frozen=True)
class Totals:
    rows: int
    balance: int


@dataclass(frozen=True)
class ConvertResult:
    parquet: Path
    source: Totals
    output: Totals
    encoding: str
    columns: list[str] = field(default_factory=list)
    scientific_values: int = 0


def detect_encoding(path: Path, probe_bytes: int = 1 << 20) -> str:
    """``"utf-8"`` if the first MB decodes as UTF-8, else ``"latin-1"``.

    The published files are UTF-8 today. The check keeps the reader correct if an edition
    comes in Latin-1, which is common in older Brazilian government data.
    """
    decoder = codecs.getincrementaldecoder("utf-8")()
    with path.open("rb") as fh:
        try:
            decoder.decode(fh.read(probe_bytes), final=False)
        except UnicodeDecodeError:
            return "latin-1"
    return "utf-8"


def read_header(path: Path, encoding: str) -> list[str]:
    with path.open(encoding=encoding, newline="") as fh:
        line = fh.readline()
    header = line.lstrip("\ufeff").rstrip("\r\n")
    if not header:
        raise LayoutError(f"{path.name}: empty file")
    return header.split(";")


def source_totals(
    path: Path, names: Sequence[str], filters: Filters, encoding: str
) -> tuple[Totals, int]:
    """Count rows and sum the balance with plain Python, independent of DuckDB.

    ``names`` are the output names of the header cells, in file order. The second value is
    the number of decimal cells written in scientific notation (``1,00e+12``): the source
    has a few of them, always absurd salaries, and they are worth a look.
    """
    names = list(names)
    i_balance = names.index("balance")
    i_state = names.index("state_code")
    i_city = names.index("municipality_code")
    i_decimals = [i for i, n in enumerate(names) if n in _DECIMAL_NAMES]
    width = len(names)
    rows = 0
    balance = 0
    scientific = 0
    with path.open(encoding=encoding, newline="") as fh:
        next(fh)
        for lineno, line in enumerate(fh, start=2):
            line = line.rstrip("\r\n")
            if not line:
                continue
            cells = line.split(";")
            if len(cells) != width:
                raise LayoutError(f"{path.name}:{lineno}: {len(cells)} fields, header has {width}")
            if filters.keeps(int(cells[i_state]), int(cells[i_city])):
                rows += 1
                balance += int(cells[i_balance])
                scientific += sum(1 for i in i_decimals if "e" in cells[i].lower())
    return Totals(rows, balance), scientific


def _select_list(columns: list[Column]) -> str:
    parts = []
    for col in columns:
        if col.sql_type.startswith(_DECIMAL_PREFIX):
            # Decimal comma. A few cells use scientific notation, which DECIMAL cannot parse:
            # those go through DOUBLE first.
            text = f"REPLACE(TRIM({col.name}), ',', '.')"
            expr = (
                f"CASE WHEN contains(lower({col.name}), 'e') "
                f"THEN CAST(CAST({text} AS DOUBLE) AS {col.sql_type}) "
                f"ELSE CAST({text} AS {col.sql_type}) END"
            )
        else:
            expr = f"CAST(NULLIF(TRIM({col.name}), '') AS {col.sql_type})"
        parts.append(f"{expr} AS {col.name}")
    return ",\n    ".join(parts)


def convert(
    text_file: Path,
    kind: Kind,
    out_file: Path,
    filters: Filters | None = None,
    *,
    threads: int | None = None,
) -> ConvertResult:
    """Write one typed Parquet file (zstd) and check it against the source text."""
    filters = filters or Filters()
    encoding = detect_encoding(text_file)
    header = read_header(text_file, encoding)
    resolved = resolve_header(header, kind)
    names = [c.name if c else f"_unused_{i}" for i, c in enumerate(resolved)]
    columns = output_columns(kind)

    where: list[str] = []
    if filters.state_codes:
        where.append(f"CAST(state_code AS INTEGER) IN ({_int_list(filters.state_codes)})")
    if filters.municipality_codes:
        codes = _int_list(filters.municipality_codes)
        where.append(f"CAST(municipality_code AS INTEGER) IN ({codes})")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    out_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_file.with_suffix(".parquet.tmp")
    con = duckdb.connect()
    try:
        if threads:
            con.execute(f"SET threads = {int(threads)}")
        # A view cannot take prepared parameters; every value below is quoted or generated here.
        names_sql = ", ".join(f"'{n}'" for n in names)
        con.execute(
            f"CREATE TEMP VIEW src AS SELECT * FROM read_csv('{_sql_path(text_file)}', "
            f"delim=';', header=true, names=[{names_sql}], all_varchar=true, "
            f"encoding='{encoding}', quote='', escape='')"
        )
        con.execute(
            f"COPY (SELECT\n    {_select_list(columns)}\nFROM src {where_sql})\n"
            f"TO '{_sql_path(tmp)}' (FORMAT parquet, COMPRESSION zstd)"
        )
        row = con.execute(
            "SELECT COUNT(*), COALESCE(SUM(balance), 0) FROM read_parquet(?)", [str(tmp)]
        ).fetchone()
    finally:
        con.close()
    if row is None:  # pragma: no cover - COUNT(*) always returns a row
        raise RuntimeError("empty result from DuckDB")
    output = Totals(int(row[0]), int(row[1]))
    source, scientific = source_totals(text_file, names, filters, encoding)
    if output != source:
        tmp.unlink(missing_ok=True)
        raise ConservationError(
            f"{text_file.name}: source has {source.rows} rows / balance {source.balance}, "
            f"Parquet has {output.rows} rows / balance {output.balance}"
        )
    tmp.replace(out_file)
    return ConvertResult(out_file, source, output, encoding, [c.name for c in columns], scientific)


def _int_list(values: Sequence[int]) -> str:
    return ", ".join(str(int(v)) for v in values)


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")
