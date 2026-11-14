"""CNPJ text file -> typed Parquet, with an independent check in plain Python."""

from __future__ import annotations

import csv
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from brazil_ingest.cnpj.cnpj_number import is_valid_cnpj
from brazil_ingest.cnpj.layout import (
    COLUMNS,
    INDIVIDUAL_ENTREPRENEUR,
    Column,
    LayoutError,
    Table,
    output_columns,
)
from brazil_ingest.partitions import duck_connect, sql_str
from brazil_ingest.text import detect_encoding

_EMPTY_DATES = ("", "0", "00000000")
# Guards against a corrupt or hostile archive. Real files: up to about 10 GB of text, and text
# compresses about 4 to 10 times.
MAX_UNCOMPRESSED = 40 * 1024**3
MAX_RATIO = 100


class CheckError(RuntimeError):
    """The Parquet output does not match the independent reading of the source text."""


@dataclass
class SourceScan:
    """What a plain Python pass over the text file found."""

    rows: int = 0
    dates: Counter[str] = field(default_factory=Counter)  # non-empty date cells per column
    invalid_cnpj: int = 0  # establishments whose check digits do not match
    alphanumeric_cnpj: int = 0


@dataclass(frozen=True)
class ConvertResult:
    parquet: Path
    rows: int
    encoding: str
    columns: list[str]
    invalid_dates: dict[str, int]
    invalid_cnpj: int
    alphanumeric_cnpj: int


def extract_member(archive: Path, dest_dir: Path, *, max_bytes: int = MAX_UNCOMPRESSED) -> Path:
    """Extract the only file inside a CNPJ ZIP and return its path."""
    with zipfile.ZipFile(archive) as zf:
        members = [m for m in zf.infolist() if not m.is_dir()]
        if len(members) != 1:
            raise LayoutError(f"{archive.name}: expected 1 file, found {len(members)}")
        member = members[0]
        name = Path(member.filename).name  # never trust paths inside the archive
        limit = min(max_bytes, max(member.compress_size, 1) * MAX_RATIO)
        if member.file_size > limit:
            raise LayoutError(f"{archive.name}: {member.file_size} bytes uncompressed is too big")
        dest_dir.mkdir(parents=True, exist_ok=True)
        out = dest_dir / (name or "data.csv")
        written = 0
        with zf.open(member) as src, out.open("wb") as dst:
            # The size in the ZIP header can lie: count the real bytes too.
            while block := src.read(1 << 20):
                written += len(block)
                if written > limit:
                    dst.close()
                    out.unlink()
                    raise LayoutError(f"{archive.name}: more data than the header says")
                dst.write(block)
    return out


def scan_source(path: Path, table: Table, encoding: str) -> SourceScan:
    """Count records with the ``csv`` module (not DuckDB) and check each one."""
    columns = COLUMNS[table]
    width = len(columns)
    date_idx = [(i, c.name) for i, c in enumerate(columns) if c.sql_type == "DATE"]
    check_cnpj = table is Table.ESTABLISHMENTS
    scan = SourceScan()
    with path.open(encoding=encoding, newline="") as fh:
        for record in csv.reader(fh, delimiter=";", quotechar='"'):
            scan.rows += 1
            if len(record) != width:
                raise LayoutError(
                    f"{path.name}: record {scan.rows} has {len(record)} fields, expected {width}"
                )
            for i, name in date_idx:
                if record[i].strip() not in _EMPTY_DATES:
                    scan.dates[name] += 1
            if check_cnpj:
                number = record[0] + record[1] + record[2]
                if not number.isdigit():
                    scan.alphanumeric_cnpj += 1
                if not is_valid_cnpj(number):
                    scan.invalid_cnpj += 1
    return scan


def _expr(col: Column, table: Table, keep_personal: bool) -> str:
    raw = f"NULLIF(TRIM({col.name}), '')"
    if col.sql_type == "VARCHAR":
        expr = raw
    elif col.sql_type == "DATE":
        expr = (
            f"CASE WHEN TRIM({col.name}) IN ('', '0', '00000000') THEN NULL "
            f"ELSE CAST(try_strptime(TRIM({col.name}), '%Y%m%d') AS DATE) END"
        )
    elif col.sql_type.startswith("DECIMAL"):
        expr = f"CAST(REPLACE({raw}, ',', '.') AS {col.sql_type})"
    elif col.sql_type == "INTEGER[]":
        # A list cast, not a lambda: the lambda syntax changed between DuckDB versions.
        expr = f"CAST(string_split(REPLACE({raw}, ' ', ''), ',') AS INTEGER[])"
    else:
        expr = f"CAST({raw} AS {col.sql_type})"
    if table is Table.COMPANIES and col.name == "company_name" and not keep_personal:
        # Fail closed: a missing or unreadable legal nature also hides the name.
        nature = "TRY_CAST(NULLIF(TRIM(legal_nature), '') AS SMALLINT)"
        expr = (
            f"CASE WHEN COALESCE({nature}, {INDIVIDUAL_ENTREPRENEUR}) = "
            f"{INDIVIDUAL_ENTREPRENEUR} THEN NULL ELSE {expr} END"
        )
    return f"{expr} AS {col.name}"


def convert(
    text_file: Path,
    table: Table,
    out_file: Path,
    *,
    keep_personal: bool = False,
    memory_limit: str | None = None,
) -> ConvertResult:
    encoding = detect_encoding(text_file)
    source_names = [c.name for c in COLUMNS[table]]
    columns = output_columns(table, keep_personal=keep_personal)
    select = ",\n    ".join(_expr(c, table, keep_personal) for c in columns)
    names_sql = ", ".join(sql_str(n) for n in source_names)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_file.with_suffix(".parquet.tmp")
    date_cols = [c.name for c in columns if c.sql_type == "DATE"]
    with duck_connect(memory_limit=memory_limit) as con:
        # A view cannot take prepared parameters; every value below is quoted or generated here.
        con.execute(
            f"CREATE TEMP VIEW src AS SELECT * FROM read_csv({sql_str(str(text_file))}, "
            f"delim=';', quote='\"', escape='\"', header=false, names=[{names_sql}], "
            f"all_varchar=true, encoding={sql_str(encoding)}, strict_mode=true)"
        )
        con.execute(
            f"COPY (SELECT\n    {select}\nFROM src) TO {sql_str(str(tmp))} "
            "(FORMAT parquet, COMPRESSION zstd)"
        )
        counts = ", ".join(["COUNT(*)", *(f"COUNT({c})" for c in date_cols)])
        row = con.execute(f"SELECT {counts} FROM read_parquet({sql_str(str(tmp))})").fetchone()
    if row is None:  # pragma: no cover - COUNT(*) always returns a row
        raise RuntimeError("empty result from DuckDB")

    scan = scan_source(text_file, table, encoding)
    rows = int(row[0])
    if rows != scan.rows:
        tmp.unlink(missing_ok=True)
        raise CheckError(f"{text_file.name}: source has {scan.rows} records, Parquet has {rows}")
    parsed = dict(zip(date_cols, (int(v) for v in row[1:]), strict=True))
    invalid_dates = {c: scan.dates[c] - parsed[c] for c in date_cols if scan.dates[c] != parsed[c]}
    tmp.replace(out_file)
    return ConvertResult(
        parquet=out_file,
        rows=rows,
        encoding=encoding,
        columns=[c.name for c in columns],
        invalid_dates=invalid_dates,
        invalid_cnpj=scan.invalid_cnpj,
        alphanumeric_cnpj=scan.alphanumeric_cnpj,
    )
