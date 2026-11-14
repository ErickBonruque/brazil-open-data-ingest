"""File layout of the Novo CAGED microdata.

The server publishes three files per month:

* ``MOV``: job movements (hires and separations) declared on time.
* ``FOR``: movements of earlier months declared late ("fora do prazo").
* ``EXC``: earlier movements that employers removed ("exclusões").

The files are ``;``-separated text with a header line. Columns are read **by header name**,
never by position, so a column that moves does not shift the data. The Portuguese header names
are mapped to English snake_case names here; the mapping is the data dictionary of this project.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum


class LayoutError(ValueError):
    """The file does not have the columns this project expects."""


class Kind(StrEnum):
    MOV = "mov"
    FOR = "for"
    EXC = "exc"


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    source: str  # normalized source header (no accents, lower case, no spaces)
    description: str


# Order follows the source file. ``kinds`` is implicit: EXC_ONLY columns exist only in EXC files.
COLUMNS: tuple[Column, ...] = (
    Column("reference_month", "INTEGER", "competenciamov", "Month of the movement, YYYYMM"),
    Column("region", "TINYINT", "regiao", "Region code (1 North .. 5 Central-West)"),
    Column("state_code", "TINYINT", "uf", "State IBGE code (2 digits)"),
    Column("municipality_code", "INTEGER", "municipio", "Municipality IBGE code (6 digits)"),
    Column("cnae_section", "VARCHAR", "secao", "Economic activity section (CNAE 2.0, A..U)"),
    Column("cnae_subclass", "INTEGER", "subclasse", "Economic activity subclass (CNAE 2.0)"),
    Column("balance", "TINYINT", "saldomovimentacao", "+1 for a hire, -1 for a separation"),
    Column("occupation_cbo", "INTEGER", "cbo2002ocupacao", "Occupation code (CBO 2002)"),
    Column("worker_category", "SMALLINT", "categoria", "Worker category code"),
    Column("education_level", "TINYINT", "graudeinstrucao", "Education level code"),
    Column("age", "SMALLINT", "idade", "Age in years"),
    Column("contract_hours", "DECIMAL(5,2)", "horascontratuais", "Weekly contract hours"),
    Column("race_color", "TINYINT", "racacor", "Race/color code"),
    Column("sex", "TINYINT", "sexo", "Sex code"),
    Column("employer_type", "TINYINT", "tipoempregador", "Employer type code"),
    Column("establishment_type", "TINYINT", "tipoestabelecimento", "Establishment type code"),
    Column("movement_type", "SMALLINT", "tipomovimentacao", "Movement type code"),
    Column("disability_type", "TINYINT", "tipodedeficiencia", "Disability type code"),
    Column("intermittent", "TINYINT", "indtrabintermitente", "Intermittent work flag"),
    Column("part_time", "TINYINT", "indtrabparcial", "Part-time work flag"),
    Column("salary", "DECIMAL(18,2)", "salario", "Monthly salary (BRL)"),
    Column("establishment_size", "TINYINT", "tamestabjan", "Establishment size band in January"),
    Column("apprentice", "TINYINT", "indicadoraprendiz", "Apprentice flag"),
    Column("information_source", "TINYINT", "origemdainformacao", "Source system code"),
    Column("declared_month", "INTEGER", "competenciadec", "Month of the declaration, YYYYMM"),
    Column("excluded_month", "INTEGER", "competenciaexc", "Month of the exclusion, YYYYMM"),
    Column("exclusion_flag", "TINYINT", "indicadordeexclusao", "Exclusion flag"),
    Column("late_flag", "TINYINT", "indicadordeforadoprazo", "Declared late flag"),
    Column("salary_unit", "TINYINT", "unidadesalariocodigo", "Unit of the fixed salary"),
    Column("fixed_salary", "DECIMAL(18,2)", "valorsalariofixo", "Fixed salary value (BRL)"),
)

EXC_ONLY = frozenset({"competenciaexc", "indicadordeexclusao"})

_BY_SOURCE = {c.source: c for c in COLUMNS}
_FILE_RE = re.compile(r"^CAGED(MOV|FOR|EXC)(\d{6})\.(7z|txt)$", re.IGNORECASE)


def normalize_header(name: str) -> str:
    """``"saldoMovimentação "`` -> ``"saldomovimentacao"``."""
    decomposed = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]", "", ascii_only.lower())


def parse_file_name(name: str) -> tuple[Kind, str]:
    """``CAGEDMOV202512.7z`` -> ``(Kind.MOV, "202512")``."""
    match = _FILE_RE.match(name)
    if not match:
        raise LayoutError(f"not a Novo CAGED file name: {name!r}")
    return Kind(match.group(1).lower()), match.group(2)


def expected_sources(kind: Kind) -> list[str]:
    return [c.source for c in COLUMNS if kind is Kind.EXC or c.source not in EXC_ONLY]


def resolve_header(header: list[str], kind: Kind) -> list[Column | None]:
    """Map each header cell to a column. Unknown cells map to ``None`` (they are dropped).

    Raises ``LayoutError`` if a required column is missing or a column appears twice.
    """
    normalized = [normalize_header(h) for h in header]
    seen: set[str] = set()
    for name in normalized:
        if name in seen and name in _BY_SOURCE:
            raise LayoutError(f"column appears twice: {name}")
        seen.add(name)
    missing = [s for s in expected_sources(kind) if s not in seen]
    if missing:
        raise LayoutError(f"{kind.value.upper()} file is missing columns: {', '.join(missing)}")
    return [_BY_SOURCE.get(name) for name in normalized]


def output_columns(kind: Kind) -> list[Column]:
    """Columns of the Parquet output for one kind, in a stable order."""
    sources = set(expected_sources(kind))
    return [c for c in COLUMNS if c.source in sources]
