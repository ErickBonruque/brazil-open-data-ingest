"""File layout of the CNPJ open data (Brazilian company registry, Receita Federal).

Each monthly release has four main tables split in up to ten ZIP files (``Empresas0.zip`` ..
``Empresas9.zip``) and six small lookup tables. The text files inside have **no header**, use
``;`` between fields, ``"`` around every field and Latin-1 encoding. Column names below follow the
official layout document ("Novo Layout para os Dados Abertos do CNPJ"), in English.

``personal`` marks columns that identify people: addresses and contacts (for an individual
entrepreneur they are the person's home and phone), and the names and documents of partners.
They are dropped unless the caller asks to keep them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum


class LayoutError(ValueError):
    """A file does not match the expected layout."""


class Table(StrEnum):
    COMPANIES = "companies"
    ESTABLISHMENTS = "establishments"
    SIMPLES = "simples"
    PARTNERS = "partners"
    CNAES = "cnaes"
    REASONS = "reasons"
    MUNICIPALITIES = "municipalities"
    LEGAL_NATURES = "legal_natures"
    COUNTRIES = "countries"
    QUALIFICATIONS = "qualifications"


# Source ZIP name (without the part number) -> table.
_ZIP_NAMES = {
    "empresas": Table.COMPANIES,
    "estabelecimentos": Table.ESTABLISHMENTS,
    "simples": Table.SIMPLES,
    "socios": Table.PARTNERS,
    "cnaes": Table.CNAES,
    "motivos": Table.REASONS,
    "municipios": Table.MUNICIPALITIES,
    "naturezas": Table.LEGAL_NATURES,
    "paises": Table.COUNTRIES,
    "qualificacoes": Table.QUALIFICATIONS,
}
_ZIP_RE = re.compile(r"^([A-Za-z]+?)(\d?)\.zip$")

# Legal nature 2135 is "Empresário (Individual)", which includes the MEI micro-entrepreneurs.
# Their company name is the owner's own name (sometimes followed by the CPF number).
INDIVIDUAL_ENTREPRENEUR = 2135


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str  # target type; VARCHAR, DATE, DECIMAL(...), integers, VARCHAR[]
    description: str
    personal: bool = False


def _c(name: str, sql_type: str, description: str, personal: bool = False) -> Column:
    return Column(name, sql_type, description, personal)


LOOKUP = (_c("code", "VARCHAR", "Code"), _c("description", "VARCHAR", "Description"))

COLUMNS: dict[Table, tuple[Column, ...]] = {
    Table.COMPANIES: (
        _c("cnpj_base", "VARCHAR", "First 8 characters of the CNPJ (can be alphanumeric)"),
        _c("company_name", "VARCHAR", "Legal name; NULL for individual entrepreneurs"),
        _c("legal_nature", "SMALLINT", "Legal nature code"),
        _c("responsible_qualification", "SMALLINT", "Qualification of the responsible person"),
        _c("share_capital", "DECIMAL(18,2)", "Share capital (BRL)"),
        _c("company_size", "TINYINT", "0 not informed, 1 micro, 3 small, 5 other"),
        _c("federal_entity", "VARCHAR", "Responsible federal entity (public bodies only)"),
    ),
    Table.ESTABLISHMENTS: (
        _c("cnpj_base", "VARCHAR", "First 8 characters of the CNPJ"),
        _c("cnpj_order", "VARCHAR", "Characters 9 to 12 of the CNPJ"),
        _c("cnpj_check", "VARCHAR", "Check digits (characters 13 and 14)"),
        _c("head_office", "TINYINT", "1 head office, 2 branch"),
        _c("trade_name", "VARCHAR", "Trade name"),
        _c("registration_status", "TINYINT", "1 null, 2 active, 3 suspended, 4 unfit, 8 closed"),
        _c("status_date", "DATE", "Date of the registration status"),
        _c("status_reason", "SMALLINT", "Reason code of the registration status"),
        _c("foreign_city", "VARCHAR", "City name, for establishments abroad"),
        _c("country_code", "SMALLINT", "Country code"),
        _c("activity_start_date", "DATE", "Start date of the activity"),
        _c("main_cnae", "INTEGER", "Main economic activity (CNAE)"),
        _c("secondary_cnaes", "INTEGER[]", "Secondary economic activities (CNAE)"),
        _c("street_type", "VARCHAR", "Street type", personal=True),
        _c("street", "VARCHAR", "Street", personal=True),
        _c("street_number", "VARCHAR", "Number", personal=True),
        _c("complement", "VARCHAR", "Address complement", personal=True),
        _c("district", "VARCHAR", "District", personal=True),
        _c("postal_code", "VARCHAR", "Postal code (CEP)", personal=True),
        _c("state", "VARCHAR", "State abbreviation (UF)"),
        _c("municipality_code", "SMALLINT", "Municipality code of the Receita (not IBGE)"),
        _c("phone1_area", "VARCHAR", "Area code 1", personal=True),
        _c("phone1", "VARCHAR", "Phone 1", personal=True),
        _c("phone2_area", "VARCHAR", "Area code 2", personal=True),
        _c("phone2", "VARCHAR", "Phone 2", personal=True),
        _c("fax_area", "VARCHAR", "Fax area code", personal=True),
        _c("fax", "VARCHAR", "Fax", personal=True),
        _c("email", "VARCHAR", "Email", personal=True),
        _c("special_status", "VARCHAR", "Special status"),
        _c("special_status_date", "DATE", "Date of the special status"),
    ),
    Table.SIMPLES: (
        _c("cnpj_base", "VARCHAR", "First 8 characters of the CNPJ"),
        _c("simples_option", "VARCHAR", "S yes, N no, empty other"),
        _c("simples_option_date", "DATE", "Date of the Simples option"),
        _c("simples_exclusion_date", "DATE", "Date of the Simples exclusion"),
        _c("mei_option", "VARCHAR", "S yes, N no, empty other"),
        _c("mei_option_date", "DATE", "Date of the MEI option"),
        _c("mei_exclusion_date", "DATE", "Date of the MEI exclusion"),
    ),
    Table.PARTNERS: (
        _c("cnpj_base", "VARCHAR", "First 8 characters of the CNPJ"),
        _c("partner_type", "TINYINT", "1 company, 2 person, 3 foreign"),
        _c("partner_name", "VARCHAR", "Partner name", personal=True),
        _c("partner_document", "VARCHAR", "Partner CPF (masked) or CNPJ", personal=True),
        _c("partner_qualification", "SMALLINT", "Partner qualification code"),
        _c("entry_date", "DATE", "Date the partner joined"),
        _c("country_code", "SMALLINT", "Country code of a foreign partner"),
        _c("representative_document", "VARCHAR", "Legal representative CPF", personal=True),
        _c("representative_name", "VARCHAR", "Legal representative name", personal=True),
        _c("representative_qualification", "SMALLINT", "Legal representative qualification"),
        _c("age_band", "TINYINT", "Age band code"),
    ),
    **{
        t: LOOKUP
        for t in (
            Table.CNAES,
            Table.REASONS,
            Table.MUNICIPALITIES,
            Table.LEGAL_NATURES,
            Table.COUNTRIES,
            Table.QUALIFICATIONS,
        )
    },
}

MAIN_TABLES = (Table.COMPANIES, Table.ESTABLISHMENTS, Table.SIMPLES, Table.PARTNERS)


def parse_zip_name(name: str) -> tuple[Table, int]:
    """``Estabelecimentos3.zip`` -> ``(Table.ESTABLISHMENTS, 3)``. Single files get part 0."""
    match = _ZIP_RE.match(name)
    if not match or match.group(1).lower() not in _ZIP_NAMES:
        raise LayoutError(f"not a CNPJ open data file name: {name!r}")
    return _ZIP_NAMES[match.group(1).lower()], int(match.group(2) or 0)


def output_columns(table: Table, *, keep_personal: bool) -> list[Column]:
    return [c for c in COLUMNS[table] if keep_personal or not c.personal]
