import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from brazil_ingest.cnpj import convert as convert_module
from brazil_ingest.cnpj.cnpj_number import check_digits, is_valid_cnpj
from brazil_ingest.cnpj.convert import CheckError, SourceScan, convert, extract_member
from brazil_ingest.cnpj.layout import LayoutError, Table, output_columns, parse_zip_name


def line(*fields: str) -> str:
    return ";".join(f'"{f}"' for f in fields)


def write(path: Path, lines: list[str], encoding: str = "latin-1") -> Path:
    path.write_bytes(("\n".join(lines) + "\n").encode(encoding))
    return path


def establishment(
    base: str, order: str, *, start: str = "20200115", uf: str = "PR", dv: str | None = None
) -> str:
    dv = dv or check_digits(base + order)
    return line(
        base,
        order,
        dv,
        "1",
        "LOJA; CENTRO",
        "02",
        "20200115",
        "00",
        "",
        "",
        start,
        "4781400",
        "4782201,4789099",
        "RUA",
        "DAS FLORES",
        "10",
        "",
        "CENTRO",
        "85000000",
        uf,
        "7535",
        "45",
        "32220000",
        "",
        "",
        "",
        "",
        "contato@example.com",
        "",
        "",
    )


def query(path: Path, sql: str) -> list[tuple[object, ...]]:
    with duckdb.connect() as con:
        return con.execute(sql.replace("{t}", f"read_parquet('{path}')")).fetchall()


def test_check_digits_numeric_and_alphanumeric() -> None:
    assert check_digits("112223330001") == "81"
    assert is_valid_cnpj("11222333000181")
    assert not is_valid_cnpj("11222333000182")
    assert check_digits("12ABC34501DE") == "35"  # example published by the Receita
    assert is_valid_cnpj("12ABC34501DE35")
    assert not is_valid_cnpj("12ABC34501DE3")
    with pytest.raises(ValueError):
        check_digits("12.ABC.345/01")


def test_parse_zip_name() -> None:
    assert parse_zip_name("Estabelecimentos3.zip") == (Table.ESTABLISHMENTS, 3)
    assert parse_zip_name("Simples.zip") == (Table.SIMPLES, 0)
    assert parse_zip_name("Qualificacoes.zip") == (Table.QUALIFICATIONS, 0)
    with pytest.raises(LayoutError):
        parse_zip_name("cnpj.tar.gz")


def test_companies_types_and_individual_names(tmp_path: Path) -> None:
    src = write(
        tmp_path / "emp.csv",
        [
            line("00000001", "ACME INDÚSTRIA LTDA", "2062", "49", "150000,50", "05", ""),
            line("00000002", "JOAO DA SILVA 12345678900", "2135", "50", "1000,00", "01", ""),
        ],
    )
    result = convert(src, Table.COMPANIES, tmp_path / "out.parquet")
    assert result.rows == 2 and result.encoding == "latin-1"
    assert query(result.parquet, "SELECT * FROM {t} ORDER BY cnpj_base") == [
        ("00000001", "ACME INDÚSTRIA LTDA", 2062, 49, Decimal("150000.50"), 5, None),
        ("00000002", None, 2135, 50, Decimal("1000.00"), 1, None),
    ]
    kept = convert(src, Table.COMPANIES, tmp_path / "kept.parquet", keep_personal=True)
    names = query(kept.parquet, "SELECT company_name FROM {t} ORDER BY cnpj_base")
    assert names[1] == ("JOAO DA SILVA 12345678900",)


def test_establishments(tmp_path: Path) -> None:
    alnum = establishment("12ABC345", "01DE")
    bad_dv = establishment("00000002", "0001", dv="00")
    bad_date = establishment("00000003", "0001", start="20230231")
    src = write(tmp_path / "est.csv", [establishment("00000001", "0001"), alnum, bad_dv, bad_date])
    result = convert(src, Table.ESTABLISHMENTS, tmp_path / "out.parquet")
    assert result.rows == 4
    assert result.alphanumeric_cnpj == 1
    assert result.invalid_cnpj == 1
    assert result.invalid_dates == {"activity_start_date": 1}
    assert "email" not in result.columns and "street" not in result.columns
    rows = query(
        result.parquet,
        "SELECT cnpj_base, trade_name, activity_start_date, secondary_cnaes, state, "
        "municipality_code FROM {t} ORDER BY cnpj_base LIMIT 1",
    )
    assert rows == [("00000001", "LOJA; CENTRO", date(2020, 1, 15), [4782201, 4789099], "PR", 7535)]


def test_keep_personal_adds_contact_columns() -> None:
    names = [c.name for c in output_columns(Table.ESTABLISHMENTS, keep_personal=True)]
    assert len(names) == 30 and "email" in names
    assert len(output_columns(Table.ESTABLISHMENTS, keep_personal=False)) == 17
    partners = [c.name for c in output_columns(Table.PARTNERS, keep_personal=False)]
    assert "partner_name" not in partners and "age_band" in partners


def test_lookup_table_utf8(tmp_path: Path) -> None:
    src = write(tmp_path / "cnae.csv", [line("0111301", "Cultivo de arroz")], encoding="utf-8")
    result = convert(src, Table.CNAES, tmp_path / "out.parquet")
    assert query(result.parquet, "SELECT * FROM {t}") == [("0111301", "Cultivo de arroz")]


def test_wrong_field_count_is_an_error(tmp_path: Path) -> None:
    src = write(tmp_path / "emp.csv", [line("00000001", "ACME", "2062")])
    with pytest.raises((LayoutError, duckdb.Error)):
        convert(src, Table.COMPANIES, tmp_path / "out.parquet")


def test_count_mismatch_publishes_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = write(tmp_path / "emp.csv", [line("00000001", "ACME", "2062", "49", "1,00", "05", "")])
    monkeypatch.setattr(convert_module, "scan_source", lambda *a, **k: SourceScan(rows=5))
    out = tmp_path / "out.parquet"
    with pytest.raises(CheckError, match="5 records"):
        convert(src, Table.COMPANIES, out)
    assert not out.exists() and not out.with_suffix(".parquet.tmp").exists()


def test_extract_member_ignores_paths_inside_the_zip(tmp_path: Path) -> None:
    archive = tmp_path / "Cnaes.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("../../evil/F.K03200$Z.D61010.CNAECSV", '"1";"x"\n')
    out = extract_member(archive, tmp_path / "x")
    assert out.parent == tmp_path / "x" and out.read_text() == '"1";"x"\n'
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("a", "1")
        zf.writestr("b", "2")
    with pytest.raises(LayoutError, match="expected 1"):
        extract_member(archive, tmp_path / "y")
