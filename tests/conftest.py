"""Synthetic Novo CAGED files with the real header (UTF-8, CRLF, accents, ``;``)."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path

import py7zr
import pytest

HEADER_MOV = (
    "competênciamov;região;uf;município;seção;subclasse;saldomovimentação;cbo2002ocupação;"
    "categoria;graudeinstrução;idade;horascontratuais;raçacor;sexo;tipoempregador;"
    "tipoestabelecimento;tipomovimentação;tipodedeficiência;indtrabintermitente;indtrabparcial;"
    "salário;tamestabjan;indicadoraprendiz;origemdainformação;competênciadec;"
    "indicadordeforadoprazo;unidadesaláriocódigo;valorsaláriofixo"
)
HEADER_EXC = HEADER_MOV.replace(
    "competênciadec;", "competênciadec;competênciaexc;indicadordeexclusão;"
)


def row(
    ref: int,
    state: int,
    city: int,
    balance: int,
    salary: str = "1521,00",
    *,
    exc: bool = False,
    declared: int | None = None,
) -> str:
    """One data line. Only the fields the tests look at are parameters."""
    declared = declared or ref
    cells = [
        str(ref),
        "4",
        str(state),
        str(city),
        "G",
        "4789001",
        str(balance),
        "414140",
        "101",
        "7",
        "25",
        "44,00",
        "3",
        "1",
        "0",
        "1",
        "97" if balance > 0 else "31",
        "0",
        "0",
        "0",
        salary,
        "6",
        "0",
        "1",
        str(declared),
    ]
    if exc:
        cells += [str(declared), "1"]
    cells += ["0", "5", salary]
    return ";".join(cells)


def write_text(path: Path, header: str, rows: Sequence[str], encoding: str = "utf-8") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(("\r\n".join([header, *rows]) + "\r\n").encode(encoding))
    return path


def write_archive(folder: Path, kind: str, compact: str, rows: Sequence[str]) -> Path:
    """``<folder>/<YYYY>/<YYYYMM>/CAGED<KIND><YYYYMM>.7z``, like the server."""
    header = HEADER_EXC if kind == "EXC" else HEADER_MOV
    month_dir = folder / compact[:4] / compact
    month_dir.mkdir(parents=True, exist_ok=True)
    txt = write_text(month_dir / f"CAGED{kind}{compact}.txt", header, rows)
    archive = month_dir / f"CAGED{kind}{compact}.7z"
    with py7zr.SevenZipFile(archive, "w") as zf:
        zf.write(txt, arcname=txt.name)
    txt.unlink()
    return archive


ArchiveFactory = Callable[[str, str, Sequence[str]], Path]


@pytest.fixture
def archives(tmp_path: Path) -> tuple[Path, ArchiveFactory]:
    root = tmp_path / "server"

    def make(kind: str, compact: str, rows: Sequence[str]) -> Path:
        return write_archive(root, kind, compact, rows)

    return root, make
