from pathlib import Path

from brazil_ingest.text import detect_encoding


def test_latin1_after_a_long_ascii_start(tmp_path: Path) -> None:
    # 2 MB of ASCII, then one Latin-1 name: a probe of the first MB would say UTF-8.
    path = tmp_path / "f.csv"
    path.write_bytes(b'"00000001";"ACME"\n' * 120_000 + '"00000002";"UNIÃO"\n'.encode("latin-1"))
    assert detect_encoding(path) == "latin-1"


def test_utf8_and_pure_ascii(tmp_path: Path) -> None:
    utf8 = tmp_path / "u.csv"
    utf8.write_bytes(b"x;y\n" * 300_000 + "a;São Paulo\n".encode())
    assert detect_encoding(utf8) == "utf-8"
    ascii_only = tmp_path / "a.csv"
    ascii_only.write_bytes(b"1;2\n")
    assert detect_encoding(ascii_only) == "utf-8"


def test_multibyte_char_cut_by_the_window(tmp_path: Path) -> None:
    path = tmp_path / "w.csv"
    path.write_bytes("ção".encode() * 10)
    assert detect_encoding(path, window=3) == "utf-8"  # the window ends inside "ç"
