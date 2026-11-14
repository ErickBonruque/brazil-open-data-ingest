import io
import json
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

import duckdb
import pytest

from brazil_ingest import cli
from brazil_ingest.cnpj.layout import Table
from brazil_ingest.cnpj.pipeline import ingest_release, partition_prefix
from brazil_ingest.cnpj.sources import LocalSource, WebDavSource
from brazil_ingest.months import Month
from brazil_ingest.partitions import FilterMismatchError, read_marker
from brazil_ingest.storage import LocalStore
from tests.test_cnpj_convert import establishment, line


def make_zip(folder: Path, name: str, inner: str, lines: list[str]) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(inner, ("\n".join(lines) + "\n").encode("latin-1"))
    return path


@pytest.fixture
def release_dir(tmp_path: Path) -> Path:
    root = tmp_path / "server"
    folder = root / "2026-10"
    make_zip(folder, "Cnaes.zip", "F.K03200$Z.D61010.CNAECSV", [line("4781400", "Comércio")])
    make_zip(
        folder,
        "Empresas0.zip",
        "K3241.EMPRECSV",
        [
            line("00000001", "ACME LTDA", "2062", "49", "1000,00", "05", ""),
        ],
    )
    make_zip(
        folder,
        "Empresas1.zip",
        "K3241.EMPRECSV",
        [
            line("00000002", "MARIA SOUZA", "2135", "50", "0,00", "01", ""),
        ],
    )
    make_zip(
        folder,
        "Estabelecimentos0.zip",
        "K3241.ESTABELE",
        [
            establishment("00000001", "0001"),
            establishment("12ABC345", "01DE", uf="SP"),
        ],
    )
    (folder / "notes.txt").write_text("not a data file")
    (root / "cnpj.tar.gz").write_bytes(b"")
    return root


def test_ingest_release_and_skip(release_dir: Path, tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "lake")
    source = LocalSource(release_dir)
    assert source.list_releases() == [Month(2026, 10)]
    results = ingest_release(source, store, Month(2026, 10))
    assert sorted((r.table, r.chunk, r.rows) for r in results) == [
        (Table.CNAES, 0, 1),
        (Table.COMPANIES, 0, 1),
        (Table.COMPANIES, 1, 1),
        (Table.ESTABLISHMENTS, 0, 2),
    ]
    marker = read_marker(store, partition_prefix(Table.ESTABLISHMENTS, Month(2026, 10), 0))
    assert marker is not None and marker["alphanumeric_cnpj"] == 1 and marker["invalid_cnpj"] == 0

    again = ingest_release(source, store, Month(2026, 10), tables=[Table.COMPANIES])
    assert [r.status for r in again] == ["skipped", "skipped"]
    with pytest.raises(FilterMismatchError):
        ingest_release(source, store, Month(2026, 10), keep_personal=True)

    with duckdb.connect() as con:
        names = con.execute(
            "SELECT cnpj_base, company_name FROM read_parquet(?, hive_partitioning = true) "
            "ORDER BY 1",
            [str(tmp_path / "lake/cnpj/table=companies/*/*/*.parquet")],
        ).fetchall()
    assert names == [("00000001", "ACME LTDA"), ("00000002", None)]


def test_missing_release(release_dir: Path, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        ingest_release(LocalSource(release_dir), LocalStore(tmp_path), Month(2026, 9))


def test_cli_cnpj(release_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["cnpj", "releases", "--source-dir", str(release_dir)]) == 0
    assert capsys.readouterr().out.split() == ["2026-10"]
    args = [
        "cnpj",
        "ingest",
        "latest",
        "--source-dir",
        str(release_dir),
        "--dest",
        str(tmp_path / "lake"),
        "--tables",
        "establishments",
    ]
    assert cli.main(args) == 0
    out = capsys.readouterr().out
    assert "release 2026-10" in out and "alphanumeric 1" in out and "1 written" in out
    assert cli.main([*args, "--keep-personal"]) == 2


# --- WebDAV source with a fake server -------------------------------------------------------

PROPFIND_ROOT = b"""<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
 <d:response><d:href>/dav/files/T/</d:href><d:propstat><d:prop>
  <d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>
 <d:response><d:href>/dav/files/T/2026-09/</d:href><d:propstat><d:prop>
  <d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>
 <d:response><d:href>/dav/files/T/2026-10/</d:href><d:propstat><d:prop>
  <d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>
 <d:response><d:href>/dav/files/T/cnpj.tar.gz</d:href><d:propstat><d:prop>
  <d:getcontentlength>9</d:getcontentlength><d:resourcetype/></d:prop></d:propstat></d:response>
</d:multistatus>"""

PROPFIND_MONTH = b"""<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
 <d:response><d:href>/dav/files/T/2026-10/</d:href><d:propstat><d:prop>
  <d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>
 <d:response><d:href>/dav/files/T/2026-10/Empresas1.zip</d:href><d:propstat><d:prop>
  <d:getcontentlength>100</d:getcontentlength><d:resourcetype/></d:prop></d:propstat></d:response>
 <d:response><d:href>/dav/files/T/2026-10/Leia%20me.txt</d:href><d:propstat><d:prop>
  <d:getcontentlength>5</d:getcontentlength><d:resourcetype/></d:prop></d:propstat></d:response>
</d:multistatus>"""


class FakeResponse(io.BytesIO):
    def __init__(self, data: bytes, status: int = 200) -> None:
        super().__init__(data)
        self.status = status

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class FakeServer:
    def __init__(self, cut_first_download_at: int | None = None) -> None:
        self.payload = bytes(range(100))
        self.cut = cut_first_download_at
        self.ranges: list[str | None] = []
        self.fail_list = 0

    def __call__(self, req: urllib.request.Request, timeout: float) -> Any:
        url = req.full_url
        if req.get_method() == "PROPFIND":
            if self.fail_list:
                self.fail_list -= 1
                raise urllib.error.HTTPError(url, 503, "busy", {}, None)  # type: ignore[arg-type]
            if url.endswith("/2026-10/"):
                return FakeResponse(PROPFIND_MONTH)
            if url.endswith("/2026-08/"):
                raise urllib.error.HTTPError(url, 404, "nf", {}, None)  # type: ignore[arg-type]
            return FakeResponse(PROPFIND_ROOT)
        rng = req.get_header("Range")
        self.ranges.append(rng)
        start = int(rng.split("=")[1].rstrip("-")) if rng else 0
        data = self.payload[start:]
        if self.cut is not None:
            data, self.cut = data[: self.cut], None
            response = FakeResponse(data, 206 if rng else 200)
            original_read = response.read

            def broken_read(size: int | None = -1) -> bytes:
                chunk = original_read(size)
                if not chunk:
                    raise ConnectionResetError("connection lost")
                return chunk

            response.read = broken_read  # type: ignore[method-assign]
            return response
        return FakeResponse(data, 206 if rng else 200)


def _source(server: FakeServer) -> tuple[WebDavSource, list[float]]:
    waits: list[float] = []
    src = WebDavSource(
        "https://example.org/dav/files/T", opener=server, sleep=waits.append, backoff=1
    )
    return src, waits


def test_webdav_lists_releases_and_files() -> None:
    server = FakeServer()
    server.fail_list = 1  # one 503 before it works
    src, waits = _source(server)
    assert src.list_releases() == [Month(2026, 9), Month(2026, 10)]
    assert waits == [1]
    assert src.list_files(Month(2026, 10)) == {"Empresas1.zip": 100}
    assert src.list_files(Month(2026, 8)) == {}


def test_webdav_download_resumes(tmp_path: Path) -> None:
    server = FakeServer(cut_first_download_at=30)
    src, _ = _source(server)
    path = src.fetch(Month(2026, 10), "Empresas1.zip", tmp_path)
    assert path.read_bytes() == bytes(range(100))
    assert server.ranges == [None, "bytes=30-"]
    with pytest.raises(FileNotFoundError):
        src.fetch(Month(2026, 10), "Socios0.zip", tmp_path)


def test_marker_records_source_hash(release_dir: Path, tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "lake")
    ingest_release(LocalSource(release_dir), store, Month(2026, 10), tables=[Table.CNAES])
    marker_path = tmp_path / "lake" / partition_prefix(Table.CNAES, Month(2026, 10), 0)
    marker = json.loads((marker_path / "_SUCCESS.json").read_text())
    assert len(marker["source_sha256"]) == 64 and marker["encoding"] == "latin-1"


def test_webdav_requires_https_and_redacts() -> None:
    from brazil_ingest.cnpj.sources import redact

    with pytest.raises(ValueError, match="https"):
        WebDavSource("http://example.org/dav")
    assert redact("list https://user:secret@example.org/x") == "list https://example.org/x"


def test_webdav_stops_at_the_advertised_size(tmp_path: Path) -> None:
    server = FakeServer()
    server.payload = bytes(range(120))  # 20 bytes more than PROPFIND says
    src, _ = _source(server)
    with pytest.raises(OSError, match="more than 100"):
        src.fetch(Month(2026, 10), "Empresas1.zip", tmp_path)


def test_duplicate_partition_is_an_error(release_dir: Path, tmp_path: Path) -> None:
    from brazil_ingest.cnpj.layout import LayoutError

    (release_dir / "2026-10" / "Empresas01.zip").write_bytes(b"")  # chunk 1 again
    with pytest.raises(LayoutError, match="same partition"):
        ingest_release(LocalSource(release_dir), LocalStore(tmp_path), Month(2026, 10))
