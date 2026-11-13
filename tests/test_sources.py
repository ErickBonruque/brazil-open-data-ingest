from collections.abc import Callable
from ftplib import FTP, error_perm
from pathlib import Path
from typing import cast

import pytest

from brazil_ingest.caged.sources import FtpSource, LocalSource
from brazil_ingest.months import Month

ROOT = "/pdet/microdados/NOVO CAGED"


class FakeFtp:
    """A tiny in-memory FTP server. ``cut`` makes the next transfers stop after N bytes."""

    def __init__(self, files: dict[str, bytes], cut: list[int] | None = None) -> None:
        self.files = files
        self.cut = cut or []
        self.closed = False
        self.rest_offsets: list[int | None] = []

    def nlst(self, path: str) -> list[str]:
        prefix = path.rstrip("/") + "/"
        children = {k[len(prefix) :].split("/")[0] for k in self.files if k.startswith(prefix)}
        if not children:
            raise error_perm("550 not found")
        return sorted(children)

    def size(self, path: str) -> int:
        return len(self.files[path])

    def voidcmd(self, cmd: str) -> str:
        return "200"

    def retrbinary(self, cmd: str, callback: Callable[[bytes], object], rest: int | None) -> str:
        self.rest_offsets.append(rest)
        data = self.files[cmd.removeprefix("RETR ")][rest or 0 :]
        if self.cut:
            data = data[: self.cut.pop(0)]
            callback(data)
            raise ConnectionResetError("connection lost")
        callback(data)
        return "226"

    def close(self) -> None:
        self.closed = True


def _source(fake: FakeFtp) -> tuple[FtpSource, list[float]]:
    waits: list[float] = []
    src = FtpSource(connect=lambda: cast(FTP, fake), sleep=waits.append, backoff=1, retries=3)
    return src, waits


FILES = {
    f"{ROOT}/2025/202501/CAGEDMOV202501.7z": b"x" * 100,
    f"{ROOT}/2025/202501/CAGEDFOR202501.7z": b"y" * 10,
    f"{ROOT}/2025/202502/CAGEDMOV202502.7z": b"z" * 5,
    f"{ROOT}/Leia-me.txt": b"readme",
}


def test_ftp_lists_months_and_files() -> None:
    src, _ = _source(FakeFtp(FILES))
    assert src.list_months() == [Month(2025, 1), Month(2025, 2)]
    assert src.list_files(Month(2025, 1)) == {"CAGEDFOR202501.7z": 10, "CAGEDMOV202501.7z": 100}
    assert src.list_files(Month(2025, 3)) == {}


def test_ftp_download_resumes_after_a_broken_transfer(tmp_path: Path) -> None:
    fake = FakeFtp(FILES, cut=[40])
    src, waits = _source(fake)
    path = src.fetch(Month(2025, 1), "CAGEDMOV202501.7z", tmp_path)
    assert path.read_bytes() == b"x" * 100
    assert fake.rest_offsets == [None, 40]  # second try starts at byte 40
    assert waits == [1]
    assert not (tmp_path / "CAGEDMOV202501.7z.part").exists()


def test_ftp_gives_up_after_retries(tmp_path: Path) -> None:
    fake = FakeFtp(FILES, cut=[1, 1, 1])
    src, waits = _source(fake)
    with pytest.raises(ConnectionResetError):
        src.fetch(Month(2025, 1), "CAGEDMOV202501.7z", tmp_path)
    assert waits == [1, 2]  # exponential backoff
    assert not (tmp_path / "CAGEDMOV202501.7z").exists()


def test_local_source_reads_server_layout(tmp_path: Path) -> None:
    for rel in ["2025/202501/CAGEDMOV202501.7z", "2025/202501/notes.7z", "CAGEDEXC202502.7z"]:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_bytes(b"abc")
    src = LocalSource(tmp_path)
    assert src.list_months() == [Month(2025, 1), Month(2025, 2)]
    assert src.list_files(Month(2025, 1)) == {"CAGEDMOV202501.7z": 3}
    assert src.fetch(Month(2025, 2), "CAGEDEXC202502.7z", tmp_path / "x").parent == tmp_path
    with pytest.raises(FileNotFoundError):
        src.fetch(Month(2025, 2), "CAGEDMOV202502.7z", tmp_path)
