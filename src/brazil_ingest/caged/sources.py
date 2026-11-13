"""Where the archives come from: the public FTP server, or a folder of downloaded archives."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from ftplib import FTP, error_perm, error_temp
from pathlib import Path
from typing import Protocol, TypeVar

from brazil_ingest.caged.layout import LayoutError, parse_file_name
from brazil_ingest.months import Month

log = logging.getLogger(__name__)

T = TypeVar("T")

FTP_HOST = "ftp.mtps.gov.br"
FTP_ROOT = "/pdet/microdados/NOVO CAGED"

# Errors worth a retry: network problems and temporary FTP replies (4xx).
RETRYABLE: tuple[type[BaseException], ...] = (OSError, EOFError, error_temp)


class Source(Protocol):
    def list_months(self) -> list[Month]: ...
    def list_files(self, month: Month) -> dict[str, int]: ...
    def fetch(self, month: Month, name: str, dest_dir: Path) -> Path: ...


class LocalSource:
    """Archives under ``root``: in the server layout (``<YYYY>/<YYYYMM>/*.7z``) or flat."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _archives(self) -> dict[Month, dict[str, Path]]:
        found: dict[Month, dict[str, Path]] = {}
        for path in sorted(self.root.rglob("*.7z")):
            try:
                _, compact = parse_file_name(path.name)
            except LayoutError:
                continue
            found.setdefault(Month.parse(compact), {})[path.name] = path
        return found

    def list_months(self) -> list[Month]:
        return sorted(self._archives())

    def list_files(self, month: Month) -> dict[str, int]:
        files = self._archives().get(month, {})
        return {name: path.stat().st_size for name, path in files.items()}

    def fetch(self, month: Month, name: str, dest_dir: Path) -> Path:
        """Local archives are read in place; nothing is copied."""
        path = self._archives().get(month, {}).get(name)
        if path is None:
            raise FileNotFoundError(f"{name} not found under {self.root}")
        return path


class FtpSource:
    """The public Novo CAGED FTP server (anonymous login, passive mode).

    Downloads go to a ``.part`` file that is renamed only when its size matches the size the
    server reports. A broken transfer resumes from the last byte (FTP ``REST``) on the next try.
    """

    def __init__(
        self,
        host: str = FTP_HOST,
        root: str = FTP_ROOT,
        *,
        timeout: float = 60.0,
        retries: int = 4,
        backoff: float = 5.0,
        connect: Callable[[], FTP] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.host = host
        self.root = root.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self._connect = connect or self._default_connect
        self._sleep = sleep
        self._ftp: FTP | None = None

    def _default_connect(self) -> FTP:
        # The government publishes these files only on anonymous FTP. Integrity is checked
        # by size here and recorded as SHA-256 in the partition marker.
        ftp = FTP(self.host, timeout=self.timeout, encoding="latin-1")  # noqa: S321
        ftp.login()
        ftp.set_pasv(True)
        return ftp

    def _session(self) -> FTP:
        if self._ftp is None:
            self._ftp = self._connect()
        return self._ftp

    def _drop(self) -> None:
        if self._ftp is not None:
            try:
                self._ftp.close()
            finally:
                self._ftp = None

    def close(self) -> None:
        self._drop()

    def _retry(self, what: str, action: Callable[[FTP], T]) -> T:
        for attempt in range(1, self.retries + 1):
            try:
                return action(self._session())
            except RETRYABLE as exc:
                self._drop()
                if attempt == self.retries:
                    raise
                wait = self.backoff * 2 ** (attempt - 1)
                log.warning("%s failed (%s); retry %d in %.0fs", what, exc, attempt, wait)
                self._sleep(wait)
        raise AssertionError("unreachable")  # pragma: no cover

    def list_months(self) -> list[Month]:
        def run(ftp: FTP) -> list[Month]:
            years = [y for y in ftp.nlst(self.root) if _tail(y).isdigit() and len(_tail(y)) == 4]
            months: list[Month] = []
            for year in years:
                for item in ftp.nlst(f"{self.root}/{_tail(year)}"):
                    name = _tail(item)
                    if name.isdigit() and len(name) == 6:
                        months.append(Month.parse(name))
            return sorted(months)

        return self._retry("list months", run)

    def _month_dir(self, month: Month) -> str:
        return f"{self.root}/{month.year}/{month.compact}"

    def list_files(self, month: Month) -> dict[str, int]:
        def run(ftp: FTP) -> dict[str, int]:
            try:
                items = ftp.nlst(self._month_dir(month))
            except error_perm:  # folder does not exist: month not published
                return {}
            out: dict[str, int] = {}
            for item in items:
                name = _tail(item)
                if name.lower().endswith(".7z"):
                    size = ftp.size(f"{self._month_dir(month)}/{name}")
                    out[name] = int(size) if size is not None else -1
            return out

        return self._retry(f"list {month}", run)

    def fetch(self, month: Month, name: str, dest_dir: Path) -> Path:
        dest_dir.mkdir(parents=True, exist_ok=True)
        final = dest_dir / name
        part = dest_dir / f"{name}.part"
        remote = f"{self._month_dir(month)}/{name}"

        def run(ftp: FTP) -> Path:
            expected = ftp.size(remote)
            offset = part.stat().st_size if part.exists() else 0
            if expected is not None and offset > expected:
                part.unlink()
                offset = 0
            ftp.voidcmd("TYPE I")
            with part.open("ab") as fh:
                ftp.retrbinary(f"RETR {remote}", fh.write, rest=offset or None)
            got = part.stat().st_size
            if expected is not None and got != expected:
                raise OSError(f"{name}: got {got} bytes, server reports {expected}")
            part.replace(final)
            return final

        log.info("download %s", remote)
        return self._retry(f"download {name}", run)


def _tail(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[-1]
