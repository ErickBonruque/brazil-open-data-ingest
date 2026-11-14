"""Where CNPJ releases come from: the public WebDAV share of the Receita, or a local folder.

Since 2025 the Receita publishes the open data on a Nextcloud server. A public share can be read
without a login through WebDAV: ``PROPFIND`` lists folders and files (with sizes), ``GET`` with a
``Range`` header downloads and resumes.
"""

from __future__ import annotations

import logging
import re
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol, TypeVar
from urllib.parse import quote, unquote, urlsplit

from defusedxml import ElementTree

from brazil_ingest.cnpj.layout import LayoutError, parse_zip_name
from brazil_ingest.months import Month

log = logging.getLogger(__name__)

T = TypeVar("T")

SHARE_URL = "https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9"
_DAV = "{DAV:}"
_PROPFIND_BODY = (
    b'<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop>'
    b"<d:getcontentlength/><d:resourcetype/></d:prop></d:propfind>"
)
USER_AGENT = "brazil-open-data-ingest (+https://github.com/ErickBonruque/brazil-open-data-ingest)"


class Source(Protocol):
    def list_releases(self) -> list[Month]: ...
    def list_files(self, release: Month) -> dict[str, int]: ...
    def fetch(self, release: Month, name: str, dest_dir: Path) -> Path: ...


def _is_cnpj_zip(name: str) -> bool:
    if not name.lower().endswith(".zip"):
        return False
    try:
        parse_zip_name(name)
    except LayoutError:
        log.warning("ignoring unknown ZIP on the source: %r", name[:128])
        return False
    return True


_SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def redact(text: str) -> str:
    """Remove ``user:password@`` from every URL in a log line (any scheme, last ``@`` wins)."""
    return re.sub(r"([a-z][a-z0-9+.-]*://)[^/\s]*@", r"\1", text, flags=re.IGNORECASE)


class _HttpsOnlyRedirects(urllib.request.HTTPRedirectHandler):
    """Follow redirects only to https URLs, so a redirect cannot downgrade the connection."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        if urlsplit(newurl).scheme != "https":
            raise urllib.error.HTTPError(newurl, code, "redirect to non-https URL", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


_HTTPS_OPENER = urllib.request.build_opener(_HttpsOnlyRedirects())


def _https_open(req: urllib.request.Request, timeout: float) -> Any:
    return _HTTPS_OPENER.open(req, timeout=timeout)


class LocalSource:
    """Releases in ``<root>/<YYYY-MM>/*.zip`` (the server layout)."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def list_releases(self) -> list[Month]:
        out = []
        for child in self.root.iterdir():
            try:
                out.append(Month.parse(child.name))
            except ValueError:
                continue
        return sorted(out)

    def list_files(self, release: Month) -> dict[str, int]:
        folder = self.root / str(release)
        if not folder.is_dir():
            return {}
        return {p.name: p.stat().st_size for p in folder.iterdir() if _is_cnpj_zip(p.name)}

    def fetch(self, release: Month, name: str, dest_dir: Path) -> Path:
        path = self.root / str(release) / name
        if not path.is_file():
            raise FileNotFoundError(path)
        return path


# Returns the response object of ``urllib.request.urlopen`` (a context manager).
Opener = Callable[[urllib.request.Request, float], Any]


class WebDavSource:
    """The public share of the Receita (or any WebDAV folder with the same layout)."""

    def __init__(
        self,
        share_url: str = SHARE_URL,
        *,
        timeout: float = 60.0,
        retries: int = 5,
        backoff: float = 5.0,
        opener: Opener | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        parts = urlsplit(share_url)
        if parts.scheme != "https":
            raise ValueError("the share URL must use https")
        self.share_url = share_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self._open = opener or _https_open
        self._sleep = sleep
        self._listing: dict[Month, dict[str, int]] = {}

    def _url(self, *parts: str) -> str:
        return "/".join([self.share_url, *(quote(p) for p in parts)])

    def _retry(self, what: str, action: Callable[[], T]) -> T:
        for attempt in range(1, self.retries + 1):
            try:
                return action()
            except urllib.error.HTTPError as exc:
                if exc.code < 500 and exc.code != 429:
                    raise
                error: Exception = exc
            except (urllib.error.URLError, OSError) as exc:
                error = exc
            if attempt == self.retries:
                raise error
            wait = self.backoff * 2 ** (attempt - 1)
            log.warning(
                "%s failed (%s); retry %d in %.0fs", redact(what), redact(str(error)), attempt, wait
            )
            self._sleep(wait)
        raise AssertionError("unreachable")  # pragma: no cover

    def _propfind(self, url: str) -> list[tuple[str, int | None, bool]]:
        """(name, size, is_folder) of the children of a folder."""

        def run() -> bytes:
            req = urllib.request.Request(  # noqa: S310 - https checked in __init__
                url + "/",
                data=_PROPFIND_BODY,
                method="PROPFIND",
                headers={"Depth": "1", "Content-Type": "application/xml", "User-Agent": USER_AGENT},
            )
            with self._open(req, self.timeout) as resp:
                body: bytes = resp.read()
                return body

        root = ElementTree.fromstring(self._retry(f"list {url}", run))
        out = []
        for response in root.iter(f"{_DAV}response"):
            href = unquote(response.findtext(f"{_DAV}href") or "").rstrip("/")
            name = href.rsplit("/", 1)[-1]
            if name and not _SAFE_NAME.match(name):
                log.warning("ignoring entry with an unexpected name: %r", name[:128])
                continue
            size_text = response.findtext(f".//{_DAV}getcontentlength")
            is_folder = response.find(f".//{_DAV}collection") is not None
            out.append((name, int(size_text) if size_text else None, is_folder))
        return out[1:]  # the first response is the folder itself

    def list_releases(self) -> list[Month]:
        releases = []
        for name, _, is_folder in self._propfind(self.share_url):
            if is_folder:
                try:
                    releases.append(Month.parse(name))
                except ValueError:
                    continue
        return sorted(releases)

    def list_files(self, release: Month) -> dict[str, int]:
        """Files of a release with their sizes. Cached: one listing per release per run."""
        if release not in self._listing:
            self._listing[release] = self._list_files(release)
        return dict(self._listing[release])

    def _list_files(self, release: Month) -> dict[str, int]:
        try:
            entries = self._propfind(self._url(str(release)))
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return {}
            raise
        return {
            name: size
            for name, size, is_folder in entries
            if not is_folder and size is not None and _is_cnpj_zip(name)
        }

    def fetch(self, release: Month, name: str, dest_dir: Path) -> Path:
        dest_dir.mkdir(parents=True, exist_ok=True)
        final = dest_dir / name
        part = dest_dir / f"{name}.part"
        expected = self.list_files(release).get(name)
        if expected is None:
            raise FileNotFoundError(f"{release}/{name} is not on the server")
        url = self._url(str(release), name)

        def run() -> Path:
            offset = part.stat().st_size if part.exists() else 0
            if offset > expected:
                part.unlink(missing_ok=True)
                offset = 0
            headers = {"User-Agent": USER_AGENT}
            if offset:
                headers["Range"] = f"bytes={offset}-"
            req = urllib.request.Request(url, headers=headers)  # noqa: S310 - https checked
            with self._open(req, self.timeout) as resp:
                if offset and resp.status != 206:  # server ignored Range: start again
                    part.unlink(missing_ok=True)
                    offset = 0
                with part.open("ab") as fh:
                    size = offset
                    while block := resp.read(1 << 20):
                        size += len(block)
                        if size > expected:  # never write past the advertised size
                            raise OSError(f"{name}: server sent more than {expected} bytes")
                        fh.write(block)
            got = part.stat().st_size
            if got != expected:
                raise OSError(f"{name}: got {got} bytes, server reports {expected}")
            part.replace(final)
            return final

        log.info("download %s", redact(url))
        return self._retry(f"download {name}", run)
