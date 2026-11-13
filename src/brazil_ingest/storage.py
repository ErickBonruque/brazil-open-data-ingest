"""Where the output goes: a local folder, S3 or any S3-compatible server (MinIO)."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse


class Store(Protocol):
    """Key/value storage. Keys use ``/`` and never start with ``/``."""

    def exists(self, key: str) -> bool: ...
    def read_bytes(self, key: str) -> bytes: ...
    def put_bytes(self, data: bytes, key: str) -> None: ...
    def put_file(self, path: Path, key: str) -> None: ...
    def uri(self, key: str) -> str: ...


def _check_key(key: str) -> str:
    if not key or key.startswith("/") or ".." in key.split("/"):
        raise ValueError(f"invalid key: {key!r}")
    return key


class LocalStore:
    """A folder on disk. Writes are atomic: temporary file, then rename."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, key: str) -> Path:
        return self.root / _check_key(key)

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def read_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def put_bytes(self, data: bytes, key: str) -> None:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
            Path(tmp).replace(target)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def put_file(self, path: Path, key: str) -> None:
        self.put_bytes(path.read_bytes(), key)

    def uri(self, key: str) -> str:
        return str(self._path(key))


class S3Store:
    """An S3 bucket and prefix. ``endpoint_url`` points to MinIO or another S3 server."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        client: Any | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        if client is None:
            try:
                import boto3
            except ImportError as exc:  # pragma: no cover - depends on the environment
                raise RuntimeError(
                    "S3 output needs boto3: pip install 'brazil-open-data-ingest[s3]'"
                ) from exc
            client = boto3.client("s3", endpoint_url=endpoint_url)
        self.client = client

    def _key(self, key: str) -> str:
        key = _check_key(key)
        return f"{self.prefix}/{key}" if self.prefix else key

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(key))
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise
        return True

    def read_bytes(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
        data: bytes = response["Body"].read()
        return data

    def put_bytes(self, data: bytes, key: str) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)

    def put_file(self, path: Path, key: str) -> None:
        self.client.upload_file(str(path), self.bucket, self._key(key))

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{self._key(key)}"


def _is_not_found(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    code = str(response.get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchKey", "NotFound"}


def open_store(dest: str, *, endpoint_url: str | None = None) -> Store:
    """``./data`` -> LocalStore, ``s3://bucket/prefix`` -> S3Store.

    ``endpoint_url`` (or the ``AWS_ENDPOINT_URL`` environment variable, read by boto3) selects
    an S3-compatible server such as MinIO.
    """
    parsed = urlparse(dest)
    if parsed.scheme == "s3":
        if not parsed.netloc:
            raise ValueError(f"missing bucket in {dest!r}")
        return S3Store(parsed.netloc, parsed.path, endpoint_url=endpoint_url)
    if parsed.scheme not in ("", "file"):
        raise ValueError(f"unsupported destination: {dest!r}")
    return LocalStore(Path(parsed.path if parsed.scheme == "file" else dest))
