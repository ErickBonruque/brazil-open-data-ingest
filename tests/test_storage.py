from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

from brazil_ingest.storage import LocalStore, S3Store, open_store


def test_local_store_roundtrip(tmp_path: Path) -> None:
    store = LocalStore(tmp_path)
    assert not store.exists("a/b.json")
    store.put_bytes(b"{}", "a/b.json")
    assert store.exists("a/b.json")
    assert store.read_bytes("a/b.json") == b"{}"
    assert list((tmp_path / "a").iterdir()) == [tmp_path / "a" / "b.json"]  # no temp left


@pytest.mark.parametrize("key", ["", "/abs", "a/../b"])
def test_invalid_keys(tmp_path: Path, key: str) -> None:
    with pytest.raises(ValueError):
        LocalStore(tmp_path).put_bytes(b"x", key)


class ClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.fail_with: str | None = None

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        if self.fail_with:
            raise ClientError(self.fail_with)
        if (Bucket, Key) not in self.objects:
            raise ClientError("404")
        return {}

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects[(Bucket, Key)] = Body

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.objects[(bucket, key)] = Path(filename).read_bytes()


def test_s3_store_with_prefix(tmp_path: Path) -> None:
    client = FakeS3()
    store = S3Store("lake", "/raw/", client=client)
    assert not store.exists("x.json")
    store.put_bytes(b"1", "x.json")
    (tmp_path / "f").write_bytes(b"2")
    store.put_file(tmp_path / "f", "y/f")
    assert set(client.objects) == {("lake", "raw/x.json"), ("lake", "raw/y/f")}
    assert store.exists("x.json") and store.read_bytes("y/f") == b"2"
    assert store.uri("x.json") == "s3://lake/raw/x.json"


def test_s3_store_does_not_hide_access_errors() -> None:
    client = FakeS3()
    client.fail_with = "403"
    with pytest.raises(ClientError):
        S3Store("lake", client=client).exists("x")


def test_open_store(tmp_path: Path) -> None:
    assert isinstance(open_store(str(tmp_path)), LocalStore)
    with pytest.raises(ValueError):
        open_store("s3://")
    with pytest.raises(ValueError):
        open_store("gs://bucket/x")
