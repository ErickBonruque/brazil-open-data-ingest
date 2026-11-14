"""Helpers for the text files inside government archives."""

from __future__ import annotations

import codecs
import re
from pathlib import Path

_NON_ASCII = re.compile(rb"[\x80-\xff]")


def detect_encoding(path: Path, *, block: int = 1 << 20, window: int = 1 << 16) -> str:
    """``"utf-8"`` or ``"latin-1"``, decided at the **first non-ASCII byte** of the file.

    Looking only at the start of a file is not enough: a file can be pure ASCII for its first
    megabytes (codes and numbers) and have its first accented name much later. ASCII is valid
    UTF-8, so a fixed-size probe would choose UTF-8 for a Latin-1 file. A file with no
    non-ASCII byte at all is read as UTF-8 (both encodings give the same text).
    """
    with path.open("rb") as fh:
        while chunk := fh.read(block):
            match = _NON_ASCII.search(chunk)
            if match is None:
                continue
            # Every byte before the match is ASCII, so the match starts a character.
            sample = chunk[match.start() : match.start() + window]
            if len(sample) < window:
                sample += fh.read(window - len(sample))
            try:
                codecs.getincrementaldecoder("utf-8")().decode(sample, final=False)
            except UnicodeDecodeError:
                return "latin-1"
            return "utf-8"
    return "utf-8"
