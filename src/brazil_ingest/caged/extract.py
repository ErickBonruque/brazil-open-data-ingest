"""Extract the single text file inside a Novo CAGED ``.7z`` archive."""

from __future__ import annotations

from pathlib import Path

import py7zr

from brazil_ingest.caged.layout import LayoutError


def extract_text(archive: Path, dest_dir: Path) -> Path:
    """Extract the archive to disk and return the path of its ``.txt`` file.

    The files are extracted to disk, not to memory: one month of movements is about 400 MB of
    text, more than a small container or a Lambda function should hold in RAM.
    """
    with py7zr.SevenZipFile(archive, mode="r") as zf:
        names = [n for n in zf.getnames() if n.lower().endswith(".txt")]
        if len(names) != 1:
            raise LayoutError(f"{archive.name}: expected 1 .txt file, found {len(names)}")
        name = names[0]
        if Path(name).is_absolute() or ".." in Path(name).parts:
            raise LayoutError(f"{archive.name}: unsafe path inside archive: {name!r}")
        zf.extract(path=dest_dir, targets=[name])
    out = dest_dir / str(name)
    if not out.is_file():
        raise LayoutError(f"{archive.name}: {name} was not extracted")
    return out
