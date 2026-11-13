"""Calendar months: parsing, ranges and the backfill plan."""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date

_MONTH_RE = re.compile(r"^(\d{4})-?(\d{2})$")


@dataclass(frozen=True, order=True)
class Month:
    """One calendar month. Ordering is chronological."""

    year: int
    month: int

    def __post_init__(self) -> None:
        if not 1 <= self.month <= 12:
            raise ValueError(f"month must be 1..12, got {self.month}")
        if not 1900 <= self.year <= 9999:
            raise ValueError(f"year out of range: {self.year}")

    @classmethod
    def parse(cls, text: str) -> Month:
        """Parse ``YYYY-MM`` or ``YYYYMM``."""
        match = _MONTH_RE.match(text.strip())
        if not match:
            raise ValueError(f"expected YYYY-MM or YYYYMM, got {text!r}")
        return cls(int(match.group(1)), int(match.group(2)))

    @classmethod
    def of(cls, day: date) -> Month:
        return cls(day.year, day.month)

    @property
    def compact(self) -> str:
        """``YYYYMM``, the format the CAGED server uses in folder and file names."""
        return f"{self.year}{self.month:02d}"

    def __str__(self) -> str:
        return f"{self.year}-{self.month:02d}"

    def shift(self, months: int) -> Month:
        index = self.year * 12 + (self.month - 1) + months
        return Month(index // 12, index % 12 + 1)


def month_range(first: Month, last: Month) -> list[Month]:
    """All months from ``first`` to ``last``, both included."""
    if last < first:
        raise ValueError(f"range is reversed: {first} > {last}")
    out: list[Month] = []
    current = first
    while current <= last:
        out.append(current)
        current = current.shift(1)
    return out


def parse_range(text: str) -> list[Month]:
    """Parse ``2025-01`` or ``2025-01..2025-06``."""
    if ".." in text:
        start, end = text.split("..", 1)
        return month_range(Month.parse(start), Month.parse(end))
    return [Month.parse(text)]


def backfill_plan(
    available: Iterable[Month],
    done: Iterable[Month],
    *,
    today: date,
    lookback: int = 12,
) -> list[Month]:
    """Months to ingest: published on the server, not done yet, inside the lookback window.

    The window ends at the month before ``today`` (the current month is never complete) and
    covers ``lookback`` months. This catches up automatically when the publisher is late and
    a scheduled run found nothing.
    """
    if lookback < 1:
        raise ValueError("lookback must be at least 1")
    newest = Month.of(today).shift(-1)
    oldest = newest.shift(-(lookback - 1))
    done_set = set(done)
    return sorted(m for m in set(available) if oldest <= m <= newest and m not in done_set)


def iter_years(months: Iterable[Month]) -> Iterator[int]:
    """Distinct years, in order."""
    yield from sorted({m.year for m in months})
