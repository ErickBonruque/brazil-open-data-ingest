from datetime import date

import pytest

from brazil_ingest.months import Month, backfill_plan, month_range, parse_range


def test_parse_both_formats() -> None:
    assert Month.parse("2025-03") == Month.parse("202503") == Month(2025, 3)
    assert Month(2025, 3).compact == "202503"
    assert str(Month(2025, 3)) == "2025-03"


@pytest.mark.parametrize("bad", ["2025-13", "2025-00", "25-01", "2025/01", ""])
def test_parse_rejects(bad: str) -> None:
    with pytest.raises(ValueError):
        Month.parse(bad)


def test_shift_crosses_years() -> None:
    assert Month(2025, 1).shift(-1) == Month(2024, 12)
    assert Month(2024, 12).shift(1) == Month(2025, 1)
    assert Month(2025, 6).shift(-18) == Month(2023, 12)


def test_range_and_parse_range() -> None:
    assert month_range(Month(2024, 11), Month(2025, 2)) == [
        Month(2024, 11),
        Month(2024, 12),
        Month(2025, 1),
        Month(2025, 2),
    ]
    assert parse_range("2024-12..2025-01") == [Month(2024, 12), Month(2025, 1)]
    with pytest.raises(ValueError):
        parse_range("2025-02..2025-01")


def test_backfill_plan_window_and_done() -> None:
    available = [Month(2024, m) for m in range(1, 13)] + [Month(2025, 1), Month(2025, 2)]
    plan = backfill_plan(available, [Month(2024, 12)], today=date(2025, 2, 10), lookback=3)
    # window: 2024-11 .. 2025-01 (February is the current month, never complete)
    assert plan == [Month(2024, 11), Month(2025, 1)]


def test_backfill_plan_late_publisher() -> None:
    # Nothing published for the last two months yet: nothing to do, no error.
    available = [Month(2024, 10)]
    assert backfill_plan(available, [], today=date(2025, 1, 5), lookback=2) == []
    assert backfill_plan(available, [], today=date(2025, 1, 5), lookback=3) == [Month(2024, 10)]


def test_backfill_plan_rejects_zero_lookback() -> None:
    with pytest.raises(ValueError):
        backfill_plan([], [], today=date(2025, 1, 1), lookback=0)
