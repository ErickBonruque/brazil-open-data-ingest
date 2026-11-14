"""CNPJ check digits, numeric and alphanumeric.

Since July 2026 new CNPJ numbers can have letters in the first 12 characters. The check digits
stay numeric and use the same modulo-11 rule: each character counts as ``ord(char) - 48``, so
``"0"``-``"9"`` keep their values and ``"A"`` is 17, ``"B"`` 18, and so on.
"""

from __future__ import annotations

_WEIGHTS_1 = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
_WEIGHTS_2 = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)


def _digit(chars: str, weights: tuple[int, ...]) -> int:
    total = sum((ord(ch) - 48) * w for ch, w in zip(chars, weights, strict=True))
    rest = total % 11
    return 0 if rest < 2 else 11 - rest


def check_digits(base12: str) -> str:
    """The two check digits for the first 12 characters of a CNPJ."""
    base12 = base12.upper()
    if len(base12) != 12 or not base12.isalnum() or not base12.isascii():
        raise ValueError(f"expected 12 letters or digits, got {base12!r}")
    first = _digit(base12, _WEIGHTS_1)
    second = _digit(base12 + str(first), _WEIGHTS_2)
    return f"{first}{second}"


def is_valid_cnpj(number: str) -> bool:
    """``True`` for a 14-character CNPJ with correct check digits (no punctuation)."""
    if len(number) != 14 or not number[12:].isdigit():
        return False
    try:
        return check_digits(number[:12]) == number[12:]
    except ValueError:
        return False
