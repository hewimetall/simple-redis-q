"""Dramatiq 1.18 ``compute_backoff``.

``retries`` is the number of retries already recorded, before this one is added.
The delay is ``int(half + uniform(0, half))`` where
``half`` is half of ``min(min_backoff * 2**retries, max_backoff)``.
"""

from __future__ import annotations

import random


def compute_backoff(
    retries: int,
    *,
    min_backoff: int,
    max_backoff: int,
    rng: random.Random | None = None,
) -> int:
    exponent = min(retries, 32)
    base = min(min_backoff * (2**exponent), max_backoff)
    half = base / 2
    draw = (rng or random).uniform(0, half)
    return int(half + draw)
