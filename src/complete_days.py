"""ПРОЛЕТР6 (26.11.2026): a price day enters the cache only when it is complete.

WHY. On 21.11.2026 the Friday run (cron 22:00 UTC, started 03:00 UTC Saturday) read 507
names from the price archive, which did not have Friday's bar yet, and 8 names from the
yfinance fallback, which did. The cache got a row for 20.11 with 8 of 520 prices. Every
later run saw last_date 20.11, said "Prices cache up to date" and never re-read, so the
page said as_of 2026-11-20 while every return ran to 19.11 (the last price of each name).

RULE. A trailing row with prices for fewer than COMPLETE_FRAC of the fullest of the last
REF_ROWS rows is a partial day: it is held back (not written, not used). Several partial
days in a row are all held back (the reference is the fullest row, not the one before). The next run reads
that day again from the archive, which by then carries it. Only the TAIL is trimmed: an
old row in the middle is history and is left as it is.
"""
from __future__ import annotations

import pandas as pd

COMPLETE_FRAC = 0.95
REF_ROWS = 10


def drop_partial_tail(df: pd.DataFrame, frac: float = COMPLETE_FRAC):
    """Return (df without its partial trailing rows, list of the dropped dates)."""
    if df is None or len(df) < 2:
        return df, []
    counts = df.notna().sum(axis=1)
    ref = counts.iloc[-REF_ROWS:].max()
    keep = len(df)
    while keep >= 2 and counts.iloc[keep - 1] < frac * ref:
        keep -= 1
    dropped = [d for d in df.index[keep:]]
    return df.iloc[:keep], dropped


def last_complete_date(df: pd.DataFrame):
    """The day the returns run to: the last row that is not a partial day."""
    trimmed, _ = drop_partial_tail(df)
    return trimmed.index.max()
