"""
Split seams in the incremental price cache.

The cache (data/prices_cache.parquet) is built incrementally: every run re-reads the last
few days and appends them to the stored history. A fetch after a split returns split-adjusted
rows while the stored history stays unadjusted, so a series can carry a one-day jump that has
nothing to do with the market (KLAC 10:1 in June 2026 -> -78.8% 12-1 before repair; DD's 1:3
reverse split the same month -> +339.1%).

SP500 constituents and the SPDR benchmark set are all USD, so the GBX/GBP minor-unit seam that
motivated the STOXX 600 twin's src/price_scale.py does not apply here (MINOR_UNIT_SUFFIXES is
empty, making repair_minor_unit_seams a no-op; kept only for interface parity with the twin).

Two repairs, both idempotent (a repaired frame yields no report):

  1. ``repair_split_seams``: a one-day ratio outside [0.55, 1.8] whose date and size match a
     split in the ticker's split calendar (Yahoo by default, injectable) is repaired. Genuine
     crashes/spikes with no matching split are left alone (EPAM -46% on 2022-02-28, the Russia
     invasion; GL -53% on 2024-04-11, the Fuzzy Panda short report -- neither has a calendar
     split, so both are left as real price history and the extreme-loser gate asks for an
     explicit reason if either ever crosses the floor).

     A single split can show up as MORE than one candidate when the vendor's feed flips back and
     forth before settling (MNST 2026-08: cache went 96 -> 47 -> 94 -> 46 across three separate
     re-reads before the real 2026-08-11 ex-date). A naive "divide everything before this seam by
     the factor" pass corrupts the series here: applied per-candidate in date order, the first and
     third matches both touch the pre-08-03 segment, dividing it by the split factor twice. Ported
     from the STOXX 600 twin, this instead assigns each confirmed candidate a signed step (a
     forward match divides what comes before, a reversion back to the pre-split basis multiplies
     it) and accumulates the steps into a per-date level anchored at zero on the LAST observation
     -- the same cumulative-level technique the twin uses for GBX/GBP, generalized to run in
     either direction so a mid-sequence reversion is corrected rather than compounded.

  2. ``rebase_history_to_overlap``: at merge time the re-read overlap days are compared with the
     cached values; a constant ratio (split, dividend re-adjustment) is applied to the whole
     cached history BEFORE the overlap, so a future split never makes it into the cache as a seam
     in the first place.

Not vendored: this is a rotation-radar specific shim over the vendored src/prices.py.
"""

from __future__ import annotations

import math
from typing import Callable

import numpy as np
import pandas as pd

# SP500 + SPDR benchmarks are USD -> no minor-currency-unit exchange in this universe.
MINOR_UNIT_SUFFIXES: tuple[str, ...] = ()
MINOR_UNIT_FACTOR = 100.0
MINOR_UNIT_LOG_BAND = math.log(2.0)

# Split candidates: a one-day ratio below/above these is not an ordinary move.
SPLIT_CANDIDATE_LOW, SPLIT_CANDIDATE_HIGH = 0.55, 1.8
# A calendar split matches a candidate seam when the dates are within this many days and the
# observed ratio is within 25% of 1/split_factor (forward match) or of split_factor itself
# (a reversion back to the pre-split basis -- see module docstring).
SPLIT_DATE_WINDOW_DAYS = 10
SPLIT_LOG_BAND = math.log(1.25)

# Overlap rebase: a constant new/cached ratio beyond this (2%) over the overlap days means the
# stored history is on another basis. Dividends below 2% are left as drift (harmless).
OVERLAP_MIN_LOG_SHIFT = math.log(1.02)
# and the ratio must be constant across the overlap (max |log| spread) to count as a basis shift
OVERLAP_MAX_LOG_SPREAD = math.log(1.005)

SplitsLookup = Callable[[str], dict]


def is_minor_unit_ticker(ticker: str, suffixes: tuple[str, ...] = MINOR_UNIT_SUFFIXES) -> bool:
    return bool(suffixes) and str(ticker).endswith(suffixes)


def _positive(series: pd.Series) -> pd.Series:
    s = series.dropna().astype(float)
    return s[s > 0]


# ---- 1. minor-unit seams (no-op for this USD-only universe; kept for twin parity) ----------

def find_minor_unit_seams(
    series: pd.Series,
    factor: float = MINOR_UNIT_FACTOR,
    log_band: float = MINOR_UNIT_LOG_BAND,
) -> list[pd.Timestamp]:
    """Dates on which the series steps by about ``factor`` or ``1/factor`` versus the
    previous observation. Empty list means the series is unit-consistent."""
    s = _positive(series)
    if len(s) < 2:
        return []
    log_ratio = np.log(s / s.shift(1))
    drop = (log_ratio + math.log(factor)).abs() <= log_band
    jump = (log_ratio - math.log(factor)).abs() <= log_band
    return list(s.index[(drop | jump).fillna(False)])


def repair_minor_unit_seams(
    prices: pd.DataFrame,
    suffixes: tuple[str, ...] = MINOR_UNIT_SUFFIXES,
    factor: float = MINOR_UNIT_FACTOR,
    log_band: float = MINOR_UNIT_LOG_BAND,
) -> tuple[pd.DataFrame, dict[str, list[pd.Timestamp]]]:
    """Repair every minor-unit column of a wide price frame. With ``suffixes`` empty (the
    SP500/SPDR default) no column ever matches, so this is a no-op that returns ``prices``
    unchanged -- kept only so the shim's interface matches the STOXX 600 twin."""
    report: dict[str, list[pd.Timestamp]] = {}
    if prices is None or prices.empty or not suffixes:
        return prices, report
    out = prices.copy()
    for col in out.columns:
        if not is_minor_unit_ticker(col, suffixes):
            continue
        seams = find_minor_unit_seams(out[col], factor, log_band)
        if not seams:
            continue
        s = _positive(out[col])
        ratio = s / s.shift(1)
        step = pd.Series(0, index=s.index, dtype=int)
        for d in seams:
            step.loc[d] = -1 if ratio.loc[d] < 1.0 else 1
        level = step.cumsum()
        divisor = np.power(factor, (level - level.min()).astype(float))
        repaired = out[col].astype(float).copy()
        repaired.loc[s.index] = s / divisor
        out[col] = repaired
        report[str(col)] = seams
    return (out if report else prices), report


# ---- 2. split seams (calendar-confirmed, bidirectional) --------------------------------------

def yahoo_splits(ticker: str) -> dict:
    """{date -> split factor} from Yahoo (10.0 = 10-for-1, 0.1 = 1-for-10). Empty on any
    failure: no evidence means no repair (the extreme-loser gate still watches)."""
    try:
        import yfinance as yf  # local import: tests inject a lookup and never hit the network
        s = yf.Ticker(ticker).splits
        return {pd.Timestamp(k).tz_localize(None).normalize(): float(v) for k, v in s.items() if v > 0}
    except Exception:  # noqa: BLE001 -- network/parse failure: no evidence, no repair
        return {}


def find_split_candidates(series: pd.Series,
                          low: float = SPLIT_CANDIDATE_LOW,
                          high: float = SPLIT_CANDIDATE_HIGH) -> list[tuple[pd.Timestamp, float]]:
    """(date, one-day ratio) for every step outside [low, high]."""
    s = _positive(series)
    if len(s) < 2:
        return []
    ratio = (s / s.shift(1)).dropna()
    hits = ratio[(ratio < low) | (ratio > high)]
    return [(d, float(v)) for d, v in hits.items()]


def _match_split(seam_date: pd.Timestamp, ratio: float, splits: dict,
                 window_days: int = SPLIT_DATE_WINDOW_DAYS,
                 log_band: float = SPLIT_LOG_BAND) -> tuple[float, float] | None:
    """The calendar split (factor, sign) that explains an observed step, else None.

    sign=+1.0: a forward match (ratio ~ 1/factor) -- the segment before ``seam_date`` is on the
    OLD basis and must be divided by ``factor`` to reach the new one.
    sign=-1.0: a reversion (ratio ~ factor) -- the vendor's feed briefly flipped back to the old
    basis; the segment before ``seam_date`` was already on the new basis and must be multiplied
    by ``factor`` to undo the flip.
    """
    for d, factor in splits.items():
        if factor <= 0 or abs((pd.Timestamp(d) - seam_date).days) > window_days:
            continue
        if abs(math.log(ratio) + math.log(factor)) <= log_band:
            return factor, 1.0
        if abs(math.log(ratio) - math.log(factor)) <= log_band:
            return factor, -1.0
    return None


def repair_split_seams(
    prices: pd.DataFrame,
    splits_lookup: SplitsLookup = yahoo_splits,
) -> tuple[pd.DataFrame, dict[str, list[tuple[pd.Timestamp, float]]]]:
    """For every column with a candidate step, ask the split calendar; confirmed steps are
    folded into a per-date level (signed, cumulative, anchored at zero on the last observation)
    so a sequence of steps for the SAME split -- including a mid-sequence reversion -- resolves
    to one consistent basis instead of compounding. Returns the repaired frame and
    ``{ticker: [(seam date, factor)]}`` (empty dict -> frame returned as-is)."""
    report: dict[str, list[tuple[pd.Timestamp, float]]] = {}
    if prices is None or prices.empty:
        return prices, report
    out = prices.copy()
    for col in out.columns:
        candidates = find_split_candidates(out[col])
        if not candidates:
            continue
        splits = splits_lookup(str(col))
        if not splits:
            continue
        s = _positive(out[col])
        step = pd.Series(0.0, index=s.index)
        applied: list[tuple[pd.Timestamp, float]] = []
        for seam_date, ratio in candidates:
            match = _match_split(seam_date, ratio, splits)
            if match is None:
                continue
            factor, sign = match
            step.loc[seam_date] += sign * math.log(factor)
            applied.append((seam_date, factor))
        if not applied:
            continue
        # level(t) = sum of signed steps at every seam date strictly AFTER t, anchored so the
        # last observation (today's basis, what every future fetch will match) sits at level 0.
        level = step.iloc[::-1].cumsum().iloc[::-1].shift(-1, fill_value=0.0)
        fixed = s / np.exp(level)
        repaired = out[col].astype(float).copy()
        repaired.loc[s.index] = fixed
        out[col] = repaired
        report[str(col)] = applied
    return (out if report else prices), report


# ---- 3. overlap rebase at merge time --------------------------------------------------------

def rebase_history_to_overlap(
    cached: pd.DataFrame,
    new: pd.DataFrame,
    min_log_shift: float = OVERLAP_MIN_LOG_SHIFT,
    max_log_spread: float = OVERLAP_MAX_LOG_SPREAD,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Compare the overlap days (present in both frames) column by column. When new/cached
    is a CONSTANT ratio k with |log k| >= ``min_log_shift`` the cached history before the
    overlap is on another basis (split, dividend re-adjustment): multiply the cached rows dated
    before the overlap by k. Returns (rebased cached, {ticker: k})."""
    report: dict[str, float] = {}
    if cached is None or cached.empty or new is None or new.empty:
        return cached, report
    overlap = cached.index.intersection(new.index)
    if len(overlap) == 0:
        return cached, report
    out = cached.copy()
    first_overlap = overlap.min()
    for col in new.columns:
        if col not in out.columns:
            continue
        a = out.loc[overlap, col].astype(float)
        b = new.loc[overlap, col].astype(float)
        mask = a.notna() & b.notna() & (a > 0) & (b > 0)
        if mask.sum() == 0:
            continue
        log_k = np.log(b[mask] / a[mask])
        if (log_k.max() - log_k.min()) > max_log_spread:
            continue  # not a constant basis shift (revisions, different sources)
        k = float(np.exp(log_k.median()))
        if abs(math.log(k)) < min_log_shift:
            continue
        before = out.index < first_overlap
        out.loc[before, col] = out.loc[before, col].astype(float) * k
        report[str(col)] = k
    return (out if report else cached), report


# ---- reporting -------------------------------------------------------------------------------

def earliest_seam(report: dict) -> pd.Timestamp | None:
    dates = []
    for seams in report.values():
        for item in seams:
            dates.append(pd.Timestamp(item[0] if isinstance(item, tuple) else item))
    return min(dates) if dates else None


def describe_report(report: dict) -> str:
    if not report:
        return "no seams"
    dates = sorted({pd.Timestamp(i[0] if isinstance(i, tuple) else i).date()
                    for seams in report.values() for i in seams})
    return f"{len(report)} columns repaired; seam dates: {[str(d) for d in dates]}"
