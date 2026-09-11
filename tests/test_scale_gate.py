"""
Scale gate over the PUBLISHED artefacts (docs/data.json, data/prices_cache.parquet).

Born 2026-09-11 (SP500 twin of the STOXX 600 GBX/GBP fix): the price cache carried 9 unrepaired
split seams -- a fetch after a split returns split-adjusted rows while the stored history stayed
unadjusted, so DD showed +339.1% and KLAC -78.8% on a pure 12-1 momentum that should have read
+46% and +112%. SP500 + SPDR benchmarks are USD, so there is no GBX/GBP unit seam here (see
src/price_scale.py); the only seam type is split/basis mismatches, repaired by
``repair_split_seams`` and, going forward, ``rebase_history_to_overlap`` at merge time.

One check, a hard fail: no ticker with mom_12_1_pct below MOM_FLOOR unless it is listed in
EXTREME_LOSERS_ALLOWED with an explicit, dated reason. (Unlike the STOXX 600 twin there is no
single sub-population -- like its .L names -- that a systematic seam would concentrate in, so
there is no bottom-tail-overrepresentation check here.)

The gate runs in CI (daily_update.yml, BEFORE the commit step) and in the test suite.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.price_scale import find_split_candidates, repair_split_seams

ROOT = Path(__file__).resolve().parents[1]
DATA_JSON = ROOT / "docs" / "data.json"
PRICES_CACHE = ROOT / "data" / "prices_cache.parquet"

MOM_FLOOR = -90.0  # percent; a 12-1 return below this needs an explicit reason

# ticker -> reason (dated, verifiable). Empty on purpose: an entry is a deliberate decision.
EXTREME_LOSERS_ALLOWED: dict[str, str] = {}


# ---- pure checks (unit-testable, no file access) -------------------------------------------

def unexplained_extreme_losers(rows: list[dict], allowed: dict[str, str] | None = None,
                               floor: float = MOM_FLOOR) -> list[tuple[str, float]]:
    """Tickers whose mom_12_1_pct is below ``floor`` and are not in ``allowed``."""
    allowed = allowed or {}
    out = []
    for r in rows:
        m = r.get("mom_12_1_pct")
        if m is None or not isinstance(m, (int, float)) or not math.isfinite(m):
            continue
        if m < floor and r.get("ticker") not in allowed:
            out.append((r["ticker"], float(m)))
    return sorted(out, key=lambda x: x[1])


# ---- the gate over the published files ------------------------------------------------------

def _rows() -> list[dict]:
    assert DATA_JSON.exists(), f"missing {DATA_JSON}"
    payload = json.loads(DATA_JSON.read_text(encoding="utf-8"))
    rows = payload.get("rank_all_stocks") or []
    assert rows, "rank_all_stocks is empty"
    return rows


def test_no_unexplained_extreme_losers():
    bad = unexplained_extreme_losers(_rows(), EXTREME_LOSERS_ALLOWED)
    assert not bad, (f"{len(bad)} tickers with mom_12_1_pct < {MOM_FLOOR} and no explicit reason "
                     f"(first 10): {bad[:10]}")


# ---- mutation proofs: the checks must FIRE on a broken table --------------------------------

def _healthy_rows(n: int = 100) -> list[dict]:
    return [{"ticker": f"T{i}", "mom_12_1_pct": -40.0 + (i * 80.0 / n)} for i in range(n)]


def test_healthy_table_passes():
    assert unexplained_extreme_losers(_healthy_rows()) == []


def test_extreme_loser_check_fires_and_allowlist_silences_it():
    rows = _healthy_rows()
    rows[5]["mom_12_1_pct"] = -99.1
    assert unexplained_extreme_losers(rows) == [("T5", -99.1)]
    assert unexplained_extreme_losers(rows, {"T5": "delisted 2026-01-01 (verified)"}) == []


@pytest.mark.parametrize("bad", [float("nan"), None, "n/a"])
def test_missing_momentum_is_ignored_not_flagged(bad):
    rows = _healthy_rows()
    rows[0]["mom_12_1_pct"] = bad
    assert unexplained_extreme_losers(rows) == []


# ---- split-repair regression: the 2026-09-11 MNST flip-flop bug ----------------------------
#
# A naive "divide everything before this seam by the split factor" repair, applied once per
# matched candidate in date order, corrupts a series when the same split shows up as more than
# one candidate (the vendor's feed flipped back to the pre-split basis before settling): the
# first and a later match both touch the earliest segment, dividing it by the factor twice.
# MNST 2026-08 did exactly this around its real 2026-08-11 2-for-1 -- three candidates
# (2026-08-03, -08-05, -08-10) for one split. This reproduces that shape with round numbers so
# the expected repaired values are exact, and asserts every seam is gone afterward.

def test_split_repair_resolves_a_flip_flop_not_just_the_first_seam():
    values = {
        "2026-07-27": 96.0, "2026-07-28": 96.0, "2026-07-29": 96.0, "2026-07-30": 96.0,
        "2026-07-31": 96.0,
        "2026-08-03": 48.0, "2026-08-04": 48.0,
        "2026-08-05": 96.0, "2026-08-06": 96.0, "2026-08-07": 96.0,
        "2026-08-10": 48.0, "2026-08-11": 48.0, "2026-08-12": 48.0, "2026-08-13": 48.0,
        "2026-08-14": 48.0,
    }
    series = pd.Series({pd.Timestamp(k): v for k, v in values.items()}).sort_index()
    frame = pd.DataFrame({"MNST": series})

    assert len(find_split_candidates(series)) == 3  # the flip-flop, reproduced

    repaired, report = repair_split_seams(frame, splits_lookup=lambda _t: {pd.Timestamp("2026-08-11"): 2.0})

    assert report["MNST"] == [
        (pd.Timestamp("2026-08-03"), 2.0),
        (pd.Timestamp("2026-08-05"), 2.0),
        (pd.Timestamp("2026-08-10"), 2.0),
    ]
    assert find_split_candidates(repaired["MNST"]) == []  # fully continuous, not just the first seam
    # every value now on the post-split (48) basis, not double-divided to 24 on the earliest segment
    assert repaired["MNST"].round(6).unique().tolist() == [48.0]


def test_split_repair_leaves_uncalendared_moves_alone():
    """A genuine crash/spike with no matching split (EPAM 2022-02-28, GL 2024-04-11 in the real
    cache) must not be touched -- repair_split_seams has no evidence to act on."""
    idx = pd.bdate_range("2026-01-01", periods=10)
    values = [100.0, 101.0, 102.0, 55.0, 54.0, 54.5, 55.0, 55.5, 56.0, 56.5]
    series = pd.Series(values, index=idx)
    frame = pd.DataFrame({"NOSPLIT": series})

    repaired, report = repair_split_seams(frame, splits_lookup=lambda _t: {})

    assert report == {}
    assert repaired["NOSPLIT"].equals(series)


def test_split_repair_handles_a_reverse_split():
    """DD 2026-06 (1-for-3 reverse split, yfinance factor 0.3333): the seam ratio is ~2.99 (an
    UP step), matched by the SAME forward-match branch because the calendar factor is already
    < 1 -- the pre-seam segment must be multiplied, not divided, to reach the new basis."""
    idx = pd.bdate_range("2026-06-10", "2026-06-24")
    before, after = 50.0, 150.0
    series = pd.Series([before] * 6 + [after] * (len(idx) - 6), index=idx)
    frame = pd.DataFrame({"DD": series})

    repaired, report = repair_split_seams(frame, splits_lookup=lambda _t: {pd.Timestamp("2026-06-24"): 1 / 3})

    assert find_split_candidates(repaired["DD"]) == []
    assert repaired["DD"].round(6).unique().tolist() == [after]
