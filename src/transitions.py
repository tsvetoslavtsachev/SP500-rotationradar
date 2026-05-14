"""
Attention layer — Lonely Strength + Premium cohorts.

Empirically validated on 10y backtest 2016-2026:
- Lonely SW (SW in sector with below-median momentum): +3.31% mean excess 63d,
  N=1170, p<0.0001, confirmed out-of-sample.
- Premium (Lonely × ≥6 consecutive months as SW): +9.15% mean excess (N=116,
  CI [+4.30%, +13.99%]).
- Consumer Staples SW underperform structurally (-1.60%, p=0.037, N=246) —
  excluded by default.

These signals identify stocks where the market shows persistent leadership
despite a lagging sector — typically the "AMD-like" story. The layer is
descriptive (attention direction), not prescriptive (trading signal).

Rebalance schedule anchored at 2022-04-27 + 21 business day increments,
matching `backtest_v2_results.parquet` cadence. Stable across daily reruns.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.rank_history import (
    BASE_END_DAYS,
    BASE_START_DAYS,
    DELTA_1M_DAYS,
    HIGH_BASE_THRESHOLD,
    LOW_BASE_THRESHOLD,
    load_history,
)

# Recent rebalance dates from `backtest_v2_results.parquet` (2024-2025).
# Anchoring to these exact dates (which are ~21-22 BD apart, month-end-ish)
# keeps the SW streak calculation consistent with the empirical backtest.
# Forward extension after the last anchor uses +21 BD increments.
BACKTEST_REBALANCE_DATES = [
    "2023-12-01", "2024-01-03", "2024-02-02", "2024-03-04", "2024-04-02",
    "2024-04-30", "2024-05-30", "2024-07-01", "2024-07-31", "2024-08-29",
    "2024-09-30", "2024-10-29", "2024-11-27", "2024-12-30", "2025-01-31",
    "2025-03-04", "2025-04-02", "2025-05-02", "2025-06-03", "2025-07-03",
    "2025-08-04", "2025-09-03", "2025-10-02",
]

# How many monthly rebalances to compute backward from the latest available
# date. 30 covers 2.5 years — enough to detect 12+ month SW streaks.
N_REBALANCES = 30

# Sectors that the 10y backtest showed structurally underperform as SW.
DEFAULT_EXCLUDED_SECTORS = ("Consumer Staples",)


def _classify(base: float, delta: float) -> str:
    if not np.isfinite(base) or not np.isfinite(delta):
        return "unknown"
    if base <= LOW_BASE_THRESHOLD and delta > 0:
        return "riser"
    if base >= HIGH_BASE_THRESHOLD and delta < 0:
        return "decayer"
    if base >= HIGH_BASE_THRESHOLD and delta >= 0:
        return "stable_winner"
    if base <= LOW_BASE_THRESHOLD and delta <= 0:
        return "chronic_loser"
    return "neutral"


def _build_rebalance_schedule(latest_date: pd.Timestamp,
                              n_rebalances: int = N_REBALANCES) -> list[pd.Timestamp]:
    """
    Build the rebalance schedule:
    1. Start with the hardcoded backtest dates (2024-2025).
    2. Extend forward via +21 BD steps from the last backtest date until
       we reach or exceed latest_date.
    3. Always include latest_date itself if it falls past the last extended
       step (gives the freshest view).

    Returns the last n_rebalances dates (oldest first).
    """
    schedule = [pd.Timestamp(d) for d in BACKTEST_REBALANCE_DATES
                if pd.Timestamp(d) <= latest_date]
    cursor = schedule[-1] if schedule else pd.Timestamp("2022-04-27")
    while True:
        cursor = cursor + pd.tseries.offsets.BusinessDay(DELTA_1M_DAYS)
        if cursor > latest_date:
            break
        schedule.append(cursor)
    if schedule[-1] < latest_date:
        schedule.append(latest_date)
    return schedule[-n_rebalances:]


def _snap_to_history(target: pd.Timestamp, available: np.ndarray) -> pd.Timestamp | None:
    """Snap target date to nearest available date <= target."""
    valid = available[available <= np.datetime64(target)]
    if len(valid) == 0:
        return None
    return pd.Timestamp(valid[-1])


def _compute_rebalance(history: pd.DataFrame,
                       as_of: pd.Timestamp,
                       all_dates: np.ndarray) -> pd.DataFrame | None:
    """Compute base_rank_6m, delta_1m, quadrant_1m at a given rebalance date."""
    snapped = _snap_to_history(as_of, all_dates)
    if snapped is None:
        return None
    as_of = snapped

    current = history[history["date"] == as_of].set_index("ticker")["percentile_rank"]
    if current.empty:
        return None

    cutoff_1m = as_of - pd.tseries.offsets.BusinessDay(DELTA_1M_DAYS)
    snap_1m = _snap_to_history(cutoff_1m, all_dates)
    if snap_1m is None:
        return None
    rank_1m = history[history["date"] == snap_1m].set_index("ticker")["percentile_rank"]

    upper = as_of - pd.tseries.offsets.BusinessDay(BASE_END_DAYS)
    lower = as_of - pd.tseries.offsets.BusinessDay(BASE_START_DAYS)
    window = history[(history["date"] >= lower) & (history["date"] <= upper)]
    base = window.groupby("ticker")["percentile_rank"].mean()

    df = pd.DataFrame({
        "current_rank": current,
        "rank_1m_ago": rank_1m,
        "base_rank_6m": base,
    })
    df["delta_1m"] = df["current_rank"] - df["rank_1m_ago"]
    df["quadrant_1m"] = [
        _classify(b, d) for b, d in zip(df["base_rank_6m"], df["delta_1m"])
    ]
    df["rebalance_date"] = as_of
    df = df.dropna(subset=["base_rank_6m", "delta_1m"])
    return df.reset_index()


def compute_attention_signals(
    history: pd.DataFrame,
    sectors: pd.DataFrame,
    n_rebalances: int = N_REBALANCES,
) -> dict:
    """
    Compute attention signals (lonely, sw_streak, premium) at the latest rebalance.

    Parameters
    ----------
    history : DataFrame with columns date, ticker, percentile_rank.
    sectors : DataFrame with columns ticker, gics_sector, gics_sub_industry, name.

    Returns
    -------
    dict with keys:
        - latest_date: ISO date string of the displayed rebalance
        - tickers: list of dicts (one per ticker classified as Stable Winner
          at the latest rebalance) with attention fields
        - schedule: list of ISO date strings (used rebalance dates, oldest first)
    """
    if history.empty:
        return {"latest_date": None, "tickers": [], "schedule": []}

    history = history.copy()
    history["date"] = pd.to_datetime(history["date"])
    all_dates = np.array(sorted(history["date"].unique()), dtype="datetime64[ns]")
    latest_date = pd.Timestamp(all_dates[-1])

    schedule = _build_rebalance_schedule(latest_date, n_rebalances=n_rebalances)

    # Compute each rebalance
    frames = []
    for d in schedule:
        df = _compute_rebalance(history, d, all_dates)
        if df is not None:
            frames.append(df)
    if not frames:
        return {"latest_date": None, "tickers": [], "schedule": []}

    bt = pd.concat(frames, ignore_index=True)
    bt = bt.merge(
        sectors[["ticker", "gics_sector", "gics_sub_industry", "name"]],
        on="ticker", how="left",
    )

    # Sector strength + Lonely flag
    sector_strength = (
        bt.groupby(["rebalance_date", "gics_sector"])["base_rank_6m"]
          .mean().reset_index(name="sector_avg_rank")
    )
    sector_strength["rebalance_median"] = (
        sector_strength.groupby("rebalance_date")["sector_avg_rank"]
                       .transform("median")
    )
    sector_strength["weak_sector"] = (
        sector_strength["sector_avg_rank"] < sector_strength["rebalance_median"]
    )
    bt = bt.merge(
        sector_strength[["rebalance_date", "gics_sector",
                         "sector_avg_rank", "weak_sector"]],
        on=["rebalance_date", "gics_sector"], how="left",
    )
    bt["is_lonely"] = (bt["quadrant_1m"] == "stable_winner") & bt["weak_sector"]

    # SW streak (consecutive months, resets on non-SW)
    bt = bt.sort_values(["ticker", "rebalance_date"]).reset_index(drop=True)
    bt["is_sw"] = bt["quadrant_1m"] == "stable_winner"
    bt["streak_id"] = (~bt["is_sw"]).groupby(bt["ticker"]).cumsum()
    bt["sw_streak"] = bt.groupby(["ticker", "streak_id"]).cumcount() + 1
    bt.loc[~bt["is_sw"], "sw_streak"] = 0

    # Premium
    bt["is_premium"] = bt["is_lonely"] & (bt["sw_streak"] >= 6)

    # Take only the latest rebalance for output
    last_rebalance = bt["rebalance_date"].max()
    last = bt[bt["rebalance_date"] == last_rebalance].copy()

    # Output only Stable Winners (the attention layer is SW-focused)
    sw_only = last[last["quadrant_1m"] == "stable_winner"].copy()
    sw_only = sw_only.sort_values(
        ["is_premium", "is_lonely", "base_rank_6m"],
        ascending=[False, False, False],
    )

    out = []
    for _, row in sw_only.iterrows():
        out.append({
            "ticker": row["ticker"],
            "name": row.get("name"),
            "sector": row.get("gics_sector"),
            "sub_industry": row.get("gics_sub_industry"),
            "base_rank_6m": _round(row["base_rank_6m"], 1),
            "delta_1m": _round(row["delta_1m"], 2),
            "sector_avg_rank": _round(row["sector_avg_rank"], 1),
            "is_lonely": bool(row["is_lonely"]),
            "sw_streak": int(row["sw_streak"]),
            "is_premium": bool(row["is_premium"]),
            "is_cs_excluded": row.get("gics_sector") in DEFAULT_EXCLUDED_SECTORS,
        })

    return {
        "latest_date": pd.Timestamp(last_rebalance).strftime("%Y-%m-%d"),
        "tickers": out,
        "schedule": [pd.Timestamp(d).strftime("%Y-%m-%d") for d in schedule],
    }


def _round(x, ndigits: int):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return None
    return round(float(x), ndigits)


if __name__ == "__main__":
    ROOT = Path(__file__).resolve().parents[1]
    from src.sector_engine import get_sector_dataframe

    history = load_history(ROOT / "data" / "ranks_history.parquet")
    sectors = get_sector_dataframe(ROOT / "data" / "sector_map.json")

    result = compute_attention_signals(history, sectors)
    print(f"Latest rebalance: {result['latest_date']}")
    print(f"Schedule ({len(result['schedule'])} dates): "
          f"{result['schedule'][0]} → {result['schedule'][-1]}")
    print(f"\nStable Winners at latest: {len(result['tickers'])}")
    print(f"  Premium: {sum(t['is_premium'] for t in result['tickers'])}")
    print(f"  Lonely (non-premium): "
          f"{sum(t['is_lonely'] and not t['is_premium'] for t in result['tickers'])}")
    print(f"  Normal (sector ≥ median): "
          f"{sum(not t['is_lonely'] for t in result['tickers'])}")
    print(f"  CS-excluded: {sum(t['is_cs_excluded'] for t in result['tickers'])}")

    print("\nTop 20 SW (sorted by premium, lonely, rank):")
    for t in result["tickers"][:20]:
        flags = []
        if t["is_premium"]:
            flags.append("PREMIUM")
        if t["is_lonely"] and not t["is_premium"]:
            flags.append("Lonely")
        if t["is_cs_excluded"]:
            flags.append("CS")
        flag_str = f" [{','.join(flags)}]" if flags else ""
        print(f"  {t['ticker']:>6} | rank {t['base_rank_6m']:>5} | "
              f"sec_avg {t['sector_avg_rank']:>4} | streak {t['sw_streak']:>2} | "
              f"{t['sector']}{flag_str}")
