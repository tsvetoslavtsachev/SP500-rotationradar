"""
Backtest V2 — pure 12-1 momentum + sector-relative z-score, без vol-нормализация.

Тества три неща едновременно:
  1. Pure 12-1 score (само класически Jegadeesh-Titman, без 6-1 / 3-1)
  2. Sector-relative z-score (премахва vol bias и intra-sector benchmarking)
  3. Тiгht прагове p20/p80 (по-малко false positives)

Пише резултатите в backtest_v2_results.parquet и сравнява със v1 baseline-а.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.signal_engine import (  # noqa: E402
    MOM_12M_DAYS,
    SKIP_DAYS,
    _period_return,
)

DATA_DIR = ROOT / "data"
HISTORY_PATH = DATA_DIR / "ranks_history.parquet"
PRICES_PATH = DATA_DIR / "prices_cache.parquet"
SECTOR_PATH = DATA_DIR / "sector_map.json"

FORWARD_HORIZONS = [21, 63, 126]
REBALANCE_FREQ_DAYS = 21
MIN_QUADRANT_SIZE = 5

# Tighter quadrant thresholds
HIGH_BASE_THRESHOLD = 80.0
LOW_BASE_THRESHOLD = 20.0

# Delta windows
DELTA_1M_DAYS = 21
DELTA_3M_DAYS = 63
BASE_START_DAYS = 126
BASE_END_DAYS = 21


def load_sector_map() -> dict[str, str]:
    with SECTOR_PATH.open(encoding="utf-8") as f:
        cache = json.load(f)
    return {
        ticker: info.get("gics_sector", "Unknown")
        for ticker, info in cache["tickers"].items()
    }


def compute_v2_cross_section(
    prices_df: pd.DataFrame,
    sector_map: dict[str, str],
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    """
    V2 score: pure 12-1 momentum, sector-relative z-score.
    Връща DataFrame с date, ticker, mom_12_1, sector_zscore, percentile_rank.
    """
    sliced = prices_df.loc[:as_of]
    if len(sliced) < MOM_12M_DAYS + 1:
        return pd.DataFrame()

    rows = []
    for ticker in sliced.columns:
        prices = sliced[ticker].dropna()
        mom = _period_return(prices, MOM_12M_DAYS, SKIP_DAYS)
        if not np.isfinite(mom):
            continue
        rows.append({
            "ticker": ticker,
            "mom_12_1": mom,
            "sector": sector_map.get(ticker, "Unknown"),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Sector-relative z-score
    df["sector_zscore"] = df.groupby("sector")["mom_12_1"].transform(
        lambda s: (s - s.mean()) / s.std() if s.std() > 0 else 0.0
    )
    # Cross-sectional percentile rank на z-score
    df["percentile_rank"] = df["sector_zscore"].rank(pct=True) * 100.0
    df["raw_score"] = df["sector_zscore"]
    df["unadj_percentile"] = df["mom_12_1"].rank(pct=True) * 100.0
    df["date"] = as_of
    return df


def build_v2_history(
    prices_df: pd.DataFrame,
    sector_map: dict[str, str],
    sample_dates: pd.DatetimeIndex,
) -> pd.DataFrame:
    rows = []
    for i, dt in enumerate(sample_dates):
        cs = compute_v2_cross_section(prices_df, sector_map, dt)
        if cs.empty:
            continue
        rows.append(cs[["date", "ticker", "raw_score", "percentile_rank", "unadj_percentile"]])
        if (i + 1) % 200 == 0:
            print(f"  Built {i+1}/{len(sample_dates)} cross-sections")
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


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


def compute_v2_deltas(history: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    current = history[history["date"] == as_of].set_index("ticker")
    if current.empty:
        latest = history[history["date"] <= as_of]["date"].max()
        if pd.isna(latest):
            return pd.DataFrame()
        current = history[history["date"] == latest].set_index("ticker")
        as_of = latest

    rank_now = current["percentile_rank"]

    cutoff_1m = as_of - pd.tseries.offsets.BusinessDay(DELTA_1M_DAYS)
    cutoff_3m = as_of - pd.tseries.offsets.BusinessDay(DELTA_3M_DAYS)
    upper_base = as_of - pd.tseries.offsets.BusinessDay(BASE_END_DAYS)
    lower_base = as_of - pd.tseries.offsets.BusinessDay(BASE_START_DAYS)

    def _at(date_cutoff):
        sub = history[history["date"] <= date_cutoff]
        if sub.empty:
            return pd.Series(dtype=float)
        last_dt = sub["date"].max()
        return sub[sub["date"] == last_dt].set_index("ticker")["percentile_rank"]

    rank_1m = _at(cutoff_1m)
    rank_3m = _at(cutoff_3m)

    base_window = history[(history["date"] >= lower_base) & (history["date"] <= upper_base)]
    base = base_window.groupby("ticker")["percentile_rank"].mean() if not base_window.empty else pd.Series(dtype=float)

    out = pd.DataFrame({
        "current_rank": rank_now,
        "rank_1m_ago": rank_1m,
        "rank_3m_ago": rank_3m,
        "base_rank_6m": base,
    })
    out["delta_1m"] = out["current_rank"] - out["rank_1m_ago"]
    out["delta_3m"] = out["current_rank"] - out["rank_3m_ago"]
    out["quadrant_1m"] = [_classify(b, d) for b, d in zip(out["base_rank_6m"], out["delta_1m"])]
    out["quadrant_3m"] = [_classify(b, d) for b, d in zip(out["base_rank_6m"], out["delta_3m"])]
    out["as_of_date"] = as_of
    return out.reset_index()


def forward_return(prices: pd.DataFrame, ticker: str, start: pd.Timestamp, h: int) -> float:
    if ticker not in prices.columns:
        return np.nan
    series = prices[ticker].dropna()
    if series.empty:
        return np.nan
    after_s = series.index[series.index >= start]
    if len(after_s) == 0:
        return np.nan
    s0 = after_s[0]
    target = s0 + pd.tseries.offsets.BusinessDay(h)
    after_e = series.index[series.index >= target]
    if len(after_e) == 0:
        return np.nan
    p0 = series.loc[s0]
    p1 = series.loc[after_e[0]]
    if not np.isfinite(p0) or not np.isfinite(p1) or p0 <= 0:
        return np.nan
    return float(p1 / p0 - 1.0)


def run_backtest_v2() -> pd.DataFrame:
    print("Loading data...")
    prices = pd.read_parquet(PRICES_PATH)
    prices.index = pd.to_datetime(prices.index)
    sector_map = load_sector_map()
    print(f"  Prices: {len(prices)} days × {len(prices.columns)} tickers")
    print(f"  Sector map: {len(sector_map)} tickers")

    print("\nBuilding V2 history (pure 12-1 + sector z-score)...")
    earliest = prices.index[MOM_12M_DAYS + 1]
    sample_dates = prices.index[prices.index >= earliest]
    history = build_v2_history(prices, sector_map, sample_dates)
    print(f"  Built {len(history):,} V2 rank records")

    history.to_parquet(DATA_DIR / "ranks_history_v2.parquet", index=False)
    print(f"  Saved ranks_history_v2.parquet ({(DATA_DIR / 'ranks_history_v2.parquet').stat().st_size / 1e6:.1f} MB)")

    print("\nRunning backtest with V2 history + p20/p80 thresholds...")
    last_horizon = max(FORWARD_HORIZONS)
    cutoff = pd.Timestamp(history["date"].max()) - pd.tseries.offsets.BusinessDay(last_horizon + 5)
    valid_dates = sorted(d for d in history["date"].unique() if d >= sample_dates[252] and d <= cutoff)
    rebalance_dates = valid_dates[::REBALANCE_FREQ_DAYS]
    print(f"  Rebalance dates: {len(rebalance_dates)} ({rebalance_dates[0].date()} → {rebalance_dates[-1].date()})")

    chunks = []
    for i, dt in enumerate(rebalance_dates):
        deltas = compute_v2_deltas(history, dt)
        if deltas.empty:
            continue
        deltas = deltas.dropna(subset=["quadrant_1m", "quadrant_3m"])
        for _, row in deltas.iterrows():
            rec = {
                "rebalance_date": dt,
                "ticker": row["ticker"],
                "quadrant_1m": row["quadrant_1m"],
                "quadrant_3m": row["quadrant_3m"],
                "base_rank_6m": row["base_rank_6m"],
                "delta_1m": row["delta_1m"],
                "delta_3m": row["delta_3m"],
            }
            for h in FORWARD_HORIZONS:
                rec[f"fwd_{h}d"] = forward_return(prices, row["ticker"], dt, h)
            chunks.append(rec)
        if (i + 1) % 6 == 0 or i == len(rebalance_dates) - 1:
            print(f"  Processed {i+1}/{len(rebalance_dates)}")

    return pd.DataFrame(chunks)


def summarize(results: pd.DataFrame, label: str) -> None:
    print(f"\n{'=' * 75}")
    print(f"{label}")
    print(f"{'=' * 75}")

    benchmarks = {
        h: results.groupby("rebalance_date")[f"fwd_{h}d"].mean().mean()
        for h in FORWARD_HORIZONS
    }
    print("\nBenchmark (cross-section mean forward return):")
    for h in FORWARD_HORIZONS:
        bm = benchmarks[h]
        if not np.isnan(bm):
            print(f"  {h:3d}d: {bm * 100:+.2f}%")

    for window in ["1m", "3m"]:
        col = f"quadrant_{window}"
        print(f"\n{'─' * 75}")
        print(f"BY QUADRANT (delta_{window})")
        print("─" * 75)
        print(f"{'Quadrant':<18}{'N obs':>8}", end="")
        for h in FORWARD_HORIZONS:
            print(f"  fwd_{h}d_mean    excess  hit_rate", end="")
        print()

        for quad in ["riser", "decayer", "stable_winner", "chronic_loser", "neutral"]:
            sub = results[results[col] == quad]
            if sub.empty:
                continue
            print(f"{quad:<18}{len(sub):>8}", end="")
            for h in FORWARD_HORIZONS:
                vals = sub[f"fwd_{h}d"].dropna()
                if len(vals) < MIN_QUADRANT_SIZE:
                    print(f"  {'—':>10}  {'—':>8}  {'—':>8}", end="")
                    continue
                m = vals.mean()
                ex = m - benchmarks[h]
                hr = (vals > benchmarks[h]).mean()
                print(f"  {m * 100:+8.2f}%  {ex * 100:+7.2f}%  {hr * 100:6.1f}%", end="")
            print()

    print(f"\n{'─' * 75}")
    print("YEARLY CONSISTENCY (excess fwd_63d по quadrant_1m)")
    print("─" * 75)
    results["year"] = pd.to_datetime(results["rebalance_date"]).dt.year
    print(f"{'Year':<6}{'Riser':>10}{'Decayer':>10}{'Stable':>10}{'Chronic':>10}")
    for year, year_df in results.groupby("year"):
        bm = year_df.groupby("rebalance_date")["fwd_63d"].mean().mean()
        line = f"{year:<6}"
        for q in ["riser", "decayer", "stable_winner", "chronic_loser"]:
            sub = year_df[year_df["quadrant_1m"] == q]["fwd_63d"].dropna()
            if len(sub) < MIN_QUADRANT_SIZE:
                line += f"{'—':>10}"
            else:
                line += f"{(sub.mean() - bm) * 100:+9.2f}%"
        print(line)

    print(f"\n{'─' * 75}")
    print("SUMMARY")
    print("─" * 75)
    riser_ex = (
        results[results["quadrant_1m"] == "riser"]["fwd_63d"].dropna().mean()
        - benchmarks[63]
    )
    decay_ex = (
        results[results["quadrant_1m"] == "decayer"]["fwd_63d"].dropna().mean()
        - benchmarks[63]
    )
    stable_ex = (
        results[results["quadrant_1m"] == "stable_winner"]["fwd_63d"].dropna().mean()
        - benchmarks[63]
    )
    chronic_ex = (
        results[results["quadrant_1m"] == "chronic_loser"]["fwd_63d"].dropna().mean()
        - benchmarks[63]
    )
    print(f"  Risers fwd_63d excess:        {riser_ex*100:+.2f}%")
    print(f"  Decayers fwd_63d excess:      {decay_ex*100:+.2f}%")
    print(f"  Stable Winners fwd_63d excess: {stable_ex*100:+.2f}%")
    print(f"  Chronic Losers fwd_63d excess: {chronic_ex*100:+.2f}%")
    print(f"  Risers − Decayers spread:     {(riser_ex - decay_ex)*100:+.2f}%")


def main() -> None:
    results = run_backtest_v2()
    out_path = DATA_DIR / "backtest_v2_results.parquet"
    results.to_parquet(out_path, index=False)
    print(f"\nWrote {out_path.name} ({out_path.stat().st_size / 1e6:.1f} MB)")
    summarize(results, "BACKTEST V2 — Pure 12-1 + Sector Z-Score + p20/p80 thresholds")


if __name__ == "__main__":
    main()
