"""
Backtest на ΔRank quadrant logic.

Въпрос: ΔRank quadrant класификацията има ли предсказателна стойност?

Дизайн:
  За всяка ребалансираща дата T (monthly):
    1. Изчисли quadrant_1m, quadrant_3m използвайки само history <= T
    2. Идентифицирай Risers, Decayers, Stable Winners, Chronic Losers
    3. Изчисли forward returns: T+21d, T+63d, T+126d
    4. Сравни с equal-weight benchmark (cross-section mean return)

Метрики:
  - Mean excess return по quadrant vs benchmark
  - Hit rate (% with positive excess return)
  - Sharpe (mean / std)
  - Yearly consistency

Известни caveats:
  - Survivorship bias: текущ SP500 universe → positive bias на резултатите
  - 5y history → ограничени market regimes (bull-heavy)
  - Multiple comparisons → не претендираме за статистическа значимост,
    а за directional sanity check
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.rank_history import compute_delta_metrics  # noqa: E402

DATA_DIR = ROOT / "data"
HISTORY_PATH = DATA_DIR / "ranks_history.parquet"
PRICES_PATH = DATA_DIR / "prices_cache.parquet"

FORWARD_HORIZONS = [21, 63, 126]
REBALANCE_FREQ_DAYS = 21  # monthly
MIN_QUADRANT_SIZE = 5  # пропусни rebalance с по-малко от N акции в quadrant


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    history = pd.read_parquet(HISTORY_PATH)
    history["date"] = pd.to_datetime(history["date"])
    prices = pd.read_parquet(PRICES_PATH)
    prices.index = pd.to_datetime(prices.index)
    return history, prices


def forward_return(
    prices: pd.DataFrame,
    ticker: str,
    start: pd.Timestamp,
    horizon_days: int,
) -> float:
    """Forward total return от start до start + horizon business days."""
    if ticker not in prices.columns:
        return np.nan
    series = prices[ticker].dropna()
    if series.empty:
        return np.nan

    after_start = series.index[series.index >= start]
    if len(after_start) == 0:
        return np.nan
    actual_start = after_start[0]

    target_end = actual_start + pd.tseries.offsets.BusinessDay(horizon_days)
    after_end = series.index[series.index >= target_end]
    if len(after_end) == 0:
        return np.nan
    actual_end = after_end[0]

    p0 = series.loc[actual_start]
    p1 = series.loc[actual_end]
    if not np.isfinite(p0) or not np.isfinite(p1) or p0 <= 0:
        return np.nan
    return float(p1 / p0 - 1.0)


def run_single_rebalance(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    rebalance_date: pd.Timestamp,
    horizons: list[int],
) -> pd.DataFrame:
    """
    На rebalance_date изчисли quadrants, върни DataFrame:
    ticker, quadrant_1m, quadrant_3m, fwd_21d, fwd_63d, fwd_126d, base_rank_6m.
    """
    hist_subset = history[history["date"] <= rebalance_date]
    if hist_subset.empty:
        return pd.DataFrame()

    deltas = compute_delta_metrics(hist_subset, as_of=rebalance_date)
    if deltas.empty:
        return pd.DataFrame()

    deltas = deltas.dropna(subset=["quadrant_1m", "quadrant_3m", "current_rank"])

    rows = []
    for _, row in deltas.iterrows():
        record = {
            "rebalance_date": rebalance_date,
            "ticker": row["ticker"],
            "quadrant_1m": row["quadrant_1m"],
            "quadrant_3m": row["quadrant_3m"],
            "base_rank_6m": row["base_rank_6m"],
            "delta_1m": row["delta_1m"],
            "delta_3m": row["delta_3m"],
        }
        for h in horizons:
            record[f"fwd_{h}d"] = forward_return(prices, row["ticker"], rebalance_date, h)
        rows.append(record)

    return pd.DataFrame(rows)


def run_backtest(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    horizons: list[int] = FORWARD_HORIZONS,
    rebalance_freq_days: int = REBALANCE_FREQ_DAYS,
) -> pd.DataFrame:
    """
    Стартира всички rebalances, връща long-form DataFrame.
    """
    all_dates = sorted(history["date"].unique())
    last_horizon = max(horizons)
    cutoff = pd.Timestamp(all_dates[-1]) - pd.tseries.offsets.BusinessDay(last_horizon + 5)
    valid_dates = [d for d in all_dates if d >= all_dates[252] and d <= cutoff]
    rebalance_dates = valid_dates[::rebalance_freq_days]

    print(f"Backtest setup:")
    print(f"  History range: {pd.Timestamp(all_dates[0]).date()} → {pd.Timestamp(all_dates[-1]).date()}")
    print(f"  Rebalance dates: {len(rebalance_dates)} ({rebalance_dates[0].date()} → {rebalance_dates[-1].date()})")
    print(f"  Forward horizons: {horizons} business days")
    print()

    chunks = []
    for i, dt in enumerate(rebalance_dates):
        chunk = run_single_rebalance(history, prices, dt, horizons)
        chunks.append(chunk)
        if (i + 1) % 6 == 0 or i == len(rebalance_dates) - 1:
            print(f"  Processed {i+1}/{len(rebalance_dates)} rebalances")

    return pd.concat(chunks, ignore_index=True)


def summarize(results: pd.DataFrame, horizons: list[int]) -> None:
    """Принтира summary на backtest резултатите."""
    print("\n" + "=" * 75)
    print("BACKTEST RESULTS — Forward Returns by Quadrant")
    print("=" * 75)

    benchmarks = {
        h: results.groupby("rebalance_date")[f"fwd_{h}d"].mean().mean()
        for h in horizons
    }
    print("\nBenchmark (cross-sectional mean forward return per rebalance, then averaged):")
    for h in horizons:
        bm = benchmarks[h]
        if not np.isnan(bm):
            print(f"  {h:3d}d horizon: {bm * 100:+.2f}%")

    for window in ["1m", "3m"]:
        col = f"quadrant_{window}"
        print(f"\n{'─' * 75}")
        print(f"BY QUADRANT (delta_{window} based)")
        print("─" * 75)
        print(f"{'Quadrant':<18} {'N obs':>8}  ", end="")
        for h in horizons:
            print(f"  fwd_{h}d_mean    excess  hit_rate", end="")
        print()

        for quad in ["riser", "decayer", "stable_winner", "chronic_loser", "neutral"]:
            sub = results[results[col] == quad]
            if sub.empty:
                continue
            n_obs = len(sub)
            print(f"{quad:<18} {n_obs:>8}", end="  ")
            for h in horizons:
                fwd_col = f"fwd_{h}d"
                vals = sub[fwd_col].dropna()
                if len(vals) < MIN_QUADRANT_SIZE:
                    print(f"  {'—':>10}  {'—':>8}  {'—':>8}", end="")
                    continue
                mean = vals.mean()
                excess = mean - benchmarks[h]
                hit_rate = (vals > benchmarks[h]).mean()
                print(f"  {mean * 100:+8.2f}%  {excess * 100:+7.2f}%  {hit_rate * 100:6.1f}%", end="")
            print()

    print(f"\n{'─' * 75}")
    print("CONFIRMED RISERS (Riser в 1m AND 3m) vs ONLY-1M Risers")
    print("─" * 75)
    confirmed = results[
        (results["quadrant_1m"] == "riser") & (results["quadrant_3m"] == "riser")
    ]
    only_1m = results[
        (results["quadrant_1m"] == "riser") & (results["quadrant_3m"] != "riser")
    ]

    for label, sub in [("Confirmed (1m+3m)", confirmed), ("Only-1m", only_1m)]:
        print(f"\n{label}: N = {len(sub)}")
        for h in horizons:
            vals = sub[f"fwd_{h}d"].dropna()
            if len(vals) < MIN_QUADRANT_SIZE:
                continue
            mean = vals.mean()
            excess = mean - benchmarks[h]
            hit = (vals > benchmarks[h]).mean()
            print(f"  fwd_{h}d:  mean {mean*100:+.2f}%  excess {excess*100:+.2f}%  hit_rate {hit*100:.1f}%")

    print(f"\n{'─' * 75}")
    print("YEARLY CONSISTENCY (excess return for fwd_63d)")
    print("─" * 75)
    results["year"] = pd.to_datetime(results["rebalance_date"]).dt.year
    print(f"\n{'Year':<6} {'Riser':>10} {'Decayer':>10} {'Stable':>10} {'Chronic':>10} {'N rebal.':>10}")
    for year, year_df in results.groupby("year"):
        n_rebal = year_df["rebalance_date"].nunique()
        bm = year_df.groupby("rebalance_date")["fwd_63d"].mean().mean()
        line = f"{year:<6} "
        for quad in ["riser", "decayer", "stable_winner", "chronic_loser"]:
            sub = year_df[year_df["quadrant_1m"] == quad]["fwd_63d"].dropna()
            if len(sub) < MIN_QUADRANT_SIZE:
                line += f"{'—':>10} "
            else:
                excess = sub.mean() - bm
                line += f"{excess*100:+9.2f}% "
        line += f"{n_rebal:>10}"
        print(line)

    print(f"\n{'─' * 75}")
    print("SUMMARY VERDICT")
    print("─" * 75)

    riser_excess_3m = (
        results[results["quadrant_1m"] == "riser"]["fwd_63d"].dropna().mean()
        - benchmarks[63]
    )
    decayer_excess_3m = (
        results[results["quadrant_1m"] == "decayer"]["fwd_63d"].dropna().mean()
        - benchmarks[63]
    )
    print(f"  Risers fwd_63d excess: {riser_excess_3m*100:+.2f}%")
    print(f"  Decayers fwd_63d excess: {decayer_excess_3m*100:+.2f}%")
    print(f"  Spread (Risers − Decayers): {(riser_excess_3m - decayer_excess_3m)*100:+.2f}%")

    if riser_excess_3m > 0.005 and decayer_excess_3m < -0.005:
        print("\n  ✓ Quadrant logic shows directional predictive power")
    elif riser_excess_3m - decayer_excess_3m > 0.01:
        print("\n  ~ Spread е положителен, но не двустранен")
    else:
        print("\n  ✗ Quadrant logic не показва ясна предсказателна стойност")

    print("\n  CAVEAT: survivorship bias е положителен — резултатите вероятно са оптимистични.")


def main() -> None:
    print("Loading data...")
    history, prices = load_data()
    print(f"  History: {len(history):,} records, {history['ticker'].nunique()} tickers")
    print(f"  Prices: {len(prices):,} days, {len(prices.columns)} tickers")
    print()

    results = run_backtest(history, prices)

    out_path = DATA_DIR / "backtest_results.parquet"
    results.to_parquet(out_path, index=False)
    print(f"\nWrote {out_path.name} ({out_path.stat().st_size / 1e6:.1f} MB)")

    summarize(results, FORWARD_HORIZONS)


if __name__ == "__main__":
    main()
