"""
ЕДНОКРАТЕН retrospective backfill — построява 5-годишна rank history.

Стъпки:
  1. Изтегля SP500 universe (текущи + наскоро премахнати за намалено survivorship bias)
  2. Изтегля 5y price history за всички ticker-и
  3. За всяка business day в [start+252d, today]: пресмята cross-section ranks
  4. Записва в data/ranks_history.parquet
  5. Опционално: --validate стартира sanity checks

Използване:
  python scripts/backfill_history.py
  python scripts/backfill_history.py --years 5 --sample-every 1
  python scripts/backfill_history.py --validate
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.prices import download_prices  # noqa: E402
from src.rank_history import HISTORY_COLUMNS, build_history_from_prices  # noqa: E402
from src.universe import fetch_full_universe  # noqa: E402

DATA_DIR = ROOT / "data"
HISTORY_PATH = DATA_DIR / "ranks_history.parquet"
PRICES_CACHE_PATH = DATA_DIR / "prices_cache.parquet"


def run_backfill(
    years: int = 5,
    sample_every: int = 1,
    use_cache: bool = True,
) -> Path:
    """
    sample_every: 1 = всеки търговски ден; 5 = веднъж седмично; полезно за бърз dev cycle.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[1/4] Fetching SP500 universe (current + removed last {years}y)...")
    universe = fetch_full_universe(years_back=years)
    tickers = universe["ticker"].tolist()
    print(f"      {len(tickers)} tickers ({universe['is_current'].sum()} current)")

    end = pd.Timestamp.today().normalize()
    start = end - pd.DateOffset(years=years + 1)  # +1y буфер за 12m lookback

    if use_cache and PRICES_CACHE_PATH.exists():
        print(f"[2/4] Loading cached prices from {PRICES_CACHE_PATH.name}...")
        prices = pd.read_parquet(PRICES_CACHE_PATH)
        prices.index = pd.to_datetime(prices.index)
    else:
        print(f"[2/4] Downloading {len(tickers)} tickers prices ({start.date()} → {end.date()})...")
        prices = download_prices(tickers, start=start, end=end)
        prices.to_parquet(PRICES_CACHE_PATH)
        print(f"      Cached to {PRICES_CACHE_PATH.name}")

    print(f"      Got {len(prices.columns)} tickers × {len(prices)} days")

    print(f"[3/4] Computing daily cross-sections (sample_every={sample_every})...")
    cutoff = prices.index[252] if len(prices) > 252 else prices.index[0]
    sample_dates = prices.index[prices.index >= cutoff][::sample_every]
    print(f"      {len(sample_dates)} sample dates from {sample_dates[0].date()} to {sample_dates[-1].date()}")

    history = build_history_from_prices(prices, sample_dates)
    print(f"      Built {len(history)} rank records")

    print(f"[4/4] Writing to {HISTORY_PATH.name}...")
    history[HISTORY_COLUMNS].to_parquet(HISTORY_PATH, index=False)
    size_mb = HISTORY_PATH.stat().st_size / 1e6
    print(f"      Done. {size_mb:.1f} MB")

    return HISTORY_PATH


def run_validation() -> None:
    """
    Sanity check: познати исторически ротации трябва да са видими в backfill-а.
    """
    if not HISTORY_PATH.exists():
        print("ERROR: History file does not exist. Run backfill first.")
        sys.exit(1)

    history = pd.read_parquet(HISTORY_PATH)
    history["date"] = pd.to_datetime(history["date"])

    print(f"\nHistory spans: {history['date'].min().date()} → {history['date'].max().date()}")
    print(f"Total records: {len(history):,}")
    print(f"Unique tickers: {history['ticker'].nunique()}")

    expectations = [
        # (ticker, low rank period, low rank target, high rank period, high rank target)
        ("NVDA", "2022-10-01", 60, "2023-06-30", 90),
        ("META", "2022-11-01", 35, "2023-09-30", 90),
        ("PYPL", "2021-09-30", 70, "2022-06-30", 30),
    ]

    print("\nSanity checks (известни ротации):")
    for ticker, low_period, low_max, high_period, high_min in expectations:
        sub = history[history["ticker"] == ticker]
        if sub.empty:
            print(f"  ❌ {ticker}: НЯМА данни в history")
            continue

        low_dt = pd.Timestamp(low_period)
        high_dt = pd.Timestamp(high_period)
        low_snap = sub[sub["date"] <= low_dt].tail(1)
        high_snap = sub[sub["date"] <= high_dt].tail(1)

        if low_snap.empty or high_snap.empty:
            print(f"  ⚠ {ticker}: липсват snapshots за валидация")
            continue

        low_rank = low_snap["percentile_rank"].iloc[0]
        high_rank = high_snap["percentile_rank"].iloc[0]

        if ticker == "PYPL":
            ok = low_rank >= low_max - 15 and high_rank <= high_min + 15
        else:
            ok = low_rank <= low_max + 15 and high_rank >= high_min - 15
        symbol = "✓" if ok else "❌"
        print(
            f"  {symbol} {ticker}: rank @ {low_period} = {low_rank:.1f} "
            f"(expected ~{low_max}), @ {high_period} = {high_rank:.1f} "
            f"(expected ~{high_min})"
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--years", type=int, default=5)
    p.add_argument("--sample-every", type=int, default=1, help="Sample stride in business days")
    p.add_argument("--no-cache", action="store_true", help="Skip prices cache")
    p.add_argument("--validate", action="store_true", help="Run sanity checks")
    args = p.parse_args()

    if args.validate:
        run_validation()
        return

    run_backfill(
        years=args.years,
        sample_every=args.sample_every,
        use_cache=not args.no_cache,
    )
    run_validation()


if __name__ == "__main__":
    main()
