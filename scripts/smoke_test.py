"""
Smoke test — end-to-end pipeline с малък universe.
Проверява че всички стъпки минават без грешка преди пълен 5y backfill.

Use:
  python scripts/smoke_test.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.prices import download_prices  # noqa: E402
from src.rank_history import (  # noqa: E402
    HISTORY_COLUMNS,
    append_snapshot,
    compute_delta_metrics,
    get_top_decayers,
    get_top_risers,
)
from src.signal_engine import compute_cross_section  # noqa: E402

# Smoke universe — известни ротации за валидация
SMOKE_TICKERS = [
    "NVDA",  # Riser 2023
    "META",  # Turnaround 2023
    "PYPL",  # Decayer 2022
    "AAPL", "MSFT", "GOOGL", "AMZN",  # Stable winners
    "JNJ", "PG", "KO",  # Stable, low-vol
    "F", "GE",  # Cyclicals
]


def main() -> None:
    print(f"[1/4] Downloading 5y prices for {len(SMOKE_TICKERS)} smoke tickers...")
    end = pd.Timestamp.today().normalize()
    start = end - pd.DateOffset(years=5)
    prices = download_prices(SMOKE_TICKERS, start=start, end=end)
    print(f"      Got {len(prices.columns)} tickers × {len(prices)} days")
    print(f"      Date range: {prices.index.min().date()} → {prices.index.max().date()}")

    print(f"\n[2/4] Computing today's cross-section...")
    cs = compute_cross_section(prices)
    cs_valid = cs.dropna(subset=["raw_score"])
    print(f"      {len(cs_valid)} valid scores")
    print(cs_valid[["ticker", "raw_score", "percentile_rank"]].sort_values("raw_score", ascending=False).to_string(index=False))

    print(f"\n[3/4] Building synthetic history (last 6 months, weekly samples)...")
    sample_dates = prices.index[-126::5]
    rows = []
    for dt in sample_dates:
        sliced = prices.loc[:dt]
        if len(sliced) < 252:
            continue
        snap = compute_cross_section(sliced, as_of=dt)
        rows.append(snap[HISTORY_COLUMNS].dropna(subset=["raw_score"]))
    history = pd.concat(rows, ignore_index=True)
    print(f"      {len(history)} history rows across {history['date'].nunique()} dates")

    print(f"\n[4/4] Computing delta metrics + quadrant classification...")
    deltas = compute_delta_metrics(history)
    print(deltas[
        ["ticker", "current_rank", "base_rank_6m", "delta_1m", "delta_3m", "quadrant_1m", "quadrant_3m"]
    ].to_string(index=False))

    risers = get_top_risers(deltas, window="1m", limit=5)
    decayers = get_top_decayers(deltas, window="1m", limit=5)
    print(f"\n  Top risers (1m): {risers['ticker'].tolist() if not risers.empty else '(none)'}")
    print(f"  Top decayers (1m): {decayers['ticker'].tolist() if not decayers.empty else '(none)'}")

    print("\n✓ Smoke test passed — pipeline e функционален end-to-end.")


if __name__ == "__main__":
    main()
