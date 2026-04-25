"""
Daily orchestrator — изпълнява се от GitHub Actions всеки delnik след US close.

Стъпки:
  1. Зареди cached prices, изтегли само липсващите дни
  2. Пресметни today's cross-section
  3. Append към ranks_history.parquet (idempotent)
  4. Refresh sector cache ако е остарял
  5. Render data.json за dashboard
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.prices import download_prices  # noqa: E402
from src.rank_history import HISTORY_COLUMNS, append_snapshot  # noqa: E402
from src.render import render_dashboard_data  # noqa: E402
from src.sector_engine import get_sector_dataframe  # noqa: E402
from src.signal_engine import compute_cross_section  # noqa: E402
from src.universe import fetch_full_universe  # noqa: E402


def load_sector_map_for_scoring(cache_path: Path) -> dict[str, str]:
    """
    Зарежда GICS sector mapping за sector-relative z-score scoring.
    Извиква sector_engine да опресни ако cache-ът е остарял.
    """
    df = get_sector_dataframe(cache_path)
    return dict(zip(df["ticker"], df["gics_sector"]))

DATA_DIR = ROOT / "data"
HISTORY_PATH = DATA_DIR / "ranks_history.parquet"
PRICES_CACHE_PATH = DATA_DIR / "prices_cache.parquet"
SECTOR_CACHE_PATH = DATA_DIR / "sector_map.json"

# За daily run-a имаме нужда само от последните ~13 месеца (252 + малко буфер)
LOOKBACK_DAYS_FOR_SCORING = 280


def update_prices_cache(tickers: list[str]) -> pd.DataFrame:
    """
    Incremental update: ако имаме cache, изтегли само от последната дата нататък.
    Иначе зареди пълен 13-месечен прозорец.
    """
    end = pd.Timestamp.today().normalize()

    if PRICES_CACHE_PATH.exists():
        cached = pd.read_parquet(PRICES_CACHE_PATH)
        cached.index = pd.to_datetime(cached.index)
        last_date = cached.index.max()

        if last_date >= end - pd.tseries.offsets.BusinessDay(1):
            print(f"  Prices cache up to date ({last_date.date()}).")
            return cached.tail(LOOKBACK_DAYS_FOR_SCORING + 30)

        # Изтегли само нови дни — добавяме малко overlap за safety
        start = last_date - pd.Timedelta(days=5)
        print(f"  Incremental download {start.date()} → {end.date()}")
        new_prices = download_prices(tickers, start=start, end=end)

        if new_prices.empty:
            return cached.tail(LOOKBACK_DAYS_FOR_SCORING + 30)

        # Merge: новите данни презаписват overlap-а
        combined = pd.concat([
            cached[~cached.index.isin(new_prices.index)],
            new_prices,
        ]).sort_index()
        # Drop колони, които сега имат изцяло NaN (де-листвани ticker-и)
        combined = combined.dropna(axis=1, how="all")

        # Trim cache: пазим само последните ~6 години (повече от достатъчно за scoring)
        cutoff = end - pd.DateOffset(years=6)
        trimmed = combined[combined.index >= cutoff]
        trimmed.to_parquet(PRICES_CACHE_PATH)
        return trimmed.tail(LOOKBACK_DAYS_FOR_SCORING + 30)

    # No cache — full download
    start = end - pd.Timedelta(days=int(LOOKBACK_DAYS_FOR_SCORING * 1.6))
    print(f"  Full download {start.date()} → {end.date()}")
    prices = download_prices(tickers, start=start, end=end)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    prices.to_parquet(PRICES_CACHE_PATH)
    return prices


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("[1/5] Fetching SP500 universe...")
    universe = fetch_full_universe()
    tickers = universe[universe["is_current"]]["ticker"].tolist()
    print(f"      {len(tickers)} current tickers")

    print("[2/5] Updating prices cache...")
    prices = update_prices_cache(tickers)
    print(f"      {len(prices.columns)} tickers × {len(prices)} days")

    print("[3/5] Computing today's cross-section (sector-relative z-score)...")
    sector_map = load_sector_map_for_scoring(SECTOR_CACHE_PATH)
    print(f"      Loaded {len(sector_map)} sector mappings")
    cs = compute_cross_section(prices, sector_map=sector_map)
    cs = cs.dropna(subset=["raw_score"])
    print(f"      {len(cs)} valid scores for {cs['date'].iloc[0].date()}")

    print("[4/5] Appending snapshot to history...")
    append_snapshot(HISTORY_PATH, cs[HISTORY_COLUMNS])
    size_mb = HISTORY_PATH.stat().st_size / 1e6
    print(f"      History now {size_mb:.1f} MB")

    print("[5/5] Rendering data.json...")
    payload = render_dashboard_data()
    print(f"      Rendered: as of {payload['metadata']['as_of']}")
    print(
        f"      Stable Winners 1m: {len(payload['stable_winners_1m'])} | "
        f"Quality Dip 1m: {len(payload['quality_dip_1m'])} | "
        f"Faded Bounces: {len(payload['faded_bounces_1m'])}"
    )
    print(
        f"      Current Strength: {len(payload['current_strength'])} | "
        f"Sectors: {len(payload['sector_rotation'])}"
    )

    print("\nDaily update complete.")


if __name__ == "__main__":
    main()
