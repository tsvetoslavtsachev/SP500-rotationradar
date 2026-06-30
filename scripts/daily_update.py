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

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.etf_universe import benchmark_universe_tickers  # noqa: E402
from src.prices import download_prices  # noqa: E402
# INIT-22 P9 strangler: read canonical daily Close FROM the price-archive (base) first, keeping the
# OLD yfinance download_prices as a CLOSED fallback so production never stops. ONE canonical reader
# (collectors/price/consumer.py) shared by every consumer. If collectors/data-core are not
# importable (a bare local run, no archive checkout) we degrade to the pure fetch. The CI guard
# (scripts/assert_base_sourced.py), gated on the read PATs, fails RED on a silent mass-fallback.
# SP500 + SPDR benchmarks are USD -> normalize_currency=False (only the 126 GBX .L STOXX names /100).
try:
    from collectors.price.consumer import load_ohlcv_base_first  # noqa: E402
    _HAVE_BASE = True
except ImportError:
    _HAVE_BASE = False
from src.rank_history import HISTORY_COLUMNS, append_snapshot  # noqa: E402
from src.render import render_dashboard_data  # noqa: E402
from src.sector_engine import get_sector_dataframe  # noqa: E402
from src.signal_engine import compute_cross_section  # noqa: E402
from src.universe import fetch_full_universe  # noqa: E402

DATA_DIR = ROOT / "data"
HISTORY_PATH = DATA_DIR / "ranks_history.parquet"
PRICES_CACHE_PATH = DATA_DIR / "prices_cache.parquet"
BENCHMARK_PRICES_PATH = DATA_DIR / "benchmark_prices.parquet"
SECTOR_CACHE_PATH = DATA_DIR / "sector_map.json"
MARKET_CAPS_PATH = DATA_DIR / "market_caps.json"

# За screener-а ни трябват 5 години → пазим минимум 6 години
LOOKBACK_DAYS_FOR_SCORING = 1500
# RS Line има нужда само от 50 SMA + 252-day high → 2y е достатъчно
BENCHMARK_LOOKBACK_DAYS = 2 * 365
MARKET_CAP_REFRESH_DAYS = 7

# INIT-22 P9 strangler helpers ------------------------------------------------
PRICE_SOURCE_PATH = DATA_DIR / "price_source.json"


def _clean_close(df: pd.DataFrame, requested) -> pd.DataFrame:
    """Normalize a download_prices result to flat ticker-string columns, keeping only requested
    names. The VENDORED download_prices single-ticker path can return a MultiIndex/tuple column
    (current yfinance) -- e.g. ('HON','HON') -- which would leak a junk column into the price frame
    AND escape the consumer's provenance stamping. We can't edit the vendored src/prices.py, so the
    shim sanitizes here (flatten tuple -> ticker, drop anything not requested)."""
    if df is None or getattr(df, "empty", True):
        return pd.DataFrame()
    req = {str(x).upper(): x for x in requested}
    out = {}
    for col in df.columns:
        key = col[-1] if isinstance(col, tuple) else col
        canon = req.get(str(key).upper())
        if canon is not None and canon not in out:
            out[canon] = df[col]
    return pd.DataFrame(out) if out else pd.DataFrame()


def _base_first_close(tickers: list[str], start, end, source_acc: dict) -> pd.DataFrame:
    """Base-first flat adjusted-Close read mirroring download_prices' shape (DatetimeIndex x
    tickers), with the OLD download_prices as a CLOSED fallback (P9 strangler -- production NEVER
    stops: it degrades to the old fetch when the base reader is unimportable AND on ANY base-read
    exception). Accumulates {ticker -> 'base'|'fetch'|'missing'} into ``source_acc`` across every
    call (both the SP500 prices cache AND the SPDR benchmark cache). Every REQUESTED ticker is
    recorded -- one the archive cannot serve AND the fallback cannot fetch is stamped 'missing' (the
    consumer pops it from its own map, so without this it would evaporate and the guard's base
    fraction would not see it). SP500 + SPDR are USD -> normalize_currency=False.
    """
    def _pure_fetch() -> pd.DataFrame:
        # The OLD path: pull the whole requested set from yfinance (the CLOSED fallback). Used when
        # the base reader is unimportable (no archive checkout) AND when a base read raises -- the
        # strangler must degrade, never hard-stop.
        df = _clean_close(download_prices(tickers, start=start, end=end), tickers)
        served = set(df.columns)
        for t in tickers:
            source_acc[t] = "fetch" if t in served else "missing"
        return df

    if not _HAVE_BASE:
        return _pure_fetch()

    def _fallback(missing, period=None):
        # download_prices has no ``period`` kwarg; reuse the window from the enclosing request so
        # the fallback path does not TypeError exactly when the archive is unavailable. Sanitize the
        # columns (single-ticker yfinance can return tuple columns) before handing back to the merge.
        return {"Close": _clean_close(download_prices(list(missing), start=start, end=end), missing)}

    try:
        ohlcv, source_map = load_ohlcv_base_first(
            tickers, fetch_fallback=_fallback, start=start, end=end, normalize_currency=False)
    except Exception as e:  # noqa: BLE001 -- strangler: ANY base failure degrades to the old fetch
        print(f"  WARN: base read raised ({e!r}); degrading to yfinance for this call (strangler).")
        return _pure_fetch()

    source_acc.update(source_map)
    for t in tickers:
        source_acc.setdefault(t, "missing")  # requested but neither base nor fetch served it
    return ohlcv.get("Close", pd.DataFrame())


def _write_price_source(source_acc: dict, expected: int) -> None:
    """Write per-symbol price provenance for scripts/assert_base_sourced.py (P9 strangler, mirror of
    the P6 ETF-rr guard). ``expected`` = requested universe size (stocks + benchmarks)."""
    n_base = sum(1 for v in source_acc.values() if v == "base")
    payload = {
        "by_symbol": dict(sorted(source_acc.items())),
        "summary": {"expected": expected, "covered": len(source_acc),
                    "base": n_base, "fetch": len(source_acc) - n_base},
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PRICE_SOURCE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_sector_map_for_scoring(cache_path: Path) -> dict[str, str]:
    """
    Зарежда GICS sector mapping за sector-relative z-score scoring.
    Извиква sector_engine да опресни ако cache-ът е остарял.
    """
    df = get_sector_dataframe(cache_path)
    return dict(zip(df["ticker"], df["gics_sector"]))


def is_market_cap_cache_stale(path: Path = MARKET_CAPS_PATH, max_age_days: int = MARKET_CAP_REFRESH_DAYS) -> bool:
    """Връща True ако market_caps.json липсва или е по-стар от max_age_days."""
    if not path.exists():
        return True
    try:
        with path.open(encoding="utf-8") as f:
            cache = json.load(f)
        updated = cache.get("updated")
        if not updated:
            return True
        from datetime import datetime, timedelta
        updated_dt = datetime.fromisoformat(updated)
        return datetime.now() - updated_dt > timedelta(days=max_age_days)
    except Exception:
        return True


def maybe_refresh_market_caps() -> None:
    """Ако market caps cache е stale, refresh."""
    if not is_market_cap_cache_stale():
        return
    print("  Market caps cache stale → refreshing (this may take ~1-2 minutes)...")
    from scripts.fetch_market_caps import fetch_all, save
    caps = fetch_all()
    save(caps)


def update_prices_cache(tickers: list[str], source_acc: dict) -> pd.DataFrame:
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
        new_prices = _base_first_close(tickers, start, end, source_acc)

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
    prices = _base_first_close(tickers, start, end, source_acc)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    prices.to_parquet(PRICES_CACHE_PATH)
    return prices


def update_benchmark_cache(tickers: list[str], source_acc: dict) -> pd.DataFrame:
    """
    Incremental update за benchmark + sector ETF prices.
    Държи се отделно от prices_cache, за да не замърси sector z-score изчислението.
    Lookback: 2 години (50 SMA + 252-day high буфер).
    """
    end = pd.Timestamp.today().normalize()

    if BENCHMARK_PRICES_PATH.exists():
        cached = pd.read_parquet(BENCHMARK_PRICES_PATH)
        cached.index = pd.to_datetime(cached.index)
        last_date = cached.index.max()

        if last_date >= end - pd.tseries.offsets.BusinessDay(1):
            print(f"  Benchmark cache up to date ({last_date.date()}).")
            return cached

        start = last_date - pd.Timedelta(days=5)
        print(f"  Benchmark incremental {start.date()} → {end.date()}")
        new_prices = _base_first_close(tickers, start, end, source_acc)

        if new_prices.empty:
            return cached

        combined = pd.concat([
            cached[~cached.index.isin(new_prices.index)],
            new_prices,
        ]).sort_index()
        combined = combined.dropna(axis=1, how="all")

        cutoff = end - pd.Timedelta(days=BENCHMARK_LOOKBACK_DAYS + 60)
        trimmed = combined[combined.index >= cutoff]
        trimmed.to_parquet(BENCHMARK_PRICES_PATH)
        return trimmed

    # No cache — full 2y download
    start = end - pd.Timedelta(days=BENCHMARK_LOOKBACK_DAYS + 60)
    print(f"  Benchmark full download {start.date()} → {end.date()} ({len(tickers)} tickers)")
    prices = _base_first_close(tickers, start, end, source_acc)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    prices.to_parquet(BENCHMARK_PRICES_PATH)
    return prices


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("[1/6] Fetching SP500 universe...")
    universe = fetch_full_universe()
    tickers = universe[universe["is_current"]]["ticker"].tolist()
    print(f"      {len(tickers)} current tickers")

    # INIT-22 P9: per-symbol price provenance accumulated across BOTH caches (base vs fetch),
    # then written for scripts/assert_base_sourced.py.
    price_source: dict[str, str] = {}

    print("[2/6] Updating prices cache (base-first canonical; yfinance CLOSED fallback)...")
    prices = update_prices_cache(tickers, price_source)
    print(f"      {len(prices.columns)} tickers × {len(prices)} days")

    print("[3/6] Updating benchmark + sector ETF cache (SPY + 11 SPDR ETFs)...")
    bench_tickers = benchmark_universe_tickers()
    benchmark_prices = update_benchmark_cache(bench_tickers, price_source)
    print(f"      {len(benchmark_prices.columns)} tickers × {len(benchmark_prices)} days")
    if price_source:
        n_base = sum(1 for v in price_source.values() if v == "base")
        print(f"      Price source: {n_base} base / {len(price_source) - n_base} fetch")
    _write_price_source(price_source, expected=len(tickers) + len(bench_tickers))

    print("[4/6] Computing today's cross-section (sector-relative z-score)...")
    sector_map = load_sector_map_for_scoring(SECTOR_CACHE_PATH)
    print(f"      Loaded {len(sector_map)} sector mappings")
    cs = compute_cross_section(prices, sector_map=sector_map)
    cs = cs.dropna(subset=["raw_score"])
    print(f"      {len(cs)} valid scores for {cs['date'].iloc[0].date()}")

    print("[5/6] Appending snapshot to history...")
    append_snapshot(HISTORY_PATH, cs[HISTORY_COLUMNS])
    size_mb = HISTORY_PATH.stat().st_size / 1e6
    print(f"      History now {size_mb:.1f} MB")

    print("[6/6] Rendering data.json (incl. market cap refresh check + screener + RS line)...")
    maybe_refresh_market_caps()
    payload = render_dashboard_data()
    print(f"      Rendered: as of {payload['metadata']['as_of']}")
    print(
        f"      Stable Winners 1m: {len(payload['stable_winners_1m'])} | "
        f"Quality Dip 1m: {len(payload['quality_dip_1m'])} | "
        f"Faded Bounces: {len(payload['faded_bounces_1m'])}"
    )
    print(
        f"      Current Strength: {len(payload['current_strength'])} | "
        f"Screener: {len(payload['screener']['stocks'])} | "
        f"Sectors: {len(payload['sector_rotation'])}"
    )

    print("\nDaily update complete.")


if __name__ == "__main__":
    main()
