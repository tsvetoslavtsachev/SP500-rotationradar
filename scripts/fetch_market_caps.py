"""
Fetch market caps for SP500 universe и кешира в data/market_caps.json.

Извиква се:
  - Еднократно (initial setup)
  - От daily_update.py ако cache е > 7 дни стар

yfinance връща market cap чрез .fast_info.market_cap (бърз) или
fallback към .info["marketCap"] (по-бавен).
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path

import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.universe import fetch_current_constituents  # noqa: E402

CACHE_PATH = ROOT / "data" / "market_caps.json"
BATCH_PAUSE = 0.05


def _try_market_cap(ticker: str) -> float | None:
    try:
        t = yf.Ticker(ticker)
        try:
            mcap = t.fast_info.market_cap
            if mcap and mcap > 0:
                return float(mcap)
        except Exception:
            pass
        info = t.info
        mcap = info.get("marketCap")
        if mcap and mcap > 0:
            return float(mcap)
    except Exception as e:
        print(f"  [{ticker}] failed: {e}")
    return None


def fetch_all() -> dict:
    universe = fetch_current_constituents()
    tickers = universe["ticker"].tolist()
    print(f"Fetching market caps for {len(tickers)} tickers...")

    caps = {}
    for i, ticker in enumerate(tickers):
        mcap = _try_market_cap(ticker)
        if mcap is not None:
            caps[ticker] = mcap
        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(tickers)}: {len(caps)} caps so far")
        time.sleep(BATCH_PAUSE)

    print(f"Got {len(caps)}/{len(tickers)} market caps")
    return caps


def save(caps: dict, path: Path = CACHE_PATH) -> None:
    payload = {
        "updated": datetime.now().isoformat(timespec="seconds"),
        "tickers": caps,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Saved {path.name} ({path.stat().st_size / 1e3:.1f} KB)")


def main() -> None:
    caps = fetch_all()
    save(caps)


if __name__ == "__main__":
    main()
