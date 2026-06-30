#!/usr/bin/env python3
"""CI guard (INIT-22 P9): fail RED if SP500-rotationradar prices silently fell back to yfinance.

Adapts the P6 ETF-rr guard to the stock radar's realities:
  * TWO caches feed one source map -- the SP500 stock universe (prices_cache) AND the SPY+11 SPDR
    benchmark set (benchmark_prices). If the benchmark set silently falls back, the RS line mixes
    provenance, so the benchmarks are checked STRICTLY (they are all archive-resident, no legit gap).
  * DYNAMIC, LIVE universe (Wikipedia scrape) + known archive gaps (HON quarantine; a freshly-added
    constituent not yet registered). So the STOCK universe uses a COVERAGE FLOOR + an explicit
    allow-list (the P6-part-2 / macro-satellite cardinality-floor pattern) rather than P6's
    all-or-nothing -- otherwise the guard would flap RED every run on a legitimately quarantined name.

Hard-RED triggers (the catastrophe this guard exists to catch -- a SILENT MASS fallback, e.g. the
archive checkout failed and the whole universe quietly reverted to un-audited yfinance):
  * base fraction below FLOOR_FRAC, or
  * any benchmark/SPDR ticker that WAS sourced this run did not come from base.
Non-allow-listed stock fetches are printed as WARNINGS (index churn -> re-register cadence), not a
build break. Run only when the read PATs are set (the workflow gates it); absent the secrets the
yfinance fallback is legitimate and this is skipped.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SOURCE = REPO / "data" / "price_source.json"
sys.path.insert(0, str(REPO))  # so the benchmark anchor (src/etf_universe.py) imports

from src.etf_universe import benchmark_universe_tickers  # noqa: E402

# Known, expected fetch fallbacks (NOT a silent regression): symbols whose px_* series is absent
# from the archive. Documented in P8b-HON-case-to-resolve.md + the P9 mandate.
ALLOWLIST = {"HON"}  # Honeywell -- upstream yfinance lagged-split quarantine (P8b)

FLOOR_FRAC = 0.90  # >= 90% of the symbols sourced this run must be base-sourced


def main() -> int:
    if not SOURCE.exists():
        print(f"assert_base_sourced: {SOURCE} missing -- daily_update must run first",
              file=sys.stderr)
        return 1
    payload = json.loads(SOURCE.read_text(encoding="utf-8"))
    by_symbol = payload.get("by_symbol", {})
    if not by_symbol:
        # Nothing sourced this run (both caches were already fresh) -- nothing to assert.
        print("assert_base_sourced: price_source empty (caches fresh this run) -- OK.")
        return 0

    base = sorted(t for t, s in by_symbol.items() if s == "base")
    fetch = sorted(t for t, s in by_symbol.items() if s != "base")
    covered = len(by_symbol)
    base_frac = len(base) / covered if covered else 0.0
    unexpected = [t for t in fetch if t not in ALLOWLIST]

    # Benchmark/SPDR set must source from base when it was sourced this run (dual-family check).
    # Only benchmarks PRESENT in price_source were sourced this run (the benchmark cache may have
    # been fresh -> early-return -> not requested); report how many were actually checked.
    benchmarks = set(benchmark_universe_tickers())
    bench_checked = [t for t in sorted(benchmarks) if t in by_symbol]
    bench_offenders = [t for t in bench_checked if by_symbol[t] != "base"]

    print(f"assert_base_sourced: {len(base)}/{covered} base ({base_frac:.1%}); "
          f"{len(fetch)} fetch ({len(unexpected)} unexpected, "
          f"{len(fetch) - len(unexpected)} allow-listed).")
    if unexpected:
        print("  WARNING -- fetched (not allow-listed; check archive registration / index churn):",
              file=sys.stderr)
        for t in unexpected:
            print(f"    - {t}", file=sys.stderr)

    failed = False
    if bench_offenders:
        print(f"FAIL: {len(bench_offenders)} benchmark/SPDR ticker(s) did NOT source from base "
              f"(RS line would mix provenance): {bench_offenders}", file=sys.stderr)
        failed = True
    if base_frac < FLOOR_FRAC:
        print(f"FAIL: base fraction {base_frac:.1%} < floor {FLOOR_FRAC:.0%} -- the strangler "
              "silently reverted to yfinance. Check the data-core/price-archive checkout + "
              "PYTHONPATH + DATACORE_ROOT, and any per-symbol archive gap.", file=sys.stderr)
        failed = True

    if not failed:
        print(f"OK: base sourcing intact ({base_frac:.1%} base; "
              f"{len(bench_checked)}/{len(benchmarks)} benchmarks checked, all base; "
              f"{len(unexpected)} tolerated churn/gap).")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
