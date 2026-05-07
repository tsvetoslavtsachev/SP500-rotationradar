"""
ETF universe + benchmark — за RS Line / Sector Rotation analysis.

11-те SPDR Select Sector ETFs покриват GICS секторите 1:1, така че RS Line на
всеки от тях vs SPY дава sector rotation timing signal независимо от Stock-level
12-1 momentum изчислението. SPY е benchmark за RS Line на S&P 500 stocks.

Тези тикъри НЕ влизат в `prices_cache.parquet` — съхраняват се в отделен
`benchmark_prices.parquet` за да не замърсят sector z-score изчислението на
S&P 500 cross-section.
"""

from __future__ import annotations

BENCHMARK = "SPY"

SPDR_SECTOR_ETFS = [
    {"symbol": "XLK",  "name": "Technology",            "gics_sector": "Information Technology"},
    {"symbol": "XLF",  "name": "Financials",            "gics_sector": "Financials"},
    {"symbol": "XLE",  "name": "Energy",                "gics_sector": "Energy"},
    {"symbol": "XLV",  "name": "Health Care",           "gics_sector": "Health Care"},
    {"symbol": "XLI",  "name": "Industrials",           "gics_sector": "Industrials"},
    {"symbol": "XLY",  "name": "Consumer Discretionary","gics_sector": "Consumer Discretionary"},
    {"symbol": "XLP",  "name": "Consumer Staples",      "gics_sector": "Consumer Staples"},
    {"symbol": "XLU",  "name": "Utilities",             "gics_sector": "Utilities"},
    {"symbol": "XLB",  "name": "Materials",             "gics_sector": "Materials"},
    {"symbol": "XLRE", "name": "Real Estate",           "gics_sector": "Real Estate"},
    {"symbol": "XLC",  "name": "Communication Services","gics_sector": "Communication Services"},
]


def benchmark_universe_tickers() -> list[str]:
    """Връща пълен списък тикъри: SPY + 11 sector ETFs (за yfinance batch fetch)."""
    return [BENCHMARK] + [e["symbol"] for e in SPDR_SECTOR_ETFS]


def sector_etf_meta(symbol: str) -> dict | None:
    """Връща meta за даден sector ETF symbol или None ако не е в universe-а."""
    for e in SPDR_SECTOR_ETFS:
        if e["symbol"] == symbol:
            return e
    return None
