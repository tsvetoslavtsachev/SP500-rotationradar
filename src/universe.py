"""
SP500 universe — текущи constituents от Wikipedia.

Wikipedia има две таблици в страницата за SP500:
  Table 0: текущи constituents
  Table 1: исторически промени (added/removed)

Връщаме обединение на текущи + наскоро премахнати, за да имаме
по-малко survivorship bias в backfill-а.
"""

from __future__ import annotations

import io

import pandas as pd
import requests

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def _fetch_html() -> str:
    headers = {
        "User-Agent": "SP500-rotationradar/1.0 (+https://github.com/tsvetoslavtsachev)"
    }
    resp = requests.get(WIKI_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text


def fetch_current_constituents() -> pd.DataFrame:
    """
    Връща DataFrame с текущите SP500 акции.
    Columns: ticker, name, gics_sector, gics_sub_industry.
    """
    html = _fetch_html()
    tables = pd.read_html(io.StringIO(html))
    df = tables[0]

    rename = {
        "Symbol": "ticker",
        "Security": "name",
        "GICS Sector": "gics_sector",
        "GICS Sub-Industry": "gics_sub_industry",
    }
    df = df.rename(columns=rename)[list(rename.values())]
    # Wikipedia използва BRK.B / BF.B → yfinance иска BRK-B / BF-B
    df["ticker"] = df["ticker"].str.replace(".", "-", regex=False)
    return df.reset_index(drop=True)


def fetch_recent_removed(years_back: int = 5) -> pd.DataFrame:
    """
    Връща акции, които са били премахнати от индекса в последните `years_back` години.
    Помага за намаляване на survivorship bias в backfill-а.
    Columns: ticker, removed_date.
    """
    html = _fetch_html()
    tables = pd.read_html(io.StringIO(html))
    if len(tables) < 2:
        return pd.DataFrame(columns=["ticker", "removed_date"])
    changes = tables[1]

    # Структурата има multi-level header. Нормализираме.
    if isinstance(changes.columns, pd.MultiIndex):
        changes.columns = [
            "_".join(str(c) for c in col if str(c) != "nan").strip()
            for col in changes.columns
        ]

    date_col = next(
        (c for c in changes.columns if c.lower().startswith("date")), None
    )
    removed_ticker_col = next(
        (c for c in changes.columns if "removed" in c.lower() and "ticker" in c.lower()),
        None,
    )
    if date_col is None or removed_ticker_col is None:
        return pd.DataFrame(columns=["ticker", "removed_date"])

    out = changes[[date_col, removed_ticker_col]].copy()
    out.columns = ["removed_date", "ticker"]
    out = out.dropna(subset=["ticker"])
    out["ticker"] = out["ticker"].astype(str).str.replace(".", "-", regex=False)
    out["removed_date"] = pd.to_datetime(out["removed_date"], errors="coerce")
    out = out.dropna(subset=["removed_date"])
    cutoff = pd.Timestamp.today() - pd.DateOffset(years=years_back)
    return out[out["removed_date"] >= cutoff].reset_index(drop=True)


def fetch_full_universe(years_back: int = 5) -> pd.DataFrame:
    """
    Обединява текущи constituents + наскоро премахнати.
    Columns: ticker, name, gics_sector, gics_sub_industry, is_current, removed_date.
    """
    current = fetch_current_constituents()
    current["is_current"] = True
    current["removed_date"] = pd.NaT

    removed = fetch_recent_removed(years_back=years_back)
    if not removed.empty:
        removed["is_current"] = False
        removed["name"] = pd.NA
        removed["gics_sector"] = pd.NA
        removed["gics_sub_industry"] = pd.NA
        removed = removed[
            ["ticker", "name", "gics_sector", "gics_sub_industry", "is_current", "removed_date"]
        ]
        # Премахвам ticker-и, които вече са в current
        removed = removed[~removed["ticker"].isin(current["ticker"])]
        full = pd.concat([current, removed], ignore_index=True)
    else:
        full = current

    return full.reset_index(drop=True)


if __name__ == "__main__":
    universe = fetch_full_universe()
    print(f"Total tickers: {len(universe)}")
    print(f"Current: {universe['is_current'].sum()}")
    print(f"Recently removed: {(~universe['is_current']).sum()}")
    print(universe.head())
