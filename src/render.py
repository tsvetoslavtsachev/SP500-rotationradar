"""
Render — генерира data.json за UI dashboard-а.

V2 architecture: главният сигнал е Stable Winners (лидери продължаващи да водят),
вторичен е Quality Dip (лидери временно отслабнали), а Faded Bounces (бившите Risers)
са показани с warning защото backtest показа отрицателен excess.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.etf_universe import BENCHMARK, SPDR_SECTOR_ETFS, sector_etf_meta  # noqa: E402
from src.rank_history import (  # noqa: E402
    compute_delta_metrics,
    get_faded_bounces,
    get_quality_dip,
    get_stable_winners,
    load_history,
)
from src.rs_line import (  # noqa: E402
    DEFAULT_CROSSOVER_LOOKBACK,
    DEFAULT_RS_HIGH_WINDOW,
    EMA_SHORT,
    SMA_LONG,
    compute_rs_signals,
    compute_rs_trajectory,
)
from src.screener import build_screener  # noqa: E402
from src.sector_engine import (  # noqa: E402
    aggregate_by_sector,
    aggregate_by_sub_industry,
    get_sector_dataframe,
)
from src.signal_engine import compute_ticker_mom  # noqa: E402
from src.transitions import compute_attention_signals  # noqa: E402

DATA_DIR = ROOT / "data"
DOCS_DIR = ROOT / "docs"
HISTORY_PATH = DATA_DIR / "ranks_history.parquet"
PRICES_CACHE_PATH = DATA_DIR / "prices_cache.parquet"
BENCHMARK_PRICES_PATH = DATA_DIR / "benchmark_prices.parquet"
SECTOR_CACHE_PATH = DATA_DIR / "sector_map.json"
MARKET_CAPS_PATH = DATA_DIR / "market_caps.json"
OUTPUT_PATH = DOCS_DIR / "data.json"
TRAJECTORY_DAYS = 90
RS_FRESH_LIMIT = 25
RS_NEW_HIGH_LIMIT = 25


def load_market_caps(path: Path = MARKET_CAPS_PATH) -> tuple[dict[str, float], str | None]:
    """Зарежда cached market caps. Връща (mapping, updated_iso_string)."""
    if not path.exists():
        return {}, None
    with path.open(encoding="utf-8") as f:
        cache = json.load(f)
    return cache.get("tickers", {}), cache.get("updated")


def build_screener_payload() -> tuple[list[dict], str | None]:
    """Изгражда screener секцията: списък с stock dicts + market_cap_updated timestamp."""
    if not PRICES_CACHE_PATH.exists():
        return [], None

    prices = pd.read_parquet(PRICES_CACHE_PATH)
    prices.index = pd.to_datetime(prices.index)

    sectors = get_sector_dataframe(SECTOR_CACHE_PATH)
    sector_map = dict(zip(sectors["ticker"], sectors["gics_sector"]))
    industry_map = dict(zip(sectors["ticker"], sectors["gics_sub_industry"]))
    name_map = dict(zip(sectors["ticker"], sectors["name"]))

    market_caps, mcap_updated = load_market_caps()

    df = build_screener(
        prices,
        sector_map=sector_map,
        industry_map=industry_map,
        name_map=name_map,
        market_caps=market_caps,
    )

    # Форматиране: round до 2 знака за всички floats, None за NaN
    def _safe(v, ndigits=2):
        if v is None or (isinstance(v, float) and not np.isfinite(v)):
            return None
        if isinstance(v, float):
            return round(v, ndigits)
        return v

    stocks = []
    for _, row in df.iterrows():
        record = {col: _safe(row[col]) for col in df.columns}
        # market_cap е голямо число; по-добре в милиони USD за компактност
        if record.get("market_cap"):
            record["market_cap_m"] = round(record["market_cap"] / 1e6)
            del record["market_cap"]
        stocks.append(record)

    return stocks, mcap_updated


def compute_current_returns(prices_cache_path: Path = PRICES_CACHE_PATH) -> dict[str, float]:
    """
    Връща dict ticker → 12-1 momentum return (актуален). Пример: 0.27 = +27%.
    Зарежда prices_cache.parquet и за всеки ticker пресмята класическия 12-1.
    """
    if not prices_cache_path.exists():
        return {}
    prices = pd.read_parquet(prices_cache_path)
    prices.index = pd.to_datetime(prices.index)
    out = {}
    for ticker in prices.columns:
        mom = compute_ticker_mom(prices[ticker])
        if np.isfinite(mom):
            out[ticker] = float(mom)
    return out


def load_benchmark_prices(path: Path = BENCHMARK_PRICES_PATH) -> pd.DataFrame | None:
    """Зарежда benchmark + sector ETF prices. Връща None ако файлът липсва."""
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    df.index = pd.to_datetime(df.index)
    return df


def _row_to_rs_dict(
    row: pd.Series,
    sectors: pd.DataFrame,
    rs_prices: pd.Series,
    benchmark_series: pd.Series,
    *,
    include_trajectory: bool = True,
) -> dict:
    """Превръща един RS Line ред в JSON dict за UI tab."""
    ticker = row["ticker"]
    sector_info = sectors[sectors["ticker"] == ticker]
    if sector_info.empty:
        name = sector = sub = None
    else:
        s = sector_info.iloc[0]
        name = _safe_str(s.get("name"))
        sector = _safe_str(s.get("gics_sector"))
        sub = _safe_str(s.get("gics_sub_industry"))

    record = {
        "ticker": ticker,
        "name": name,
        "sector": sector,
        "sub_industry": sub,
        "price": _safe_round(row.get("price_value"), 2),
        "rs_value": _safe_round(row.get("rs_value"), 4),
        "cloud_color": row.get("cloud_color"),
        "bullish_crossover": bool(row.get("bullish_crossover")),
        "bearish_crossover": bool(row.get("bearish_crossover")),
        "days_since_crossover": (
            int(row["days_since_crossover"])
            if row.get("days_since_crossover") is not None
            and not pd.isna(row.get("days_since_crossover"))
            else None
        ),
        "rs_new_high": bool(row.get("rs_new_high")),
        "price_new_high": bool(row.get("price_new_high")),
        "confluence": bool(row.get("confluence")),
        "rs_slope_norm": _safe_round(row.get("rs_slope_norm"), 4),
        "trend_quality": int(row.get("trend_quality") or 0),
        "pct_from_52w_high": _safe_round(row.get("pct_from_52w_high"), 1),
        "score": _safe_round(row.get("score"), 1),
    }
    if include_trajectory and rs_prices is not None:
        record["rs_trajectory"] = compute_rs_trajectory(rs_prices, benchmark_series, days=TRAJECTORY_DAYS)
    return record


def _build_sector_rs_payload(
    etf_signals: pd.DataFrame,
    etf_prices: pd.DataFrame,
    benchmark_series: pd.Series,
    sector_rotation: list[dict],
) -> list[dict]:
    """
    Изграждa Sector RS таблица за UI.
    Cross-link: за всеки ETF добавя n_risers/n_decayers от sector_rotation
    payload (вече агрегирано в render_dashboard_data).
    """
    rotation_by_sector = {row["sector"]: row for row in sector_rotation}
    out = []
    for _, row in etf_signals.iterrows():
        meta = sector_etf_meta(row["ticker"]) or {}
        gics = meta.get("gics_sector")
        rot = rotation_by_sector.get(gics, {})
        record = {
            "symbol": row["ticker"],
            "name": meta.get("name") or row["ticker"],
            "gics_sector": gics,
            "price": _safe_round(row.get("price_value"), 2),
            "rs_value": _safe_round(row.get("rs_value"), 4),
            "cloud_color": row.get("cloud_color"),
            "bullish_crossover": bool(row.get("bullish_crossover")),
            "bearish_crossover": bool(row.get("bearish_crossover")),
            "days_since_crossover": (
                int(row["days_since_crossover"])
                if row.get("days_since_crossover") is not None
                and not pd.isna(row.get("days_since_crossover"))
                else None
            ),
            "rs_new_high": bool(row.get("rs_new_high")),
            "rs_slope_norm": _safe_round(row.get("rs_slope_norm"), 4),
            "score": _safe_round(row.get("score"), 1),
            # Cross-link с stock-level sector rotation (от съществуващия heatmap)
            "stocks_n_risers": int(rot.get("n_risers", 0)),
            "stocks_n_decayers": int(rot.get("n_decayers", 0)),
            "stocks_n_total": int(rot.get("n_total", 0)),
        }
        if row["ticker"] in etf_prices.columns:
            record["rs_trajectory"] = compute_rs_trajectory(
                etf_prices[row["ticker"]], benchmark_series, days=TRAJECTORY_DAYS
            )
        out.append(record)
    # Sort: bullish crossovers first, then by score
    out.sort(key=lambda r: (-int(r["bullish_crossover"]), -(r["score"] or 0)))
    return out


def _safe_round(x, ndigits: int = 1):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return None
    return round(float(x), ndigits)


def _safe_str(x):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return None
    return str(x) if not pd.isna(x) else None


def _row_to_dict(
    row: pd.Series,
    sectors: pd.DataFrame,
    returns: dict[str, float] | None = None,
) -> dict:
    sector_info = sectors[sectors["ticker"] == row["ticker"]]
    if sector_info.empty:
        name = sector = sub = None
    else:
        s = sector_info.iloc[0]
        name = _safe_str(s.get("name"))
        sector = _safe_str(s.get("gics_sector"))
        sub = _safe_str(s.get("gics_sub_industry"))

    mom_return = (returns or {}).get(row["ticker"])
    mom_return_pct = round(mom_return * 100, 1) if mom_return is not None else None

    return {
        "ticker": row["ticker"],
        "name": name,
        "sector": sector,
        "sub_industry": sub,
        "current_rank": _safe_round(row.get("current_rank")),
        "abs_strength": _safe_round(row.get("abs_strength")),
        "mom_12_1_pct": mom_return_pct,
        "base_rank_6m": _safe_round(row.get("base_rank_6m")),
        "delta_1m": _safe_round(row.get("delta_1m")),
        "delta_3m": _safe_round(row.get("delta_3m")),
        "quadrant_1m": row.get("quadrant_1m"),
        "quadrant_3m": row.get("quadrant_3m"),
    }


def _trajectory(history: pd.DataFrame, ticker: str, days: int = TRAJECTORY_DAYS) -> list:
    sub = history[history["ticker"] == ticker].sort_values("date").tail(days)
    return [
        {"date": d.strftime("%Y-%m-%d"), "rank": _safe_round(r)}
        for d, r in zip(sub["date"], sub["percentile_rank"])
    ]


def render_dashboard_data(
    history_path: Path = HISTORY_PATH,
    sector_cache_path: Path = SECTOR_CACHE_PATH,
    output_path: Path = OUTPUT_PATH,
    limit: int = 25,
) -> dict:
    history = load_history(history_path)
    if history.empty:
        raise RuntimeError(f"History file is empty: {history_path}")

    sectors = get_sector_dataframe(sector_cache_path)
    deltas = compute_delta_metrics(history)
    if deltas.empty:
        raise RuntimeError("Could not compute delta metrics — insufficient history")

    as_of = deltas["as_of_date"].iloc[0]
    returns = compute_current_returns()

    stable_winners_1m = get_stable_winners(deltas, window="1m", limit=limit)
    stable_winners_3m = get_stable_winners(deltas, window="3m", limit=limit)
    quality_dip_1m = get_quality_dip(deltas, window="1m", limit=limit)
    quality_dip_3m = get_quality_dip(deltas, window="3m", limit=limit)
    faded_bounces_1m = get_faded_bounces(deltas, window="1m", limit=limit)

    # Current Strength leaderboard — топ 50 акции по абсолютна 12-1 momentum
    current_strength = (
        deltas.dropna(subset=["abs_strength"])
        .sort_values("abs_strength", ascending=False)
        .head(50)
    )

    sector_agg = aggregate_by_sector(deltas, sectors)
    sub_industry_agg = aggregate_by_sub_industry(deltas, sectors)

    screener_stocks, mcap_updated = build_screener_payload()

    # ── RS Line layer (Pristine RS Line) ─────────────────────────────────────
    benchmark_prices = load_benchmark_prices()
    rs_stock_signals = pd.DataFrame()
    rs_etf_signals = pd.DataFrame()
    rs_meta = None
    benchmark_series = None
    stock_prices_df = None
    etf_prices_df = None

    if benchmark_prices is not None and BENCHMARK in benchmark_prices.columns:
        benchmark_series = benchmark_prices[BENCHMARK].dropna()
        # S&P 500 stock-level RS signals (ползваме prices_cache.parquet)
        if PRICES_CACHE_PATH.exists():
            stock_prices_df = pd.read_parquet(PRICES_CACHE_PATH)
            stock_prices_df.index = pd.to_datetime(stock_prices_df.index)
            try:
                rs_stock_signals = compute_rs_signals(
                    stock_prices_df,
                    benchmark_series,
                    crossover_lookback_days=DEFAULT_CROSSOVER_LOOKBACK,
                    rs_high_window=DEFAULT_RS_HIGH_WINDOW,
                )
            except ValueError as e:
                print(f"  RS Line skipped for stocks: {e}")

        # Sector ETF RS signals
        etf_symbols = [e["symbol"] for e in SPDR_SECTOR_ETFS if e["symbol"] in benchmark_prices.columns]
        if etf_symbols:
            etf_prices_df = benchmark_prices[etf_symbols]
            try:
                rs_etf_signals = compute_rs_signals(
                    etf_prices_df,
                    benchmark_series,
                    crossover_lookback_days=DEFAULT_CROSSOVER_LOOKBACK,
                    rs_high_window=DEFAULT_RS_HIGH_WINDOW,
                )
            except ValueError as e:
                print(f"  RS Line skipped for ETFs: {e}")

        rs_meta = {
            "benchmark": BENCHMARK,
            "ema_period": EMA_SHORT,
            "sma_period": SMA_LONG,
            "crossover_lookback_days": DEFAULT_CROSSOVER_LOOKBACK,
            "rs_high_window": DEFAULT_RS_HIGH_WINDOW,
            "source": "Pristine Capital RS Line indicator (TradingView free)",
            "released": "2026-05-05",
            "stocks_signals": int(len(rs_stock_signals)),
            "etfs_signals": int(len(rs_etf_signals)),
        }

    # Rank All Stocks — пълно подреждане по Score (sector-relative percentile).
    # Score е V2 еквивалент на старото "weighted_return / vol + 0.3*sharpe".
    rank_all_df = (
        deltas.dropna(subset=["current_rank"])
        .sort_values("current_rank", ascending=False)
        .reset_index(drop=True)
    )
    rank_all_df["rank_position"] = rank_all_df.index + 1

    quadrant_label = {
        "riser": "Faded Bounce",
        "decayer": "Quality Dip",
        "stable_winner": "Stable Winner",
        "chronic_loser": "Chronic Loser",
        "neutral": "Neutral",
        "unknown": "—",
    }

    rank_all_payload = []
    for _, row in rank_all_df.iterrows():
        ticker = row["ticker"]
        sector_info = sectors[sectors["ticker"] == ticker]
        if sector_info.empty:
            name = sector = sub = None
        else:
            s = sector_info.iloc[0]
            name = _safe_str(s.get("name"))
            sector = _safe_str(s.get("gics_sector"))
            sub = _safe_str(s.get("gics_sub_industry"))

        mom = returns.get(ticker)
        rank_all_payload.append({
            "rank_position": int(row["rank_position"]),
            "ticker": ticker,
            "name": name,
            "sector": sector,
            "sub_industry": sub,
            "score": _safe_round(row.get("current_rank")),
            "abs_strength": _safe_round(row.get("abs_strength")),
            "mom_12_1_pct": round(mom * 100, 1) if mom is not None else None,
            "base_rank_6m": _safe_round(row.get("base_rank_6m")),
            "delta_1m": _safe_round(row.get("delta_1m")),
            "delta_3m": _safe_round(row.get("delta_3m")),
            "quadrant_1m": quadrant_label.get(row.get("quadrant_1m"), "—"),
            "quadrant_3m": quadrant_label.get(row.get("quadrant_3m"), "—"),
        })

    def _with_trajectory(df: pd.DataFrame) -> list:
        if df.empty:
            return []
        out = []
        for _, row in df.iterrows():
            d = _row_to_dict(row, sectors, returns=returns)
            d["trajectory"] = _trajectory(history, row["ticker"])
            out.append(d)
        return out

    # ── Build RS Line payload sections ───────────────────────────────────────
    rs_inflection_bullish: list = []
    rs_inflection_bearish: list = []
    rs_new_highs: list = []
    power_confluence: list = []
    sector_rs_payload: list = []

    if not rs_stock_signals.empty and stock_prices_df is not None:
        bullish = (
            rs_stock_signals[rs_stock_signals["bullish_crossover"]]
            .sort_values("score", ascending=False)
            .head(RS_FRESH_LIMIT)
        )
        bearish = (
            rs_stock_signals[rs_stock_signals["bearish_crossover"]]
            .sort_values("score", ascending=False)
            .head(RS_FRESH_LIMIT)
        )
        rs_highs = (
            rs_stock_signals[
                rs_stock_signals["rs_new_high"]
                & ~rs_stock_signals["bullish_crossover"]
            ]
            .sort_values("score", ascending=False)
            .head(RS_NEW_HIGH_LIMIT)
        )

        for sub_df, target in (
            (bullish, rs_inflection_bullish),
            (bearish, rs_inflection_bearish),
            (rs_highs, rs_new_highs),
        ):
            for _, row in sub_df.iterrows():
                ticker = row["ticker"]
                rs_prices = stock_prices_df[ticker] if ticker in stock_prices_df.columns else None
                target.append(_row_to_rs_dict(row, sectors, rs_prices, benchmark_series))

        # Power Confluence: пресичането на Stable Winners 1m с fresh bullish RS crossovers
        sw_tickers = set(stable_winners_1m["ticker"].astype(str))
        bull_lookup = {row["ticker"]: row for _, row in bullish.iterrows()}
        # Включи всички stable winners — с или без crossover. UI решава какво да показва.
        for _, sw in stable_winners_1m.iterrows():
            ticker = sw["ticker"]
            rs_row = bull_lookup.get(ticker)
            if rs_row is None:
                continue  # Power Confluence изисква активен bullish crossover
            sector_info = sectors[sectors["ticker"] == ticker]
            if sector_info.empty:
                name = sector = sub = None
            else:
                s = sector_info.iloc[0]
                name = _safe_str(s.get("name"))
                sector = _safe_str(s.get("gics_sector"))
                sub = _safe_str(s.get("gics_sub_industry"))
            momentum_pct = (returns or {}).get(ticker)
            momentum_pct = round(momentum_pct * 100, 1) if momentum_pct is not None else None
            combined_score = round(
                0.6 * float(rs_row.get("score") or 0) + 0.4 * float(sw.get("current_rank") or 0),
                1,
            )
            rs_prices = stock_prices_df[ticker] if ticker in stock_prices_df.columns else None
            traj = (
                compute_rs_trajectory(rs_prices, benchmark_series, days=TRAJECTORY_DAYS)
                if rs_prices is not None
                else []
            )
            power_confluence.append({
                "ticker": ticker,
                "name": name,
                "sector": sector,
                "sub_industry": sub,
                "current_rank": _safe_round(sw.get("current_rank")),
                "base_rank_6m": _safe_round(sw.get("base_rank_6m")),
                "delta_1m": _safe_round(sw.get("delta_1m")),
                "mom_12_1_pct": momentum_pct,
                "rs_value": _safe_round(rs_row.get("rs_value"), 4),
                "days_since_crossover": (
                    int(rs_row["days_since_crossover"])
                    if rs_row.get("days_since_crossover") is not None
                    and not pd.isna(rs_row.get("days_since_crossover"))
                    else None
                ),
                "rs_score": _safe_round(rs_row.get("score"), 1),
                "combined_score": combined_score,
                "rs_trajectory": traj,
            })
        power_confluence.sort(key=lambda r: -(r["combined_score"] or 0))

    if not rs_etf_signals.empty and etf_prices_df is not None:
        # Sector RS изисква sector_rotation payload — ще го запълним след създаване на payload-а
        sector_rs_payload = rs_etf_signals  # placeholder; конвертираме после

    payload = {
        "metadata": {
            "as_of": as_of.strftime("%Y-%m-%d"),
            "total_universe": int(deltas["ticker"].nunique()),
            "history_start": history["date"].min().strftime("%Y-%m-%d"),
            "history_end": history["date"].max().strftime("%Y-%m-%d"),
            "thresholds": {
                "high_base": 80,
                "low_base": 20,
            },
            "scoring": "pure 12-1 momentum + GICS-sector-relative z-score",
            "backtest_summary": {
                "stable_winners_excess_63d": "+3.34%",
                "quality_dip_excess_63d": "+1.72%",
                "faded_bounces_excess_63d": "-0.44%",
                "tested_period": "2022-04 to 2025-10",
                "rebalance_count": 42,
                "caveat": "survivorship bias positive; current SP500 universe used historically",
            },
        },
        "stable_winners_1m": _with_trajectory(stable_winners_1m),
        "stable_winners_3m": _with_trajectory(stable_winners_3m),
        "quality_dip_1m": _with_trajectory(quality_dip_1m),
        "quality_dip_3m": _with_trajectory(quality_dip_3m),
        "faded_bounces_1m": _with_trajectory(faded_bounces_1m),
        "current_strength": _with_trajectory(current_strength),
        "rank_all_stocks": rank_all_payload,
        "screener": {
            "as_of": as_of.strftime("%Y-%m-%d"),
            "market_cap_updated": mcap_updated,
            "stocks": screener_stocks,
        },
        "sector_rotation": [
            {
                "sector": row["gics_sector"],
                "mean_delta_1m": _safe_round(row["mean_delta_1m"], 2),
                "mean_delta_3m": _safe_round(row["mean_delta_3m"], 2),
                "n_total": int(row["n_total"]),
                "n_risers": int(row["n_risers"]),
                "n_decayers": int(row["n_decayers"]),
            }
            for _, row in sector_agg.iterrows()
        ],
        "sub_industry_rotation": [
            {
                "sector": row["gics_sector"],
                "sub_industry": row["gics_sub_industry"],
                "mean_delta_1m": _safe_round(row["mean_delta_1m"], 2),
                "mean_delta_3m": _safe_round(row["mean_delta_3m"], 2),
                "n_total": int(row["n_total"]),
            }
            for _, row in sub_industry_agg.iterrows()
        ],
        "rs_inflection_bullish": rs_inflection_bullish,
        "rs_inflection_bearish": rs_inflection_bearish,
        "rs_new_highs": rs_new_highs,
        "power_confluence": power_confluence,
        "sector_rs": [],  # populated below after sector_rotation is built
        "attention_layer": compute_attention_signals(history, sectors),
    }

    if rs_meta is not None:
        payload["metadata"]["rs_line"] = rs_meta

    # Cross-link Sector RS с stock-level sector_rotation
    if not rs_etf_signals.empty and etf_prices_df is not None:
        payload["sector_rs"] = _build_sector_rs_payload(
            rs_etf_signals, etf_prices_df, benchmark_series, payload["sector_rotation"]
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return payload


if __name__ == "__main__":
    payload = render_dashboard_data()
    print(f"Wrote {OUTPUT_PATH}")
    print(f"  As of: {payload['metadata']['as_of']}")
    print(f"  Stable Winners 1m: {len(payload['stable_winners_1m'])}")
    print(f"  Stable Winners 3m: {len(payload['stable_winners_3m'])}")
    print(f"  Quality Dip 1m: {len(payload['quality_dip_1m'])}")
    print(f"  Quality Dip 3m: {len(payload['quality_dip_3m'])}")
    print(f"  Faded Bounces (warning): {len(payload['faded_bounces_1m'])}")
    print(f"  Current Strength: {len(payload['current_strength'])}")
    print(f"  Rank All Stocks: {len(payload['rank_all_stocks'])}")
    print(f"  Screener stocks: {len(payload['screener']['stocks'])}")
    print(f"  Sectors: {len(payload['sector_rotation'])}")
    print(f"  Sub-industries: {len(payload['sub_industry_rotation'])}")
    print(f"  RS Inflection Bullish: {len(payload['rs_inflection_bullish'])}")
    print(f"  RS Inflection Bearish: {len(payload['rs_inflection_bearish'])}")
    print(f"  RS New Highs: {len(payload['rs_new_highs'])}")
    print(f"  Power Confluence: {len(payload['power_confluence'])}")
    print(f"  Sector RS: {len(payload['sector_rs'])}")
    al = payload["attention_layer"]
    n_premium = sum(t["is_premium"] for t in al["tickers"])
    n_lonely = sum(t["is_lonely"] and not t["is_premium"] for t in al["tickers"])
    print(f"  Attention Layer: {len(al['tickers'])} SW @ {al['latest_date']} "
          f"({n_premium} Premium, {n_lonely} Lonely)")
