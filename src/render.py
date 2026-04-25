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

from src.rank_history import (  # noqa: E402
    compute_delta_metrics,
    get_faded_bounces,
    get_quality_dip,
    get_stable_winners,
    load_history,
)
from src.sector_engine import (  # noqa: E402
    aggregate_by_sector,
    aggregate_by_sub_industry,
    get_sector_dataframe,
)

DATA_DIR = ROOT / "data"
DOCS_DIR = ROOT / "docs"
HISTORY_PATH = DATA_DIR / "ranks_history.parquet"
SECTOR_CACHE_PATH = DATA_DIR / "sector_map.json"
OUTPUT_PATH = DOCS_DIR / "data.json"
TRAJECTORY_DAYS = 90


def _safe_round(x, ndigits: int = 1):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return None
    return round(float(x), ndigits)


def _safe_str(x):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return None
    return str(x) if not pd.isna(x) else None


def _row_to_dict(row: pd.Series, sectors: pd.DataFrame) -> dict:
    sector_info = sectors[sectors["ticker"] == row["ticker"]]
    if sector_info.empty:
        name = sector = sub = None
    else:
        s = sector_info.iloc[0]
        name = _safe_str(s.get("name"))
        sector = _safe_str(s.get("gics_sector"))
        sub = _safe_str(s.get("gics_sub_industry"))
    return {
        "ticker": row["ticker"],
        "name": name,
        "sector": sector,
        "sub_industry": sub,
        "current_rank": _safe_round(row.get("current_rank")),
        "abs_strength": _safe_round(row.get("abs_strength")),
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

    stable_winners_1m = get_stable_winners(deltas, window="1m", limit=limit)
    stable_winners_3m = get_stable_winners(deltas, window="3m", limit=limit)
    quality_dip_1m = get_quality_dip(deltas, window="1m", limit=limit)
    quality_dip_3m = get_quality_dip(deltas, window="3m", limit=limit)
    faded_bounces_1m = get_faded_bounces(deltas, window="1m", limit=limit)

    sector_agg = aggregate_by_sector(deltas, sectors)
    sub_industry_agg = aggregate_by_sub_industry(deltas, sectors)

    def _with_trajectory(df: pd.DataFrame) -> list:
        if df.empty:
            return []
        out = []
        for _, row in df.iterrows():
            d = _row_to_dict(row, sectors)
            d["trajectory"] = _trajectory(history, row["ticker"])
            out.append(d)
        return out

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
    }

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
    print(f"  Sectors: {len(payload['sector_rotation'])}")
    print(f"  Sub-industries: {len(payload['sub_industry_rotation'])}")
