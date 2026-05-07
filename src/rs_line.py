"""
Pristine RS Line — улавя момента на промяна в относителното лидерство.

RS Line = Stock close / Benchmark close (обикновено SPY).
Bullish signal = 10 EMA на RS пресича 50 SMA на RS отгоре.

Този слой е timing-orientated и комплементарен на 12-1 momentum + ΔRank engine
от signal_engine.py / rank_history.py:
- Quadrant класификацията реагира на 21d/63d snapshot diffs (бавна).
- RS Line crossover-ът улавя пресичането в деня (бърза).

Защо не SMA-only crossover (10/50 SMA на RS): EMA реагира на скорошни промени по-бързо,
което за inflection-detection е желано. Pristine indicator-ът на TradingView (free)
ползва 10 EMA / 50 SMA — копираме същата конвенция за консистентност.

Източник: Pristine Capital RS Line indicator (TradingView, free), released 2026-05-05.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

EMA_SHORT = 10
SMA_LONG = 50
DEFAULT_CROSSOVER_LOOKBACK = 5
DEFAULT_RS_HIGH_WINDOW = 252  # 52 weeks of trading days
PRICE_SMA_50 = 50
PRICE_SMA_200 = 200

# Минимум дни за да има smysl от RS Line изчислението.
# 50 SMA + малко buffer = 60.
MIN_HISTORY_DAYS = SMA_LONG + 10

# За freshness decay в score: e^(-days/5).
FRESHNESS_TAU = 5.0


@dataclass
class _Signals:
    """Помощен dataclass за вътрешно подреждане на полетата."""
    ticker: str
    rs_value: float
    rs_ema10: float
    rs_sma50: float
    cloud_color: str
    bullish_crossover: bool
    bearish_crossover: bool
    days_since_crossover: int | None
    rs_new_high: bool
    price_value: float
    price_new_high: bool
    confluence: bool
    rs_slope: float
    rs_slope_norm: float
    trend_quality: int
    pct_from_52w_high: float | None
    score: float


def _safe(value, default=None):
    if value is None:
        return default
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return default
    return value


def _days_since_crossover(above: pd.Series, lookback: int) -> int | None:
    """
    Намира деня на най-скорошното пресичане в последните `lookback` дни.

    Връща 0 ако пресичането е днес, 1 ако вчера, ..., None ако няма
    пресичане в прозореца.
    """
    if len(above) < 2:
        return None
    flips = above.ne(above.shift(1))
    flips.iloc[0] = False  # първият ред не е flip
    if not flips.iloc[-lookback:].any():
        return None
    # Намираме последния flip в прозореца
    recent = flips.iloc[-lookback - 1:].iloc[1:]
    if not recent.any():
        return None
    flip_positions = np.where(recent.values)[0]
    last_flip_idx = flip_positions[-1]
    # last_flip_idx е offset спрямо началото на recent (lookback дни назад)
    days_back = (lookback - 1) - last_flip_idx
    return int(days_back)


def _compute_one(
    ticker: str,
    prices: pd.Series,
    benchmark: pd.Series,
    crossover_lookback_days: int,
    rs_high_window: int,
) -> _Signals | None:
    """Изчислява RS Line signals за един ticker. Връща None ако няма достатъчно история."""
    prices = prices.dropna()
    benchmark = benchmark.dropna()
    aligned = pd.concat([prices, benchmark], axis=1, join="inner").dropna()
    if len(aligned) < MIN_HISTORY_DAYS:
        return None

    p = aligned.iloc[:, 0]
    b = aligned.iloc[:, 1]

    rs = p / b
    rs_ema = rs.ewm(span=EMA_SHORT, adjust=False).mean()
    rs_sma = rs.rolling(window=SMA_LONG, min_periods=SMA_LONG).mean()

    if pd.isna(rs_sma.iloc[-1]) or pd.isna(rs_ema.iloc[-1]):
        return None

    above = rs_ema > rs_sma
    cloud_color = "green" if bool(above.iloc[-1]) else "red"

    # Crossover detection: имаме crossover ако above[t] != above[t-N]
    # за някое N в [1..lookback]. Точният ден е първият flip в прозореца.
    days_since = _days_since_crossover(above, crossover_lookback_days)
    bullish = False
    bearish = False
    if days_since is not None:
        # Проверка какъв тип е пресичането: погледни състоянието преди flip-а.
        flip_idx = len(above) - 1 - days_since
        if flip_idx >= 1:
            before = above.iloc[flip_idx - 1]
            after = above.iloc[flip_idx]
            if (not before) and after:
                bullish = True
            elif before and (not after):
                bearish = True

    # 52w highs (пълни прозорци; ако историята е по-къса, използваме каквото имаме)
    window = min(rs_high_window, len(rs))
    rs_window_max = rs.iloc[-window:].max()
    price_window_max = p.iloc[-window:].max()
    rs_new_high = bool(rs.iloc[-1] >= rs_window_max - 1e-9)
    price_new_high = bool(p.iloc[-1] >= price_window_max - 1e-9)
    confluence = rs_new_high and price_new_high
    pct_from_52w_high = float((p.iloc[-1] / price_window_max - 1.0) * 100.0)

    # RS slope: change in RS_EMA10 over последните 10 дни, normalized by SMA50 level
    if len(rs_ema) >= 11 and rs_sma.iloc[-11] > 0:
        rs_slope = float(rs_ema.iloc[-1] - rs_ema.iloc[-11])
        rs_slope_norm = float(rs_slope / rs_sma.iloc[-11])
    else:
        rs_slope = 0.0
        rs_slope_norm = 0.0

    # Trend quality: цена > SMA50 > SMA200 (uptrend stack)
    p_sma50 = p.rolling(PRICE_SMA_50).mean()
    p_sma200 = p.rolling(PRICE_SMA_200).mean()
    trend_quality = 0
    if (
        not pd.isna(p_sma50.iloc[-1])
        and not pd.isna(p_sma200.iloc[-1])
        and p.iloc[-1] > p_sma50.iloc[-1] > p_sma200.iloc[-1]
    ):
        trend_quality = 1

    # Score (0..100): freshness * 0.5 + slope_norm * 0.3 + trend_quality * 0.2
    if days_since is None:
        freshness = 0.0
    else:
        freshness = float(np.exp(-days_since / FRESHNESS_TAU))
    # Normalize slope: clip rs_slope_norm в [-0.05, 0.05], map в [0, 1].
    # Емпирично: за S&P 500 акции типичен diapason е ±0.02–0.03 на 10-дневен EMA лак.
    slope_clipped = float(np.clip(rs_slope_norm, -0.05, 0.05))
    slope_score = (slope_clipped + 0.05) / 0.10  # ∈ [0, 1]
    score_raw = 0.5 * freshness + 0.3 * slope_score + 0.2 * float(trend_quality)
    score = float(np.clip(score_raw * 100.0, 0.0, 100.0))

    return _Signals(
        ticker=ticker,
        rs_value=float(rs.iloc[-1]),
        rs_ema10=float(rs_ema.iloc[-1]),
        rs_sma50=float(rs_sma.iloc[-1]),
        cloud_color=cloud_color,
        bullish_crossover=bullish,
        bearish_crossover=bearish,
        days_since_crossover=days_since,
        rs_new_high=rs_new_high,
        price_value=float(p.iloc[-1]),
        price_new_high=price_new_high,
        confluence=confluence,
        rs_slope=rs_slope,
        rs_slope_norm=rs_slope_norm,
        trend_quality=trend_quality,
        pct_from_52w_high=pct_from_52w_high,
        score=score,
    )


def compute_rs_signals(
    prices: pd.DataFrame,
    benchmark: pd.Series,
    crossover_lookback_days: int = DEFAULT_CROSSOVER_LOOKBACK,
    rs_high_window: int = DEFAULT_RS_HIGH_WINDOW,
) -> pd.DataFrame:
    """
    Изчислява Pristine RS Line signals за всички тикъри.

    Args:
        prices: DataFrame с adjusted close — index = date, columns = ticker.
        benchmark: Series — adjusted close на benchmark (обикновено SPY).
        crossover_lookback_days: брой дни в които смятаме crossover за "fresh" (default 5).
        rs_high_window: прозорец за RS new high (default 252 = 52 weeks).

    Returns:
        DataFrame с колони:
          ticker, rs_value, rs_ema10, rs_sma50, cloud_color (green/red),
          bullish_crossover, bearish_crossover, days_since_crossover,
          rs_new_high, price_value, price_new_high, confluence,
          rs_slope, rs_slope_norm, trend_quality, pct_from_52w_high, score.
        Тикери с недостатъчна история се пропускат.
    """
    if benchmark is None or len(benchmark) < MIN_HISTORY_DAYS:
        raise ValueError(
            f"Benchmark series too short for RS Line ({len(benchmark) if benchmark is not None else 0} < {MIN_HISTORY_DAYS} days)."
        )

    rows: list[dict] = []
    for ticker in prices.columns:
        sig = _compute_one(
            ticker=ticker,
            prices=prices[ticker],
            benchmark=benchmark,
            crossover_lookback_days=crossover_lookback_days,
            rs_high_window=rs_high_window,
        )
        if sig is None:
            continue
        rows.append(
            {
                "ticker": sig.ticker,
                "rs_value": sig.rs_value,
                "rs_ema10": sig.rs_ema10,
                "rs_sma50": sig.rs_sma50,
                "cloud_color": sig.cloud_color,
                "bullish_crossover": sig.bullish_crossover,
                "bearish_crossover": sig.bearish_crossover,
                "days_since_crossover": sig.days_since_crossover,
                "rs_new_high": sig.rs_new_high,
                "price_value": sig.price_value,
                "price_new_high": sig.price_new_high,
                "confluence": sig.confluence,
                "rs_slope": sig.rs_slope,
                "rs_slope_norm": sig.rs_slope_norm,
                "trend_quality": sig.trend_quality,
                "pct_from_52w_high": sig.pct_from_52w_high,
                "score": sig.score,
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "ticker", "rs_value", "rs_ema10", "rs_sma50", "cloud_color",
                "bullish_crossover", "bearish_crossover", "days_since_crossover",
                "rs_new_high", "price_value", "price_new_high", "confluence",
                "rs_slope", "rs_slope_norm", "trend_quality", "pct_from_52w_high", "score",
            ]
        )
    return pd.DataFrame(rows)


def compute_rs_trajectory(
    prices: pd.Series,
    benchmark: pd.Series,
    days: int = 90,
) -> list[dict]:
    """
    Връща последните `days` точки на RS Line за trajectory sparkline.
    Each point: {"date": "YYYY-MM-DD", "rs": float}.
    """
    aligned = pd.concat([prices, benchmark], axis=1, join="inner").dropna()
    if aligned.empty:
        return []
    rs = aligned.iloc[:, 0] / aligned.iloc[:, 1]
    tail = rs.tail(days)
    return [
        {"date": d.strftime("%Y-%m-%d"), "rs": _safe(round(float(v), 6))}
        for d, v in zip(tail.index, tail.values)
        if not pd.isna(v)
    ]
