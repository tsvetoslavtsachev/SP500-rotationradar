"""
Tests за Pristine RS Line изчислителния модул.

Покрити случаи:
  - Bullish crossover detection върху synthetic series
  - Bearish crossover detection
  - No crossover (flat RS) ⇒ days_since_crossover = None
  - 52w RS new high detection
  - Price + RS confluence
  - Edge: <50 дни история ⇒ ticker се пропуска
  - Score нормализация в [0, 100]
  - Empty benchmark ⇒ ValueError
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.rs_line import (
    DEFAULT_CROSSOVER_LOOKBACK,
    MIN_HISTORY_DAYS,
    compute_rs_signals,
    compute_rs_trajectory,
)


def _make_dates(n: int) -> pd.DatetimeIndex:
    """Връща `n` business days, завършващи днес."""
    return pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=n)


def _flat_benchmark(n: int = 300) -> pd.Series:
    """Constant SPY = 100 за лесна аритметика — RS Line = stock_price / 100."""
    return pd.Series([100.0] * n, index=_make_dates(n), name="SPY")


def test_bullish_crossover_detected():
    """
    Construct: 220 дни flat (stock=80, EMA10≈SMA50), после 10 дни остър ramp
    (80→100) така че EMA10 пресича SMA50 в последните 5 дни.
    """
    n = 230
    benchmark = _flat_benchmark(n)
    prices = np.concatenate([
        np.full(220, 80.0),
        np.linspace(80.0, 100.0, 10),
    ])
    s = pd.Series(prices, index=benchmark.index, name="STK")
    df = pd.DataFrame({"STK": s})

    out = compute_rs_signals(df, benchmark, crossover_lookback_days=10)
    assert len(out) == 1
    row = out.iloc[0]
    assert row["cloud_color"] == "green"
    assert bool(row["bullish_crossover"]), "Очаквах bullish crossover при flat→ramp"
    assert row["days_since_crossover"] is not None
    assert row["days_since_crossover"] <= 10


def test_bearish_crossover_detected():
    """Огледално: 220 дни ramp до high, после 10 дни остър slide ⇒ bearish."""
    n = 230
    benchmark = _flat_benchmark(n)
    prices = np.concatenate([
        np.linspace(80.0, 130.0, 220),
        np.linspace(130.0, 105.0, 10),
    ])
    s = pd.Series(prices, index=benchmark.index, name="STK")
    df = pd.DataFrame({"STK": s})

    out = compute_rs_signals(df, benchmark, crossover_lookback_days=10)
    row = out.iloc[0]
    assert row["cloud_color"] == "red", f"Очаквах red cloud, видях {row['cloud_color']}"
    assert bool(row["bearish_crossover"]), "Очаквах bearish crossover при ramp→slide"
    assert row["days_since_crossover"] is not None


def test_no_crossover_flat_rs():
    """RS постоянно равно (stock и benchmark в lockstep) ⇒ no crossover flag."""
    n = 200
    benchmark = _flat_benchmark(n)
    s = pd.Series([100.0] * n, index=benchmark.index, name="STK")
    df = pd.DataFrame({"STK": s})

    out = compute_rs_signals(df, benchmark)
    row = out.iloc[0]
    assert not bool(row["bullish_crossover"])
    assert not bool(row["bearish_crossover"])


def test_rs_new_high():
    """Monotonic increase в RS ⇒ rs_new_high=True; price също расте ⇒ confluence."""
    n = 260
    benchmark = _flat_benchmark(n)
    prices = np.linspace(50.0, 150.0, n)
    s = pd.Series(prices, index=benchmark.index, name="STK")
    df = pd.DataFrame({"STK": s})

    out = compute_rs_signals(df, benchmark, rs_high_window=252)
    row = out.iloc[0]
    assert row["rs_new_high"], "RS трябва да е на 52w high при monotonic ramp"
    assert row["price_new_high"]
    assert row["confluence"]


def test_score_in_valid_range():
    """Score трябва винаги да е в [0, 100]."""
    n = 230
    benchmark = _flat_benchmark(n)
    rng = np.random.default_rng(42)
    # Random walk
    walk = 100 + np.cumsum(rng.normal(0, 0.5, n))
    s = pd.Series(walk, index=benchmark.index, name="STK")
    df = pd.DataFrame({"STK": s})

    out = compute_rs_signals(df, benchmark)
    if not out.empty:
        scores = out["score"].dropna()
        assert (scores >= 0).all() and (scores <= 100).all()


def test_short_history_skipped():
    """Тикър с <50 дни история трябва да се пропусне (no row in output)."""
    n = 30
    benchmark = pd.Series([100.0] * 200, index=_make_dates(200))
    short = pd.Series(
        np.linspace(50, 80, n),
        index=_make_dates(n),
        name="SHORT",
    )
    df = pd.DataFrame({"SHORT": short})

    out = compute_rs_signals(df, benchmark)
    assert out.empty, "Кратка история трябва да дава empty DataFrame"


def test_empty_benchmark_raises():
    """Празен benchmark ⇒ ValueError."""
    df = pd.DataFrame({"AAPL": pd.Series([100.0] * 200, index=_make_dates(200))})
    with pytest.raises(ValueError, match="Benchmark"):
        compute_rs_signals(df, pd.Series(dtype=float))


def test_min_history_threshold():
    """Точно на ръба MIN_HISTORY_DAYS ⇒ не панира, връща нещо или skipва."""
    n = MIN_HISTORY_DAYS
    benchmark = _flat_benchmark(n)
    s = pd.Series(np.linspace(80, 100, n), index=benchmark.index, name="EDGE")
    df = pd.DataFrame({"EDGE": s})

    out = compute_rs_signals(df, benchmark)
    # Може да има или да няма ред (зависи от точния brой), но не трябва да хвърля
    # exception. Ако има ред — score трябва да е float.
    if not out.empty:
        assert isinstance(out.iloc[0]["score"], float)


def test_trajectory_returns_recent_days():
    """compute_rs_trajectory връща точки за последните `days` дни."""
    n = 200
    benchmark = _flat_benchmark(n)
    prices = pd.Series(np.linspace(50, 100, n), index=benchmark.index, name="STK")

    traj = compute_rs_trajectory(prices, benchmark, days=30)
    assert len(traj) == 30
    assert all("date" in p and "rs" in p for p in traj)
    # Trajectory е monotonic increasing → последна стойност > първа
    assert traj[-1]["rs"] > traj[0]["rs"]


def test_multiple_tickers_one_skipped():
    """Mix от тикъри: един с full история, един прекалено къс."""
    long_dates = _make_dates(200)
    short_dates = _make_dates(20)
    benchmark = pd.Series([100.0] * 200, index=long_dates)
    df = pd.DataFrame({
        "FULL": pd.Series(np.linspace(80, 110, 200), index=long_dates),
        "SHORT": pd.Series(np.linspace(50, 60, 20), index=short_dates),
    })
    out = compute_rs_signals(df, benchmark)
    assert "FULL" in out["ticker"].values
    assert "SHORT" not in out["ticker"].values
