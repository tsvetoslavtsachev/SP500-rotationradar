"""ПРОЛЕТР6 (26.11.2026): непълен ден не влиза в кеша, а as_of е денят на доходностите.

Случаят: на 21.11.2026 кешът получи ред за 20.11 с 8 от 520 цени и всички следващи ходове
казаха „up to date“; страницата пишеше as_of 20.11 при доходности към 19.11.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from src.complete_days import drop_partial_tail, last_complete_date  # noqa: E402


def _frame(counts, n_cols=520, start="2026-11-16"):
    idx = pd.bdate_range(start, periods=len(counts))
    data = np.full((len(counts), n_cols), np.nan)
    for i, c in enumerate(counts):
        data[i, :c] = 100.0 + i
    return pd.DataFrame(data, index=idx, columns=[f"T{j}" for j in range(n_cols)])


def test_partial_friday_is_dropped():
    df = _frame([503, 503, 503, 503, 8])
    out, dropped = drop_partial_tail(df)
    assert [str(d.date()) for d in dropped] == ["2026-11-20"]
    assert str(out.index.max().date()) == "2026-11-19"
    assert str(last_complete_date(df).date()) == "2026-11-19"


def test_two_partial_days_are_both_dropped():
    out, dropped = drop_partial_tail(_frame([503, 503, 503, 8, 8]))
    assert len(dropped) == 2 and str(out.index.max().date()) == "2026-11-18"


def test_full_days_and_small_changes_are_kept():
    # нови имена в индекса (+2) и няколко без бар (-5, под 5%) не са непълен ден
    for counts in ([503, 503, 505], [503, 503, 498], [503, 503, 503]):
        out, dropped = drop_partial_tail(_frame(counts))
        assert dropped == [] and len(out) == len(counts)


def test_old_partial_row_in_the_middle_is_history():
    out, dropped = drop_partial_tail(_frame([503, 8, 503, 503]))
    assert dropped == [] and len(out) == 4


def _patch_update(monkeypatch, tmp_path, cached, fresh, today):
    import daily_update as du
    path = tmp_path / "prices_cache.parquet"
    cached.to_parquet(path)
    calls = []

    def fake_fetch(tickers, start, end, acc):
        calls.append((start, end))
        return fresh

    monkeypatch.setattr(du, "PRICES_CACHE_PATH", path)
    monkeypatch.setattr(du, "_today", lambda: pd.Timestamp(today))
    monkeypatch.setattr(du, "_base_first_close", fake_fetch)
    monkeypatch.setattr(du, "_repair_scale", lambda df, stage, seams: df)
    monkeypatch.setattr(du, "rebase_history_to_overlap", lambda c, n: (c, {}))
    return du, path, calls


def test_saturday_run_rereads_the_partial_friday(monkeypatch, tmp_path):
    cached = _frame([503, 503, 503, 503, 8])                     # 16.11 .. 20.11, петъкът непълен
    fresh = _frame([503, 503], start="2026-11-19")               # архивът вече има петъка
    du, path, calls = _patch_update(monkeypatch, tmp_path, cached, fresh, "2026-11-21")
    prices, _ = du.update_prices_cache(list(cached.columns), {})
    assert calls, "ходът каза „up to date“ по непълния петък и не прочете пак"
    assert str(prices.index.max().date()) == "2026-11-20"
    assert int(prices.loc["2026-11-20"].notna().sum()) == 503
    on_disk = pd.read_parquet(path)
    assert int(on_disk.iloc[-1].notna().sum()) == 503


def test_friday_night_run_does_not_write_the_partial_day(monkeypatch, tmp_path):
    cached = _frame([503, 503, 503, 503])                        # 16.11 .. 19.11
    fresh = _frame([503, 8], start="2026-11-19")                 # 8 имена с петък от yfinance
    du, path, calls = _patch_update(monkeypatch, tmp_path, cached, fresh, "2026-11-21")
    prices, _ = du.update_prices_cache(list(cached.columns), {})
    assert str(prices.index.max().date()) == "2026-11-19"
    assert str(pd.read_parquet(path).index.max().date()) == "2026-11-19"


def test_as_of_gate_fails_when_as_of_is_not_the_returns_day():
    import daily_update as du
    prices = _frame([503, 503, 503, 503])                        # доходностите до 19.11
    du.check_as_of_is_returns_day({"metadata": {"as_of": "2026-11-19"},
                                   "screener": {"as_of": "2026-11-19"}}, prices)
    with pytest.raises(SystemExit):
        du.check_as_of_is_returns_day({"metadata": {"as_of": "2026-11-20"},
                                       "screener": {"as_of": "2026-11-20"}}, prices)
