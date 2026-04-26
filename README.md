# SP500 Rotation Radar

> **"Купи лидерите, особено когато временно отслабнат."**
> Не "хвани падналите ангели".

Daily-refreshed дашборд който идентифицира **Stable Winners** (лидери, които продължават да водят) и **Quality Dip** (лидери, които временно отслабват) в SP500. Backtest 2022-2025 показва +3.34% и +1.72% respective excess return на 3-месечен horizon.

## Какво научихме (важно)

Първоначалното проектиране целеше да хваща "fallen angels" (бивши изоставащи, които започват да се качват). Backtest на 5 години SP500 данни показа **обратното**:

| Quadrant | Excess fwd_63d | Hit rate | Verdict |
|---|---|---|---|
| **Stable Winners** (висока база + ↑) | **+3.34%** | 53.2% | 🎯 **Главен сигнал** |
| **Quality Dip** (висока база + ↓) | **+1.72%** | 49.6% | 💎 Secondary, Nike-style |
| **Faded Bounces** (ниска база + ↑) | -0.44% | 44.7% | ⚠ Avoid — обикновено избледняват |
| Chronic Losers (ниска база + →↓) | +0.41% | 47.5% | Без сигнал |

**Заключение:** В bull-heavy режима 2022-2025, mean reversion на quality stocks бие momentum bottom-fishing. Падналите ангели рядко се възстановяват реално; бившите лидери — често.

## Архитектура

### Слой 1 — Signal Engine V2

Pure 12-1 momentum (Jegadeesh-Titman 1993, classic), normalized като **sector-relative z-score**:

```
mom_12_1 = price[t-21] / price[t-252] - 1
sector_zscore = (mom_12_1 - sector_mean) / sector_std
raw_score = sector_zscore
```

Защо така:
- **Pure 12-1, без 6-1/3-1**: краткосрочните периоди шумиха ranking-а; стабилно score → по-чист ΔRank сигнал
- **Без vol normalization**: създаваше defensive bias (ниско-вол акции системно водеха)
- **Sector-relative z-score**: рангирането е **спрямо peer-ите** в сектора. Energy stock с +25% return в bull energy market не е забележителен; но Consumer Staples stock с +25% return е 3σ събитие
- **Премахва sector momentum dominance**: вместо целия Energy сектор да оccupy-ва Top Risers, виждаш **кои Energy акции водят** vs кои изостават

### Слой 2 — ΔRank Engine

Persistent дневни snapshots в `data/ranks_history.parquet`. Изчисления:
- `base_rank_6m` = средна percentile_rank в [t-126, t-21]
- `delta_1m` = current_rank − rank преди 21 ден
- `delta_3m` = current_rank − rank преди 63 дни

**4-quadrant класификация** (прагове p20/p80 — по-стриктни от стандартните p25/p75 за по-фокусиран watchlist):

| | Висока база (≥p80) | Ниска база (≤p20) |
|---|---|---|
| Положителна Δ | 🎯 **Stable Winner** | ⚠ Faded Bounce |
| Отрицателна Δ | 💎 **Quality Dip** | Chronic Loser |

### Слой 3 — Sector Context

GICS Sector + Sub-Industry от Wikipedia. Sector heatmap показва интрасекторно лидерство (не overall sector beta).

## UI Tabs

1. **🎯 Stable Winners (1m)** — primary watchlist
2. **🎯 Stable Winners (3m)** — стабилно тестваните
3. **💎 Quality Dip (1m)** — Nike-style buy points
4. **💎 Quality Dip (3m)** — по-сериозни pullbacks
5. **⚠ Faded Bounces** — contrarian warning, **what NOT to research**
6. **🌡 Sector Heatmap**
7. **🔬 Sub-Industry Drilldown**

## Setup

```bash
pip install -r requirements.txt

# Еднократен 5y backfill (~10-15 минути, ~30-40 MB output)
python scripts/backfill_history.py

# Daily incremental update (за GitHub Actions)
python scripts/daily_update.py

# Validate известни ротации
python scripts/backfill_history.py --validate

# Backtest на quadrant logic
python scripts/backtest.py       # V1 (vol-adjusted, ще покаже минимална стойност)
python scripts/backtest_v2.py    # V2 (current — pure 12-1 + sector z-score)

# Tests
pytest tests/ -v
```

## Структура

```
src/
├── universe.py        # SP500 constituents + recently removed (намален survivorship bias)
├── prices.py          # yfinance batch download
├── signal_engine.py   # V2: pure 12-1 + sector z-score
├── rank_history.py    # ΔRank metrics + quadrant + watchlist getters
├── sector_engine.py   # GICS sector aggregation
└── render.py          # Генерира data.json

scripts/
├── backfill_history.py  # Еднократно: 5y retrospective
├── daily_update.py      # Ежедневно: incremental + render
├── backtest.py          # V1 backtest
├── backtest_v2.py       # V2 backtest (current architecture)
└── smoke_test.py        # E2E pipeline check

docs/
├── index.html, styles.css, app.js
└── data.json            # Auto-generated

tests/
├── test_signal_engine.py
└── test_delta_rank.py
```

## Caveats — какво НЕ е този инструмент

- **Не е trading strategy.** Quadrant класификацията е research starting point.
- **Survivorship bias е положителен.** Backtest използва текущ SP500 universe; де-листвани акции отсъстват → резултатите са оптимистично-биасирани.
- **Не е regime-aware.** В 2022 (bear) сигналите се преобърнаха частично — Risers работеха, Decayers не. Сегашните prag-ове са оптимизирани за bull/recovery regimes.
- **Не вижда фундаменти директно.** Цените → ровене ръчно/Perplexity за earnings, news, mood. Това е "research filter", не "screen for buy signals".
- **Не е trained ML.** Heuristic-based scoring + persistent rank tracking. Бъдеща v2 може да добави LambdaMART (Lin, Su, Zhu 2026 SSRN paper).

## Свързани материали

- SSRN paper: Lin, Su, Zhu (2026), "Empirical Asset Pricing via Learning-to-Rank"
- [SP500-momentumrank](https://github.com/tsvetoslavtsachev/SP500-momentumrank) — старият "current strength view"
- [stoxx600-momentumrank](https://github.com/tsvetoslavtsachev/stoxx600-momentumrank) — EU вариант
