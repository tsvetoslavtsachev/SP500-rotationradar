# SP500 Rotation Radar

> **Наблюдавай лидерите — особено когато временно отслабнат.** Не сигнал за покупка, а радар за относителна сила ВЪТРЕ в сектора.

**Етикет на лицето (КОКПИТ вълна 1, O4):** унифициран едноредов бадж под заглавието — `Наблюдение, не сигнал · относителна сила (cross-sectional, сега)`. Той е единственият стандартизиран епистемичен носител; „не сигнал за покупка" беше преместено от подзаглавието в баджа, за да не се дублира, а честната бележка „наблюдение, не доказан edge" остава като дълбочина.

Daily-refreshed дашборд който идентифицира **Stable Winners** (лидери, които продължават да водят) и **Quality Dip** (лидери, които временно отслабват) в SP500. **Честно преизмерено (2026-07-07, `_rotation_twins_analytics/C-EMPIRICS.md §T2`):** само Stable Winners е значим — +1.30% (1m) / +1.47% (3m) excess fwd_63d (t 2.37/2.16), но режимно-зависим. Старите in-sample „+3.34/+1.72" бяха надути.

## Какво научихме (важно)

Първоначалното проектиране целеше да хваща "fallen angels" (бивши изоставащи, които започват да се качват). Backtest на 5 години SP500 данни показа **обратното** — но честното преизмерване (2026-07-07: membership-filtered TR, месечни ребаланси, block-bootstrap значимост; `_rotation_twins_analytics/C-EMPIRICS.md §T2`) свали рекламните числа наполовина и уби Quality Dip claim-а:

| Quadrant | Excess fwd_63d (1m / 3m) | t | Verdict (честно) |
|---|---|---|---|
| **Stable Winners** (висока база + ↑) | **+1.30% / +1.47%** | 2.37 / 2.16 | 🎯 **Единствен значим** — но режимно-зависим (2022: −0.62%) |
| **Quality Dip** (висока база + ↓) | +0.36% / +0.04% | 0.62 / 0.09 | ⚪ Незначим — наблюдение, не buy-point |
| **Faded Bounces** (ниска база + ↑) | −0.25% / −0.30% | — | ⚠ Слаб отрицателен |
| Chronic Losers (ниска база + →↓) | шум (−0.17% … +0.17%) | — | Без сигнал |

*(N=57–58 месечни срезa, 2021-06 → 2026-03; SP500 membership-filtered — pre-inclusion махнат, delisting не → лек survivorship таван. Старите числа бяха in-sample, дневно припокриване, price-vs-TR.)*

**Заключение:** Заглавната стойност е „наблюдавай установените лидери" (Stable Winners) — реален, но по-тесен и **режимно-зависим** сигнал (силен в бика 2023-24, отрицателен в rate-shock 2022), не all-weather edge. „Купи дипа на качеството" не оцелява честното измерване в SP500: рафтът системно държи вече-обърнати имена ~9 седмици (T3). За по-силния сигнал чети суровия 12-1 ранг — той бие продуктовия sector-z ранг (IC 63d +0.0234 vs +0.0100).

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
3. **💎 Quality Dip (1m)** — отслабващи лидери (наблюдение, не buy point)
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
