// SP500 Rotation Radar — UI rendering logic.
// Чете data.json и рендерира всички views.

// Държи текущите данни за всеки tab — за Excel export-а.
// За tabs с filters (rank-all, screener), стойността е CURRENT FILTERED set.
// Речник — обяснение на всяка колона НА МЯСТО (ФОРМА-КАНОН §1, правило 3).
// Ключ = data-key на колоната; стойност = човешка подсказка (native title tooltip).
const COLUMN_TIPS = {
  ticker: "Борсов код. Клик отваря външния профил на компанията (Finviz).",
  symbol: "Секторен SPDR ETF (напр. XLK = технологии). Клик отваря външния профил (Finviz).",
  name: "Име на компанията / сектора.",
  sector: "GICS сектор — към кой отрасъл спада компанията.",
  sub_industry: "GICS под-индустрия — по-фина категория вътре в сектора.",
  abs_strength: "Абсолютен 12-1 моментум като percentile 0-100: колко силна е 11-месечната доходност спрямо целия пазар (100 = най-силна). По-силният предиктор.",
  mom_12_1_pct: "Реалната 11-месечна доходност (12-1: последните 12 месеца без най-скорошния). Действително число, не percentile.",
  current_rank: "Секторно-неутрален ранг 0-100: лидер ли е ВЪТРЕ в сектора си, изчистено от общата секторна вълна. По-слаб предиктор от абсолютния.",
  base_rank_6m: "Средният секторен ранг за месеци 6-1 назад — 'базата' / рафтът. Висока база = устойчив лидер, не еднодневка.",
  delta_1m: "Промяна (Δ) на ранга спрямо преди ~21 търговски дни (1 месец). Плюс = засилва се, минус = отслабва.",
  delta_3m: "Промяна (Δ) на ранга спрямо преди ~63 търговски дни (3 месеца). Плюс = засилва се, минус = отслабва.",
  trajectory: "Линията на ранга през последните ~90 търговски дни (рангът, НЕ доходността). Нагоре = засилващ се ранг.",
  score: "Композитен резултат за подреждане (sector-relative percentile). По-висок = по-напред в класацията.",
  rank_position: "Позиция в подреждането. 1 = най-отпред.",
  rank_index: "Позиция в подреждането. 1 = най-отпред.",
  quadrant_1m: "Квадрант (1м): Stable Winner = силен и се засилва · Quality Dip = силен, но отслабва · Faded Bounce = слаб, но подскочи · Chronic Loser = слаб и отслабва.",
  quadrant_3m: "Квадрант (3м): същата логика, но на 3-месечен прозорец.",
  sw_streak: "Колко поредни месечни ребаланса акцията остава Stable Winner. По-дълга серия = по-устойчиво лидерство.",
  sector_avg_rank: "Среден ранг на сектора наоколо — колко силен/слаб е самият сектор.",
  market_cap_m: "Пазарна капитализация (в млн. $).",
  size_bucket: "Размер по капитализация: Large / Mid / Small.",
  ret_1m: "Ценова доходност за 1 месец.",
  ret_3m: "Ценова доходност за 3 месеца.",
  ret_6m: "Ценова доходност за 6 месеца.",
  ret_ytd: "Ценова доходност от началото на годината.",
  ret_1y: "Ценова доходност за 1 година.",
  ret_3y: "Ценова доходност за 3 години.",
  ret_5y: "Ценова доходност за 5 години.",
  vol_1y: "Годишна волатилност — колебливостта на дневните доходности, анюализирана. По-високо = по-люлеещо.",
  sharpe_1y: "Sharpe (1г): доходност на единица риск (доходност ÷ волатилност). По-високо = по-добре платен риск.",
  sharpe_3y: "Sharpe (3г): доходност на единица риск.",
  maxdd_1y: "Максимален drawdown (1г): най-голямото падане от връх до дъно.",
  maxdd_3y: "Максимален drawdown (3г): най-голямото падане от връх до дъно.",
  maxdd_5y: "Максимален drawdown (5г): най-голямото падане от връх до дъно.",
  calmar_3y: "Calmar (3г): доходност ÷ максимален drawdown. Възвръщаемост спрямо най-лошото падане.",
  dist_52w_high: "Разстояние до 52-седмичния връх. 0% = на върха; -20% = 20% под върха.",
  days_since_52w_high: "Дни от последния 52-седмичен връх.",
  beta_1y: "Бета (1г) спрямо индекса: колко се движи спрямо пазара. 1 = като пазара, >1 = по-рязко.",
  price: "Последна цена.",
  pct_from_52w_high: "Разстояние до 52-седмичния връх (%).",
  rs_value: "Relative Strength линия = цена на акцията ÷ цена на SPY. Расте, когато акцията изпреварва пазара.",
  cloud_color: "Цвят на RS-облака: зелено = RS над средните си (10 EMA над 50 SMA), червено = под тях.",
  days_since_crossover: "Дни от последното пресичане на RS-линията през пълзящите ѝ средни (10 EMA / 50 SMA).",
  rs_slope_norm: "Наклон на RS-линията (нормализиран). Плюс = ускорява изпреварването на пазара.",
  trend_quality: "Качество на тренда (✓ = чист възходящ RS-тренд).",
  rs_trajectory: "RS-линията през последните ~90 търговски дни.",
  rs_score: "Резултат само по RS-компонента (timing).",
  combined_score: "Комбиниран резултат: Stable Winner × свеж bullish RS crossover.",
  rs_new_high: "★ = RS-линията на сектора е на нов 52-седмичен връх.",
  stocks_n_risers: "Брой акции ВЪТРЕ в сектора с положителна ΔRank (засилващи се).",
  stocks_n_decayers: "Брой акции ВЪТРЕ в сектора с отрицателна ΔRank (отслабващи).",
};

// Слага native tooltip + hover-affordance на header по неговия data-key.
function applyColTip(th) {
  if (!th || th.title) return;
  const tip = COLUMN_TIPS[th.dataset.key];
  if (tip) { th.title = tip; th.classList.add("has-tip"); }
}

const exportState = {
  asOf: null,
  "attention": [],
  "stable-winners-1m": [],
  "stable-winners-3m": [],
  "quality-dip-1m": [],
  "quality-dip-3m": [],
  "faded-bounces": [],
  "current-strength": [],
  "rank-all": [],
  "screener": [],
  "rs-inflection": [],
  "power-confluence": [],
  "sector-rs": [],
};

(async () => {
  const data = await fetchData();
  if (!data) return;

  exportState.asOf = data.metadata?.as_of || "unknown";

  renderMetadata(data.metadata);
  renderAttention(data.attention_layer);
  renderWatchlist("stable-winners-1m", data.stable_winners_1m, "1m");
  renderWatchlist("stable-winners-3m", data.stable_winners_3m, "3m");
  renderWatchlist("quality-dip-1m", data.quality_dip_1m, "1m");
  renderWatchlist("quality-dip-3m", data.quality_dip_3m, "3m");
  renderWatchlist("faded-bounces", data.faded_bounces_1m, "1m");
  renderCurrentStrength("current-strength", data.current_strength);
  renderRankAll("rank-all", data.rank_all_stocks);
  renderScreener("screener", data.screener);
  renderHeatmap("sectors", data.sector_rotation);
  renderSubIndustryTable("sub-industries", data.sub_industry_rotation);
  renderRSInflection(data);
  renderPowerConfluence("power-confluence", data.power_confluence);
  renderSectorRS("sector-rs", data.sector_rs);
  setupTabs();
  setupExportButtons();
})();

async function fetchData() {
  try {
    const res = await fetch("data.json");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    document.querySelector("main").innerHTML =
      `<div class="empty-state">⚠ data.json не може да се зареди.<br><small>${err.message}</small></div>`;
    return null;
  }
}

function renderMetadata(meta) {
  const host = document.getElementById("metadata");
  host.innerHTML = `
    <span>📅 As of: <strong>${meta.as_of}</strong></span>
    <span>📊 Universe: <strong>${meta.total_universe}</strong></span>
    <span>📚 History: ${meta.history_start} → ${meta.history_end}</span>
  `;
}

// ──────────────────────────────────────────────────────────────────────
// Attention Layer — Premium + Lonely + Normal cohorts
// ──────────────────────────────────────────────────────────────────────
function renderAttention(layer) {
  if (!layer) return;
  const tickers = layer.tickers || [];

  const premium = tickers.filter((t) => t.is_premium);
  const lonely = tickers.filter((t) => t.is_lonely && !t.is_premium);
  const normal = tickers.filter((t) => !t.is_lonely);

  // Export combines all three cohorts with a "cohort" column
  exportState["attention"] = [
    ...premium.map((t) => ({ ...t, cohort: "Premium" })),
    ...lonely.map((t) => ({ ...t, cohort: "Lonely" })),
    ...normal.map((t) => ({ ...t, cohort: "Normal" })),
  ];

  // Meta line
  const meta = document.getElementById("attention-meta");
  if (meta) {
    const csCount = lonely.filter((t) => t.is_cs_excluded).length;
    const lonelyVisible = lonely.length - csCount;
    meta.innerHTML = `
      <span>📅 Ребаланс: <strong>${layer.latest_date ?? "—"}</strong></span>
      <span class="pill premium"><strong>${premium.length}</strong> Premium (+9.15% in-sample)</span>
      <span class="pill lonely"><strong>${lonelyVisible}</strong> Lonely (+3.31% in-sample)
        ${csCount ? `<small style="color:var(--text-dim)"> + ${csCount} CS скрити</small>` : ""}</span>
      <span class="pill normal"><strong>${normal.length}</strong> Normal SW (+0.31% in-sample)</span>
      <span><strong>${tickers.length}</strong> SW общо</span>
    `;
  }

  renderAttentionTable("attention-premium-host", premium, { variant: "premium" });
  renderAttentionTable("attention-lonely-host", lonely, { variant: "lonely", withCsToggle: true });
  renderAttentionTable("attention-normal-host", normal, { variant: "normal" });

  // Wire CS toggle
  const toggle = document.getElementById("attention-cs-toggle");
  if (toggle) {
    toggle.checked = false;
    toggle.addEventListener("change", () => {
      const lonelyTable = document.querySelector("#attention-lonely-host table");
      if (lonelyTable) {
        lonelyTable.classList.toggle("show-cs", toggle.checked);
      }
    });
  }
}

function renderAttentionTable(hostId, rows, opts = {}) {
  const host = document.getElementById(hostId);
  if (!host) return;
  if (!rows || rows.length === 0) {
    const msg = opts.variant === "premium"
      ? "На текущия ребаланс няма Premium кандидат. Това е нормално — Premium кохортата е около 1 акция на месец."
      : "Няма данни.";
    host.innerHTML = `<div class="empty-state">${msg}</div>`;
    return;
  }

  const headers = [
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name" },
    { key: "sector", label: "Sector" },
    { key: "sub_industry", label: "Sub-Industry" },
    { key: "base_rank_6m", label: "Rank (6m)" },
    { key: "sector_avg_rank", label: "Sector avg" },
    { key: "delta_1m", label: "Δ 1m" },
    { key: "sw_streak", label: "Streak (мес)" },
  ];

  const table = document.createElement("table");
  table.className = "attention-table";
  if (opts.variant === "lonely" && opts.withCsToggle) {
    table.id = `${hostId}-table`;
  }

  // Header
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  headers.forEach((h, idx) => {
    const th = document.createElement("th");
    th.textContent = h.label;
    th.dataset.col = idx;
    th.dataset.key = h.key;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);

  // Body
  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    if (row.is_premium) tr.classList.add("premium-row");
    if (!row.is_premium && row.sw_streak >= 5) tr.classList.add("near-premium");
    if (row.is_cs_excluded) tr.classList.add("cs-row");

    headers.forEach((h) => {
      const td = document.createElement("td");
      const v = row[h.key];

      if (h.key === "ticker") {
        td.innerHTML = `<a class="ticker" href="https://finviz.com/quote.ashx?t=${row.ticker}" target="_blank" rel="noopener">${row.ticker}</a>`;
        if (row.is_cs_excluded) {
          td.innerHTML += '<span class="cs-tag">CS</span>';
        }
      } else if (h.key === "sw_streak") {
        td.innerHTML = `<span class="streak-badge">${v} мес</span>`;
        td.dataset.value = v;
      } else if (h.key === "delta_1m") {
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + v.toFixed(1);
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else if (h.key === "base_rank_6m" || h.key === "sector_avg_rank") {
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(1);
        td.dataset.value = v ?? "";
      } else {
        td.textContent = v ?? "—";
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  host.replaceChildren(table);
  attachSorting(table, headers);
}

function renderWatchlist(viewId, rows, deltaWindow) {
  const host = document.querySelector(`#${viewId} .table-host`);
  if (Object.prototype.hasOwnProperty.call(exportState, viewId)) {
    exportState[viewId] = rows || [];
  }
  if (!rows || rows.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма kandidaти в този quadrant сега.</div>`;
    return;
  }

  const showBoth = deltaWindow === "both";
  const headers = [
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name" },
    { key: "sector", label: "Sector" },
    { key: "abs_strength", label: "12-1 Rank" },
    { key: "mom_12_1_pct", label: "12-1 Mom %" },
    { key: "current_rank", label: "Sector-rel." },
    { key: "base_rank_6m", label: "Base (6m)" },
  ];
  if (showBoth) {
    headers.push({ key: "delta_1m", label: "Δ 1m" });
    headers.push({ key: "delta_3m", label: "Δ 3m" });
  } else {
    headers.push({ key: `delta_${deltaWindow}`, label: `Δ ${deltaWindow}` });
  }
  headers.push({ key: "trajectory", label: "Rank Path (90d)" });

  const table = document.createElement("table");
  table.appendChild(buildThead(headers));
  table.appendChild(buildTbody(rows, headers));
  host.replaceChildren(table);
  attachSorting(table, headers);
}

function buildThead(headers) {
  const thead = document.createElement("thead");
  const tr = document.createElement("tr");
  headers.forEach((h, idx) => {
    const th = document.createElement("th");
    th.textContent = h.label;
    th.dataset.col = idx;
    th.dataset.key = h.key;
    tr.appendChild(th);
  });
  thead.appendChild(tr);
  return thead;
}

function buildTbody(rows, headers) {
  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    headers.forEach((h) => {
      const td = document.createElement("td");
      if (h.key === "ticker") {
        td.innerHTML = `<a class="ticker" href="https://finviz.com/quote.ashx?t=${row.ticker}" target="_blank" rel="noopener">${row.ticker}</a>`;
      } else if (h.key === "trajectory") {
        td.appendChild(makeTrajectorySVG(row.trajectory));
      } else if (h.key.startsWith("delta_")) {
        const v = row[h.key];
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + v.toFixed(1);
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else if (h.key === "current_rank" || h.key === "base_rank_6m" || h.key === "abs_strength") {
        const v = row[h.key];
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(1);
        td.dataset.value = v ?? "";
      } else if (h.key === "mom_12_1_pct") {
        const v = row[h.key];
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + v.toFixed(1) + "%";
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else {
        td.textContent = row[h.key] ?? "—";
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  return tbody;
}

function makeTrajectorySVG(points) {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("class", "trajectory");
  svg.setAttribute("viewBox", "0 0 100 24");
  svg.setAttribute("preserveAspectRatio", "none");

  if (!points || points.length < 2) {
    return svg;
  }

  const ranks = points.map((p) => p.rank).filter((r) => r !== null && r !== undefined);
  if (ranks.length < 2) return svg;

  const w = 100, h = 24;
  const xStep = w / (points.length - 1);
  const path = points
    .map((p, i) => {
      const x = i * xStep;
      const y = h - ((p.rank ?? 50) / 100) * h;
      return (i === 0 ? "M" : "L") + x.toFixed(1) + "," + y.toFixed(1);
    })
    .join(" ");

  const last = ranks[ranks.length - 1];
  const first = ranks[0];
  const stroke = last > first ? "var(--riser)" : "var(--decayer)";

  const pathEl = document.createElementNS("http://www.w3.org/2000/svg", "path");
  pathEl.setAttribute("d", path);
  pathEl.setAttribute("stroke", stroke);
  pathEl.setAttribute("stroke-width", "1.5");
  pathEl.setAttribute("fill", "none");
  svg.appendChild(pathEl);

  // Hover tooltip — изяснява точно какво показва линията
  const titleEl = document.createElementNS("http://www.w3.org/2000/svg", "title");
  const startDate = points[0]?.date ?? "";
  const endDate = points[points.length - 1]?.date ?? "";
  titleEl.textContent =
    `Sector Rank trajectory: ${first.toFixed(1)} → ${last.toFixed(1)} ` +
    `(${startDate} → ${endDate}, ${points.length} търговски дни)`;
  svg.appendChild(titleEl);

  return svg;
}

function renderCurrentStrength(viewId, rows) {
  const host = document.querySelector(`#${viewId} .table-host`);
  exportState["current-strength"] = rows || [];
  if (!rows || rows.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма данни за Current Strength.</div>`;
    return;
  }

  const headers = [
    { key: "rank_index", label: "#" },
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name" },
    { key: "sector", label: "Sector" },
    { key: "mom_12_1_pct", label: "12-1 Mom %" },
    { key: "abs_strength", label: "Abs %ile" },
    { key: "current_rank", label: "Sector Rank" },
    { key: "trajectory", label: "Rank Path (90d)" },
  ];

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  headers.forEach((h, idx) => {
    const th = document.createElement("th");
    th.textContent = h.label;
    th.dataset.col = idx;
    th.dataset.key = h.key;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row, idx) => {
    const tr = document.createElement("tr");
    headers.forEach((h) => {
      const td = document.createElement("td");
      if (h.key === "rank_index") {
        td.textContent = idx + 1;
        td.dataset.value = idx + 1;
      } else if (h.key === "ticker") {
        td.innerHTML = `<a class="ticker" href="https://finviz.com/quote.ashx?t=${row.ticker}" target="_blank" rel="noopener">${row.ticker}</a>`;
      } else if (h.key === "trajectory") {
        td.appendChild(makeTrajectorySVG(row.trajectory));
      } else if (h.key === "mom_12_1_pct") {
        const v = row[h.key];
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + v.toFixed(1) + "%";
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else if (h.key === "abs_strength" || h.key === "current_rank") {
        const v = row[h.key];
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(1);
        td.dataset.value = v ?? "";
      } else {
        td.textContent = row[h.key] ?? "—";
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  host.replaceChildren(table);
  attachSorting(table, headers);
}

function renderHeatmap(viewId, sectors) {
  const host = document.querySelector(`#${viewId} .heatmap-host`);
  if (!sectors || sectors.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма секторни данни.</div>`;
    return;
  }

  const allDeltas = sectors.flatMap((s) => [s.mean_delta_1m, s.mean_delta_3m]).filter((v) => v !== null);
  const maxAbs = Math.max(1, ...allDeltas.map(Math.abs));

  const wrap = document.createElement("div");
  wrap.className = "heatmap";

  const header = document.createElement("div");
  header.className = "heatmap-row header";
  header.innerHTML = `
    <div title="GICS сектор.">Sector</div>
    <div style="text-align:center" title="Средна промяна (Δ) на интрасекторния ранг спрямо преди ~21 търговски дни (1 месец). Плюс = секторът се засилва отвътре.">Δ 1m</div>
    <div style="text-align:center" title="Средна промяна (Δ) на интрасекторния ранг спрямо преди ~63 търговски дни (3 месеца).">Δ 3m</div>
    <div style="text-align:center" title="Брой акции в сектора.">Total</div>
    <div style="text-align:center" title="Брой акции с положителна ΔRank (засилващи се).">Risers</div>
    <div style="text-align:center" title="Брой акции с отрицателна ΔRank (отслабващи).">Decayers</div>
  `;
  wrap.appendChild(header);

  sectors.forEach((s) => {
    const row = document.createElement("div");
    row.className = "heatmap-row";
    row.innerHTML = `
      <div><strong>${s.sector ?? "Unknown"}</strong></div>
      <div class="heat-cell" style="background:${heatColor(s.mean_delta_1m, maxAbs)}">${formatDelta(s.mean_delta_1m)}</div>
      <div class="heat-cell" style="background:${heatColor(s.mean_delta_3m, maxAbs)}">${formatDelta(s.mean_delta_3m)}</div>
      <div style="text-align:center">${s.n_total}</div>
      <div style="text-align:center; color:var(--riser)">${s.n_risers}</div>
      <div style="text-align:center; color:var(--decayer)">${s.n_decayers}</div>
    `;
    wrap.appendChild(row);
  });

  host.replaceChildren(wrap);
}

function renderSubIndustryTable(viewId, subs) {
  const host = document.querySelector(`#${viewId} .table-host`);
  if (!subs || subs.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма sub-industry данни.</div>`;
    return;
  }

  const headers = [
    { key: "sector", label: "Sector" },
    { key: "sub_industry", label: "Sub-Industry" },
    { key: "mean_delta_1m", label: "Δ 1m" },
    { key: "mean_delta_3m", label: "Δ 3m" },
    { key: "n_total", label: "N" },
  ];

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  headers.forEach((h, idx) => {
    const th = document.createElement("th");
    th.textContent = h.label;
    th.dataset.col = idx;
    th.dataset.key = h.key;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  subs.forEach((s) => {
    const tr = document.createElement("tr");
    headers.forEach((h) => {
      const td = document.createElement("td");
      if (h.key === "mean_delta_1m" || h.key === "mean_delta_3m") {
        const v = s[h.key];
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + v.toFixed(2);
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else {
        td.textContent = s[h.key] ?? "—";
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  host.replaceChildren(table);
  attachSorting(table, headers);
}

function renderRankAll(viewId, stocks) {
  const host = document.getElementById("rank-table-host");
  const sectorSelect = document.getElementById("rank-sector");
  const quadrantSelect = document.getElementById("rank-quadrant");
  const searchInput = document.getElementById("rank-search");
  const countPill = document.getElementById("rank-count");

  if (!stocks || stocks.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма rank данни.</div>`;
    return;
  }

  const sectors = Array.from(new Set(stocks.map((s) => s.sector).filter(Boolean))).sort();
  sectors.forEach((s) => {
    const opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    sectorSelect.appendChild(opt);
  });

  const headers = [
    { key: "rank_position", label: "#" },
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name" },
    { key: "sector", label: "Sector" },
    { key: "sub_industry", label: "Sub-Industry" },
    { key: "score", label: "Score" },
    { key: "abs_strength", label: "Abs %ile" },
    { key: "mom_12_1_pct", label: "12-1 Mom %" },
    { key: "base_rank_6m", label: "Base (6m)" },
    { key: "delta_1m", label: "Δ 1m" },
    { key: "delta_3m", label: "Δ 3m" },
    { key: "quadrant_1m", label: "Quad 1m" },
    { key: "quadrant_3m", label: "Quad 3m" },
  ];

  let currentSort = { key: "rank_position", desc: false };

  function fmtCell(td, key, value) {
    if (value === null || value === undefined) {
      td.textContent = "—";
      td.dataset.value = "";
      return;
    }
    if (key === "ticker") {
      td.innerHTML = `<a class="ticker" href="https://finviz.com/quote.ashx?t=${value}" target="_blank" rel="noopener">${value}</a>`;
      td.dataset.value = value;
      return;
    }
    if (key === "rank_position") {
      td.textContent = value;
      td.dataset.value = value;
      td.style.fontWeight = "600";
      td.style.color = "var(--text-dim)";
      return;
    }
    if (key === "mom_12_1_pct") {
      td.textContent = (value > 0 ? "+" : "") + value.toFixed(1) + "%";
      td.className = value > 0 ? "delta-positive" : value < 0 ? "delta-negative" : "";
      td.dataset.value = value;
      return;
    }
    if (key === "delta_1m" || key === "delta_3m") {
      td.textContent = (value > 0 ? "+" : "") + value.toFixed(1);
      td.className = value > 0 ? "delta-positive" : value < 0 ? "delta-negative" : "";
      td.dataset.value = value;
      return;
    }
    if (key === "quadrant_1m" || key === "quadrant_3m") {
      const cls = {
        "Stable Winner": "quadrant-stable_winner",
        "Quality Dip": "quadrant-decayer",
        "Faded Bounce": "quadrant-riser",
        "Chronic Loser": "quadrant-chronic_loser",
        "Neutral": "quadrant-neutral",
      }[value] || "quadrant-neutral";
      td.innerHTML = `<span class="quadrant ${cls}">${value}</span>`;
      td.dataset.value = value;
      return;
    }
    if (typeof value === "number") {
      td.textContent = value.toFixed(1);
      td.dataset.value = value;
      return;
    }
    td.textContent = value;
    td.dataset.value = value;
  }

  function applyFilters() {
    const sector = sectorSelect.value;
    const quadrant = quadrantSelect.value;
    const query = searchInput.value.trim().toLowerCase();

    let filtered = stocks.filter((s) => {
      if (sector && s.sector !== sector) return false;
      if (quadrant && s.quadrant_1m !== quadrant) return false;
      if (query) {
        const t = (s.ticker || "").toLowerCase();
        const n = (s.name || "").toLowerCase();
        if (!t.includes(query) && !n.includes(query)) return false;
      }
      return true;
    });

    if (currentSort.key) {
      const k = currentSort.key;
      const dir = currentSort.desc ? -1 : 1;
      filtered = [...filtered].sort((a, b) => {
        const va = a[k];
        const vb = b[k];
        if (va === null || va === undefined) return 1;
        if (vb === null || vb === undefined) return -1;
        if (typeof va === "number" && typeof vb === "number") return (va - vb) * dir;
        return String(va).localeCompare(String(vb)) * dir;
      });
    }

    countPill.textContent = `${filtered.length} / ${stocks.length} акции`;
    exportState["rank-all"] = filtered;
    renderTable(filtered);
  }

  function renderTable(rows) {
    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    headers.forEach((h, idx) => {
      const th = document.createElement("th");
      th.textContent = h.label;
      th.dataset.col = idx;
      th.dataset.key = h.key;
      applyColTip(th);
      if (currentSort.key === h.key) {
        th.classList.add(currentSort.desc ? "sort-desc" : "sort-asc");
      }
      th.addEventListener("click", () => {
        currentSort.desc = !(currentSort.key === h.key && currentSort.desc);
        currentSort.key = h.key;
        applyFilters();
      });
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      headers.forEach((h) => {
        const td = document.createElement("td");
        fmtCell(td, h.key, row[h.key]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    host.replaceChildren(table);
  }

  sectorSelect.addEventListener("change", applyFilters);
  quadrantSelect.addEventListener("change", applyFilters);
  searchInput.addEventListener("input", applyFilters);

  applyFilters();
}

function renderScreener(viewId, screenerData) {
  const host = document.querySelector(`#${viewId} .screener-table-host`);
  const sectorSelect = document.getElementById("screener-sector");
  const sizeSelect = document.getElementById("screener-size");
  const searchInput = document.getElementById("screener-search");
  const countPill = document.getElementById("screener-count");

  if (!screenerData || !screenerData.stocks || screenerData.stocks.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма screener данни.</div>`;
    return;
  }

  const stocks = screenerData.stocks;

  // Populate sector dropdown
  const sectors = Array.from(new Set(stocks.map((s) => s.sector).filter(Boolean))).sort();
  sectors.forEach((s) => {
    const opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    sectorSelect.appendChild(opt);
  });

  const headers = [
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name", cls: "col-name" },
    { key: "sector", label: "Sector" },
    { key: "industry", label: "Sub-Industry" },
    { key: "market_cap_m", label: "Mcap" },
    { key: "size_bucket", label: "Size" },
    { key: "ret_1m", label: "1M %" },
    { key: "ret_3m", label: "3M %" },
    { key: "ret_6m", label: "6M %" },
    { key: "ret_ytd", label: "YTD %" },
    { key: "ret_1y", label: "1Y %" },
    { key: "ret_3y", label: "3Y %" },
    { key: "ret_5y", label: "5Y %" },
    { key: "vol_1y", label: "Vol 1Y %" },
    { key: "sharpe_1y", label: "Sharpe 1Y" },
    { key: "sharpe_3y", label: "Sharpe 3Y" },
    { key: "maxdd_1y", label: "MaxDD 1Y %" },
    { key: "maxdd_3y", label: "MaxDD 3Y %" },
    { key: "maxdd_5y", label: "MaxDD 5Y %" },
    { key: "calmar_3y", label: "Calmar 3Y" },
    { key: "dist_52w_high", label: "from 52w-H %" },
    { key: "days_since_52w_high", label: "Days since H" },
    { key: "beta_1y", label: "Beta 1Y" },
  ];

  let currentSort = { key: null, desc: true };

  function fmtCell(td, key, value) {
    if (value === null || value === undefined) {
      td.textContent = "—";
      td.dataset.value = "";
      return;
    }
    if (key === "ticker") {
      td.innerHTML = `<a class="ticker" href="https://finviz.com/quote.ashx?t=${value}" target="_blank" rel="noopener">${value}</a>`;
      td.dataset.value = value;
      return;
    }
    if (key === "market_cap_m") {
      const billions = value / 1000;
      td.textContent = billions >= 100 ? `${billions.toFixed(0)}B` : `${billions.toFixed(1)}B`;
      td.dataset.value = value;
      return;
    }
    if (typeof value === "number") {
      const isReturnLike = key.startsWith("ret_") || key.startsWith("maxdd_") || key === "dist_52w_high";
      if (isReturnLike) {
        td.textContent = (value > 0 ? "+" : "") + value.toFixed(1) + "%";
        td.className = value > 0 ? "delta-positive" : value < 0 ? "delta-negative" : "";
      } else if (key.startsWith("vol_")) {
        td.textContent = value.toFixed(1) + "%";
      } else if (key === "days_since_52w_high") {
        td.textContent = Math.round(value);
      } else {
        td.textContent = value.toFixed(2);
      }
      td.dataset.value = value;
      return;
    }
    td.textContent = value;
    td.dataset.value = value;
  }

  function applyFilters() {
    const sector = sectorSelect.value;
    const size = sizeSelect.value;
    const query = searchInput.value.trim().toLowerCase();

    let filtered = stocks.filter((s) => {
      if (sector && s.sector !== sector) return false;
      if (size && s.size_bucket !== size) return false;
      if (query) {
        const t = (s.ticker || "").toLowerCase();
        const n = (s.name || "").toLowerCase();
        if (!t.includes(query) && !n.includes(query)) return false;
      }
      return true;
    });

    if (currentSort.key) {
      const k = currentSort.key;
      const dir = currentSort.desc ? -1 : 1;
      filtered = [...filtered].sort((a, b) => {
        const va = a[k];
        const vb = b[k];
        if (va === null || va === undefined) return 1;
        if (vb === null || vb === undefined) return -1;
        if (typeof va === "number" && typeof vb === "number") return (va - vb) * dir;
        return String(va).localeCompare(String(vb)) * dir;
      });
    }

    countPill.textContent = `${filtered.length} / ${stocks.length} акции`;
    exportState["screener"] = filtered;
    renderTable(filtered);
  }

  function renderTable(rows) {
    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    headers.forEach((h, idx) => {
      const th = document.createElement("th");
      th.textContent = h.label;
      th.dataset.col = idx;
      th.dataset.key = h.key;
      if (h.cls) th.classList.add(h.cls);
      applyColTip(th);
      if (currentSort.key === h.key) {
        th.classList.add(currentSort.desc ? "sort-desc" : "sort-asc");
      }
      th.addEventListener("click", () => {
        currentSort.desc = !(currentSort.key === h.key && currentSort.desc);
        currentSort.key = h.key;
        applyFilters();
      });
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      headers.forEach((h) => {
        const td = document.createElement("td");
        if (h.cls) td.classList.add(h.cls);
        fmtCell(td, h.key, row[h.key]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    host.replaceChildren(table);
  }

  sectorSelect.addEventListener("change", applyFilters);
  sizeSelect.addEventListener("change", applyFilters);
  searchInput.addEventListener("input", applyFilters);

  applyFilters();
}

// ── RS Line tabs ─────────────────────────────────────────────────────────────

function makeRSTrajectorySVG(points) {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("class", "trajectory");
  svg.setAttribute("viewBox", "0 0 100 24");
  svg.setAttribute("preserveAspectRatio", "none");

  if (!points || points.length < 2) return svg;

  const values = points.map((p) => p.rs).filter((v) => v !== null && v !== undefined);
  if (values.length < 2) return svg;

  const w = 100, h = 24;
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  const range = hi - lo || 1e-9;
  const xStep = w / (points.length - 1);

  const path = points
    .map((p, i) => {
      const x = i * xStep;
      const y = h - ((p.rs - lo) / range) * h;
      return (i === 0 ? "M" : "L") + x.toFixed(1) + "," + y.toFixed(1);
    })
    .join(" ");

  const last = values[values.length - 1];
  const first = values[0];
  const stroke = last > first ? "var(--riser)" : "var(--decayer)";

  const pathEl = document.createElementNS("http://www.w3.org/2000/svg", "path");
  pathEl.setAttribute("d", path);
  pathEl.setAttribute("stroke", stroke);
  pathEl.setAttribute("stroke-width", "1.5");
  pathEl.setAttribute("fill", "none");
  svg.appendChild(pathEl);

  const titleEl = document.createElementNS("http://www.w3.org/2000/svg", "title");
  const startDate = points[0]?.date ?? "";
  const endDate = points[points.length - 1]?.date ?? "";
  titleEl.textContent =
    `RS Line trajectory: ${first.toFixed(4)} → ${last.toFixed(4)} ` +
    `(${startDate} → ${endDate}, ${points.length} търговски дни)`;
  svg.appendChild(titleEl);

  return svg;
}

function buildRSTable(rows, opts = {}) {
  const headers = [
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name" },
    { key: "sector", label: "Sector" },
    { key: "price", label: "Price" },
    { key: "pct_from_52w_high", label: "from 52w-H %" },
    { key: "rs_value", label: "RS" },
    { key: "cloud_color", label: "Cloud" },
    { key: "days_since_crossover", label: "Days since" },
    { key: "rs_slope_norm", label: "RS Slope" },
    { key: "trend_quality", label: "Trend" },
    { key: "score", label: "Score" },
    { key: "rs_trajectory", label: "RS Path (90d)" },
  ];

  if (opts.skipCloud) {
    const idx = headers.findIndex((h) => h.key === "cloud_color");
    if (idx >= 0) headers.splice(idx, 1);
  }

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  headers.forEach((h, idx) => {
    const th = document.createElement("th");
    th.textContent = h.label;
    th.dataset.col = idx;
    th.dataset.key = h.key;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    headers.forEach((h) => {
      const td = document.createElement("td");
      const v = row[h.key];
      if (h.key === "ticker") {
        td.innerHTML = `<a class="ticker" href="https://finviz.com/quote.ashx?t=${row.ticker}" target="_blank" rel="noopener">${row.ticker}</a>`;
      } else if (h.key === "rs_trajectory") {
        td.appendChild(makeRSTrajectorySVG(row.rs_trajectory));
      } else if (h.key === "cloud_color") {
        td.innerHTML = v === "green"
          ? `<span class="cloud cloud-green">▲ green</span>`
          : `<span class="cloud cloud-red">▼ red</span>`;
        td.dataset.value = v;
      } else if (h.key === "trend_quality") {
        td.textContent = v === 1 ? "✓" : "—";
        td.dataset.value = v ?? 0;
        if (v === 1) td.style.color = "var(--riser)";
      } else if (h.key === "days_since_crossover") {
        td.textContent = v === null || v === undefined ? "—" : v;
        td.dataset.value = v ?? "";
      } else if (h.key === "score") {
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(1);
        td.dataset.value = v ?? "";
        td.style.fontWeight = "600";
      } else if (h.key === "pct_from_52w_high") {
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + v.toFixed(1) + "%";
          td.className = v >= -3 ? "delta-positive" : v < -15 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else if (h.key === "rs_slope_norm") {
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + (v * 100).toFixed(2) + "%";
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else if (h.key === "rs_value") {
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(4);
        td.dataset.value = v ?? "";
      } else if (h.key === "price") {
        td.textContent = v === null || v === undefined ? "—" : "$" + v.toFixed(2);
        td.dataset.value = v ?? "";
      } else {
        td.textContent = v === null || v === undefined ? "—" : v;
        td.dataset.value = v ?? "";
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  attachSorting(table, headers);
  return table;
}

function renderRSInflection(data) {
  // Combine three lists into single export blob (с разделителна marker)
  const combined = [];
  (data.rs_inflection_bullish || []).forEach((r) => combined.push({ category: "Bullish", ...r }));
  (data.rs_inflection_bearish || []).forEach((r) => combined.push({ category: "Bearish", ...r }));
  (data.rs_new_highs || []).forEach((r) => combined.push({ category: "RS New High", ...r }));
  exportState["rs-inflection"] = combined;

  const renderInto = (hostId, rows, emptyMsg) => {
    const host = document.getElementById(hostId);
    if (!host) return;
    if (!rows || rows.length === 0) {
      host.innerHTML = `<div class="empty-state">${emptyMsg}</div>`;
      return;
    }
    host.replaceChildren(buildRSTable(rows));
  };

  renderInto("rs-bullish-host", data.rs_inflection_bullish, "Няма bullish crossovers в последните 5 дни.");
  renderInto("rs-bearish-host", data.rs_inflection_bearish, "Няма bearish crossovers в последните 5 дни.");
  renderInto("rs-newhighs-host", data.rs_new_highs, "Няма RS new highs (без crossover).");
}

function renderPowerConfluence(viewId, rows) {
  const host = document.querySelector(`#${viewId} .table-host`);
  exportState["power-confluence"] = rows || [];
  if (!rows || rows.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма Power Confluence сигнали днес — Stable Winner + fresh bullish RS crossover не съвпадат.</div>`;
    return;
  }

  const headers = [
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name" },
    { key: "sector", label: "Sector" },
    { key: "current_rank", label: "Sector Rank" },
    { key: "base_rank_6m", label: "Base 6m" },
    { key: "delta_1m", label: "Δ 1m" },
    { key: "mom_12_1_pct", label: "12-1 Mom %" },
    { key: "rs_value", label: "RS" },
    { key: "days_since_crossover", label: "Cross day" },
    { key: "rs_score", label: "RS Score" },
    { key: "combined_score", label: "Combined" },
    { key: "rs_trajectory", label: "RS Path (90d)" },
  ];

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  headers.forEach((h, idx) => {
    const th = document.createElement("th");
    th.textContent = h.label;
    th.dataset.col = idx;
    th.dataset.key = h.key;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    headers.forEach((h) => {
      const td = document.createElement("td");
      const v = row[h.key];
      if (h.key === "ticker") {
        td.innerHTML = `<a class="ticker" href="https://finviz.com/quote.ashx?t=${row.ticker}" target="_blank" rel="noopener">${row.ticker}</a>`;
      } else if (h.key === "rs_trajectory") {
        td.appendChild(makeRSTrajectorySVG(row.rs_trajectory));
      } else if (h.key === "delta_1m" || h.key === "mom_12_1_pct") {
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          const suffix = h.key === "mom_12_1_pct" ? "%" : "";
          td.textContent = (v > 0 ? "+" : "") + v.toFixed(1) + suffix;
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else if (h.key === "current_rank" || h.key === "base_rank_6m" || h.key === "rs_score") {
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(1);
        td.dataset.value = v ?? "";
      } else if (h.key === "combined_score") {
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(1);
        td.dataset.value = v ?? "";
        td.style.fontWeight = "600";
        td.style.color = "var(--accent)";
      } else if (h.key === "rs_value") {
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(4);
        td.dataset.value = v ?? "";
      } else if (h.key === "days_since_crossover") {
        td.textContent = v === null || v === undefined ? "—" : v === 0 ? "today" : v + "d ago";
        td.dataset.value = v ?? "";
      } else {
        td.textContent = v === null || v === undefined ? "—" : v;
        td.dataset.value = v ?? "";
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  host.replaceChildren(table);
  attachSorting(table, headers);
}

function renderSectorRS(viewId, rows) {
  const host = document.querySelector(`#${viewId} .table-host`);
  exportState["sector-rs"] = rows || [];
  if (!rows || rows.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма sector RS данни.</div>`;
    return;
  }

  const headers = [
    { key: "symbol", label: "ETF" },
    { key: "name", label: "Sector" },
    { key: "price", label: "Price" },
    { key: "rs_value", label: "RS" },
    { key: "cloud_color", label: "Cloud" },
    { key: "days_since_crossover", label: "Days since cross" },
    { key: "rs_slope_norm", label: "RS Slope" },
    { key: "rs_new_high", label: "52w RS High" },
    { key: "stocks_n_risers", label: "Bull Stocks" },
    { key: "stocks_n_decayers", label: "Bear Stocks" },
    { key: "score", label: "Score" },
    { key: "rs_trajectory", label: "RS Path (90d)" },
  ];

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  headers.forEach((h, idx) => {
    const th = document.createElement("th");
    th.textContent = h.label;
    th.dataset.col = idx;
    th.dataset.key = h.key;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    headers.forEach((h) => {
      const td = document.createElement("td");
      const v = row[h.key];
      if (h.key === "symbol") {
        td.innerHTML = `<a class="ticker" href="https://finviz.com/quote.ashx?t=${row.symbol}" target="_blank" rel="noopener">${row.symbol}</a>`;
      } else if (h.key === "rs_trajectory") {
        td.appendChild(makeRSTrajectorySVG(row.rs_trajectory));
      } else if (h.key === "cloud_color") {
        td.innerHTML = v === "green"
          ? `<span class="cloud cloud-green">▲ green</span>`
          : `<span class="cloud cloud-red">▼ red</span>`;
        td.dataset.value = v;
      } else if (h.key === "rs_new_high") {
        td.textContent = v ? "★" : "—";
        td.dataset.value = v ? 1 : 0;
        if (v) td.style.color = "var(--accent)";
      } else if (h.key === "rs_slope_norm") {
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + (v * 100).toFixed(2) + "%";
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else if (h.key === "stocks_n_risers") {
        td.textContent = v ?? 0;
        td.dataset.value = v ?? 0;
        td.style.color = "var(--riser)";
      } else if (h.key === "stocks_n_decayers") {
        td.textContent = v ?? 0;
        td.dataset.value = v ?? 0;
        td.style.color = "var(--decayer)";
      } else if (h.key === "days_since_crossover") {
        td.textContent = v === null || v === undefined ? "—" : v === 0 ? "today" : v + "d ago";
        td.dataset.value = v ?? "";
      } else if (h.key === "rs_value") {
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(4);
        td.dataset.value = v ?? "";
      } else if (h.key === "price") {
        td.textContent = v === null || v === undefined ? "—" : "$" + v.toFixed(2);
        td.dataset.value = v ?? "";
      } else if (h.key === "score") {
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(1);
        td.dataset.value = v ?? "";
        td.style.fontWeight = "600";
      } else {
        td.textContent = v === null || v === undefined ? "—" : v;
        td.dataset.value = v ?? "";
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  host.replaceChildren(table);
  attachSorting(table, headers);
}

function heatColor(value, maxAbs) {
  if (value === null || value === undefined) return "var(--bg-elev-2)";
  const t = Math.max(-1, Math.min(1, value / maxAbs));
  if (t > 0) {
    const alpha = 0.15 + t * 0.55;
    return `rgba(46, 160, 67, ${alpha.toFixed(2)})`;
  } else {
    const alpha = 0.15 + Math.abs(t) * 0.55;
    return `rgba(248, 81, 73, ${alpha.toFixed(2)})`;
  }
}

function formatDelta(v) {
  if (v === null || v === undefined) return "—";
  return (v > 0 ? "+" : "") + v.toFixed(2);
}

function attachSorting(table, headers) {
  const ths = table.querySelectorAll("th");
  ths.forEach((th, idx) => {
    applyColTip(th);
    th.addEventListener("click", () => {
      const desc = !th.classList.contains("sort-desc");
      ths.forEach((x) => x.classList.remove("sort-asc", "sort-desc"));
      th.classList.add(desc ? "sort-desc" : "sort-asc");
      sortTableByCol(table, idx, desc, headers[idx].key);
    });
  });
}

function sortTableByCol(table, colIdx, desc, key) {
  const tbody = table.querySelector("tbody");
  const rows = Array.from(tbody.querySelectorAll("tr"));
  rows.sort((a, b) => {
    const va = a.children[colIdx]?.dataset.value ?? a.children[colIdx]?.textContent ?? "";
    const vb = b.children[colIdx]?.dataset.value ?? b.children[colIdx]?.textContent ?? "";
    const na = parseFloat(va);
    const nb = parseFloat(vb);
    if (!isNaN(na) && !isNaN(nb)) {
      return desc ? nb - na : na - nb;
    }
    return desc ? vb.localeCompare(va) : va.localeCompare(vb);
  });
  rows.forEach((r) => tbody.appendChild(r));
}

// Map от tab id към human-readable label + sheet column conf за Excel export.
// За всеки tab дефинираме кои полета да се пишат и под какви имена.
const EXPORT_CONFIG = {
  "attention": {
    label: "Attention Layer",
    columns: [
      ["cohort", "Cohort"], ["ticker", "Ticker"], ["name", "Name"],
      ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["base_rank_6m", "Rank 6m"], ["sector_avg_rank", "Sector Avg Rank"],
      ["delta_1m", "Δ 1m"], ["sw_streak", "SW Streak (мес)"],
      ["is_lonely", "Lonely"], ["is_premium", "Premium"],
      ["is_cs_excluded", "Consumer Staples"],
    ],
  },
  "stable-winners-1m": {
    label: "Stable Winners 1m",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["current_rank", "Sector Rank"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "stable-winners-3m": {
    label: "Stable Winners 3m",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["current_rank", "Sector Rank"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "quality-dip-1m": {
    label: "Quality Dip 1m",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["current_rank", "Sector Rank"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "quality-dip-3m": {
    label: "Quality Dip 3m",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["current_rank", "Sector Rank"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "faded-bounces": {
    label: "Faded Bounces",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["current_rank", "Sector Rank"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "current-strength": {
    label: "Current Strength",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["mom_12_1_pct", "12-1 Mom %"], ["abs_strength", "Abs %ile"], ["current_rank", "Sector Rank"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
    ],
  },
  "rank-all": {
    label: "Rank All",
    columns: [
      ["rank_position", "#"], ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["score", "Score"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "screener": {
    label: "Universe Screener",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["industry", "Sub-Industry"],
      ["market_cap_m", "Mcap (M$)"], ["size_bucket", "Size"],
      ["ret_1m", "1M %"], ["ret_3m", "3M %"], ["ret_6m", "6M %"], ["ret_ytd", "YTD %"],
      ["ret_1y", "1Y %"], ["ret_3y", "3Y %"], ["ret_5y", "5Y %"],
      ["vol_1y", "Vol 1Y %"], ["sharpe_1y", "Sharpe 1Y"], ["sharpe_3y", "Sharpe 3Y"],
      ["maxdd_1y", "MaxDD 1Y %"], ["maxdd_3y", "MaxDD 3Y %"], ["maxdd_5y", "MaxDD 5Y %"],
      ["calmar_3y", "Calmar 3Y"], ["dist_52w_high", "from 52w-H %"],
      ["days_since_52w_high", "Days since H"], ["beta_1y", "Beta 1Y"],
    ],
  },
  "rs-inflection": {
    label: "RS Inflection",
    columns: [
      ["category", "List"], ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["price", "Price"], ["pct_from_52w_high", "from 52w-H %"],
      ["rs_value", "RS"], ["cloud_color", "Cloud"],
      ["bullish_crossover", "Bull Cross"], ["bearish_crossover", "Bear Cross"],
      ["days_since_crossover", "Days Since Cross"],
      ["rs_new_high", "RS 52w High"], ["price_new_high", "Price 52w High"], ["confluence", "Confluence"],
      ["rs_slope_norm", "RS Slope"], ["trend_quality", "Trend OK"], ["score", "Score"],
    ],
  },
  "power-confluence": {
    label: "Power Confluence",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["current_rank", "Sector Rank"], ["base_rank_6m", "Base 6m"],
      ["delta_1m", "Δ 1m"], ["mom_12_1_pct", "12-1 Mom %"],
      ["rs_value", "RS"], ["days_since_crossover", "Days Since Cross"],
      ["rs_score", "RS Score"], ["combined_score", "Combined Score"],
    ],
  },
  "sector-rs": {
    label: "Sector RS",
    columns: [
      ["symbol", "ETF"], ["name", "Sector Name"], ["gics_sector", "GICS Sector"],
      ["price", "Price"], ["rs_value", "RS"], ["cloud_color", "Cloud"],
      ["bullish_crossover", "Bull Cross"], ["bearish_crossover", "Bear Cross"],
      ["days_since_crossover", "Days Since Cross"],
      ["rs_new_high", "RS 52w High"], ["rs_slope_norm", "RS Slope"], ["score", "Score"],
      ["stocks_n_risers", "Stock Risers"], ["stocks_n_decayers", "Stock Decayers"], ["stocks_n_total", "Stocks N"],
    ],
  },
};

function exportTabToExcel(tabId) {
  if (typeof XLSX === "undefined") {
    alert("XLSX library още не е заредена. Изчакай един момент и опитай пак.");
    return;
  }
  const config = EXPORT_CONFIG[tabId];
  if (!config) return;

  const rows = exportState[tabId] || [];
  if (rows.length === 0) {
    alert("Няма данни за export.");
    return;
  }

  const sheetData = rows.map((row) => {
    const out = {};
    config.columns.forEach(([key, label]) => {
      const v = row[key];
      out[label] = v === null || v === undefined ? "" : v;
    });
    return out;
  });

  const ws = XLSX.utils.json_to_sheet(sheetData);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, config.label.substring(0, 31));

  const dateStr = (exportState.asOf || "data").replace(/[^0-9-]/g, "");
  const safeName = tabId.replace(/-/g, "_");
  const filename = `rotation_radar_${safeName}_${dateStr}.xlsx`;
  XLSX.writeFile(wb, filename);
}

function setupExportButtons() {
  document.querySelectorAll(".export-btn").forEach((btn) => {
    const tabId = btn.dataset.export;
    btn.addEventListener("click", () => exportTabToExcel(tabId));
  });
}

function setupTabs() {
  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      const target = btn.dataset.tab;
      document.querySelectorAll(".tab").forEach((b) => b.classList.toggle("active", b === btn));
      document.querySelectorAll(".view").forEach((v) => v.classList.toggle("active", v.id === target));
      // Full-width body (max-width: none) за tabs с пълни таблици — повече място
      // за хоризонтална таблица. Sticky thead работи защото table-host е
      // overflow:auto с фиксирана височина (75vh).
      const fullTableTabs = ["screener", "rank-all"];
      document.body.classList.toggle("screener-mode", fullTableTabs.includes(target));
    });
  });
}
