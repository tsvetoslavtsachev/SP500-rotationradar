// SP500 Rotation Radar — UI rendering logic.
// Чете data.json и рендерира 7 различни views.

(async () => {
  const data = await fetchData();
  if (!data) return;

  renderMetadata(data.metadata);
  renderWatchlist("stable-winners-1m", data.stable_winners_1m, "1m");
  renderWatchlist("stable-winners-3m", data.stable_winners_3m, "3m");
  renderWatchlist("quality-dip-1m", data.quality_dip_1m, "1m");
  renderWatchlist("quality-dip-3m", data.quality_dip_3m, "3m");
  renderWatchlist("faded-bounces", data.faded_bounces_1m, "1m");
  renderCurrentStrength("current-strength", data.current_strength);
  renderScreener("screener", data.screener);
  renderHeatmap("sectors", data.sector_rotation);
  renderSubIndustryTable("sub-industries", data.sub_industry_rotation);
  setupTabs();
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

function renderWatchlist(viewId, rows, deltaWindow) {
  const host = document.querySelector(`#${viewId} .table-host`);
  if (!rows || rows.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма kandidaти в този quadrant сега.</div>`;
    return;
  }

  const showBoth = deltaWindow === "both";
  const headers = [
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name" },
    { key: "sector", label: "Sector" },
    { key: "current_rank", label: "Sector Rank" },
    { key: "abs_strength", label: "Abs %ile" },
    { key: "mom_12_1_pct", label: "12-1 Mom %" },
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
        td.innerHTML = `<a class="ticker" href="https://finance.yahoo.com/quote/${row.ticker}" target="_blank" rel="noopener">${row.ticker}</a>`;
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
        td.innerHTML = `<a class="ticker" href="https://finance.yahoo.com/quote/${row.ticker}" target="_blank" rel="noopener">${row.ticker}</a>`;
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
    <div>Sector</div>
    <div style="text-align:center">Δ 1m</div>
    <div style="text-align:center">Δ 3m</div>
    <div style="text-align:center">Total</div>
    <div style="text-align:center">Risers</div>
    <div style="text-align:center">Decayers</div>
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

  const sectors = Array.from(new Set(stocks.map((s) => s.sector).filter(Boolean))).sort();
  sectors.forEach((s) => {
    const opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    sectorSelect.appendChild(opt);
  });

  const frozenHeaders = [
    { key: "ticker", label: "Ticker", cls: "col-ticker" },
    { key: "name", label: "Name", cls: "col-name" },
  ];
  const scrollHeaders = [
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
  const allHeaders = [...frozenHeaders, ...scrollHeaders];

  function fmtCell(td, key, value) {
    if (value === null || value === undefined) {
      td.textContent = "—";
      td.dataset.value = "";
      return;
    }
    if (key === "ticker") {
      td.innerHTML = `<a class="ticker" href="https://finance.yahoo.com/quote/${value}" target="_blank" rel="noopener">${value}</a>`;
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

  function buildPane(headers, rows) {
    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    headers.forEach((h, idx) => {
      const th = document.createElement("th");
      th.textContent = h.label;
      th.dataset.col = idx;
      th.dataset.key = h.key;
      if (h.cls) th.classList.add(h.cls);
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
    return table;
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

    // Прилагаме текущия sort state ако има
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
    renderSplit(filtered);
  }

  let currentSort = { key: null, desc: true };

  function attachSplitSorting(frozenTable, scrollTable) {
    const allTh = [
      ...frozenTable.querySelectorAll("thead th"),
      ...scrollTable.querySelectorAll("thead th"),
    ];
    allTh.forEach((th) => {
      th.style.cursor = "pointer";
      th.addEventListener("click", () => {
        const key = th.dataset.key;
        currentSort.desc = !(currentSort.key === key && currentSort.desc);
        currentSort.key = key;
        allTh.forEach((x) => x.classList.remove("sort-asc", "sort-desc"));
        th.classList.add(currentSort.desc ? "sort-desc" : "sort-asc");
        applyFilters();
      });
    });
  }

  function renderSplit(rows) {
    host.innerHTML = "";

    const wrapper = document.createElement("div");
    wrapper.className = "screener-split";

    const frozenPane = document.createElement("div");
    frozenPane.className = "screener-frozen-pane";
    const scrollPane = document.createElement("div");
    scrollPane.className = "screener-scroll-pane";

    const frozenTable = buildPane(frozenHeaders, rows);
    const scrollTable = buildPane(scrollHeaders, rows);
    frozenPane.appendChild(frozenTable);
    scrollPane.appendChild(scrollTable);

    wrapper.appendChild(frozenPane);
    wrapper.appendChild(scrollPane);
    host.appendChild(wrapper);

    // Sync vertical scroll: scroll pane drives, frozen pane mirrors
    let syncing = false;
    scrollPane.addEventListener("scroll", () => {
      if (syncing) return;
      syncing = true;
      frozenPane.scrollTop = scrollPane.scrollTop;
      requestAnimationFrame(() => { syncing = false; });
    });
    frozenPane.addEventListener("wheel", (e) => {
      // Forward wheel events on frozen pane to scroll pane
      scrollPane.scrollTop += e.deltaY;
      e.preventDefault();
    }, { passive: false });

    attachSplitSorting(frozenTable, scrollTable);

    // Restore sort indicator
    if (currentSort.key) {
      const all = [
        ...frozenTable.querySelectorAll("thead th"),
        ...scrollTable.querySelectorAll("thead th"),
      ];
      all.forEach((th) => {
        if (th.dataset.key === currentSort.key) {
          th.classList.add(currentSort.desc ? "sort-desc" : "sort-asc");
        }
      });
    }
  }

  sectorSelect.addEventListener("change", applyFilters);
  sizeSelect.addEventListener("change", applyFilters);
  searchInput.addEventListener("input", applyFilters);

  applyFilters();
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

function setupTabs() {
  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      const target = btn.dataset.tab;
      document.querySelectorAll(".tab").forEach((b) => b.classList.toggle("active", b === btn));
      document.querySelectorAll(".view").forEach((v) => v.classList.toggle("active", v.id === target));
      // Screener tab gets fullscreen mode (sticky headers + frozen left cols rely on
      // a properly sized scroll container; without screener-mode the page-level
      // scroll takes over and sticky positioning becomes inconsistent).
      document.body.classList.toggle("screener-mode", target === "screener");
    });
  });
}
