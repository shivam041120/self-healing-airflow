const STATUS_LABEL = {
  fixed: "Fixed",
  escalated: "Escalated",
  unresolved: "Unresolved",
  no_action: "No action needed",
  crashed: "Crashed",
};

const NODE_GROUP = {
  webhook_received: "webhook_received",
  analyze_start: "analyze",
  analyze: "analyze",
  analyze_error: "analyze",
  diagnose: "diagnose",
  action: "action",
  verify: "verify",
  graph_crashed: "crashed",
};

const GROUP_ORDER = ["webhook_received", "analyze", "diagnose", "action", "verify"];

let currentIncidents = [];
let pipelineIdleTimer = null;

function qs(id) { return document.getElementById(id); }

function fmtTime(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  return d.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

function buildQuery() {
  const params = new URLSearchParams();
  const dag = qs("filter-dag")?.value;
  const task = qs("filter-task")?.value;
  const status = qs("filter-status")?.value;
  const range = qs("filter-range")?.value;
  if (dag) params.set("dag_id", dag);
  if (task) params.set("task_id", task);
  if (status) params.set("status", status);
  if (range) params.set("since_hours", range);
  return params.toString();
}

async function loadIncidents() {
  const res = await fetch(`/api/incidents?${buildQuery()}`);
  const data = await res.json();
  currentIncidents = data.incidents;
  renderStatCards(data.summary);
  renderTable(data.incidents);
  populateFilterOptions(data.incidents);
}

async function loadStats() {
  const range = qs("filter-range")?.value || 14 * 24;
  const res = await fetch(`/api/stats?since_hours=${range}`);
  const data = await res.json();
  renderTrendChart(data.trend);
}

// --- Heatmap (self-contained: builds its own DOM if the page doesn't
// already have a #heatmap-grid container, so it can never crash init()
// just because a given layout hasn't been wired up for it yet) ---
async function loadHeatmap() {
  let grid = qs("heatmap-grid");
  if (!grid) {
    const card = document.createElement("section");
    card.className = "heatmap-card";
    card.innerHTML = `
      <div class="heatmap-header">
        <span>Incident density — last 90 days</span>
        <div class="heatmap-legend">
          <span>fewer</span>
          <span class="legend-swatch" style="background:#e5e7eb;"></span>
          <span class="legend-swatch" style="background:#a7f3d0;"></span>
          <span class="legend-swatch" style="background:#059669;"></span>
          <span class="legend-swatch" style="background:#dc2626;"></span>
          <span>more / worse</span>
        </div>
      </div>
      <div class="heatmap-grid" id="heatmap-grid"></div>
    `;
    const anchor = document.querySelector(".pipeline-card") || document.querySelector("main") || document.body;
    anchor.insertAdjacentElement("afterend", card);
    grid = qs("heatmap-grid");
  }
  const res = await fetch(`/api/heatmap?days=90`);
  const data = await res.json();
  renderHeatmap(data.days);
}

function heatmapColor(day) {
  if (day.total === 0) return "#e5e7eb";
  const ratio = day.bad_ratio;
  const color = ratio > 0.5 ? "220,38,38" : ratio > 0 ? "217,119,6" : "5,150,105";
  const intensity = Math.min(1, 0.35 + day.total * 0.15);
  return `rgba(${color},${intensity})`;
}

function renderHeatmap(days) {
  const grid = qs("heatmap-grid");
  if (!grid) return;
  grid.innerHTML = days
    .map((d) => {
      const label = d.total === 0
        ? `${d.date}: no incidents`
        : `${d.date}: ${d.total} incident${d.total === 1 ? "" : "s"}, ${Math.round(d.bad_ratio * 100)}% needed escalation`;
      return `<div class="heatmap-cell" title="${label}" style="background:${heatmapColor(d)};"></div>`;
    })
    .join("");
}

// --- Theme toggle: uses the real #icon-sun/#icon-moon SVGs already in
// the button, rather than overwriting them with text (which is what the
// previous version of this function did — it worked, but silently
// destroyed the icons the first time you clicked it). ---
function initTheme() {
  const btn = qs("theme-toggle");
  if (!btn) return;
  const sun = qs("icon-sun");
  const moon = qs("icon-moon");

  function applyTheme(isDark) {
    if (isDark) {
      document.documentElement.setAttribute("data-theme", "dark");
      if (sun) sun.style.display = "none";
      if (moon) moon.style.display = "";
    } else {
      document.documentElement.removeAttribute("data-theme");
      if (sun) sun.style.display = "";
      if (moon) moon.style.display = "none";
    }
  }

  applyTheme(localStorage.getItem("sh-theme") === "dark");

  btn.addEventListener("click", () => {
    const isDark = document.documentElement.getAttribute("data-theme") === "dark";
    applyTheme(!isDark);
    localStorage.setItem("sh-theme", !isDark ? "dark" : "light");
    if (trendChart) trendChart.update();
  });
}

// --- Tab switching: the actual missing piece. The HTML already has
// .tab-btn buttons with data-tab="live"/"history" and matching
// #panel-live/#panel-history sections using the `hidden` attribute —
// nothing was ever listening for clicks to toggle it. ---
function initTabs() {
  const buttons = document.querySelectorAll(".tab-btn");
  if (!buttons.length) return;

  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const target = btn.dataset.tab;

      buttons.forEach((b) => b.setAttribute("aria-selected", b === btn ? "true" : "false"));

      document.querySelectorAll(".tab-panel").forEach((panel) => {
        panel.hidden = panel.id !== `panel-${target}`;
      });

      const title = qs("page-title");
      if (title) title.textContent = target === "live" ? "Live monitoring" : "Past actions";

      // Past actions' data was fetched at page load, but re-fetch on
      // first visit to that tab too, in case it loaded before Postgres
      // was ready or filters changed while the tab was hidden.
      if (target === "history") {
        safeRun(loadIncidents, "loadIncidents");
        safeRun(loadStats, "loadStats");
      }
    });
  });
}

// --- Mobile sidebar toggle: same class of gap as the tabs — the
// hamburger/close buttons and backdrop exist in the HTML with no
// listener wired to them yet. ---
function initSidebar() {
  const menuBtn = qs("menu-btn");
  const closeBtn = qs("sidebar-close");
  const backdrop = qs("sidebar-backdrop");
  const sidebar = qs("sidebar");
  if (!sidebar) return;

  const open = () => { sidebar.classList.add("open"); backdrop?.classList.add("open"); };
  const close = () => { sidebar.classList.remove("open"); backdrop?.classList.remove("open"); };

  menuBtn?.addEventListener("click", open);
  closeBtn?.addEventListener("click", close);
  backdrop?.addEventListener("click", close);
}

function populateFilterOptions(incidents) {
  const dagSel = qs("filter-dag");
  const taskSel = qs("filter-task");
  if (!dagSel || !taskSel) return;
  const keep = (sel, values, allLabel) => {
    const current = sel.value;
    const seen = new Set();
    sel.innerHTML = `<option value="">${allLabel}</option>`;
    values.forEach((v) => {
      if (v && !seen.has(v)) {
        seen.add(v);
        const opt = document.createElement("option");
        opt.value = v;
        opt.textContent = v;
        sel.appendChild(opt);
      }
    });
    if (seen.has(current)) sel.value = current;
  };
  if (!dagSel.dataset.populated) {
    keep(dagSel, incidents.map((i) => i.dag_id), "All DAGs");
    keep(taskSel, incidents.map((i) => i.task_id), "All tasks");
    dagSel.dataset.populated = "1";
  }
}

function renderStatCards(summary) {
  const el = qs("stat-cards");
  if (!el) return;
  const s = summary.by_status;
  const cards = [
    { label: "Incidents", value: summary.total, cls: "" },
    { label: "Fixed", value: s["Fixed"] || 0, cls: "green" },
    { label: "Escalated", value: s["Escalated"] || 0, cls: "amber" },
    { label: "Unresolved", value: s["Unresolved"] || 0, cls: "red" },
    { label: "No action needed", value: s["No action needed"] || 0, cls: "" },
  ];
  el.innerHTML = cards
    .map(
      (c) => `
    <div class="stat-card">
      <div class="value" style="${c.cls ? `color:var(--${c.cls})` : ""}">${c.value}</div>
      <div class="label">${c.label}</div>
    </div>`
    )
    .join("");
}

let trendChart = null;
function renderTrendChart(trend) {
  const ctx = qs("trend-chart");
  if (!ctx) return;
  const labels = trend.map((t) => t.date.slice(5));
  const datasets = [
    { key: "Fixed", color: "#059669" },
    { key: "Escalated", color: "#d97706" },
    { key: "Unresolved", color: "#dc2626" },
    { key: "No action needed", color: "#9ca3af" },
    { key: "Crashed", color: "#7f1d1d" },
  ].map((d) => ({
    label: d.key,
    data: trend.map((t) => t[d.key] || 0),
    backgroundColor: d.color,
    stack: "s",
  }));

  if (trendChart) {
    trendChart.data.labels = labels;
    trendChart.data.datasets = datasets;
    trendChart.update();
    return;
  }
  if (typeof Chart === "undefined") {
    console.error("Chart.js not loaded — trend chart skipped.");
    return;
  }
  trendChart = new Chart(ctx, {
    type: "bar",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: true, position: "bottom", labels: { boxWidth: 10, font: { size: 11 } } } },
      scales: {
        x: { stacked: true, grid: { display: false } },
        y: { stacked: true, ticks: { precision: 0 }, grid: { color: "#eee" } },
      },
    },
  });
}

function timelineChips(timeline) {
  return timeline
    .map((t) => `<span class="chip chip-${t.node}">${t.label}</span>`)
    .join("");
}

function renderTable(incidents) {
  const body = qs("incident-rows");
  if (!body) return;
  if (!incidents.length) {
    body.innerHTML = `<tr><td colspan="7" class="empty-state">No agent activity for this filter yet.</td></tr>`;
    return;
  }
  body.innerHTML = incidents
    .map(
      (i) => `
    <tr data-key="${i.dag_id}|${i.task_id}|${i.dag_run_id}">
      <td class="dag-task"><div class="dag">${i.dag_id}</div><div class="task">${i.task_id}</div></td>
      <td>${i.dag_run_id}</td>
      <td><span class="pill pill-${i.status}">${i.status_label}</span></td>
      <td><div class="timeline-chips">${timelineChips(i.timeline)}</div></td>
      <td>${i.attempts}</td>
      <td>${fmtTime(i.ended_at)}</td>
      <td><button class="row-view-btn" onclick="openDrawer('${i.dag_id}','${i.task_id}','${i.dag_run_id}')">view</button></td>
    </tr>`
    )
    .join("");
}

function upsertIncidentRow(incident) {
  const idx = currentIncidents.findIndex(
    (i) => i.dag_id === incident.dag_id && i.task_id === incident.task_id && i.dag_run_id === incident.dag_run_id
  );
  if (idx >= 0) currentIncidents[idx] = incident;
  else currentIncidents.unshift(incident);
  renderTable(currentIncidents);
  renderStatCards({
    total: currentIncidents.length,
    by_status: currentIncidents.reduce((acc, i) => {
      acc[i.status_label] = (acc[i.status_label] || 0) + 1;
      return acc;
    }, {}),
  });
  const row = document.querySelector(`tr[data-key="${incident.dag_id}|${incident.task_id}|${incident.dag_run_id}"]`);
  if (row) {
    row.classList.add("flash-update");
    setTimeout(() => row.classList.remove("flash-update"), 1400);
  }
}

async function openDrawer(dagId, taskId, dagRunId) {
  const res = await fetch(`/api/incidents/${encodeURIComponent(dagId)}/${encodeURIComponent(taskId)}/${encodeURIComponent(dagRunId)}`);
  const incident = await res.json();
  if (incident.error) return;

  const titleEl = qs("drawer-title");
  const bodyEl = qs("drawer-body");
  if (!titleEl || !bodyEl) return;

  titleEl.textContent = `${incident.dag_id} / ${incident.task_id}`;
  bodyEl.innerHTML = `
    <div style="margin-bottom:14px;">
      <span class="pill pill-${incident.status}">${incident.status_label}</span>
      <span style="color:var(--text-muted);font-size:12.5px;margin-left:8px;">run ${incident.dag_run_id}</span>
    </div>
    ${
      incident.fix_kind
        ? `<div style="margin-bottom:14px;padding:10px 12px;border-left:3px solid ${incident.fix_kind === "auto_fix" ? "var(--green)" : "var(--amber)"};background:${incident.fix_kind === "auto_fix" ? "var(--green-bg)" : "var(--amber-bg)"};border-radius:6px;font-size:13px;">
            <b>${incident.fix_kind === "auto_fix" ? "Auto-fixed" : "Suggested fix"}:</b> ${incident.fix_message || ""}
          </div>`
        : ""
    }
    ${incident.rows
      .map(
        (r) => `
      <div class="detail-node n-${r.node}">
        <div class="detail-node-head">
          <b>${r.node}</b>
          <span class="detail-node-time">${fmtTime(r.created_at)}</span>
        </div>
        <div style="font-size:12.5px;color:var(--text-muted);">
          ${r.action_decision ? `action: <b>${r.action_decision}</b>` : ""}
          ${r.verification_result !== null && r.verification_result !== undefined ? ` · verified: <b>${r.verification_result ? "yes" : "no"}</b>` : ""}
          ${r.attempt !== null && r.attempt !== undefined ? ` · attempt ${r.attempt}` : ""}
        </div>
        <div class="detail-node-body">
          ${r.reasoning ? `<pre>${escapeHtml(r.reasoning)}</pre>` : ""}
        </div>
      </div>`
      )
      .join("")}
  `;
  qs("drawer")?.classList.add("open");
  qs("drawer-backdrop")?.classList.add("open");
}

function closeDrawer() {
  qs("drawer")?.classList.remove("open");
  qs("drawer-backdrop")?.classList.remove("open");
}

function escapeHtml(s) {
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
}

function showToast(msg) {
  const el = qs("toast");
  if (!el) return;
  el.textContent = msg;
  el.classList.add("show");
  clearTimeout(el._timer);
  el._timer = setTimeout(() => el.classList.remove("show"), 3200);
}

function resetPipeline() {
  document.querySelectorAll(".pipeline-node").forEach((n) => n.classList.remove("active", "done", "success", "warning", "error"));
  document.querySelectorAll(".pipeline-arrow").forEach((a) => a.classList.remove("lit"));
  const caption = qs("pipeline-caption");
  if (caption) {
    caption.textContent = "No incident in flight — the rail lights up the moment a task fails.";
    caption.classList.remove("active");
  }
}

function animatePipeline(incident) {
  clearTimeout(pipelineIdleTimer);
  const lastRow = incident.rows[incident.rows.length - 1];
  const group = NODE_GROUP[lastRow.node] || "webhook_received";

  const caption = qs("pipeline-caption");
  if (caption) {
    caption.textContent = `${incident.dag_id} / ${incident.task_id} (run ${incident.dag_run_id})`;
    caption.classList.add("active");
  }

  document.querySelectorAll(".pipeline-node").forEach((n) => n.classList.remove("active", "done", "success", "warning", "error"));
  document.querySelectorAll(".pipeline-arrow").forEach((a) => a.classList.remove("lit"));

  if (group === "crashed") {
    document.querySelectorAll(".pipeline-node").forEach((n) => n.classList.add("error"));
    scheduleIdleReset();
    return;
  }

  const idx = GROUP_ORDER.indexOf(group);
  GROUP_ORDER.forEach((g, i) => {
    const node = document.querySelector(`.pipeline-node[data-group="${g}"]`);
    if (!node) return;
    if (i < idx) node.classList.add("done");
  });
  document.querySelectorAll(".pipeline-arrow").forEach((a, i) => {
    if (i < idx) a.classList.add("lit");
  });

  const activeNode = document.querySelector(`.pipeline-node[data-group="${group}"]`);
  if (activeNode) {
    if (group === "verify") {
      activeNode.classList.add(incident.status === "fixed" ? "success" : "error");
      document.querySelectorAll(".pipeline-arrow").forEach((a) => a.classList.add("lit"));
    } else if (lastRow.node === "analyze_error") {
      activeNode.classList.add("warning");
    } else {
      activeNode.classList.add("active");
    }
  }

  scheduleIdleReset();
}

function scheduleIdleReset() {
  clearTimeout(pipelineIdleTimer);
  pipelineIdleTimer = setTimeout(resetPipeline, 9000);
}

// The agent can write many decision rows per incident (each node, each
// retry loop), and each write fires an SSE event. Reacting to every event
// individually means one busy incident — or several failing at once —
// turns into a flood of detail+stats fetches that can bury a single
// uvicorn worker. Instead, coalesce bursts: track which incidents changed
// and how many events came in, then do at most one refresh pass per
// DEBOUNCE_MS, covering every incident that changed in that window.
const DEBOUNCE_MS = 400;
let pendingKeys = new Map(); // "dag_id/task_id/dag_run_id" -> key
let pendingEventCount = 0;
let debounceTimer = null;

function scheduleDebouncedRefresh() {
  if (debounceTimer) return;
  debounceTimer = setTimeout(async () => {
    const keys = Array.from(pendingKeys.values());
    const eventCount = pendingEventCount;
    pendingKeys = new Map();
    pendingEventCount = 0;
    debounceTimer = null;

    for (const key of keys) {
      try {
        const res = await fetch(
          `/api/incidents/${encodeURIComponent(key.dag_id)}/${encodeURIComponent(key.task_id)}/${encodeURIComponent(key.dag_run_id)}`
        );
        const incident = await res.json();
        if (incident.error) continue;

        animatePipeline(incident);
        upsertIncidentRow(incident);

        const lastRow = incident.rows[incident.rows.length - 1];
        showToast(`${lastRow.node}: ${lastRow.action_decision || lastRow.node} — ${incident.dag_id}/${incident.task_id}`);
      } catch (e) {
        console.error("[stream] incident refresh failed", e);
      }
    }

    // One stats refresh per batch, not one per event — this is the
    // expensive query (up to 5000 rows, regrouped), so it's the one most
    // worth collapsing.
    if (eventCount > 0) {
      loadStats().catch((e) => console.error("loadStats failed", e));
    }
  }, DEBOUNCE_MS);
}

function connectStream() {
  const indicator = qs("live-indicator");
  const source = new EventSource("/api/stream");

  source.onopen = () => indicator?.classList.remove("offline");
  source.onerror = () => indicator?.classList.add("offline");

  source.addEventListener("incident_update", (e) => {
    let key;
    try {
      key = JSON.parse(e.data);
    } catch {
      return;
    }
    pendingKeys.set(`${key.dag_id}/${key.task_id}/${key.dag_run_id}`, key);
    pendingEventCount += 1;
    scheduleDebouncedRefresh();
  });
}

// Runs each init step independently — one failing (missing element,
// failed fetch, whatever) no longer takes the rest of the app down with
// it. This is the actual fix for "page loads but no data ever appears."
function safeRun(fn, label) {
  try {
    const result = fn();
    if (result && typeof result.catch === "function") {
      result.catch((e) => console.error(`[init] ${label} failed:`, e));
    }
  } catch (e) {
    console.error(`[init] ${label} failed:`, e);
  }
}

// Small signature touch: the rail already sits in 3D space (see
// styles.css .pipeline-rail), so a gentle pointer-driven tilt makes that
// depth readable instead of static — the card leans toward the cursor
// like you're looking at a physical console. Skipped for touch devices
// (no meaningful pointer position) and reduced-motion users.
function initPipelineTilt() {
  const flow = qs("pipeline-flow");
  const rail = document.querySelector(".pipeline-rail");
  if (!flow || !rail) return;
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  if (window.matchMedia("(pointer: coarse)").matches) return;

  const BASE_TILT = 10; // matches the resting rotateX in CSS
  flow.addEventListener("mousemove", (e) => {
    const rect = flow.getBoundingClientRect();
    const px = (e.clientX - rect.left) / rect.width - 0.5; // -0.5..0.5
    const py = (e.clientY - rect.top) / rect.height - 0.5;
    rail.style.transform = `rotateX(${BASE_TILT - py * 8}deg) rotateY(${px * 6}deg)`;
  });
  flow.addEventListener("mouseleave", () => {
    rail.style.transform = `rotateX(${BASE_TILT}deg)`;
  });
}

function init() {
  safeRun(initTheme, "initTheme");
  safeRun(initTabs, "initTabs");
  safeRun(initSidebar, "initSidebar");
  safeRun(initPipelineTilt, "initPipelineTilt");
  safeRun(loadIncidents, "loadIncidents");
  safeRun(loadHeatmap, "loadHeatmap");
  safeRun(connectStream, "connectStream");

  ["filter-dag", "filter-task", "filter-status", "filter-range"].forEach((id) => {
    qs(id)?.addEventListener("change", () => {
      safeRun(loadIncidents, "loadIncidents");
      safeRun(loadStats, "loadStats");
    });
  });
  qs("refresh-btn")?.addEventListener("click", () => {
    safeRun(loadIncidents, "loadIncidents");
    safeRun(loadStats, "loadStats");
  });
  qs("drawer-close")?.addEventListener("click", closeDrawer);
  qs("drawer-backdrop")?.addEventListener("click", closeDrawer);
}

document.addEventListener("DOMContentLoaded", init);