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
let activeTab = "live";
let historyUnread = 0;
let historyLoadedOnce = false;
const MAX_FEED_LINES = 250;

function qs(id) { return document.getElementById(id); }

function fmtTime(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  return d.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

function buildQuery() {
  const params = new URLSearchParams();
  const dag = qs("filter-dag").value;
  const task = qs("filter-task").value;
  const status = qs("filter-status").value;
  const range = qs("filter-range").value;
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
  const range = qs("filter-range").value || 14 * 24;
  const res = await fetch(`/api/stats?since_hours=${range}`);
  const data = await res.json();
  renderTrendChart(data.trend);
}

function populateFilterOptions(incidents) {
  const dagSel = qs("filter-dag");
  const taskSel = qs("filter-task");
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
  renderHealthRing(summary);
}

const RING_CIRCUMFERENCE = 2 * Math.PI * 42;

function renderHealthRing(summary) {
  const s = summary.by_status;
  const fixed = s["Fixed"] || 0;
  const decided = fixed + (s["Escalated"] || 0) + (s["Unresolved"] || 0);
  const pct = decided > 0 ? Math.round((fixed / decided) * 100) : null;

  const arc = qs("health-ring-arc");
  const valueEl = qs("health-ring-value");
  if (pct === null) {
    arc.style.strokeDashoffset = RING_CIRCUMFERENCE;
    valueEl.textContent = "—";
    return;
  }
  const offset = RING_CIRCUMFERENCE * (1 - pct / 100);
  arc.style.strokeDasharray = RING_CIRCUMFERENCE;
  arc.style.strokeDashoffset = offset;
  arc.style.stroke = pct >= 66 ? "var(--green)" : pct >= 33 ? "var(--amber)" : "var(--red)";
  valueEl.textContent = `${pct}%`;
}

function chartThemeColors() {
  const dark = document.documentElement.getAttribute("data-theme") === "dark";
  return {
    grid: dark ? "#2c3038" : "#eee",
    text: dark ? "#9aa0aa" : "#6b7280",
  };
}

let trendChart = null;
function renderTrendChart(trend) {
  const ctx = qs("trend-chart");
  const labels = trend.map((t) => t.date.slice(5));
  const colors = chartThemeColors();
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
    trendChart.options.plugins.legend.labels.color = colors.text;
    trendChart.options.scales.x.ticks.color = colors.text;
    trendChart.options.scales.y.ticks.color = colors.text;
    trendChart.options.scales.y.grid.color = colors.grid;
    trendChart.update();
    return;
  }
  trendChart = new Chart(ctx, {
    type: "bar",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true, position: "bottom", labels: { boxWidth: 10, font: { size: 11 }, color: colors.text } },
      },
      scales: {
        x: { stacked: true, grid: { display: false }, ticks: { color: colors.text } },
        y: { stacked: true, ticks: { precision: 0, color: colors.text }, grid: { color: colors.grid } },
      },
    },
  });
}

const TAB_TITLES = { live: "Live monitoring", history: "Past actions" };

function switchTab(tab, { pushHash = true } = {}) {
  if (tab !== "live" && tab !== "history") tab = "live";
  activeTab = tab;

  qs("panel-live").hidden = tab !== "live";
  qs("panel-history").hidden = tab !== "history";
  qs("page-title").textContent = TAB_TITLES[tab];

  qs("tab-btn-live").setAttribute("aria-selected", String(tab === "live"));
  qs("tab-btn-history").setAttribute("aria-selected", String(tab === "history"));

  if (tab === "history") {
    historyUnread = 0;
    updateHistoryBadge();
    if (!historyLoadedOnce) {
      historyLoadedOnce = true;
      loadIncidents();
      loadStats();
    }
  }

  if (pushHash) location.hash = tab;
}

function updateHistoryBadge() {
  const badge = qs("tab-badge-history");
  if (historyUnread > 0) {
    badge.hidden = false;
    badge.textContent = historyUnread > 99 ? "99+" : String(historyUnread);
  } else {
    badge.hidden = true;
  }
}

function initTabs() {
  qs("tab-btn-live").addEventListener("click", () => switchTab("live"));
  qs("tab-btn-history").addEventListener("click", () => switchTab("history"));
  window.addEventListener("hashchange", () => switchTab(location.hash.replace("#", ""), { pushHash: false }));
  switchTab(location.hash.replace("#", "") || "live", { pushHash: false });
}

function feedLine(row, incident) {
  const dot = qs("feed-empty");
  if (dot) dot.remove();

  const body = qs("feed-body");
  const time = new Date(row.created_at || Date.now()).toLocaleTimeString();
  const detail = row.action_decision || (row.verification_result === true ? "verified ok" : row.verification_result === false ? "not verified" : "");
  const line = document.createElement("div");
  line.className = "feed-line";
  line.innerHTML = `<span class="feed-time">${time}</span> <span class="feed-node feed-node-${row.node}">[${row.node}]</span> ${incident.dag_id}/${incident.task_id} ${detail ? "&mdash; " + escapeHtml(detail) : ""}`;
  body.appendChild(line);
  while (body.children.length > MAX_FEED_LINES) body.removeChild(body.firstChild);
  body.scrollTop = body.scrollHeight;

  qs("feed-meta").textContent = `last event ${time}`;
}

function setLiveDot(on) {
  qs("tab-live-dot").classList.toggle("active", !!on);
}

function timelineChips(timeline) {
  return timeline
    .map((t) => `<span class="chip chip-${t.node}">${t.label}</span>`)
    .join("");
}

function renderTable(incidents) {
  const body = qs("incident-rows");
  if (!incidents.length) {
    body.innerHTML = `<tr><td colspan="7" class="empty-state">No agent activity for this filter yet.</td></tr>`;
    return;
  }
  body.innerHTML = incidents
    .map(
      (i) => `
    <tr data-key="${i.dag_id}|${i.task_id}|${i.dag_run_id}">
      <td class="dag-task" data-label="DAG / task"><div class="dag">${i.dag_id}</div><div class="task">${i.task_id}</div></td>
      <td data-label="Run">${i.dag_run_id}</td>
      <td data-label="Status"><span class="pill pill-${i.status}">${i.status_label}</span></td>
      <td data-label="Timeline"><div class="timeline-chips">${timelineChips(i.timeline)}</div></td>
      <td data-label="Attempts">${i.attempts}</td>
      <td data-label="Ended">${fmtTime(i.ended_at)}</td>
      <td data-label=""><button class="row-view-btn" onclick="openDrawer('${i.dag_id}','${i.task_id}','${i.dag_run_id}')">view</button></td>
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

  qs("drawer-title").textContent = `${incident.dag_id} / ${incident.task_id}`;
  qs("drawer-body").innerHTML = `
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
  qs("drawer").classList.add("open");
  qs("drawer-backdrop").classList.add("open");
}

function closeDrawer() {
  qs("drawer").classList.remove("open");
  qs("drawer-backdrop").classList.remove("open");
}

function escapeHtml(s) {
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
}

function showToast(msg) {
  const el = qs("toast");
  el.textContent = msg;
  el.classList.add("show");
  clearTimeout(el._timer);
  el._timer = setTimeout(() => el.classList.remove("show"), 3200);
}

function resetPipeline() {
  document.querySelectorAll(".pipeline-node").forEach((n) => n.classList.remove("active", "done", "success", "warning", "error"));
  document.querySelectorAll(".pipeline-arrow").forEach((a) => a.classList.remove("lit"));
  qs("pipeline-caption").textContent = "Waiting for activity…";
  qs("pipeline-caption").classList.remove("active");
}

function animatePipeline(incident) {
  clearTimeout(pipelineIdleTimer);
  const lastRow = incident.rows[incident.rows.length - 1];
  const group = NODE_GROUP[lastRow.node] || "webhook_received";

  qs("pipeline-caption").textContent = `${incident.dag_id} / ${incident.task_id} (run ${incident.dag_run_id})`;
  qs("pipeline-caption").classList.add("active");

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

function connectStream() {
  const indicator = qs("live-indicator");
  const source = new EventSource("/api/stream");

  source.onopen = () => indicator.classList.remove("offline");
  source.onerror = () => indicator.classList.add("offline");

  source.addEventListener("incident_update", async (e) => {
    let key;
    try {
      key = JSON.parse(e.data);
    } catch {
      return;
    }
    const res = await fetch(
      `/api/incidents/${encodeURIComponent(key.dag_id)}/${encodeURIComponent(key.task_id)}/${encodeURIComponent(key.dag_run_id)}`
    );
    const incident = await res.json();
    if (incident.error) return;

    setLiveDot(true);
    clearTimeout(connectStream._dotTimer);
    connectStream._dotTimer = setTimeout(() => setLiveDot(false), 5000);

    animatePipeline(incident);
    upsertIncidentRow(incident);
    if (historyLoadedOnce) loadStats();

    const lastRow = incident.rows[incident.rows.length - 1];
    feedLine(lastRow, incident);
    showToast(`${lastRow.node}: ${lastRow.action_decision || lastRow.node} — ${incident.dag_id}/${incident.task_id}`);

    if (activeTab !== "history") {
      historyUnread += 1;
      updateHistoryBadge();
    }
  });
}

function initTheme() {
  const saved = localStorage.getItem("console-theme");
  const preferred = saved || (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  applyTheme(preferred);

  qs("theme-toggle").addEventListener("click", () => {
    const next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
    applyTheme(next);
    localStorage.setItem("console-theme", next);
  });
}

function applyTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  qs("icon-sun").style.display = theme === "dark" ? "none" : "block";
  qs("icon-moon").style.display = theme === "dark" ? "block" : "none";
  if (trendChart) loadStats();
}

function initMobileSidebar() {
  const sidebar = qs("sidebar");
  const backdrop = qs("sidebar-backdrop");
  const open = () => {
    sidebar.classList.add("mobile-open");
    backdrop.classList.add("open");
  };
  const close = () => {
    sidebar.classList.remove("mobile-open");
    backdrop.classList.remove("open");
  };
  qs("menu-btn").addEventListener("click", open);
  qs("sidebar-close").addEventListener("click", close);
  backdrop.addEventListener("click", close);
}

function init() {
  initTheme();
  initMobileSidebar();
  initTabs();
  connectStream();

  ["filter-dag", "filter-task", "filter-status", "filter-range"].forEach((id) => {
    qs(id).addEventListener("change", () => {
      loadIncidents();
      loadStats();
    });
  });
  qs("refresh-btn").addEventListener("click", () => {
    if (activeTab === "history") {
      loadIncidents();
      loadStats();
    }
  });
  qs("drawer-close").addEventListener("click", closeDrawer);
  qs("drawer-backdrop").addEventListener("click", closeDrawer);
}

document.addEventListener("DOMContentLoaded", init);
