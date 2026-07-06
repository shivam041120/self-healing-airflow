"""
Read-only dashboard over the agent_decisions table. Plain server-rendered
HTML — no separate frontend build needed for this to be useful.

Rows are grouped into per-incident cards (one card per dag_id/task_id/
dag_run_id), each showing a status pill (Fixed / Escalated / Unresolved /
No action needed / Crashed), a node-by-node timeline, and full detail on
demand — instead of one long flat table where you have to piece incidents
together by eye.
"""

import html
from collections import OrderedDict
from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from app.services.decision_log import get_recent_decisions

router = APIRouter()

NODE_LABELS = {
    "webhook_received": "Webhook received",
    "analyze_start": "Analyze (started)",
    "analyze": "Analyze",
    "analyze_error": "Analyze (LLM error)",
    "diagnose": "Diagnose",
    "action": "Action",
    "verify": "Verify",
    "graph_crashed": "Crashed",
}

NODE_COLORS = {
    "webhook_received": "#6b7280",
    "analyze_start": "#6b7280",
    "analyze": "#2563eb",
    "analyze_error": "#dc2626",
    "diagnose": "#7c3aed",
    "action": "#d97706",
    "verify": "#059669",
    "graph_crashed": "#dc2626",
}


def _group_by_incident(decisions):
    """
    decisions arrive most-recent-first overall. Group by
    (dag_id, task_id, dag_run_id); within a group, order chronologically
    (oldest first) since that's how you'd want to read one incident's story.
    Groups themselves stay ordered by most-recently-active first.
    """
    groups = OrderedDict()
    for d in decisions:
        key = (d["dag_id"], d["task_id"], d["dag_run_id"])
        groups.setdefault(key, []).append(d)
    incidents = []
    for key, rows in groups.items():
        rows_chrono = list(reversed(rows))
        incidents.append({"key": key, "rows": rows_chrono})
    return incidents


def _incident_status(rows):
    """Returns (label, color) describing how an incident ended up (so far)."""
    if any(r["node"] == "graph_crashed" for r in rows):
        return "Crashed", "#dc2626"
    if any(r.get("verification_result") is True for r in rows):
        return "Fixed", "#059669"
    last = rows[-1]
    last_action = (last.get("action_decision") or "").upper()
    if "ESCALATE" in last_action or last["node"] == "analyze_error":
        return "Escalated", "#b45309"
    if last["node"] == "verify" and last.get("verification_result") is False:
        max_attempt = max((r.get("attempt") or 0) for r in rows)
        if max_attempt >= 3:
            return "Unresolved (retries exhausted)", "#dc2626"
        return "Unresolved", "#dc2626"
    return "No action needed", "#6b7280"


def _fix_summary(rows):
    """Pulls out the schema_healer's message, if this incident had one."""
    for r in rows:
        if r["node"] == "diagnose":
            action = r.get("action_decision") or ""
            kind = "Auto-fixed" if action == "AUTO_FIX_APPLIED" else "Suggested fix"
            return kind, r.get("reasoning") or ""
    return None, None


def _badge(text, color):
    return (
        f'<span style="background:{color}1A;color:{color};border:1px solid {color}55;'
        f'padding:2px 10px;border-radius:999px;font-size:12px;font-weight:600;'
        f'white-space:nowrap;">{html.escape(text)}</span>'
    )


def _render_timeline(rows):
    chips = []
    for r in rows:
        color = NODE_COLORS.get(r["node"], "#6b7280")
        label = NODE_LABELS.get(r["node"], r["node"])
        chips.append(
            f'<span style="display:inline-flex;align-items:center;gap:4px;'
            f'background:{color}14;color:{color};border:1px solid {color}40;'
            f'padding:3px 9px;border-radius:6px;font-size:12px;font-weight:600;">'
            f'{html.escape(label)}</span>'
        )
    return '<span style="color:#c7cad1;margin:0 2px;">→</span>'.join(chips)


def _render_detail_rows(rows):
    out = []
    for r in rows:
        verified = r.get("verification_result")
        verified_label = "✅ yes" if verified is True else ("❌ no" if verified is False else "—")
        color = NODE_COLORS.get(r["node"], "#6b7280")
        out.append(f"""
        <tr>
          <td style="white-space:nowrap;color:#666;">{r['created_at']}</td>
          <td><span style="color:{color};font-weight:600;">{html.escape(NODE_LABELS.get(r['node'], r['node']))}</span></td>
          <td>{r['attempt'] if r['attempt'] is not None else '-'}</td>
          <td>{html.escape(r['action_decision'] or '-')}</td>
          <td>{verified_label}</td>
          <td>
            <details>
              <summary style="cursor:pointer;color:#2563eb;">view</summary>
              <b>Reasoning:</b>
              <pre>{html.escape(r['reasoning'] or '(none)')}</pre>
              <b>Logs excerpt:</b>
              <pre>{html.escape((r['logs_excerpt'] or '(none)')[:1500])}</pre>
            </details>
          </td>
        </tr>
        """)
    return "".join(out)


def _render_incident_card(incident):
    dag_id, task_id, dag_run_id = incident["key"]
    rows = incident["rows"]
    status_label, status_color = _incident_status(rows)
    fix_kind, fix_message = _fix_summary(rows)
    attempts = max((r.get("attempt") or 0) for r in rows)
    started, ended = rows[0]["created_at"], rows[-1]["created_at"]

    fix_html = ""
    if fix_kind:
        fix_color = "#059669" if fix_kind == "Auto-fixed" else "#b45309"
        fix_html = f"""
        <div style="margin-top:8px;padding:8px 12px;background:{fix_color}0D;
                    border-left:3px solid {fix_color};border-radius:4px;font-size:13px;">
          <b style="color:{fix_color};">{html.escape(fix_kind)}:</b>
          {html.escape(fix_message)}
        </div>
        """

    return f"""
    <div style="border:1px solid #e5e7eb;border-radius:10px;margin-bottom:14px;
                background:#fff;box-shadow:0 1px 2px rgba(0,0,0,0.04);">
      <div style="padding:14px 16px;">
        <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
          <div>
            <span style="font-weight:700;font-size:14px;">{html.escape(dag_id)}</span>
            <span style="color:#999;"> / </span>
            <span style="font-weight:600;font-size:14px;">{html.escape(task_id)}</span>
            <span style="color:#999;font-size:12px;"> (run {html.escape(dag_run_id)})</span>
          </div>
          <div style="display:flex;gap:8px;align-items:center;">
            <span style="font-size:12px;color:#888;">attempts: {attempts}</span>
            {_badge(status_label, status_color)}
          </div>
        </div>
        <div style="margin-top:10px;">{_render_timeline(rows)}</div>
        {fix_html}
        <div style="margin-top:8px;font-size:12px;color:#999;">{started} → {ended}</div>
        <details style="margin-top:8px;">
          <summary style="cursor:pointer;color:#2563eb;font-size:13px;">node-by-node detail</summary>
          <table style="border-collapse:collapse;width:100%;margin-top:8px;">
            <tr>
              <th style="text-align:left;">Time</th><th style="text-align:left;">Node</th>
              <th style="text-align:left;">Attempt</th><th style="text-align:left;">Action</th>
              <th style="text-align:left;">Verified</th><th style="text-align:left;">Detail</th>
            </tr>
            {_render_detail_rows(rows)}
          </table>
        </details>
      </div>
    </div>
    """


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    try:
        decisions = await get_recent_decisions(300)
    except Exception as e:
        return HTMLResponse(f"<p>Could not load decision log: {html.escape(str(e))}</p>", status_code=500)

    incidents = _group_by_incident(decisions)

    fixed = sum(1 for i in incidents if _incident_status(i["rows"])[0] == "Fixed")
    escalated = sum(1 for i in incidents if _incident_status(i["rows"])[0] == "Escalated")
    unresolved = sum(1 for i in incidents if _incident_status(i["rows"])[0].startswith("Unresolved"))
    no_action = sum(1 for i in incidents if _incident_status(i["rows"])[0] == "No action needed")

    if not incidents:
        cards_html = "<p style='color:#888;'>No agent activity recorded yet.</p>"
    else:
        cards_html = "".join(_render_incident_card(i) for i in incidents)

    def stat_card(label, value, color):
        return f"""
        <div style="flex:1;min-width:120px;background:#fff;border:1px solid #e5e7eb;
                    border-radius:10px;padding:14px 16px;">
          <div style="font-size:22px;font-weight:700;color:{color};">{value}</div>
          <div style="font-size:12px;color:#888;margin-top:2px;">{label}</div>
        </div>
        """

    stats_html = f"""
    <div style="display:flex;gap:12px;flex-wrap:wrap;margin:16px 0 22px 0;">
      {stat_card("Incidents", len(incidents), "#111")}
      {stat_card("Fixed", fixed, "#059669")}
      {stat_card("Escalated", escalated, "#b45309")}
      {stat_card("Unresolved", unresolved, "#dc2626")}
      {stat_card("No action needed", no_action, "#6b7280")}
    </div>
    """

    return f"""
    <html>
    <head>
      <title>Self-healing agent dashboard</title>
      <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                margin: 0; padding: 24px 32px 48px 32px; color: #1f2328; background: #f7f8fa; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border-bottom: 1px solid #eee; padding: 6px 10px; font-size: 12.5px;
                  vertical-align: top; text-align: left; }}
        th {{ color: #888; font-weight: 600; }}
        pre {{ white-space: pre-wrap; max-width: 640px; font-size: 12px; background: #f7f8fa;
               padding: 8px; border-radius: 6px; }}
        h2 {{ margin: 0 0 4px 0; }}
        p.note {{ color: #666; font-size: 13px; margin-top: 0; }}
        summary {{ user-select: none; }}
        #refresh-indicator {{ font-size: 12px; color: #aaa; }}
      </style>
    </head>
    <body>
      <div style="display:flex;justify-content:space-between;align-items:baseline;">
        <div>
          <h2>Self-healing agent — decision trace</h2>
          <p class="note">One card per incident. Each timeline step is real node output — not a summary.</p>
        </div>
        <span id="refresh-indicator">auto-refreshing…</span>
      </div>
      <div id="dashboard-content">
        {stats_html}
        {cards_html}
      </div>
      <script>
        async function refreshDashboard() {{
          try {{
            const res = await fetch(window.location.pathname, {{ cache: "no-store" }});
            const text = await res.text();
            const doc = new DOMParser().parseFromString(text, "text/html");
            const fresh = doc.getElementById("dashboard-content");
            const current = document.getElementById("dashboard-content");
            if (fresh && current) current.innerHTML = fresh.innerHTML;
          }} catch (e) {{
            console.error("dashboard refresh failed", e);
          }}
        }}
        setInterval(refreshDashboard, 8000);
      </script>
    </body>
    </html>
    """
