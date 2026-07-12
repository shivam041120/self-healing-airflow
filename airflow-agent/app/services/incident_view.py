"""
View-model layer between raw `agent_decisions` rows and anything that
presents them (JSON API, HTML dashboard, a future Slack bot, etc). Keeping
this separate from both the DB layer and the presentation layer means the
"what does this incident's status mean" logic exists in exactly one place.
"""

from collections import OrderedDict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional

NODE_LABELS = {
    "webhook_received": "Webhook received",
    "analyze_start": "Analyze (started)",
    "analyze": "Analyze",
    "analyze_error": "Analyze (LLM error)",
    "diagnose": "Diagnose",
    "action": "Action",
    "verify": "Verify",
    "escalate_after_retry": "Escalated (retry didn't fix it)",
    "graph_crashed": "Crashed",
    "python_specialist": "Propose fix",
    "critic": "Review fix",
    "open_pr": "Open PR",
    "retry_after_merge": "Retry after merge",
}

STATUS_FIXED = "fixed"
STATUS_ESCALATED = "escalated"
STATUS_UNRESOLVED = "unresolved"
STATUS_NO_ACTION = "no_action"
STATUS_CRASHED = "crashed"
STATUS_PENDING_REVIEW = "pending_review"

STATUS_LABELS = {
    STATUS_FIXED: "Fixed",
    STATUS_ESCALATED: "Escalated",
    STATUS_UNRESOLVED: "Unresolved",
    STATUS_NO_ACTION: "No action needed",
    STATUS_CRASHED: "Crashed",
    STATUS_PENDING_REVIEW: "Pending PR review",
}


@dataclass
class Incident:
    dag_id: str
    task_id: str
    dag_run_id: str
    rows: list = field(default_factory=list)  # chronological, oldest first
    status: str = STATUS_NO_ACTION
    fix_kind: Optional[str] = None  # "auto_fix" | "suggested" | None
    fix_message: Optional[str] = None
    attempts: int = 0
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None

    def to_dict(self, include_rows: bool = False):
        d = {
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "dag_run_id": self.dag_run_id,
            "status": self.status,
            "status_label": STATUS_LABELS[self.status],
            "fix_kind": self.fix_kind,
            "fix_message": self.fix_message,
            "attempts": self.attempts,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "timeline": [
                {"node": r["node"], "label": NODE_LABELS.get(r["node"], r["node"])}
                for r in self.rows
            ],
        }
        if include_rows:
            d["rows"] = [
                {
                    **{k: v for k, v in r.items() if k != "created_at"},
                    "created_at": r["created_at"].isoformat(),
                }
                for r in self.rows
            ]
        return d


def _compute_status_and_fix(rows):
    if any(r["node"] == "graph_crashed" for r in rows):
        return STATUS_CRASHED, None, None
    if any(r.get("verification_result") is True for r in rows):
        status = STATUS_FIXED
    else:
        last = rows[-1]
        last_action = (last.get("action_decision") or "").upper()
        if last["node"] == "open_pr" and last_action == "PR_OPENED":
            # A PR is open and awaiting a human merge/close — this is an
            # active, in-progress state, not "nothing to do here" and not
            # "stuck/needs escalation" either. It gets its own status so
            # it can't be mistaken for either.
            status = STATUS_PENDING_REVIEW
        elif "ESCALATE" in last_action or last["node"] == "analyze_error" or last_action == "REJECTED":
            # REJECTED covers critic_node explicitly giving up on a
            # proposed fix (see critic_node.py) — documented as
            # "escalate to a human instead of opening a PR", so it
            # belongs here, not in the no_action fallback.
            status = STATUS_ESCALATED
        elif last["node"] == "verify" and last.get("verification_result") is False:
            status = STATUS_UNRESOLVED
        else:
            status = STATUS_NO_ACTION

    fix_kind, fix_message = None, None
    for r in rows:
        if r["node"] == "diagnose":
            action = r.get("action_decision") or ""
            fix_kind = "auto_fix" if action == "AUTO_FIX_APPLIED" else "suggested"
            fix_message = r.get("reasoning") or ""
            break

    return status, fix_kind, fix_message


def group_into_incidents(decisions: list) -> list[Incident]:
    """
    `decisions` arrives most-recent-first overall (as returned by the DB
    layer). Groups by (dag_id, task_id, dag_run_id); incidents themselves
    stay ordered by most-recently-active first, and each incident's own
    rows are chronological (oldest first) since that's how a human reads
    a single incident's story.
    """
    groups = OrderedDict()
    for d in decisions:
        key = (d["dag_id"], d["task_id"], d["dag_run_id"])
        groups.setdefault(key, []).append(d)

    incidents = []
    for (dag_id, task_id, dag_run_id), rows in groups.items():
        rows_chrono = list(reversed(rows))
        status, fix_kind, fix_message = _compute_status_and_fix(rows_chrono)
        attempts = max((r.get("attempt") or 0) for r in rows_chrono)
        incidents.append(
            Incident(
                dag_id=dag_id,
                task_id=task_id,
                dag_run_id=dag_run_id,
                rows=rows_chrono,
                status=status,
                fix_kind=fix_kind,
                fix_message=fix_message,
                attempts=attempts,
                started_at=rows_chrono[0]["created_at"],
                ended_at=rows_chrono[-1]["created_at"],
            )
        )
    return incidents


def compute_summary(incidents: list) -> dict:
    counts = {s: 0 for s in STATUS_LABELS}
    for i in incidents:
        counts[i.status] += 1
    return {
        "total": len(incidents),
        "by_status": {STATUS_LABELS[k]: v for k, v in counts.items()},
    }


def compute_heatmap_days(incidents: list, days: int = 90) -> list[dict]:
    """
    Bucket incidents by the day they ended, for the density heatmap: total
    count per day plus bad_ratio (share of escalated/unresolved/crashed —
    the outcomes that actually needed a human) so the frontend can color
    by both volume and severity, not just volume. Mirrors
    compute_daily_trend's bucketing so the two never drift apart.
    """
    from collections import defaultdict
    from datetime import timedelta, timezone

    bad_statuses = {STATUS_ESCALATED, STATUS_UNRESOLVED, STATUS_CRASHED}

    now = datetime.now(timezone.utc)
    buckets = OrderedDict()
    for i in range(days - 1, -1, -1):
        day = (now - timedelta(days=i)).date().isoformat()
        buckets[day] = {"total": 0, "bad": 0}

    for incident in incidents:
        if not incident.ended_at:
            continue
        day = incident.ended_at.date().isoformat()
        if day in buckets:
            buckets[day]["total"] += 1
            if incident.status in bad_statuses:
                buckets[day]["bad"] += 1

    return [
        {
            "date": day,
            "total": counts["total"],
            "bad_ratio": (counts["bad"] / counts["total"]) if counts["total"] else 0,
        }
        for day, counts in buckets.items()
    ]


def compute_daily_trend(incidents: list, days: int = 14) -> list[dict]:
    """
    Bucket incidents by the day they ended, counting per status — feeds the
    trend chart. Days with zero incidents still appear (as zero rows) so
    the chart doesn't silently skip gaps.
    """
    from collections import defaultdict
    from datetime import timedelta, timezone

    now = datetime.now(timezone.utc)
    buckets = OrderedDict()
    for i in range(days - 1, -1, -1):
        day = (now - timedelta(days=i)).date().isoformat()
        buckets[day] = {s: 0 for s in STATUS_LABELS}

    for incident in incidents:
        if not incident.ended_at:
            continue
        day = incident.ended_at.date().isoformat()
        if day in buckets:
            buckets[day][incident.status] += 1

    return [
        {"date": day, **{STATUS_LABELS[s]: c for s, c in counts.items()}}
        for day, counts in buckets.items()
    ]