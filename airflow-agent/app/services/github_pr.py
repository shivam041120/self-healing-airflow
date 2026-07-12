"""
Opens a real GitHub pull request containing a suggested fix for code-shaped
failures (SQL syntax, wrong column/table reference, a plain Python bug in a
DAG file) — the agent proposes, it never merges on its own. A human
engineer reviews and merges (or closes) the PR through GitHub as normal;
the agent only acts again once that merge actually happens (see
app/api/github_webhook.py).

Requires three environment variables to actually create anything:
  GITHUB_TOKEN         - a PAT (or fine-grained token) with contents+PR
                          write access to the target repo
  GITHUB_REPO          - "owner/repo"
  GITHUB_BASE_BRANCH   - branch to open PRs against (default: "main")

If these aren't set, functions here return a clear "not configured" error
dict instead of raising — a missing PR integration shouldn't crash the
agent loop, it should just fall back to a normal ESCALATE.
"""

import base64
import os
import time

import httpx

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")  # "owner/repo"
GITHUB_BASE_BRANCH = os.getenv("GITHUB_BASE_BRANCH", "main")
GITHUB_API_URL = "https://api.github.com"

AIRFLOW_DAGS_CONTAINER_PATH = "/opt/airflow/dags"


def is_configured() -> bool:
    return bool(GITHUB_TOKEN and GITHUB_REPO)


def _headers():
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def fileloc_to_repo_path(fileloc: str) -> str | None:
    """
    Converts Airflow's in-container DAG path to a repo-relative path.
    Relies on docker-compose.yaml's existing bind mount
    ('./dags:/opt/airflow/dags'), which means the two are the same
    directory — just seen from different filesystems.
    """
    if not fileloc:
        return None
    fileloc = fileloc.replace("\\", "/")
    marker = AIRFLOW_DAGS_CONTAINER_PATH
    if marker in fileloc:
        return "dags/" + fileloc.split(marker, 1)[1].lstrip("/")
    # Fallback: just take the filename under dags/, better than failing
    # outright if the mount path ever changes.
    return "dags/" + fileloc.rsplit("/", 1)[-1]


def get_file_contents(repo_path: str, ref: str = None) -> dict:
    """Returns {"content": str, "sha": str} or {"error": str}."""
    if not is_configured():
        return {"error": "GitHub integration not configured (GITHUB_TOKEN / GITHUB_REPO missing)"}
    url = f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/{repo_path}"
    params = {"ref": ref or GITHUB_BASE_BRANCH}
    try:
        resp = httpx.get(url, headers=_headers(), params=params, timeout=15)
    except Exception as e:
        return {"error": f"Could not reach GitHub: {e}"}
    if resp.status_code != 200:
        return {"error": f"Could not fetch {repo_path} (status {resp.status_code}): {resp.text}"}
    data = resp.json()
    content = base64.b64decode(data["content"]).decode("utf-8")
    return {"content": content, "sha": data["sha"]}


def open_fix_pr(
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    repo_path: str,
    original_content: str,
    fixed_content: str,
    reasoning: str,
) -> dict:
    """
    Creates a branch, commits the proposed fix to it, opens a PR against
    GITHUB_BASE_BRANCH. Returns {"pr_number": int, "pr_url": str,
    "branch": str} on success, or {"error": str} on failure — this must
    never raise, since a broken PR integration should degrade to ESCALATE,
    not crash the agent run.
    """
    if not is_configured():
        return {"error": "GitHub integration not configured (GITHUB_TOKEN / GITHUB_REPO missing)"}
    if original_content == fixed_content:
        return {"error": "Proposed fix is identical to the current file content — nothing to PR"}

    branch = f"self-healing-agent/{task_id}-{dag_run_id}-{int(time.time())}"
    branch = branch.replace(":", "-").replace("+", "-")  # dag_run_ids often contain ':'/'+' (ISO timestamps)

    try:
        # 1. Find the base branch's current commit SHA
        ref_resp = httpx.get(
            f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/git/ref/heads/{GITHUB_BASE_BRANCH}",
            headers=_headers(), timeout=15,
        )
        if ref_resp.status_code != 200:
            return {"error": f"Could not read base branch ref (status {ref_resp.status_code}): {ref_resp.text}"}
        base_sha = ref_resp.json()["object"]["sha"]

        # 2. Create the new branch pointing at that same commit
        create_ref_resp = httpx.post(
            f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/git/refs",
            headers=_headers(),
            json={"ref": f"refs/heads/{branch}", "sha": base_sha},
            timeout=15,
        )
        if create_ref_resp.status_code not in (201,):
            return {"error": f"Could not create branch (status {create_ref_resp.status_code}): {create_ref_resp.text}"}

        # 3. Look up the file's current sha ON THE NEW BRANCH (needed to update it)
        file_info = get_file_contents(repo_path, ref=branch)
        if "error" in file_info:
            return file_info

        # 4. Commit the fixed content to that branch
        update_resp = httpx.put(
            f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/contents/{repo_path}",
            headers=_headers(),
            json={
                "message": f"self-healing-agent: fix {task_id} ({dag_id}, run {dag_run_id})",
                "content": base64.b64encode(fixed_content.encode("utf-8")).decode("ascii"),
                "sha": file_info["sha"],
                "branch": branch,
            },
            timeout=15,
        )
        if update_resp.status_code not in (200, 201):
            return {"error": f"Could not commit fix (status {update_resp.status_code}): {update_resp.text}"}

        # 5. Open the PR — human review happens here, the agent stops
        pr_body = (
            f"Opened automatically by the self-healing agent after `{task_id}` failed "
            f"in `{dag_id}` (run `{dag_run_id}`).\n\n"
            f"**Reasoning:**\n{reasoning}\n\n"
            f"This PR is not merged automatically. Once a human merges it, the agent "
            f"will clear and retry the task exactly once — it does not keep looping "
            f"while this is open.\n\n"
            f"_Please review the diff carefully before merging — this fix was "
            f"generated by an LLM._"
        )
        pr_resp = httpx.post(
            f"{GITHUB_API_URL}/repos/{GITHUB_REPO}/pulls",
            headers=_headers(),
            json={
                "title": f"[self-healing-agent] Fix {task_id} in {dag_id}",
                "head": branch,
                "base": GITHUB_BASE_BRANCH,
                "body": pr_body,
            },
            timeout=15,
        )
        if pr_resp.status_code not in (201,):
            return {"error": f"Could not open PR (status {pr_resp.status_code}): {pr_resp.text}"}

        pr_data = pr_resp.json()
        return {"pr_number": pr_data["number"], "pr_url": pr_data["html_url"], "branch": branch}

    except Exception as e:
        return {"error": f"Unexpected error opening PR: {e}"}
