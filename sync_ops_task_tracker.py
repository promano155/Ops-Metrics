"""
sync_ops_task_tracker.py

Tracks the ad-hoc ops task Asana project. Two things, one daily run:

1. Friday-only Slack DM digest: open/completed-this-week counts per
   assignee, plus any Blocked task missing a reason, sent via Slack's
   Web API directly (not the MCP Slack connector - that's for
   interactive chat use, not an unattended script). Recipient is a
   fixed Slack user ID, not necessarily whoever owns the Asana project.

2. Monthly per-person counts (assigned / completed / avg days open),
   upserted with the same current/closed lock pattern used everywhere
   else in this pipeline, feeding a Lovable dashboard section. This is
   for pattern-spotting ("who keeps getting the fire drills"), not
   performance management - keep it framed that way if it is ever shown
   to anyone beyond internal ops visibility.

--- No due dates, no SLA/overdue concept - deliberate ---
This project's reporting only measures COMPLETION TIME (how long a task
actually took, start to finish), not an SLA or deadline. Earlier
versions of this script reset due_on based on section-change history
specifically so Asana's native overdue-red rendering would show staleness
for free - that mechanism has been removed entirely. This script now
never reads or writes due_on at all, and tasks in this project should
carry no due dates. "Completed" is still derived from section state
(moving to Done), not Asana's checkbox field - see note below.

--- Setup still needed before this can run ---
- ASANA_PROJECT_GID below is a placeholder - fill in once the project
  exists.
- SECTION_NAMES below must match your actual section names EXACTLY
  (case-insensitive). Section GIDs are looked up by name at runtime, not
  hardcoded, matching the pattern already used in the billing-sheet
  automation - robust to you renaming things later as long as the
  Open/Blocked/Done concept stays.
- Blocked-reason capture is handled here, not via an Asana rule. Rules
  can't truly block a status change (reactive, not preventive), and the
  actual goal - "don't have to guess later" - is a visibility problem,
  not an enforcement one. This script checks a "Blocked Reason" custom
  field on every task and calls out any currently-Blocked task where
  it's empty, in the same Friday digest - no separate Asana rule, no
  separate schedule, just one more line riding on infrastructure that's
  already running daily. Create a plain text custom field named exactly
  "Blocked Reason" in the project for this to have something to check.

--- Now-unused, left alone on purpose (minimal schema disruption) ---
ops_task_state.was_overdue, ops_monthly_person_counts.currently_overdue,
and the ops_overdue_events table are no longer written to by this
script. Safe to drop whenever convenient - not done here automatically.
If a Lovable dashboard section displays "Overdue" from these columns,
it will now show stale, frozen values rather than updating - worth
removing that stat from the dashboard UI to match.
"""

import os
import time
import datetime as dt

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ASANA_TOKEN = os.environ["ASANA_PAT"]
ASANA_PROJECT_GID = "1217131749747219"

SECTION_NAMES = {
    "open": "Open",
    "blocked": "Blocked",
    "done": "Done",
}

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_DM_USER_ID = "U06V9PY2STY"

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
STATE_TABLE = "ops_task_state"
MONTHLY_TABLE = "ops_monthly_person_counts"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_utc_datetime(value):
    """Same robust parser used in the billing-sheet automation - handles
    Supabase's real timestamp formats ('+00', '+00:00', 'Z', bare)."""
    if value is None:
        return None
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return parsed


# ---------------------------------------------------------------------------
# Asana
# ---------------------------------------------------------------------------


def asana_headers():
    return {"Authorization": f"Bearer {ASANA_TOKEN}", "Content-Type": "application/json"}


def asana_request(method, path, **kwargs):
    url = f"https://app.asana.com/api/1.0{path}"
    for attempt in range(5):
        resp = requests.request(method, url, headers=asana_headers(), timeout=30, **kwargs)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"Asana rate limited, waiting {wait}s (attempt {attempt + 1}/5)")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError("Asana still rate limited after 5 retries")


def get_sections():
    resp = asana_request("GET", f"/projects/{ASANA_PROJECT_GID}/sections")
    return resp.json()["data"]


def find_section_gid(sections, name):
    for s in sections:
        if s["name"].strip().lower() == name.strip().lower():
            return s["gid"]
    raise RuntimeError(f"No section named '{name}' found in project {ASANA_PROJECT_GID}.")


def fetch_all_tasks():
    """Paginates through every task in the project. Deliberately does NOT
    fetch due_on (this script never reads or writes it) or Asana's
    native completed/completed_at fields - this workflow tracks status
    via SECTION (Open/Blocked/Done), not the completion checkbox, and
    those two can disagree (a task can sit in Done with the checkbox
    never touched). 'Completed' is derived entirely from section-state
    tracking below, not from Asana's separate field.

    Fetches custom_fields too, so a missing 'Blocked Reason' can be
    detected purely from data already being pulled - no separate Asana
    rule needed for that check at all."""
    tasks = []
    base_params = {
        "project": ASANA_PROJECT_GID,
        "opt_fields": "name,assignee.name,memberships.section.name,created_at,custom_fields.name,custom_fields.text_value,custom_fields.display_value",
        "limit": 100,
    }
    url = "https://app.asana.com/api/1.0/tasks"
    params = dict(base_params)
    while True:
        resp = requests.get(url, headers=asana_headers(), params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        tasks.extend(body["data"])
        next_page = body.get("next_page")
        if not next_page:
            break
        params = {**base_params, "offset": next_page["offset"]}
        time.sleep(0.3)
    return tasks


def get_blocked_reason(task):
    for cf in task.get("custom_fields", []):
        if cf.get("name", "").strip().lower() == "blocked reason":
            return (cf.get("text_value") or cf.get("display_value") or "").strip()
    return ""


def get_task_section_name(task, section_gid_to_name):
    for m in task.get("memberships", []):
        section = m.get("section")
        if section and section["gid"] in section_gid_to_name:
            return section_gid_to_name[section["gid"]]
    return None


# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------


def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def get_all_task_state():
    url = f"{SUPABASE_URL}/rest/v1/{STATE_TABLE}?select=*"
    resp = requests.get(url, headers=supabase_headers(), timeout=30)
    resp.raise_for_status()
    return {row["task_gid"]: row for row in resp.json()}


def upsert_task_state(task_gid, assignee, current_section, last_section_change_at, completed_at):
    payload = {
        "task_gid": task_gid,
        "assignee": assignee,
        "current_section": current_section,
        "last_section_change_at": last_section_change_at.isoformat(),
        "completed_at": completed_at.isoformat() if completed_at else None,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{STATE_TABLE}",
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()


def upsert_monthly_count(month_key, assignee, assigned, completed, avg_days_open, status):
    payload = {
        "month_key": month_key,
        "assignee": assignee,
        "tasks_assigned": assigned,
        "tasks_completed": completed,
        "avg_days_open": avg_days_open,
        "status": status,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{MONTHLY_TABLE}",
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()


def fetch_closed_months():
    url = f"{SUPABASE_URL}/rest/v1/{MONTHLY_TABLE}?select=month_key,status"
    resp = requests.get(url, headers=supabase_headers(), timeout=30)
    resp.raise_for_status()
    return {r["month_key"] for r in resp.json() if r["status"] == "closed"}


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------


def send_slack_dm(text):
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"},
        json={"channel": SLACK_DM_USER_ID, "text": text},
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    if not body.get("ok"):
        raise RuntimeError(f"Slack API error: {body}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(force_digest=False):
    now = dt.datetime.utcnow()
    today = now.date()

    sections = get_sections()
    section_gid_to_name = {s["gid"]: s["name"] for s in sections}
    find_section_gid(sections, SECTION_NAMES["done"])  # validates the section exists

    tasks = fetch_all_tasks()
    print(f"Fetched {len(tasks)} tasks from project {ASANA_PROJECT_GID}.")

    existing_state = get_all_task_state()

    # Track for digest + monthly counts as we go, one pass.
    open_by_assignee = {}
    completed_this_week_by_assignee = {}
    monthly_assigned = {}
    monthly_completed = {}
    monthly_days_open = {}  # assignee -> list of day counts, for avg

    week_start = today - dt.timedelta(days=today.weekday())  # Monday this week
    month_key = f"{today.year:04d}-{today.month:02d}"
    blocked_without_reason = []  # (task_name, assignee) pairs for the digest

    for task in tasks:
        task_gid = task["gid"]
        assignee = (task.get("assignee") or {}).get("name", "Unassigned")
        current_section = get_task_section_name(task, section_gid_to_name)
        is_done = current_section is not None and current_section.lower() == SECTION_NAMES["done"].lower()
        created_at = parse_utc_datetime(task["created_at"])

        prior = existing_state.get(task_gid)
        section_changed = (prior is None) or (prior["current_section"] != current_section)

        if section_changed:
            last_section_change_at = now
        else:
            last_section_change_at = parse_utc_datetime(prior["last_section_change_at"])

        # "Completed" is derived entirely from section state, not Asana's
        # checkbox field - the moment it's sitting in Done IS "completed"
        # for this workflow, and last_section_change_at already tells us
        # exactly when it arrived there (or was last touched, if it's
        # been sitting there unchanged since a prior run).
        effective_completed_at = last_section_change_at if is_done else None

        upsert_task_state(task_gid, assignee, current_section, last_section_change_at, effective_completed_at)

        # --- Digest tallies ---
        if not is_done:
            open_by_assignee[assignee] = open_by_assignee.get(assignee, 0) + 1
        if current_section is not None and current_section.lower() == SECTION_NAMES["blocked"].lower():
            if not get_blocked_reason(task):
                blocked_without_reason.append((task["name"], assignee))
        if is_done and effective_completed_at and effective_completed_at.date() >= week_start:
            completed_this_week_by_assignee[assignee] = completed_this_week_by_assignee.get(assignee, 0) + 1

        # --- Monthly tallies (this calendar month only) ---
        if created_at.date().strftime("%Y-%m") == month_key:
            monthly_assigned[assignee] = monthly_assigned.get(assignee, 0) + 1
        if is_done and effective_completed_at and effective_completed_at.date().strftime("%Y-%m") == month_key:
            monthly_completed[assignee] = monthly_completed.get(assignee, 0) + 1
            days_open = (effective_completed_at.date() - created_at.date()).days
            monthly_days_open.setdefault(assignee, []).append(days_open)

    # --- Friday-only digest ---
    if today.weekday() == 4 or force_digest:  # Monday=0 ... Friday=4
        lines = ["*Weekly ops task digest*"]
        all_assignees = sorted(set(open_by_assignee) | set(completed_this_week_by_assignee))
        if not all_assignees:
            lines.append("Nothing open or completed this week.")
        for person in all_assignees:
            lines.append(
                f"*{person}* - open: {open_by_assignee.get(person, 0)}, "
                f"completed this week: {completed_this_week_by_assignee.get(person, 0)}"
            )
        if blocked_without_reason:
            lines.append("\n*Blocked with no reason noted:*")
            for name, assignee in blocked_without_reason:
                lines.append(f"- {name} ({assignee})")
        send_slack_dm("\n".join(lines))
        print("Sent Friday digest.")

    # --- Monthly counts, same current/closed lock pattern as everything else ---
    already_closed = fetch_closed_months()
    status = "closed" if month_key in already_closed else "current"
    all_people = sorted(set(monthly_assigned) | set(monthly_completed))
    for person in all_people:
        assigned = monthly_assigned.get(person, 0)
        completed = monthly_completed.get(person, 0)
        days_list = monthly_days_open.get(person, [])
        avg_days = round(sum(days_list) / len(days_list), 1) if days_list else None
        upsert_monthly_count(month_key, person, assigned, completed, avg_days, status)
    print(f"Upserted monthly counts for {len(all_people)} people ({month_key}, {status}).")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-digest", action="store_true",
                         help="Send the digest regardless of day-of-week, for testing.")
    args = parser.parse_args()
    main(force_digest=args.force_digest)
