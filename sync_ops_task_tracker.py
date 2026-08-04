"""
sync_ops_task_tracker.py

Tracks the ad-hoc ops task Asana project. Three things, one daily run:

1. SLA via due-date reset, not a custom "stale" flag. Sections ARE status
   (Open / Blocked / Done - no separate Status field). Each run, for
   every open/blocked task, this checks whether its section has changed
   since the last run. If it HAS changed, the due date resets to
   "now + 1 business day" - the clock restarts because something real
   happened. If it HASN'T changed, the due date is left alone, so once
   it passes, Asana's own native overdue-red rendering shows it - zero
   extra code needed for the visual. This is deliberately NOT a custom
   tag/field: "overdue" already means exactly the right thing once the
   due date is being driven by real status-change history instead of
   raw task age.

2. Friday-only Slack DM digest: open/overdue/completed-this-week counts
   per assignee, sent via Slack's Web API directly (not the MCP Slack
   connector - that's for interactive chat use, not an unattended
   script). Recipient is a fixed Slack user ID, not necessarily whoever
   owns the Asana project.

3. Monthly per-person counts (assigned / completed / avg days open),
   upserted with the same current/closed lock pattern used everywhere
   else in this pipeline, feeding a future Lovable dashboard section.
   This is for pattern-spotting ("who keeps getting the fire drills"),
   not performance management - keep it framed that way if it is ever
   shown to anyone beyond internal ops visibility.

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
"""

import os
import time
import calendar
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
OVERDUE_EVENTS_TABLE = "ops_overdue_events"

SLA_BUSINESS_DAYS = 1

# ---------------------------------------------------------------------------
# Business day math (same pattern as the data-processing script)
# ---------------------------------------------------------------------------


def business_days_after(start_dt, n_days):
    """Returns start_dt shifted forward by n_days WEEKDAYS (Mon-Fri)."""
    current = start_dt
    added = 0
    while added < n_days:
        current += dt.timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


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
    fetch Asana's native completed/completed_at fields - this workflow
    tracks status via SECTION (Open/Blocked/Done), not the completion
    checkbox, and those two can disagree (a task can sit in Done with
    the checkbox never touched). 'Completed' is derived entirely from
    section-state tracking below, not from Asana's separate field.

    Fetches custom_fields too, so a missing 'Blocked Reason' can be
    detected purely from data already being pulled - no separate Asana
    rule needed for that check at all."""
    tasks = []
    base_params = {
        "project": ASANA_PROJECT_GID,
        "opt_fields": "name,assignee.name,memberships.section.name,created_at,due_on,custom_fields.name,custom_fields.text_value,custom_fields.display_value",
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


def set_task_due_on(task_gid, due_date):
    payload = {"data": {"due_on": due_date.strftime("%Y-%m-%d")}}
    asana_request("PUT", f"/tasks/{task_gid}", json=payload)


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


def upsert_task_state(task_gid, assignee, current_section, last_section_change_at, completed_at, was_overdue):
    payload = {
        "task_gid": task_gid,
        "assignee": assignee,
        "current_section": current_section,
        "last_section_change_at": last_section_change_at.isoformat(),
        "completed_at": completed_at.isoformat() if completed_at else None,
        "was_overdue": was_overdue,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{STATE_TABLE}",
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()


def record_overdue_event(task_gid, task_name, assignee):
    """Logs ONE row per distinct overdue incident - called only at the
    moment a task transitions from on-time to overdue, not on every run
    while it remains overdue. If it later gets fixed and goes overdue
    again, that's a new, separate incident. Week/month/quarter rollups
    are computed at query time from occurred_at - no pre-bucketing here."""
    payload = {
        "task_gid": task_gid,
        "task_name": task_name,
        "assignee": assignee,
        "occurred_at": dt.datetime.utcnow().isoformat(),
    }
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{OVERDUE_EVENTS_TABLE}",
        headers=supabase_headers(),
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()


def upsert_monthly_count(month_key, assignee, assigned, completed, avg_days_open, currently_overdue, status):
    payload = {
        "month_key": month_key,
        "assignee": assignee,
        "tasks_assigned": assigned,
        "tasks_completed": completed,
        "avg_days_open": avg_days_open,
        "currently_overdue": currently_overdue,
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
    done_gid = find_section_gid(sections, SECTION_NAMES["done"])

    tasks = fetch_all_tasks()
    print(f"Fetched {len(tasks)} tasks from project {ASANA_PROJECT_GID}.")

    existing_state = get_all_task_state()

    # Track for digest + monthly counts as we go, one pass.
    open_by_assignee = {}
    overdue_by_assignee = {}
    overdue_tasks = []  # (task_name, assignee) pairs for the digest - not just a count
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

        # --- Overdue transition detection - BEFORE we potentially reset
        # due_on below, so this reflects whether it was ALREADY overdue
        # at the start of this run, not an artifact of our own reset.
        due_on_before_reset = task.get("due_on")
        is_overdue_now = (
            not is_done and due_on_before_reset is not None
            and dt.date.fromisoformat(due_on_before_reset) < today
        )
        was_overdue_before = bool(prior and prior.get("was_overdue"))
        if is_overdue_now and not was_overdue_before:
            record_overdue_event(task_gid, task["name"], assignee)
            print(f"NEW overdue incident logged for '{task['name']}' ({assignee})")

        upsert_task_state(task_gid, assignee, current_section, last_section_change_at,
                           effective_completed_at, is_overdue_now)

        # --- SLA due-date reset: only touch due_on when something real
        # changed, so Asana's own overdue-red does the rest passively.
        if not is_done and section_changed:
            new_due = business_days_after(last_section_change_at, SLA_BUSINESS_DAYS).date()
            if task.get("due_on") != new_due.strftime("%Y-%m-%d"):
                set_task_due_on(task_gid, new_due)
                print(f"Section changed for '{task['name']}' -> due_on reset to {new_due}")

        # --- Digest tallies ---
        if not is_done:
            open_by_assignee[assignee] = open_by_assignee.get(assignee, 0) + 1
            if is_overdue_now:
                overdue_by_assignee[assignee] = overdue_by_assignee.get(assignee, 0) + 1
                overdue_tasks.append((task["name"], assignee))
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
        all_assignees = sorted(set(open_by_assignee) | set(overdue_by_assignee) | set(completed_this_week_by_assignee))
        if not all_assignees:
            lines.append("Nothing open or completed this week.")
        for person in all_assignees:
            lines.append(
                f"*{person}* - open: {open_by_assignee.get(person, 0)}, "
                f"overdue: {overdue_by_assignee.get(person, 0)}, "
                f"completed this week: {completed_this_week_by_assignee.get(person, 0)}"
            )
        if overdue_tasks:
            lines.append("\n*Overdue (open >1 business day, no status change):*")
            for name, assignee in overdue_tasks:
                lines.append(f"- {name} ({assignee})")
        if blocked_without_reason:
            lines.append("\n*Blocked with no reason noted:*")
            for name, assignee in blocked_without_reason:
                lines.append(f"- {name} ({assignee})")
        send_slack_dm("\n".join(lines))
        print("Sent Friday digest.")

    # --- Monthly counts, same current/closed lock pattern as everything else ---
    already_closed = fetch_closed_months()
    status = "closed" if month_key in already_closed else "current"
    all_people = sorted(set(monthly_assigned) | set(monthly_completed) | set(overdue_by_assignee))
    for person in all_people:
        assigned = monthly_assigned.get(person, 0)
        completed = monthly_completed.get(person, 0)
        days_list = monthly_days_open.get(person, [])
        avg_days = round(sum(days_list) / len(days_list), 1) if days_list else None
        overdue = overdue_by_assignee.get(person, 0)
        upsert_monthly_count(month_key, person, assigned, completed, avg_days, overdue, status)
    print(f"Upserted monthly counts for {len(all_people)} people ({month_key}, {status}).")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-digest", action="store_true",
                         help="Send the digest regardless of day-of-week, for testing.")
    args = parser.parse_args()
    main(force_digest=args.force_digest)
