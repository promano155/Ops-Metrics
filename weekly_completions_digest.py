"""
weekly_completions_digest.py

Daily Slack DM reporting:
1. A running week-to-date count of completed tasks in Data Processing
   Requests - both top-level tasks (batch/summary parents, genuine
   one-offs) and subtasks (individual hotels), since the individual
   hotel subtasks are where most of the real completed work happens.
2. A count of currently in-progress items per section, EXCLUDING
   Backlog. "In progress" = not completed. If a top-level task has
   subtasks, only its incomplete SUBTASKS count (the parent itself is
   just a container, not a unit of work) - if it has no subtasks, the
   top-level task itself counts if incomplete. Real project sections as
   of writing: Priority (Within 24hrs), 48 hr SLA, Email Contact,
   Transferred to Integrations, Complete, Backlog (excluded).

"Completed" here means Asana's own native completion checkbox
(completed / completed_at) - this project doesn't use a Done-section
workflow the way Ops Task Tracker does, so the checkbox is the right
signal to use here specifically.

Week boundary: Monday through today, UTC calendar dates - resets each
Monday, grows day by day through the week. Sent as a Slack DM to Pia
only.
"""

import os
import time
import datetime as dt

import requests

ASANA_TOKEN = os.environ["ASANA_PAT"]
PROJECT_GID = "1207448572741662"  # Data Processing Requests
EXCLUDED_SECTION = "Backlog"
OPT_FIELDS = "name,completed,completed_at,memberships.section.name"

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_DM_USER_ID = "U0BBU2YRQ72"  # Pia


def asana_headers():
    return {"Authorization": f"Bearer {ASANA_TOKEN}", "Content-Type": "application/json"}


def asana_get(path, params):
    url = f"https://app.asana.com/api/1.0{path}"
    for attempt in range(5):
        resp = requests.get(url, headers=asana_headers(), params=params, timeout=30)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"Rate limited, waiting {wait}s (attempt {attempt + 1}/5)")
            time.sleep(wait)
            continue
        if not resp.ok:
            print(f"Request failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("Still rate limited after 5 retries")


def fetch_top_level_tasks():
    tasks = []
    params = {"project": PROJECT_GID, "opt_fields": OPT_FIELDS, "limit": 100}
    while True:
        body = asana_get("/tasks", params)
        tasks.extend(body["data"])
        next_page = body.get("next_page")
        if not next_page:
            break
        params = {"project": PROJECT_GID, "opt_fields": OPT_FIELDS, "limit": 100, "offset": next_page["offset"]}
        time.sleep(0.2)
    return tasks


def fetch_subtasks(parent_gid):
    subtasks = []
    params = {"opt_fields": OPT_FIELDS, "limit": 100}
    while True:
        body = asana_get(f"/tasks/{parent_gid}/subtasks", params)
        subtasks.extend(body["data"])
        next_page = body.get("next_page")
        if not next_page:
            break
        params = {"opt_fields": OPT_FIELDS, "limit": 100, "offset": next_page["offset"]}
        time.sleep(0.2)
    return subtasks


def get_section_name(task):
    for m in task.get("memberships", []):
        section = m.get("section")
        if section:
            return section.get("name")
    return None


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


def main():
    today = dt.date.today()
    week_start = today - dt.timedelta(days=today.weekday())  # Monday this week

    print("Fetching all top-level tasks...")
    top_level = fetch_top_level_tasks()
    print(f"Found {len(top_level)} top-level tasks. Fetching subtasks for each...")

    all_items_for_completion = list(top_level)
    in_progress_by_section = {}

    for i, task in enumerate(top_level):
        subtasks = fetch_subtasks(task["gid"])
        all_items_for_completion.extend(subtasks)  # unaffected by the Backlog exclusion below

        parent_section_name = get_section_name(task)
        if subtasks:
            # A subtask can be independently multi-homed into its OWN
            # section (e.g. "Transferred to Integrations"), separate
            # from its parent batch - that direct membership wins over
            # the parent's section whenever it's present. Confirmed real:
            # hotels showing as still-subtasks of "July Batch 1" while
            # ALSO sitting directly in Transferred to Integrations.
            for sub in subtasks:
                sub_section_name = get_section_name(sub) or parent_section_name
                if sub_section_name == EXCLUDED_SECTION:
                    continue
                if not sub.get("completed"):
                    in_progress_by_section[sub_section_name] = in_progress_by_section.get(sub_section_name, 0) + 1
        else:
            # No subtasks (a standalone item, e.g. Email Contact) - the
            # task itself is the unit of work.
            if parent_section_name != EXCLUDED_SECTION and not task.get("completed"):
                in_progress_by_section[parent_section_name] = in_progress_by_section.get(parent_section_name, 0) + 1

        if (i + 1) % 20 == 0:
            print(f"  ...processed {i + 1}/{len(top_level)} parents, "
                  f"{len(all_items_for_completion)} items so far")
        time.sleep(0.15)

    print(f"Total items (top-level + subtasks): {len(all_items_for_completion)}")

    completed_this_week = 0
    for item in all_items_for_completion:
        if not item.get("completed") or not item.get("completed_at"):
            continue
        completed_date = dt.date.fromisoformat(item["completed_at"][:10])
        if completed_date >= week_start:
            completed_this_week += 1

    print(f"Completed since {week_start.isoformat()}: {completed_this_week}")
    print(f"In progress by section (excl. {EXCLUDED_SECTION}): {in_progress_by_section}")

    lines = [
        "*Data Processing Requests - weekly digest*",
        f"Week of {week_start.strftime('%b %-d')}: *{completed_this_week}* task(s) completed so far.",
        "",
        f"*In progress by section* (excludes {EXCLUDED_SECTION}):",
    ]
    if not in_progress_by_section:
        lines.append("  Nothing in progress outside Backlog.")
    else:
        for section, count in sorted(in_progress_by_section.items(), key=lambda kv: -kv[1]):
            lines.append(f"  {section or '(no section)'}: *{count}*")

    send_slack_dm("\n".join(lines))
    print("Digest sent.")


if __name__ == "__main__":
    main()
