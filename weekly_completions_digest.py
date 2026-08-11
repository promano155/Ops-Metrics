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
SLACK_CHANNEL_ID = "C06C898CN4C"  # ops-team-only
LIST_COMPLETED = False  # set via --list-completed, one-time use only


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

    # Some tasks are MULTI-HOMED: a subtask of its batch parent AND
    # independently a direct member of the project itself (that's what
    # having its own section membership requires). Without dedup, such
    # a task gets fetched and counted TWICE - once via fetch_top_level_tasks
    # directly, once again via fetch_subtasks() on its parent. Confirmed
    # real via direct comparison: Asana showed 4 real tasks in Transferred
    # to Integrations, the undeduped script reported exactly 8 - a clean
    # 2x, not a rounding error. seen_gids ensures each task is only ever
    # counted once, regardless of which path encounters it first.
    seen_gids = set()
    all_items_for_completion = []
    in_progress_by_section = {}

    def register(item):
        if item["gid"] in seen_gids:
            return False
        seen_gids.add(item["gid"])
        all_items_for_completion.append(item)
        return True

    for i, task in enumerate(top_level):
        is_new_task = register(task)
        subtasks = fetch_subtasks(task["gid"])

        parent_section_name = get_section_name(task)
        if subtasks:
            # If the PARENT batch itself is marked complete, treat every
            # subtask as resolved for in-progress purposes - regardless
            # of whether each individual hotel's own checkbox was ever
            # ticked. Confirmed real: old batches (May/April/March/Nov/
            # Dec) marked completed=true at the container level, sitting
            # right in Priority/48hr SLA, with dozens of subtasks never
            # individually checked off - that alone was inflating "in
            # progress" counts with months-old, already-closed work.
            parent_is_done = task.get("completed", False)
            for sub in subtasks:
                sub_is_new = register(sub)
                sub_section_name = get_section_name(sub) or parent_section_name
                if sub_section_name == EXCLUDED_SECTION:
                    continue
                if sub_is_new and not parent_is_done and not sub.get("completed"):
                    in_progress_by_section[sub_section_name] = in_progress_by_section.get(sub_section_name, 0) + 1
        else:
            # No subtasks (a standalone item, e.g. Email Contact) - the
            # task itself is the unit of work.
            if is_new_task and parent_section_name != EXCLUDED_SECTION and not task.get("completed"):
                in_progress_by_section[parent_section_name] = in_progress_by_section.get(parent_section_name, 0) + 1

        if (i + 1) % 20 == 0:
            print(f"  ...processed {i + 1}/{len(top_level)} parents, "
                  f"{len(all_items_for_completion)} items so far")
        time.sleep(0.15)

    print(f"Total items (top-level + subtasks): {len(all_items_for_completion)}")

    completed_this_week = 0
    completed_names = []
    for item in all_items_for_completion:
        if not item.get("completed") or not item.get("completed_at"):
            continue
        completed_date = dt.date.fromisoformat(item["completed_at"][:10])
        if completed_date >= week_start:
            completed_this_week += 1
            completed_names.append(item["name"])

    print(f"Completed since {week_start.isoformat()}: {completed_this_week}")
    print(f"In progress by section (excl. {EXCLUDED_SECTION}): {in_progress_by_section}")

    lines = [
        "*Data Processing Requests - weekly digest*",
        f"Week of {week_start.strftime('%b %-d')}: *{completed_this_week}* task(s) completed so far.",
    ]
    if LIST_COMPLETED and completed_names:
        lines.append("")
        lines.append("*Completed this week:*")
        for name in sorted(completed_names):
            lines.append(f"  - {name}")
    lines += [
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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--list-completed", action="store_true",
                         help="One-time use: include actual hotel/task names completed this week in the Slack message.")
    args = parser.parse_args()
    LIST_COMPLETED = args.list_completed
    main()
