"""
decouple_moved_subtasks.py

Detaches subtasks from their parents to match the new no-parent, fully-
standalone task design going forward - scoped to ACTIVE parents only.

Two exclusions, both deliberate:
- Parent's section is "Backlog" - maintained manually going forward, by
  choice, left batched exactly as it is.
- Parent is marked completed/archived - narrowed to this after seeing
  the full unrestricted preview: decoupling months of old, already-
  closed batches (Nov 2025, etc.) produced a wall of noise with no real
  benefit, since nothing's actively being worked on those anymore.

Originally built even narrower (only subtasks whose own section no
longer matched their parent's, from an assignee-triggered rule moving
them elsewhere), then briefly broadened to everything before narrowing
back here. Same root mechanism throughout: a rule-driven "move" changes
a task's own section membership but never touches a pre-existing parent
relationship, so old nested structures don't self-heal on their own.

AUTO_DECOUPLE repo variable ("true"/"false", default false/preview-only)
controls the SCHEDULED daily run. A manual dispatch can override this
for a one-off real run via the run_for_real input, regardless of what
the repo variable is currently set to. Always sends a Slack DM summary,
whichever mode it's running in.
"""

import os
import time
import datetime as dt

import requests

ASANA_TOKEN = os.environ["ASANA_PAT"]
PROJECT_GID = "1207448572741662"  # Data Processing Requests
BACKLOG_SECTION_NAME = "Backlog"
OPT_FIELDS = "name,completed,memberships.section.name,permalink_url"

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_DM_USER_ID = "U0BBU2YRQ72"  # Pia

AUTO_DECOUPLE = os.environ.get("AUTO_DECOUPLE", "false").strip().lower() == "true"


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


def asana_post(path, json_payload):
    url = f"https://app.asana.com/api/1.0{path}"
    for attempt in range(5):
        resp = requests.post(url, headers=asana_headers(), json=json_payload, timeout=30)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"Rate limited, waiting {wait}s (attempt {attempt + 1}/5)")
            time.sleep(wait)
            continue
        if not resp.ok:
            print(f"Request failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()
        return
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


def decouple_task(task_gid):
    asana_post(f"/tasks/{task_gid}/setParent", {"data": {"parent": None}})


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
    print(f"AUTO_DECOUPLE is {'ON' if AUTO_DECOUPLE else 'OFF'}.")
    print("Fetching all top-level tasks...")
    top_level = fetch_top_level_tasks()
    print(f"Found {len(top_level)} top-level tasks. Fetching subtasks for each...")

    to_decouple = []  # (subtask, parent_name, parent_section, sub_section)

    for i, task in enumerate(top_level):
        subtasks = fetch_subtasks(task["gid"])
        if subtasks:
            parent_section = get_section_name(task)
            if parent_section == BACKLOG_SECTION_NAME or task.get("completed"):
                # Backlog: maintained manually, left batched on purpose.
                # Completed/archived parent: narrowing scope after
                # seeing the full preview - decoupling months of old,
                # already-closed batches produced too much noise to be
                # useful. Only active (non-completed, non-Backlog)
                # parents get decoupled now.
                continue
            for sub in subtasks:
                to_decouple.append((sub, task["name"], parent_section, get_section_name(sub)))
        if (i + 1) % 20 == 0:
            print(f"  ...processed {i + 1}/{len(top_level)} parents")
        time.sleep(0.15)

    today = dt.date.today().isoformat()
    if not to_decouple:
        send_slack_dm(f"*Decouple check - {today}*\nNothing to decouple - no subtasks remain nested under "
                       f"an active, non-Backlog parent. :white_check_mark:")
        print("Nothing to decouple.")
        return

    lines = [f"*Decouple check - {today}*", f"Found {len(to_decouple)} subtask(s) still nested under a "
             f"parent whose section no longer matches their own.\n"]

    for sub, parent_name, parent_section, sub_section in to_decouple:
        if AUTO_DECOUPLE:
            decouple_task(sub["gid"])
            time.sleep(0.2)
            lines.append(f"- *{sub['name']}*: detached from '{parent_name}' "
                         f"({parent_section} -> now standalone in {sub_section})")
            print(f"Decoupled '{sub['name']}' from '{parent_name}'")
        else:
            lines.append(f"- *{sub['name']}*: still nested under '{parent_name}' ({parent_section}), "
                         f"but its own section is {sub_section} [preview only]")

    lines.append("")
    if AUTO_DECOUPLE:
        lines.append(f"_AUTO_DECOUPLE is ON - {len(to_decouple)} subtask(s) detached just now._")
    else:
        lines.append(f"_AUTO_DECOUPLE is OFF - preview only, nothing was changed. "
                      f"Set the AUTO_DECOUPLE repo variable to 'true' to enable._")

    send_slack_dm("\n".join(lines))
    print(f"Digest sent. {len(to_decouple)} subtask(s) {'decoupled' if AUTO_DECOUPLE else 'flagged'}.")


if __name__ == "__main__":
    main()
