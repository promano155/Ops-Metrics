"""
sla_breach_digest.py

Consolidated SLA breach digest for Data Processing Requests.

Replaces the native Asana rule that used to post an SLA breach alert to
#data-processing directly from the "Priority (Within 24hrs)" section.
That rule was disabled because every hotel is now a standalone top-level
task (see sync_yellow_rows_to_asana.py) - so a per-task rule meant one
Slack message PER HOTEL, and volume made the channel unusable.

This script instead does one pass, one message: it looks at both SLA
sections -

- "Priority (Within 24hrs)"
- "48 hr SLA"

- and reports ONLY the tasks that are actually past due right now (not
completed, and due_at/due_on earlier than the moment this runs). Tasks
that are within their SLA window are not mentioned at all - this is a
"what needs attention" list, not a full section dump. (The full
breakdown of everything outstanding by team already exists elsewhere,
per the #ops-team-only breakdown bot - this is deliberately narrower.)

Due date handling:
- Uses due_at (full timestamp) when present - this is what
  sync_yellow_rows_to_asana.py stamps on "48 hr SLA" tasks.
- Falls back to due_on (date-only, midnight UTC) when due_at isn't set -
  this is what the native Asana rule on "Priority (Within 24hrs)" sets.

Slack destination:
#data-processing (the Innova/data-partner channel)

Use --dry-run to print the digest without posting to Slack.
"""

import argparse
import datetime as dt
import os
import time

import requests


ASANA_TOKEN = os.environ["ASANA_PAT"]
PROJECT_GID = "1207448572741662"  # Data Processing Requests

PRIORITY_SECTION_NAME = "Priority (Within 24hrs)"
STANDARD_SECTION_NAME = "48 hr SLA"
SLA_SECTIONS = (PRIORITY_SECTION_NAME, STANDARD_SECTION_NAME)

OPT_FIELDS = (
    "name,"
    "completed,"
    "due_at,"
    "due_on,"
    "assignee.name,"
    "memberships.section.name"
)

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = "C06FNRQBQRE"  # #data-processing

DRY_RUN = False


def asana_headers():
    return {
        "Authorization": f"Bearer {ASANA_TOKEN}",
        "Content-Type": "application/json",
    }


def asana_get(path, params):
    url = f"https://app.asana.com/api/1.0{path}"

    for attempt in range(5):
        resp = requests.get(
            url,
            headers=asana_headers(),
            params=params,
            timeout=30,
        )

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


def fetch_all_tasks():
    tasks = []

    params = {
        "project": PROJECT_GID,
        "opt_fields": OPT_FIELDS,
        "limit": 100,
    }

    while True:
        body = asana_get("/tasks", params)
        tasks.extend(body["data"])

        next_page = body.get("next_page")
        if not next_page:
            break

        params = {
            "project": PROJECT_GID,
            "opt_fields": OPT_FIELDS,
            "limit": 100,
            "offset": next_page["offset"],
        }

        time.sleep(0.2)

    return tasks


def get_section_name(task):
    for membership in task.get("memberships", []):
        section = membership.get("section")
        if section:
            return section.get("name")
    return None


def get_due_datetime(task):
    """Prefer due_at (full timestamp, set by sync_yellow_rows_to_asana.py
    on '48 hr SLA' tasks). Fall back to due_on (date-only, set by the
    native Asana rule on 'Priority (Within 24hrs)'), treated as due at
    midnight UTC that day."""
    due_at = task.get("due_at")
    if due_at:
        return dt.datetime.fromisoformat(due_at.replace("Z", "+00:00"))

    due_on = task.get("due_on")
    if due_on:
        return dt.datetime.fromisoformat(due_on + "T00:00:00+00:00")

    return None


def format_overdue(now, due):
    delta = now - due
    hours = delta.total_seconds() / 3600

    if hours < 1:
        return "just passed SLA"
    if hours < 24:
        return f"{int(hours)}h past due"

    days = int(hours // 24)
    return f"{days}d past due"


def send_slack_message(text):
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        },
        json={
            "channel": SLACK_CHANNEL_ID,
            "text": text,
        },
        timeout=30,
    )

    resp.raise_for_status()

    body = resp.json()
    if not body.get("ok"):
        raise RuntimeError(f"Slack API error: {body}")


def main():
    now = dt.datetime.now(dt.timezone.utc)

    print("Fetching Data Processing Requests tasks...")
    tasks = fetch_all_tasks()
    print(f"Fetched {len(tasks)} tasks. Filtering to past-due SLA tasks...")

    overdue_by_section = {name: [] for name in SLA_SECTIONS}

    for task in tasks:
        if task.get("completed"):
            continue

        section_name = get_section_name(task)
        if section_name not in SLA_SECTIONS:
            continue

        due = get_due_datetime(task)
        if due is None or due >= now:
            continue

        overdue_by_section[section_name].append(
            {
                "name": task.get("name", "(unnamed task)"),
                "assignee": (task.get("assignee") or {}).get("name", "Unassigned"),
                "due": due,
            }
        )

    total_overdue = sum(len(v) for v in overdue_by_section.values())
    print(f"Found {total_overdue} past-due task(s) across SLA sections.")

    lines = ["*SLA Breach Digest — needs attention*"]

    if total_overdue == 0:
        lines.append("")
        lines.append("Nothing past due right now. :white_check_mark:")
    else:
        for section_name in SLA_SECTIONS:
            items = overdue_by_section[section_name]
            if not items:
                continue

            # Most overdue first.
            items.sort(key=lambda item: item["due"])

            lines.append("")
            lines.append(f"*{section_name} ({len(items)})*")

            for item in items:
                overdue_text = format_overdue(now, item["due"])
                lines.append(
                    f"  - {item['name']} — {overdue_text} — {item['assignee']}"
                )

    message = "\n".join(lines)

    if DRY_RUN:
        print("")
        print("=" * 60)
        print("DRY RUN - Slack message NOT sent")
        print("=" * 60)
        print(message)
        print("=" * 60)
        return

    if total_overdue == 0:
        print("Nothing past due - skipping Slack post (no noise on quiet days).")
        return

    send_slack_message(message)
    print("SLA Breach Digest sent to #data-processing.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the digest without actually sending it.",
    )

    args = parser.parse_args()

    DRY_RUN = args.dry_run

    main()
