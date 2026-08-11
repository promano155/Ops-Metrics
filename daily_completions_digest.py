"""
daily_completions_digest.py

Daily Completions Digest for Data Processing Requests.

Reports:

1. Week-to-date completed count.
2. Month-to-date completed count.
3. Currently in-progress items by section, EXCLUDING Backlog.
4. Each in-progress task/subtask name and the number of calendar days
   it has been open.

"In progress" follows the existing project-specific rules:

- If a top-level task has subtasks, the subtasks are the units of work.
- If the parent is complete, its subtasks are treated as resolved for
  in-progress purposes.
- If a top-level task has no subtasks, the top-level task itself is the
  unit of work.
- Backlog is excluded.
- Multi-homed tasks are deduplicated by Asana GID.

"Completed" means Asana's native completed/completed_at fields.

Week boundary:
Monday through today.

Month boundary:
First calendar day of the current month through today.

Slack destination:
#ops-team-only

Use --dry-run to print the Slack message without sending it.

Use --list-completed to include the names of tasks completed this week.
"""

import argparse
import datetime as dt
import os
import time

import requests


ASANA_TOKEN = os.environ["ASANA_PAT"]
PROJECT_GID = "1207448572741662"  # Data Processing Requests
EXCLUDED_SECTION = "Backlog"

OPT_FIELDS = (
    "name,"
    "completed,"
    "completed_at,"
    "created_at,"
    "memberships.section.name"
)

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = "C06C898CN4C"  # #ops-team-only

LIST_COMPLETED = False
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
            print(
                f"Rate limited, waiting {wait}s "
                f"(attempt {attempt + 1}/5)"
            )
            time.sleep(wait)
            continue

        if not resp.ok:
            print(
                f"Request failed ({resp.status_code}): "
                f"{resp.text}"
            )
            resp.raise_for_status()

        return resp.json()

    raise RuntimeError("Still rate limited after 5 retries")


def fetch_top_level_tasks():
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


def fetch_subtasks(parent_gid):
    subtasks = []

    params = {
        "opt_fields": OPT_FIELDS,
        "limit": 100,
    }

    while True:
        body = asana_get(
            f"/tasks/{parent_gid}/subtasks",
            params,
        )

        subtasks.extend(body["data"])

        next_page = body.get("next_page")
        if not next_page:
            break

        params = {
            "opt_fields": OPT_FIELDS,
            "limit": 100,
            "offset": next_page["offset"],
        }

        time.sleep(0.2)

    return subtasks


def get_section_name(task):
    for membership in task.get("memberships", []):
        section = membership.get("section")

        if section:
            return section.get("name")

    return None


def get_days_open(task, today):
    """
    Return calendar days since task creation.

    Asana timestamps are ISO strings such as:
    2026-08-01T14:23:10.123Z
    """
    created_at = task.get("created_at")

    if not created_at:
        return None

    created_date = dt.date.fromisoformat(
        created_at[:10]
    )

    return max(
        0,
        (today - created_date).days,
    )


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
        raise RuntimeError(
            f"Slack API error: {body}"
        )


def main():
    today = dt.date.today()

    week_start = today - dt.timedelta(
        days=today.weekday()
    )

    month_start = today.replace(day=1)

    print("Fetching all top-level tasks...")

    top_level = fetch_top_level_tasks()

    print(
        f"Found {len(top_level)} top-level tasks. "
        "Fetching subtasks for each..."
    )

    # Some tasks are MULTI-HOMED: a subtask of its batch parent AND
    # independently a direct member of the project itself.
    #
    # seen_gids ensures every Asana task is only counted once.

    seen_gids = set()
    all_items_for_completion = []
    in_progress_items_by_section = {}

    def register(item):
        if item["gid"] in seen_gids:
            return False

        seen_gids.add(item["gid"])
        all_items_for_completion.append(item)

        return True

    def register_in_progress(item, section_name):
        if section_name == EXCLUDED_SECTION:
            return

        days_open = get_days_open(
            item,
            today,
        )

        in_progress_items_by_section.setdefault(
            section_name,
            [],
        ).append(
            {
                "name": item.get(
                    "name",
                    "(unnamed task)",
                ),
                "days_open": days_open,
            }
        )

    for i, task in enumerate(top_level):
        is_new_task = register(task)

        subtasks = fetch_subtasks(
            task["gid"]
        )

        parent_section_name = get_section_name(
            task
        )

        if subtasks:
            # If the parent batch itself is complete, treat its
            # subtasks as resolved for in-progress purposes.

            parent_is_done = task.get(
                "completed",
                False,
            )

            for sub in subtasks:
                sub_is_new = register(sub)

                sub_section_name = (
                    get_section_name(sub)
                    or parent_section_name
                )

                if sub_section_name == EXCLUDED_SECTION:
                    continue

                if (
                    sub_is_new
                    and not parent_is_done
                    and not sub.get("completed")
                ):
                    register_in_progress(
                        sub,
                        sub_section_name,
                    )

        else:
            # Standalone task: the top-level task itself is the
            # unit of work.

            if (
                is_new_task
                and parent_section_name != EXCLUDED_SECTION
                and not task.get("completed")
            ):
                register_in_progress(
                    task,
                    parent_section_name,
                )

        if (i + 1) % 20 == 0:
            print(
                f"  ...processed "
                f"{i + 1}/{len(top_level)} parents, "
                f"{len(all_items_for_completion)} "
                "items so far"
            )

        time.sleep(0.15)

    print(
        "Total items "
        "(top-level + subtasks): "
        f"{len(all_items_for_completion)}"
    )

    completed_this_week = 0
    completed_this_month = 0
    completed_names = []

    for item in all_items_for_completion:
        if (
            not item.get("completed")
            or not item.get("completed_at")
        ):
            continue

        completed_date = dt.date.fromisoformat(
            item["completed_at"][:10]
        )

        if completed_date >= month_start:
            completed_this_month += 1

        if completed_date >= week_start:
            completed_this_week += 1
            completed_names.append(
                item.get(
                    "name",
                    "(unnamed task)",
                )
            )

    in_progress_counts = {
        section: len(items)
        for section, items
        in in_progress_items_by_section.items()
    }

    print(
        f"Completed since "
        f"{week_start.isoformat()}: "
        f"{completed_this_week}"
    )

    print(
        f"Completed since "
        f"{month_start.isoformat()}: "
        f"{completed_this_month}"
    )

    print(
        f"In progress by section "
        f"(excl. {EXCLUDED_SECTION}): "
        f"{in_progress_counts}"
    )

    # Build the Slack message.
    lines = [
        "*Daily Completions Digest*",
        "",
        "*Completed*",
        (
            f"Week of "
            f"{week_start.strftime('%b %-d')}: "
            f"*{completed_this_week}*"
        ),
        (
            f"Month to date "
            f"({month_start.strftime('%b %-d')} - "
            f"{today.strftime('%b %-d')}): "
            f"*{completed_this_month}*"
        ),
    ]

    if LIST_COMPLETED and completed_names:
        lines.append("")
        lines.append(
            "*Completed this week:*"
        )

        for name in sorted(completed_names):
            lines.append(
                f"  - {name}"
            )

    lines += [
        "",
        (
            "*Currently in progress* "
            f"(excludes {EXCLUDED_SECTION})"
        ),
    ]

    if not in_progress_items_by_section:
        lines.append(
            "Nothing in progress outside Backlog."
        )

    else:
        # Sections with the most work appear first.
        sorted_sections = sorted(
            in_progress_items_by_section.items(),
            key=lambda kv: -len(kv[1]),
        )

        for section, items in sorted_sections:
            section_display = (
                section or "(no section)"
            )

            lines.append("")
            lines.append(
                f"*{section_display} "
                f"({len(items)})*"
            )

            # Oldest items first.
            sorted_items = sorted(
                items,
                key=lambda item: (
                    item["days_open"]
                    if item["days_open"] is not None
                    else -1
                ),
                reverse=True,
            )

            for item in sorted_items:
                days_open = item[
                    "days_open"
                ]

                if days_open is None:
                    age_text = (
                        "age unavailable"
                    )
                elif days_open == 0:
                    age_text = "opened today"
                elif days_open == 1:
                    age_text = "1 day open"
                else:
                    age_text = (
                        f"{days_open} days open"
                    )

                lines.append(
                    f"  - {item['name']} "
                    f"— {age_text}"
                )

    message = "\n".join(lines)

    # Dry run: fetch live Asana data and construct the real message,
    # but do NOT send anything to Slack.
    if DRY_RUN:
        print("")
        print("=" * 60)
        print("DRY RUN - Slack message NOT sent")
        print("=" * 60)
        print(message)
        print("=" * 60)
        return

    send_slack_message(message)

    print(
        "Daily Completions Digest sent "
        "to #ops-team-only."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--list-completed",
        action="store_true",
        help=(
            "Include the names of tasks "
            "completed this week."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print the Slack message without "
            "actually sending it."
        ),
    )

    args = parser.parse_args()

    LIST_COMPLETED = args.list_completed
    DRY_RUN = args.dry_run

    main()
