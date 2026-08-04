"""
backfill_revenue_impact.py

ONE-TIME backfill, not part of any daily automation. Scans the Data
Processing Requests project (both top-level tasks and their subtasks,
since individual hotels live as subtasks under batch parents), and for
every CURRENTLY-OPEN task/subtask with an empty "Monthly Revenue Impact"
field, fills it in from the most recent PAST occurrence of that same
hotel name that already has a value - completed or not.

Only ever reads/writes "Monthly Revenue Impact". "Repeat Instance Count"
and every other field are left completely untouched, on purpose.

Only touches currently-open items - completed history is read from, but
never written to.

Run --dry-run first. This writes to real, live tasks - not something to
run blind.

Usage:
    python backfill_revenue_impact.py --dry-run
    python backfill_revenue_impact.py          # the real thing
"""

import os
import re
import time
import argparse
import datetime as dt

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ASANA_TOKEN = os.environ["ASANA_PAT"]
PROJECT_GID = "1207448572741662"  # Data Processing Requests
REVENUE_FIELD_GID = "1216668700417665"  # Monthly Revenue Impact (number, USD)

# ---------------------------------------------------------------------------
# Asana
# ---------------------------------------------------------------------------


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


def asana_set_revenue_impact(task_gid, value):
    url = f"https://app.asana.com/api/1.0/tasks/{task_gid}"
    payload = {"data": {"custom_fields": {REVENUE_FIELD_GID: value}}}
    resp = requests.put(url, headers=asana_headers(), json=payload, timeout=30)
    if not resp.ok:
        print(f"Request failed ({resp.status_code}): {resp.text}")
    resp.raise_for_status()


def get_revenue_value(task):
    for cf in task.get("custom_fields", []):
        if cf.get("gid") == REVENUE_FIELD_GID:
            return cf.get("number_value")
    return None


def normalize_name(name):
    """Strips the old 'Follow up: ' prefix some older tasks still carry,
    so they match cleanly against newer plain-hotel-name entries."""
    name = re.sub(r"^follow up:\s*", "", name.strip(), flags=re.IGNORECASE)
    return name.strip().lower()


OPT_FIELDS = "name,completed,created_at,custom_fields.gid,custom_fields.number_value"


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
        params = {"offset": next_page["offset"], "opt_fields": OPT_FIELDS, "limit": 100}
        time.sleep(0.2)
    return subtasks


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(dry_run=False):
    print("Fetching all top-level tasks...")
    top_level = fetch_top_level_tasks()
    print(f"Found {len(top_level)} top-level tasks. Fetching subtasks for each...")

    all_items = list(top_level)  # top-level tasks are candidates too (real one-offs)
    for i, task in enumerate(top_level):
        subtasks = fetch_subtasks(task["gid"])
        all_items.extend(subtasks)
        if (i + 1) % 20 == 0:
            print(f"  ...processed {i + 1}/{len(top_level)} parents, {len(all_items)} items so far")
        time.sleep(0.15)

    print(f"Total items (top-level + subtasks): {len(all_items)}")

    # Build history: normalized name -> list of (value, created_at), from
    # EVERY item with a value set, completed or not.
    history = {}
    for item in all_items:
        value = get_revenue_value(item)
        if value is None:
            continue
        key = normalize_name(item["name"])
        history.setdefault(key, []).append((value, item["created_at"]))

    # Candidates: currently OPEN items with no value set yet.
    open_candidates = [
        item for item in all_items
        if not item.get("completed") and get_revenue_value(item) is None
    ]
    print(f"\n{len(open_candidates)} currently-open items have no Monthly Revenue Impact set.")

    filled = 0
    no_history = []

    for item in open_candidates:
        key = normalize_name(item["name"])
        matches = history.get(key)
        if not matches:
            no_history.append(item["name"])
            continue
        # Most recent by created_at.
        matches_sorted = sorted(matches, key=lambda m: m[1], reverse=True)
        value, source_created_at = matches_sorted[0]
        if dry_run:
            print(f"[DRY RUN] Would set '{item['name']}' -> ${value:.2f} "
                  f"(from {len(matches)} prior occurrence(s), most recent {source_created_at})")
        else:
            asana_set_revenue_impact(item["gid"], value)
            print(f"Set '{item['name']}' -> ${value:.2f} "
                  f"(from {len(matches)} prior occurrence(s), most recent {source_created_at})")
            time.sleep(0.2)
        filled += 1

    print(f"\n{'Would fill' if dry_run else 'Filled'} {filled} of {len(open_candidates)} open items.")
    if no_history:
        print(f"\n{len(no_history)} open items have NO prior history anywhere - these need manual entry:")
        for name in no_history:
            print(f"  - {name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                         help="Preview what would be filled in, without writing anything.")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
