"""
delete_duplicate_hotel_tasks.py

Companion to find_duplicate_hotel_tasks.py - same exact grouping logic
(same hotel name, same calendar month based on created_at), so the two
scripts can never disagree on what counts as a duplicate.

For every group with more than one occurrence, keeps the EARLIEST one
and deletes the rest.

Run --dry-run first. Deletion is irreversible - this is not something to
run blind, even though it's scoped narrowly to genuine duplicates.

Known side effect, not fixed here on purpose (kept simple): deleting a
subtask does NOT decrement that batch's task_count stored in Supabase's
asana_batch_sections table. That count will run slightly high afterward,
which just means a batch might close a little earlier than strictly
necessary on a future run - a minor efficiency issue, not a correctness
one, and not worth the complexity of correlating each deletion back to
its batch's row in this pass.

Usage:
    python delete_duplicate_hotel_tasks.py --dry-run
    python delete_duplicate_hotel_tasks.py          # the real thing
"""

import os
import re
import time
import argparse

import requests

ASANA_TOKEN = os.environ["ASANA_PAT"]
PROJECT_GID = "1207448572741662"  # Data Processing Requests
OPT_FIELDS = "name,created_at,parent.name,permalink_url"


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


def asana_delete_task(task_gid):
    url = f"https://app.asana.com/api/1.0/tasks/{task_gid}"
    for attempt in range(5):
        resp = requests.delete(url, headers=asana_headers(), timeout=30)
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


def normalize_name(name):
    name = re.sub(r"^follow up:\s*", "", name.strip(), flags=re.IGNORECASE)
    return name.strip().lower()


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


def main(dry_run=False):
    print("Fetching all top-level tasks...")
    top_level = fetch_top_level_tasks()
    print(f"Found {len(top_level)} top-level tasks. Fetching subtasks for each...")

    all_subtasks = []
    for i, task in enumerate(top_level):
        subtasks = fetch_subtasks(task["gid"])
        all_subtasks.extend(subtasks)
        if (i + 1) % 20 == 0:
            print(f"  ...processed {i + 1}/{len(top_level)} parents, {len(all_subtasks)} subtasks so far")
        time.sleep(0.15)

    print(f"Total subtasks found: {len(all_subtasks)}\n")

    groups = {}
    for item in all_subtasks:
        key = (normalize_name(item["name"]), item["created_at"][:7])  # YYYY-MM
        groups.setdefault(key, []).append(item)

    duplicate_groups = {k: v for k, v in groups.items() if len(v) > 1}

    if not duplicate_groups:
        print("No duplicates found - nothing to delete.")
        return

    total_to_delete = sum(len(v) - 1 for v in duplicate_groups.values())
    print(f"Found {len(duplicate_groups)} hotel(s) with duplicates - "
          f"{'would delete' if dry_run else 'deleting'} {total_to_delete} extra task(s), "
          f"keeping the earliest occurrence in each group.\n")

    deleted = 0
    for (hotel, month), items in sorted(duplicate_groups.items(), key=lambda kv: -len(kv[1])):
        items_sorted = sorted(items, key=lambda t: t["created_at"])
        keep = items_sorted[0]
        to_delete = items_sorted[1:]

        print(f"'{hotel.title()}' - {month}:")
        print(f"    KEEPING: {keep['created_at']}  {keep['permalink_url']}")
        for item in to_delete:
            parent_name = (item.get("parent") or {}).get("name", "(no parent)")
            if dry_run:
                print(f"    [DRY RUN] Would delete: {item['created_at']}  under '{parent_name}'  "
                      f"{item['permalink_url']}")
            else:
                asana_delete_task(item["gid"])
                print(f"    DELETED: {item['created_at']}  under '{parent_name}'  {item['permalink_url']}")
                time.sleep(0.2)
            deleted += 1
        print()

    print(f"{'Would delete' if dry_run else 'Deleted'} {deleted} duplicate task(s) total.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                         help="Preview what would be deleted, without deleting anything.")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
