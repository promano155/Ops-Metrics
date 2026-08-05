"""
delete_duplicate_hotel_tasks.py

Companion to find_duplicate_hotel_tasks.py - same exact grouping logic
(same hotel name, same calendar month based on created_at), so the two
scripts can never disagree on what counts as a duplicate.

Resolution rule: if ANY occurrence in a duplicate group currently lives
under a parent in the "Priority (Within 24hrs)" section, that one is
ALWAYS kept (earliest among Priority ones, if more than one) and every
other occurrence is deleted - regardless of creation order. This matters
because a hotel that became priority after already being routed into a
regular batch should have the Priority copy win, not whichever was
created first. Only falls back to plain "keep earliest" when no
occurrence in the group is in Priority at all.

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
PRIORITY_SECTION_NAME = "Priority (Within 24hrs)"
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


def get_task_section(task_gid, cache):
    """Looks up which section a task (a batch/summary parent) currently
    sits in, caching by gid so each unique parent is only fetched once
    even though it may show up across many duplicate groups."""
    if task_gid in cache:
        return cache[task_gid]
    url = f"https://app.asana.com/api/1.0/tasks/{task_gid}"
    resp = requests.get(url, headers=asana_headers(), params={"opt_fields": "memberships.section.name"}, timeout=30)
    resp.raise_for_status()
    memberships = resp.json()["data"].get("memberships", [])
    section_name = None
    for m in memberships:
        section = m.get("section")
        if section:
            section_name = section.get("name")
            break
    cache[task_gid] = section_name
    return section_name


def choose_keeper(items, section_cache):
    """Priority-section occurrences always win, regardless of creation
    order - a hotel that became priority after already being routed into
    a regular batch should keep the Priority copy, not whichever
    happened to be created first. Falls back to keep-earliest only when
    nothing in the group is in Priority at all."""
    priority_items = []
    for item in items:
        parent = item.get("parent")
        if not parent:
            continue
        section_name = get_task_section(parent["gid"], section_cache)
        if section_name and section_name.strip().lower() == PRIORITY_SECTION_NAME.strip().lower():
            priority_items.append(item)

    if priority_items:
        priority_items.sort(key=lambda t: t["created_at"])
        return priority_items[0], "kept: in Priority section"

    items_sorted = sorted(items, key=lambda t: t["created_at"])
    return items_sorted[0], "kept: earliest (no Priority-section occurrence in this group)"


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
          f"{'would delete' if dry_run else 'deleting'} {total_to_delete} extra task(s). "
          f"Priority-section occurrences are always kept over regular batches.\n")

    section_cache = {}
    deleted = 0
    for (hotel, month), items in sorted(duplicate_groups.items(), key=lambda kv: -len(kv[1])):
        keep, reason = choose_keeper(items, section_cache)
        to_delete = [item for item in items if item["gid"] != keep["gid"]]

        print(f"'{hotel.title()}' - {month}:")
        print(f"    KEEPING ({reason}): {keep['created_at']}  {keep['permalink_url']}")
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
