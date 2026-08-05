"""
find_duplicate_hotel_tasks.py

Read-only sweep of Data Processing Requests. Reports any hotel name that
appears more than once AS A SUBTASK within the same calendar month
(based on each subtask's own created_at) - that's the actual duplicate
signature. The same hotel appearing in different months (June AND July)
is expected and NOT flagged - that's just the normal monthly cycle, not
a bug.

Reports which occurrence WOULD be kept using the same priority-section-
aware rule as delete_duplicate_hotel_tasks.py: a Priority-section
occurrence always wins over a regular batch, regardless of creation
order, since a hotel that became priority after already being routed
into a batch should keep the Priority copy. Falls back to keep-earliest
only when nothing in the group is in Priority.

Does not delete anything. Prints a clear report grouped by hotel+month,
with each duplicate's parent batch name and a direct Asana link.
"""

import os
import re
import time

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


def get_task_section(task_gid, cache):
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
        return priority_items[0], "in Priority section"

    items_sorted = sorted(items, key=lambda t: t["created_at"])
    return items_sorted[0], "earliest (no Priority-section occurrence)"


def normalize_name(name):
    """Strips the old 'Follow up: ' prefix some older tasks still carry."""
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


def main():
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

    # Group by (normalized hotel name, calendar month of creation).
    groups = {}
    for item in all_subtasks:
        key = (normalize_name(item["name"]), item["created_at"][:7])  # YYYY-MM
        groups.setdefault(key, []).append(item)

    duplicate_groups = {k: v for k, v in groups.items() if len(v) > 1}

    if not duplicate_groups:
        print("No duplicates found - every hotel appears at most once per calendar month.")
        return

    total_extra = sum(len(v) - 1 for v in duplicate_groups.values())
    print(f"Found {len(duplicate_groups)} hotel(s) with duplicates, "
          f"{total_extra} extra task(s) beyond the one that would be kept:\n")

    section_cache = {}
    for (hotel, month), items in sorted(duplicate_groups.items(), key=lambda kv: -len(kv[1])):
        keep, reason = choose_keeper(items, section_cache)
        print(f"'{hotel.title()}' - {month} - {len(items)} occurrences:")
        for item in sorted(items, key=lambda t: t["created_at"]):
            parent_name = (item.get("parent") or {}).get("name", "(no parent)")
            marker = f"WOULD KEEP ({reason})" if item["gid"] == keep["gid"] else "would delete"
            print(f"    [{marker}] {item['created_at']}  under '{parent_name}'  {item['permalink_url']}")
        print()


if __name__ == "__main__":
    main()
