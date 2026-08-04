"""
find_duplicate_hotel_tasks.py

Read-only sweep of Data Processing Requests. Reports any hotel name that
appears more than once AS A SUBTASK within the same calendar month
(based on each subtask's own created_at) - that's the actual duplicate
signature. The same hotel appearing in different months (June AND July)
is expected and NOT flagged - that's just the normal monthly cycle, not
a bug.

Does not delete anything. Prints a clear report grouped by hotel+month,
with each duplicate's parent batch name and a direct Asana link, so they
can be reviewed and removed by hand (or ask for a companion delete
script once you've reviewed this list).
"""

import os
import re
import time

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
          f"{total_extra} extra task(s) beyond the first legitimate one:\n")

    for (hotel, month), items in sorted(duplicate_groups.items(), key=lambda kv: -len(kv[1])):
        print(f"'{hotel.title()}' - {month} - {len(items)} occurrences:")
        for item in sorted(items, key=lambda t: t["created_at"]):
            parent_name = (item.get("parent") or {}).get("name", "(no parent)")
            print(f"    - {item['created_at']}  under '{parent_name}'  {item['permalink_url']}")
        print()


if __name__ == "__main__":
    main()
