"""
cleanup_v1_transition_duplicates.py

ONE-TIME cleanup script - not part of the regular daily pipeline, not
scheduled, not imported by anything else. Run by hand, reviewed by eye,
then re-run with --confirm.

--- Why this exists ---
The regular pipeline's sweep_duplicate_hotel_tasks() (in
sync_yellow_rows_to_asana.py) already catches and deletes duplicate
hotel tasks automatically on every run - but ONLY when the earliest
task in a duplicate-name group is still OPEN. When the earliest task is
already COMPLETED, that sweep deliberately leaves the duplicates in
place for manual review:

    "leaving duplicates in place for manual review rather than
    assuming a completed hotel's extra task is a true duplicate"

That's the right call for ongoing operation (a second task for an
already-completed hotel can be legitimate new work - a new billing
cycle, a reopened data issue). But the V1-to-new-UI transition created
a batch of these specifically because some hotels were created AND
resolved entirely by hand during that window, with no Supabase dedup
record and no longer an open task - invisible to every automated check,
so the pipeline (reasonably) treated them as new and duplicated them.
This script is the one-time manual pass to clear that specific backlog.

--- Scope ---
- Finds every group of tasks in the project sharing the same hotel name
  (after stripping any "IGNORE" prefix - see NORMALIZE below), the same
  way the regular sweep does.
- Reports EVERY duplicate-name group found, regardless of whether any
  task in it carries an "IGNORE" prefix - Pia flagged a few manually
  while this was being tracked down, but there may be others she didn't
  catch, so this does not limit itself to IGNORE-tagged groups.
- Only DELETES from groups where the earliest-created task is already
  COMPLETED (the specific gap described above). Groups where the
  earliest is still OPEN are reported for visibility only and never
  touched here - those are the regular daily sweep's job, not this
  script's, to avoid the two overlapping.

--- IGNORE prefix handling ---
Pia prefixed a few flagged hotel names with "IGNORE" (e.g. "IGNORE
Ocean Edge Resort & Golf Club") to reduce confusion while this was
being tracked down. NORMALIZE strips a leading "IGNORE" (any of
"IGNORE ", "IGNORE:", "IGNORE - ", case-insensitive) before grouping,
so an IGNORE-tagged task and its plain-named sibling(s) are still
recognized as the same hotel and grouped together. The ORIGINAL raw
name (prefix included) is still shown in the report so it's clear which
ones were manually marked.

--- Safe-by-default ---
Dry-run is the default. Nothing is deleted until you pass --confirm.
Even with --confirm, only duplicates from completed-earliest groups are
removed - the earliest (kept) task in every group is never touched,
and open-earliest groups are never touched by this script at all.
"""

import os
import re
import time
import datetime as dt

import requests

ASANA_TOKEN = os.environ["ASANA_PAT"]
ASANA_PROJECT_GID = "1207448572741662"  # Data Processing Requests

_IGNORE_PREFIX_RE = re.compile(r"^\s*ignore\s*[:\-]?\s*", re.IGNORECASE)


def normalize_name(raw_name):
    """Strips a leading 'IGNORE' marker (with an optional ':'/'-'
    separator) so an IGNORE-tagged task groups with its plain-named
    sibling(s). Everything else about the name is left untouched -
    this is only about the one marker Pia added, not general name
    cleanup."""
    return _IGNORE_PREFIX_RE.sub("", raw_name).strip()


def asana_headers():
    return {"Authorization": f"Bearer {ASANA_TOKEN}", "Content-Type": "application/json"}


def asana_request(method, path, **kwargs):
    url = f"https://app.asana.com/api/1.0{path}"
    for attempt in range(5):
        resp = requests.request(method, url, headers=asana_headers(), timeout=30, **kwargs)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"Asana rate limited, waiting {wait}s (attempt {attempt + 1}/5)")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()["data"]
    raise RuntimeError("Asana still rate limited after 5 retries")


def fetch_all_project_tasks(project_gid):
    """Paginates through every TOP-LEVEL task in the project. Same
    approach as sync_yellow_rows_to_asana.py's fetch_all_project_tasks -
    this project uses standalone tasks only, so a single top-level
    listing is complete."""
    tasks = []
    params = {"project": project_gid, "opt_fields": "name,created_at,completed", "limit": 100}
    url = "https://app.asana.com/api/1.0/tasks"
    while True:
        for attempt in range(5):
            resp = requests.get(url, headers=asana_headers(), params=params, timeout=30)
            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", 2 ** attempt))
                print(f"Asana rate limited fetching tasks, waiting {wait}s (attempt {attempt + 1}/5)")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        else:
            raise RuntimeError("Asana still rate limited after 5 retries while fetching tasks")
        body = resp.json()
        tasks.extend(body["data"])
        next_page = body.get("next_page")
        if not next_page:
            break
        params = {**params, "offset": next_page["offset"]}
        time.sleep(0.3)
    return tasks


def delete_asana_task(task_gid):
    asana_request("DELETE", f"/tasks/{task_gid}")


def find_duplicate_groups(tasks):
    """Groups tasks by normalized name (IGNORE-prefix stripped - see
    normalize_name). Returns {normalized_name: [task, ...]} for every
    name with more than one task, sorted earliest-created first within
    each group."""
    by_name = {}
    for t in tasks:
        key = normalize_name(t["name"])
        by_name.setdefault(key, []).append(t)

    duplicates = {}
    for key, group in by_name.items():
        if len(group) < 2:
            continue
        duplicates[key] = sorted(group, key=lambda t: t["created_at"])
    return duplicates


def main(confirm=False):
    print(f"Fetching all tasks in project {ASANA_PROJECT_GID}...")
    tasks = fetch_all_project_tasks(ASANA_PROJECT_GID)
    print(f"Fetched {len(tasks)} top-level tasks.\n")

    duplicate_groups = find_duplicate_groups(tasks)
    if not duplicate_groups:
        print("No duplicate-name groups found. Nothing to do.")
        return

    completed_earliest_groups = {}
    open_earliest_groups = {}
    for key, group in duplicate_groups.items():
        keeper = group[0]
        if keeper["completed"]:
            completed_earliest_groups[key] = group
        else:
            open_earliest_groups[key] = group

    if open_earliest_groups:
        print(f"SKIPPING {len(open_earliest_groups)} group(s) where the earliest task is still OPEN - "
              f"these are the regular daily pipeline's job (sweep_duplicate_hotel_tasks), not this "
              f"script's. Listed for visibility only, nothing touched:")
        for key, group in open_earliest_groups.items():
            keeper = group[0]
            dupes = group[1:]
            any_ignore_tagged = any(t["name"] != normalize_name(t["name"]) for t in group)
            tag_note = " [includes an IGNORE-tagged task]" if any_ignore_tagged else ""
            print(f"  '{key}'{tag_note}: keeper {keeper['gid']} (open, created {keeper['created_at']}), "
                  f"{len(dupes)} duplicate(s): {[d['gid'] for d in dupes]}")
        print()

    if not completed_earliest_groups:
        print("No completed-earliest duplicate groups found - nothing for this script to clean up.")
        return

    print(f"Found {len(completed_earliest_groups)} group(s) where the earliest task is COMPLETED "
          f"- these are the ones left in place by the regular sweep, and what this script targets:\n")

    total_to_delete = 0
    for key, group in completed_earliest_groups.items():
        keeper = group[0]
        dupes = group[1:]
        total_to_delete += len(dupes)
        any_ignore_tagged = any(t["name"] != normalize_name(t["name"]) for t in group)
        tag_note = " [includes an IGNORE-tagged task]" if any_ignore_tagged else ""
        print(f"'{key}'{tag_note}")
        print(f"  KEEP   {keeper['gid']}  completed  created {keeper['created_at']}  name={keeper['name']!r}")
        for d in dupes:
            status = "completed" if d["completed"] else "OPEN"
            action = "would DELETE" if not confirm else "DELETING"
            print(f"  {action:12s} {d['gid']}  {status}  created {d['created_at']}  name={d['name']!r}")
        print()

    if not confirm:
        print(f"[DRY RUN] {total_to_delete} duplicate task(s) across {len(completed_earliest_groups)} "
              f"group(s) would be deleted. Re-run with --confirm to actually delete them.")
        return

    print(f"Deleting {total_to_delete} duplicate task(s)...")
    deleted = 0
    failed = []
    for key, group in completed_earliest_groups.items():
        for d in group[1:]:
            try:
                delete_asana_task(d["gid"])
                deleted += 1
                print(f"  Deleted {d['gid']} ('{d['name']}')")
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else "unknown"
                failed.append((d["gid"], d["name"], status))
                print(f"  FAILED to delete {d['gid']} ('{d['name']}') - HTTP {status}, skipping and continuing")

    print(f"\nDone. Deleted {deleted}/{total_to_delete}.")
    if failed:
        print(f"{len(failed)} deletion(s) failed - review manually:")
        for gid, name, status in failed:
            print(f"  {gid} ('{name}') - HTTP {status}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirm", action="store_true",
                         help="Actually delete duplicates. Without this flag, only reports what would happen.")
    args = parser.parse_args()
    main(confirm=args.confirm)
