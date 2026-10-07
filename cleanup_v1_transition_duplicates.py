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
cycle, a reopened data issue). The V1-to-new-UI transition created a
batch of these specifically because some hotels were created AND
resolved entirely by hand during that window, with no Supabase dedup
record and no longer an open task - invisible to every automated check,
so the pipeline (reasonably) treated them as new and duplicated them.
This script is the one-time manual pass to clear that specific backlog.

--- FIXED: same-name != same-cycle ---
An earlier version of this script grouped ALL tasks by name, with no
time bound, and treated every group-with-a-completed-earliest-task as
a duplicate to clean up. That's wrong for any hotel that is legitimately
flagged every month (confirmed real case: "Park Shore Waikiki" had four
separate, correctly-completed tasks from four different billing
cycles - Aug, two in Sep, and Oct - which the old logic would have
deleted down to one, destroying real historical work records, not
cleaning up a duplicate at all. The script had no way to distinguish
"same hotel, same cycle, created twice by accident" from "same hotel,
different cycles, each correctly actioned once," because it only
looked at name and creation order with no concept of billing period.

Fix: this script now ONLY considers tasks created on/after CUTOFF_DATE
(default 2026-10-01, the start of the window the V1-transition manual
work actually happened in) for duplicate grouping. Tasks created before
the cutoff are excluded entirely before grouping even starts - not just
filtered out of the delete list, but never considered when forming
groups at all. This means a hotel like Park Shore Waikiki, which has
only ONE task falling inside the cutoff window, has nothing to group
with and never shows up as a "duplicate" in the first place. Legitimate
monthly recurrence almost never produces two tasks for the same hotel
within this narrow a window, while the actual V1-transition duplicates
were all created close together within it - so restricting the
candidate pool by date, rather than trying to infer intent from notes
or other heuristics, is what actually separates the two cases here.

--- Scope ---
- Among tasks created on/after CUTOFF_DATE only: finds every group of
  tasks sharing the same hotel name (after stripping any "IGNORE"
  prefix - see NORMALIZE below), the same way the regular sweep does.
- Reports EVERY duplicate-name group found within that window,
  regardless of whether any task in it carries an "IGNORE" prefix - Pia
  flagged a few manually while this was being tracked down, but there
  may be others she didn't catch, so this does not limit itself to
  IGNORE-tagged groups.
- Only DELETES from groups where the earliest-created task (within the
  window) is already COMPLETED. Groups where the earliest is still OPEN
  are reported for visibility only and never touched here - those are
  the regular daily sweep's job, not this script's.

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
Even with --confirm, only duplicates from completed-earliest groups
within the cutoff window are removed - the earliest (kept) task in
every group is never touched, open-earliest groups are never touched,
and anything created before CUTOFF_DATE is never even considered.
"""

import os
import re
import time
import datetime as dt

import requests

ASANA_TOKEN = os.environ["ASANA_PAT"]
ASANA_PROJECT_GID = "1207448572741662"  # Data Processing Requests

# Only tasks created ON OR AFTER this date are considered for duplicate
# grouping at all - see the "FIXED: same-name != same-cycle" note above
# for why this matters. Confirmed with Pia 2026-10-07: restrict to
# tasks created since 10/1, the start of the V1-transition window.
CUTOFF_DATE = dt.date(2026, 10, 1)

_IGNORE_PREFIX_RE = re.compile(r"^\s*ignore\s*[:\-]?\s*", re.IGNORECASE)


def normalize_name(raw_name):
    """Strips a leading 'IGNORE' marker (with an optional ':'/'-'
    separator) so an IGNORE-tagged task groups with its plain-named
    sibling(s). Everything else about the name is left untouched -
    this is only about the one marker Pia added, not general name
    cleanup."""
    return _IGNORE_PREFIX_RE.sub("", raw_name).strip()


def parse_asana_created_at(value):
    """Asana's created_at comes back like '2026-09-17T15:33:59.685Z'.
    Returns a date() for comparing against CUTOFF_DATE."""
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).date()


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
    listing is complete. Fetches the full history (no date filter at
    the API level) - the CUTOFF_DATE filter is applied client-side in
    main(), after fetching, so the raw task list stays reusable/
    inspectable if needed."""
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


def find_duplicate_groups(tasks, cutoff_date):
    """Groups tasks by normalized name (IGNORE-prefix stripped - see
    normalize_name), considering ONLY tasks created on/after
    cutoff_date. A task created before the cutoff is excluded entirely
    before grouping - it doesn't even count as a sibling for some other
    task that IS within the window, since including it would reintroduce
    exactly the same-name-but-different-cycle false positive this
    script was fixed to avoid (see module docstring).

    Returns {normalized_name: [task, ...]} for every name with more than
    one task WITHIN THE WINDOW, sorted earliest-created first within
    each group."""
    by_name = {}
    excluded_count = 0
    for t in tasks:
        if parse_asana_created_at(t["created_at"]) < cutoff_date:
            excluded_count += 1
            continue
        key = normalize_name(t["name"])
        by_name.setdefault(key, []).append(t)

    print(f"{excluded_count} task(s) created before {cutoff_date.isoformat()} excluded from "
          f"consideration entirely (not grouped, not reported, not touched).")

    duplicates = {}
    for key, group in by_name.items():
        if len(group) < 2:
            continue
        duplicates[key] = sorted(group, key=lambda t: t["created_at"])
    return duplicates


def main(confirm=False, cutoff_date=CUTOFF_DATE):
    print(f"Fetching all tasks in project {ASANA_PROJECT_GID}...")
    tasks = fetch_all_project_tasks(ASANA_PROJECT_GID)
    print(f"Fetched {len(tasks)} top-level tasks total.\n")

    duplicate_groups = find_duplicate_groups(tasks, cutoff_date)
    if not duplicate_groups:
        print(f"\nNo duplicate-name groups found among tasks created on/after "
              f"{cutoff_date.isoformat()}. Nothing to do.")
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
        print(f"\nSKIPPING {len(open_earliest_groups)} group(s) where the earliest task (within "
              f"the window) is still OPEN - these are the regular daily pipeline's job "
              f"(sweep_duplicate_hotel_tasks), not this script's. Listed for visibility only, "
              f"nothing touched:")
        for key, group in open_earliest_groups.items():
            keeper = group[0]
            dupes = group[1:]
            any_ignore_tagged = any(t["name"] != normalize_name(t["name"]) for t in group)
            tag_note = " [includes an IGNORE-tagged task]" if any_ignore_tagged else ""
            print(f"  '{key}'{tag_note}: keeper {keeper['gid']} (open, created {keeper['created_at']}), "
                  f"{len(dupes)} duplicate(s): {[d['gid'] for d in dupes]}")

    if not completed_earliest_groups:
        print(f"\nNo completed-earliest duplicate groups found within the "
              f"{cutoff_date.isoformat()}-onward window - nothing for this script to clean up.")
        return

    print(f"\nFound {len(completed_earliest_groups)} group(s), among tasks created on/after "
          f"{cutoff_date.isoformat()}, where the earliest task is COMPLETED - these are the ones "
          f"left in place by the regular sweep, and what this script targets:\n")

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
    parser.add_argument("--since", type=str, default=None,
                         help="Override the cutoff date (YYYY-MM-DD). Only tasks created on/after this "
                              "date are considered. Default: 2026-10-01.")
    args = parser.parse_args()
    cutoff = dt.date.fromisoformat(args.since) if args.since else CUTOFF_DATE
    main(confirm=args.confirm, cutoff_date=cutoff)
