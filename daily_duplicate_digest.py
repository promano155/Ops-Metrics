"""
daily_duplicate_digest.py

Daily scan of Data Processing Requests for duplicate hotel tasks (same
hotel, same calendar month - same signature as find_duplicate_hotel_tasks.py
and delete_duplicate_hotel_tasks.py). Always sends a Slack digest of what
it found. Auto-deletion is OFF by default and controlled entirely by the
AUTO_DEDUPE repo variable - flip it to "true" in GitHub's Settings ->
Secrets and variables -> Actions -> Variables tab once you've reviewed
enough digests to trust it. No code or workflow change needed to turn it
on later - that's the point.

Resolution rule, same as the other two scripts:
  - Exactly ONE Priority-section occurrence in a group -> that one is
    kept, the rest deleted. This is the actual scenario driving this
    script: someone manually adds a hotel to Priority after it already
    exists in a regular batch - the Priority copy should always win.
  - ZERO Priority-section occurrences -> keep the earliest, delete the
    rest (deterministic, low-risk - especially now that the original
    URL-encoding bug that caused most of these is fixed).
  - TWO OR MORE Priority-section occurrences in the same group -> this
    is genuinely ambiguous (multiple manual adds for the same hotel) and
    is NEVER auto-resolved, regardless of the AUTO_DEDUPE toggle. Always
    flagged in the digest for manual review instead.

This does NOT replace a one-time cleanup of whatever duplicate backlog
already exists - it only catches duplicates created from today forward.
Run delete_duplicate_hotel_tasks.py separately, once, for the backlog.
"""

import os
import re
import time
import datetime as dt

import requests

ASANA_TOKEN = os.environ["ASANA_PAT"]
PROJECT_GID = "1207448572741662"  # Data Processing Requests
PRIORITY_SECTION_NAME = "Priority (Within 24hrs)"
OPT_FIELDS = "name,created_at,parent.name,permalink_url"

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_DM_USER_ID = "U0BBU2YRQ72"  # Pia

AUTO_DEDUPE = os.environ.get("AUTO_DEDUPE", "false").strip().lower() == "true"


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


def classify_group(items, section_cache):
    """Returns (keep, to_delete, reason, is_ambiguous)."""
    priority_items = []
    for item in items:
        parent = item.get("parent")
        if not parent:
            continue
        section_name = get_task_section(parent["gid"], section_cache)
        if section_name and section_name.strip().lower() == PRIORITY_SECTION_NAME.strip().lower():
            priority_items.append(item)

    if len(priority_items) == 1:
        keep = priority_items[0]
        return keep, [i for i in items if i["gid"] != keep["gid"]], "in Priority section", False
    if len(priority_items) == 0:
        items_sorted = sorted(items, key=lambda t: t["created_at"])
        keep = items_sorted[0]
        return keep, [i for i in items if i["gid"] != keep["gid"]], "earliest (no Priority occurrence)", False
    # 2+ priority occurrences - genuinely ambiguous, never auto-resolved.
    return None, [], "ambiguous - multiple Priority-section duplicates", True


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
    print(f"AUTO_DEDUPE is {'ON' if AUTO_DEDUPE else 'OFF'}.")
    print("Fetching all top-level tasks...")
    top_level = fetch_top_level_tasks()
    print(f"Found {len(top_level)} top-level tasks. Fetching subtasks for each...")

    all_subtasks = []
    for i, task in enumerate(top_level):
        all_subtasks.extend(fetch_subtasks(task["gid"]))
        if (i + 1) % 20 == 0:
            print(f"  ...processed {i + 1}/{len(top_level)} parents, {len(all_subtasks)} subtasks so far")
        time.sleep(0.15)

    groups = {}
    for item in all_subtasks:
        key = (normalize_name(item["name"]), item["created_at"][:7])
        groups.setdefault(key, []).append(item)

    duplicate_groups = {k: v for k, v in groups.items() if len(v) > 1}

    today = dt.date.today().isoformat()
    if not duplicate_groups:
        send_slack_dm(f"*Daily duplicate check - {today}*\nNo duplicates found. :white_check_mark:")
        print("No duplicates found.")
        return

    section_cache = {}
    lines = [f"*Daily duplicate check - {today}*", f"Found {len(duplicate_groups)} hotel(s) with duplicates.\n"]
    deleted_count = 0
    resolved_count = 0
    ambiguous = []

    for (hotel, month), items in sorted(duplicate_groups.items(), key=lambda kv: -len(kv[1])):
        keep, to_delete, reason, is_ambiguous = classify_group(items, section_cache)

        if is_ambiguous:
            ambiguous.append((hotel, month))
            lines.append(f":warning: *{hotel.title()}* ({month}) - {reason}, NEEDS MANUAL REVIEW")
            continue

        resolved_count += 1
        if AUTO_DEDUPE:
            for item in to_delete:
                asana_delete_task(item["gid"])
                time.sleep(0.2)
            deleted_count += len(to_delete)
            lines.append(f"*{hotel.title()}* ({month}) - kept ({reason}), deleted {len(to_delete)}")
        else:
            lines.append(f"*{hotel.title()}* ({month}) - would keep ({reason}), "
                          f"would delete {len(to_delete)} [preview only]")

    lines.append("")
    if AUTO_DEDUPE:
        lines.append(f"_Auto-dedupe is ON - {deleted_count} task(s) actually deleted just now._")
    else:
        lines.append(f"_Auto-dedupe is OFF - preview only, nothing was deleted. "
                      f"Set the AUTO_DEDUPE repo variable to 'true' to enable._")
    if ambiguous:
        lines.append(f"\n{len(ambiguous)} group(s) need manual review - never auto-resolved regardless of the toggle.")

    send_slack_dm("\n".join(lines))
    print(f"Digest sent. {resolved_count} group(s) resolved, {len(ambiguous)} flagged for manual review.")


if __name__ == "__main__":
    main()
