"""
sync_yellow_rows_to_asana.py

Scans the CURRENT month's billing tab only (not historical tabs) for
manually color-highlighted rows. Green rows = no action. Yellow rows
become a subtask of the appropriate batch, which lives inside one of two
existing sections:

- "Priority (Within 24hrs)" - due today or already passed.
- "48 hr SLA"                - everything else, regardless of how far out.
  (There is no third, lower-urgency destination - confirmed.)

Structure inside each section: batches are PARENT TASKS named
"{Month} Batch {N}" (e.g. "July Batch 1"), grouped by Priority Due Date
first (rows due the same day-of-month stay together; overdue rows all
share the group "overdue"), capped at 25 subtasks per batch. Individual
hotels are created as SUBTASKS of that batch task, not as their own
top-level project tasks. Batches accumulate across days within the same
month - a batch only gets a new parent task once it's full.

Same-due-date rows are never split across two batches, even partially:
each run's new arrivals for a given due date are placed as one cohort -
either all of them fit in the currently open batch for that due date, or
none do and a brand new batch is opened for the whole cohort. This can
leave a batch permanently short of 25 (e.g. stuck at 24) - that's
intentional, not a bug.

The actual due date value is NEVER written into any Asana-visible field
(task name, subtask name, or notes) - it's used only for internal
routing and logged to Supabase. All the third party sees is which
section/batch a hotel's subtask lands in.

--- Run --list-colors first ---
I can't verify this sheet's actual RGB values from here. Run
`python sync_yellow_rows_to_asana.py --list-colors` first, tell me the
output, and I'll hardcode YELLOW_RGB / GREEN_RGB precisely.

--- Confirmed decisions (see chat) ---
- "Due today" counts as overdue -> routes to Priority (Within 24hrs).
- Everything else -> "48 hr SLA", regardless of how far away the due
  date is. No third tier.
- This script does not set any due date or SLA field - existing Asana
  rules on those sections handle the SLA.
- Note for later: if you ever want to programmatically check subtask
  completion status for reporting, Asana's nested subtask fields are
  unreliable via include_subtasks - each subtask needs its own get_task
  call. Not a concern for this script (it only creates), just flagging
  it as a known constraint if this gets extended.
"""

import argparse
import os
import re
import sys
import json
import time
import calendar
import datetime as dt

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SHEET_ID = "10osrvx4zsemAQy3rAci2tbV3cAzRBSM8ocecbnuw76I"
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

ASANA_TOKEN = os.environ["ASANA_PAT"]
ASANA_PROJECT_GID = "1207448572741662"  # Data Processing Requests
PRIORITY_SECTION_NAME = "Priority (Within 24hrs)"  # due today/overdue
STANDARD_SECTION_NAME = "48 hr SLA"                # everything else
BATCH_SIZE = 25

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
DEDUP_TABLE = "yellow_row_asana_tasks"
BATCH_TABLE = "asana_batch_sections"

# PLACEHOLDER VALUES - calibrate with --list-colors before trusting this.
YELLOW_RGB = (1.0, 0.949, 0.8)
GREEN_RGB = (0.851, 0.918, 0.827)
COLOR_TOLERANCE = 0.03

COLUMN_ALIASES = {
    "billing_period": ["Billing Period Analyzed", "Period Being Analyzed"],
    "hotel_name": ["Hotel Name", "Hotel"],
    "priority_due_date": ["Priority Due Date"],
}

# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------


def get_credentials():
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    return Credentials.from_service_account_info(creds_dict, scopes=SHEETS_SCOPES)


def sheets_get_with_retry(service, **kwargs):
    for attempt in range(5):
        try:
            return service.spreadsheets().get(**kwargs).execute()
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                wait = (2 ** attempt) * 2
                print(f"Sheets API rate limited, waiting {wait}s (attempt {attempt + 1}/5)")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("Still rate limited after 5 retries")


def current_month_key():
    today = dt.date.today()
    return f"{today.year:04d}-{today.month:02d}"


def month_name_for(month_key):
    year, month = (int(x) for x in month_key.split("-"))
    return calendar.month_name[month]


def parse_billing_period(value):
    if not value:
        return None
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", value)
    if not m:
        return None
    month, _day, year = m.groups()
    year = int(year)
    if year < 100:
        year += 2000
    return f"{year:04d}-{int(month):02d}"


def parse_due_day(value):
    """'3rd of the month' / '21st' / '1st of month' -> 3 / 21 / 1.
    Returns None for blank or unparseable values."""
    if not value or not str(value).strip():
        return None
    m = re.match(r"^\s*(\d{1,2})(st|nd|rd|th)?", str(value).strip(), re.IGNORECASE)
    if not m:
        return None
    day = int(m.group(1))
    if 1 <= day <= 31:
        return day
    return None


def find_col_index(headers, alias_key):
    def norm(s):
        return re.sub(r"\s+", " ", (s or "").strip()).lower()
    normalized = [norm(h) for h in headers]
    for alias in COLUMN_ALIASES[alias_key]:
        if norm(alias) in normalized:
            return normalized.index(norm(alias))
    return None


def find_current_month_worksheet(values_by_title, target_month_key):
    for title, values in values_by_title.items():
        if not values:
            continue
        headers = values[0]
        col_period = find_col_index(headers, "billing_period")
        col_hotel = find_col_index(headers, "hotel_name")
        col_due = find_col_index(headers, "priority_due_date")
        if col_period is None or col_hotel is None:
            continue
        for row in values[1:]:
            if len(row) <= col_period:
                continue
            if parse_billing_period(row[col_period]) == target_month_key:
                return title, headers, col_period, col_hotel, col_due
    return None


def rgb_close(c1, c2, tolerance=COLOR_TOLERANCE):
    return all(abs(a - b) <= tolerance for a, b in zip(c1, c2))


def get_row_colors(service, sheet_title, hotel_col_index, num_rows):
    col_letter = chr(ord("A") + hotel_col_index)
    range_str = f"'{sheet_title}'!{col_letter}2:{col_letter}{num_rows + 1}"
    result = sheets_get_with_retry(
        service,
        spreadsheetId=SHEET_ID,
        ranges=[range_str],
        fields="sheets(data(rowData(values(userEnteredFormat.backgroundColor,formattedValue))))",
    )
    row_data = result["sheets"][0]["data"][0].get("rowData", [])
    colors = []
    for row in row_data:
        cell = (row.get("values") or [{}])[0]
        bg = cell.get("userEnteredFormat", {}).get("backgroundColor", {})
        rgb = (bg.get("red", 1.0), bg.get("green", 1.0), bg.get("blue", 1.0))
        colors.append(rgb)
    return colors


# ---------------------------------------------------------------------------
# Asana
# ---------------------------------------------------------------------------


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


def get_asana_sections():
    return asana_request("GET", f"/projects/{ASANA_PROJECT_GID}/sections")


def find_section_gid(sections, name):
    for s in sections:
        if s["name"].strip().lower() == name.strip().lower():
            return s["gid"]
    raise RuntimeError(
        f"No Asana section named '{name}' found in project {ASANA_PROJECT_GID}. "
        f"Check the exact section name and update the config constant if it differs."
    )


def create_batch_task(name, section_gid):
    """Creates the parent 'batch' task and places it in the given section."""
    payload = {
        "data": {
            "name": name,
            "projects": [ASANA_PROJECT_GID],
            "memberships": [{"project": ASANA_PROJECT_GID, "section": section_gid}],
        }
    }
    data = asana_request("POST", "/tasks", json=payload)
    return data["gid"]


def create_hotel_subtask(hotel_name, month_key, parent_task_gid):
    """Creates the hotel-specific task as a SUBTASK of the batch task.
    Inherits visibility from the parent - no project/section needed here."""
    payload = {
        "data": {
            "name": f"Follow up: {hotel_name}",
            "notes": f"Flagged yellow on the Curacity Billing Overview sheet for {month_key}.",
        }
    }
    data = asana_request("POST", f"/tasks/{parent_task_gid}/subtasks", json=payload)
    return data["gid"]


# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------


def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def already_actioned(dedup_key):
    url = f"{SUPABASE_URL}/rest/v1/{DEDUP_TABLE}?dedup_key=eq.{dedup_key}&select=dedup_key"
    resp = requests.get(url, headers=supabase_headers(), timeout=30)
    resp.raise_for_status()
    return len(resp.json()) > 0


def record_actioned(dedup_key, month_key, hotel_name, subtask_gid, due_day_group):
    payload = {
        "dedup_key": dedup_key,
        "month_key": month_key,
        "hotel_name": hotel_name,
        "asana_task_gid": subtask_gid,
        "due_day_group": due_day_group,
        "created_at": dt.datetime.utcnow().isoformat(),
    }
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{DEDUP_TABLE}",
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()


def get_batch_state(month_key, due_day_group):
    url = (
        f"{SUPABASE_URL}/rest/v1/{BATCH_TABLE}"
        f"?month_key=eq.{month_key}&due_day_group=eq.{due_day_group}"
        f"&order=batch_number.desc&limit=1"
    )
    resp = requests.get(url, headers=supabase_headers(), timeout=30)
    resp.raise_for_status()
    rows = resp.json()
    return rows[0] if rows else None


def upsert_batch_state(month_key, due_day_group, batch_number, batch_task_gid, task_count):
    payload = {
        "month_key": month_key,
        "due_day_group": due_day_group,
        "batch_number": batch_number,
        "batch_task_gid": batch_task_gid,
        "task_count": task_count,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{BATCH_TABLE}",
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()


def get_or_create_batch_task_for_group(month_key, due_day_group, month_name, section_gid, count_needed):
    """Places an entire same-due-date cohort (count_needed new subtasks
    from this run) into one batch task - never split across two.

    If the currently open batch has enough remaining room for ALL of
    count_needed, they all go there. If not, a brand new batch task is
    created for the WHOLE cohort, even if that leaves the previous batch
    permanently short of 25. This is intentional: a due date is never
    allowed to straddle two batches, even partially."""
    state = get_batch_state(month_key, due_day_group)
    remaining = (BATCH_SIZE - state["task_count"]) if state else 0

    if state and remaining >= count_needed:
        new_count = state["task_count"] + count_needed
        upsert_batch_state(month_key, due_day_group, state["batch_number"], state["batch_task_gid"], new_count)
        return state["batch_task_gid"]

    next_batch_number = (state["batch_number"] + 1) if state else 1
    batch_name = f"{month_name} Batch {next_batch_number}"
    batch_task_gid = create_batch_task(batch_name, section_gid)
    upsert_batch_state(month_key, due_day_group, next_batch_number, batch_task_gid, count_needed)
    print(f"Created new batch task '{batch_name}' for due-day group '{due_day_group}' ({count_needed} rows)")
    return batch_task_gid


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(list_colors_only=False, dry_run=False, month_override=None, as_of_day_override=None):
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)

    import gspread
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)

    target_month = month_override or current_month_key()
    month_name = month_name_for(target_month)

    if as_of_day_override:
        today_day = as_of_day_override
    elif month_override:
        # Testing a past month with no explicit day given - default to
        # end of that month so the overdue/batched split has something
        # sensible to compare against.
        year, month = (int(x) for x in target_month.split("-"))
        today_day = calendar.monthrange(year, month)[1]
    else:
        today_day = dt.date.today().day

    if dry_run:
        print(f"[DRY RUN] Testing month={target_month}, simulated today_day={today_day}. "
              f"No Asana calls or Supabase writes will be made.\n")

    values_by_title = {}
    for ws in spreadsheet.worksheets():
        try:
            values_by_title[ws.title] = ws.get_all_values()
            time.sleep(1.1)
        except Exception as e:
            print(f"Skipping worksheet '{ws.title}': {e}")

    found = find_current_month_worksheet(values_by_title, target_month)
    if not found:
        print(f"No worksheet found with rows matching {target_month}.")
        return

    sheet_title, headers, col_period, col_hotel, col_due = found
    values = values_by_title[sheet_title]
    data_rows = values[1:]
    print(f"Using worksheet '{sheet_title}' for {target_month}, {len(data_rows)} rows.")

    colors = get_row_colors(service, sheet_title, col_hotel, len(data_rows))

    if list_colors_only:
        seen = {}
        for row, color in zip(data_rows, colors):
            seen[color] = seen.get(color, 0) + 1
        print("\nDistinct background colors found in the Hotel Name column:")
        for color, count in sorted(seen.items(), key=lambda x: -x[1]):
            print(f"  RGB{tuple(round(c, 3) for c in color)} - {count} row(s)")
        print("\nNo Asana tasks created (list-colors mode).")
        return

    if not dry_run:
        sections = get_asana_sections()
        priority_section_gid = find_section_gid(sections, PRIORITY_SECTION_NAME)
        standard_section_gid = find_section_gid(sections, STANDARD_SECTION_NAME)
    else:
        priority_section_gid = "DRY_RUN_PRIORITY_SECTION"
        standard_section_gid = "DRY_RUN_STANDARD_SECTION"

    # Pass 1: figure out which rows are new (yellow, not yet actioned),
    # and group them by due_day_group so each group can be placed as one
    # atomic cohort - never split across the 25-cap.
    groups = {}  # due_day_group -> list of hotel_name
    for row, color in zip(data_rows, colors):
        if len(row) <= max(col_period, col_hotel):
            continue
        hotel_name = row[col_hotel].strip()
        if not hotel_name:
            continue

        if rgb_close(color, GREEN_RGB):
            continue  # no action, by design
        if not rgb_close(color, YELLOW_RGB):
            continue  # not a color we act on

        dedup_key = f"{target_month}:{hotel_name}"
        if already_actioned(dedup_key):  # read-only either way, safe in dry-run
            continue

        due_value = row[col_due].strip() if (col_due is not None and len(row) > col_due) else ""
        due_day = parse_due_day(due_value)

        if due_day is not None and due_day <= today_day:
            due_day_group = "overdue"
        else:
            due_day_group = str(due_day) if due_day is not None else "blank"

        groups.setdefault(due_day_group, []).append(hotel_name)

    if not groups:
        print("No new yellow rows to action.")
        return

    # Pass 2: place each group's entire cohort into one batch task
    # (or simulate doing so, in dry-run mode).
    for due_day_group, hotel_names in groups.items():
        section_label = "Priority (Within 24hrs)" if due_day_group == "overdue" else "48 hr SLA"
        target_section_gid = priority_section_gid if due_day_group == "overdue" else standard_section_gid

        if dry_run:
            state = get_batch_state(target_month, due_day_group)  # read-only, safe
            remaining = (BATCH_SIZE - state["task_count"]) if state else 0
            count_needed = len(hotel_names)
            if state and remaining >= count_needed:
                batch_label = f"{month_name} Batch {state['batch_number']}"
                action = f"REUSE existing batch (currently {state['task_count']}/{BATCH_SIZE}, room for {remaining})"
            else:
                next_num = (state["batch_number"] + 1) if state else 1
                batch_label = f"{month_name} Batch {next_num}"
                action = "CREATE NEW batch" if state else "CREATE FIRST batch"
            print(f"[DRY RUN] Group '{due_day_group}' -> section '{section_label}', {action}: "
                  f"'{batch_label}', would add {count_needed} subtask(s): {hotel_names}")
            continue

        batch_task_gid = get_or_create_batch_task_for_group(
            target_month, due_day_group, month_name, target_section_gid, len(hotel_names)
        )
        for hotel_name in hotel_names:
            dedup_key = f"{target_month}:{hotel_name}"
            subtask_gid = create_hotel_subtask(hotel_name, target_month, batch_task_gid)
            record_actioned(dedup_key, target_month, hotel_name, subtask_gid, due_day_group)
            print(f"Group '{due_day_group}' -> subtask {subtask_gid} under batch {batch_task_gid} for '{hotel_name}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--list-colors", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                         help="No Asana calls or Supabase writes - just prints what would happen.")
    parser.add_argument("--month", type=str, default=None,
                         help="Override target month, e.g. 2026-06, to test against a past tab.")
    parser.add_argument("--as-of-day", type=int, default=None,
                         help="Simulate 'today' as this day-of-month, for testing overdue logic on a past month.")
    args = parser.parse_args()

    main(
        list_colors_only=args.list_colors,
        dry_run=args.dry_run,
        month_override=args.month,
        as_of_day_override=args.as_of_day,
    )
