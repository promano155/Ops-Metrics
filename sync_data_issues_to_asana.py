"""
sync_data_issues_to_asana.py

Adapted from sync_yellow_rows_to_asana.py to read from the Invoice Horizon
sheet instead of the old Curacity Billing Overview sheet, with a new
trigger model matching Invoice Horizon's Data Status / Processing Status
split, and a write-back loop that resolves hotels in the sheet once their
Asana task is completed. ALL dedup, retry, and batching machinery is
unchanged from the original script - see its own comments for the full
history of why this logic looks the way it does.

--- What "flagged" means now (replaces the old 'Flag to Innova' checkbox) ---
A row is flagged if ANY of these is true:
  - Data Status (Data Uploaded (Yes/No)) = "Error"        -> File Error
  - Processing Status (Next Action) contains "Data Issue"  -> Data Issue
  - Processing Status (Next Action) contains "Integration
    Issue"                                                 -> Integration Issue
These are recorded via a 3-option "Issue Type" custom field on the created
task (File Error takes precedence if a row somehow matches more than one -
an intake-time failure is the more fundamental problem).

--- Routing ---
File Error / Data Issue (unchanged from the first adapted version):
  - Priority = Yes -> standalone task straight to Priority (Within 24hrs),
    due_at = creation time + 24h, regardless of Send by Date.
  - Otherwise, routed by whether Send by Date is populated: populated ->
    Priority (due_at = +24h); blank -> 48hr SLA, batched (25-cap, staggered
    due-date chaining, unchanged from the original script).

Integration Issue (new - a fixed destination, not a Priority/48hr choice):
  - ALWAYS routes to "Transferred to Integrations", ignoring the Priority
    flag entirely - destination doesn't depend on urgency for this type.
  - Send by Date still determines the DUE DATE within that fixed
    destination: populated -> due_at = +24h, same as Priority's own
    behavior. Blank -> batched (25-cap, staggered), but using its OWN
    independent staggering sequence, so a burst of Integration Issues
    doesn't compress File Error/Data Issue's deadlines or vice versa -
    they're not competing for attention in the same section.

--- Write-back ---
Runs first, before any new flagging, on every invocation. For every hotel
this script has previously flagged (tracked via the Supabase dedup table),
checks whether its Asana task is now completed. On completion, ONE uniform
resolution applies regardless of which issue type originally flagged it:
Data Uploaded -> Yes, Next Action -> "Processing Invoice" - the same
default state any freshly-graduated hotel starts in. The hotel reappears
in Work Queue, ready to work through normally (Mark as Sent is available
regardless of Processing Status, so no distinct "resolved" label is
needed). The dedup record is then
cleared so the hotel is eligible for a fresh flag if a new problem occurs.

This is the first version of this script family that WRITES to Sheets, not
just reads - requires the broadened SHEETS_SCOPES below, and the Google
service account needs Editor (not just Viewer) access on the Invoice
Horizon sheet.

--- Test-safe ---
Points at the disposable TEST Asana project and _test-suffixed Supabase
tables. Swap ASANA_PROJECT_GID and the three table-name constants once
validated and ready to point at the real project.
"""

import os
import re
import json
import time
import calendar
import datetime as dt

import gspread
import requests
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SHEET_ID = "1aI-Z02YviqcFhvpIF_pGZd0GRaCnWPg88d-09ZsvryE"  # Invoice Horizon
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
# Full read/write - this script now writes back to the sheet on task
# completion, unlike its read-only predecessor. The service account needs
# Editor access on the actual sheet, not just Viewer.
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SHEETS_REQUEST_DELAY_SECONDS = 1.1
SHEETS_MAX_RETRIES = 5

ASANA_TOKEN = os.environ["ASANA_PAT"]

# TEST PROJECT - swap to the real "Data Processing Requests" project
# (1207448572741662) once validated. Section names/GIDs below are already
# mirrored 1:1 in the real project.
ASANA_PROJECT_GID = "1218503183805242"  # TEST - Data Processing Requests (Invoice Horizon)
PRIORITY_SECTION_NAME = "Priority (Within 24hrs)"     # Send by Date populated (File Error/Data Issue)
STANDARD_SECTION_NAME = "48 hr SLA"                    # Send by Date blank (File Error/Data Issue)
INTEGRATIONS_SECTION_GID = "1218507635372087"          # ALWAYS - Integration Issue, regardless of urgency

# "Data Automated" mirrors the sheet's checkbox column. Test project's
# version has two options (Yes/No); only "Yes" is ever set, matching the
# real project's single-option-workaround semantics either way.
DATA_AUTOMATED_FIELD_GID = "1218503183888433"
DATA_AUTOMATED_YES_OPTION_GID = "1218503183888434"

# Records which condition triggered the flag.
ISSUE_TYPE_FIELD_GID = "1218503183888428"
ISSUE_TYPE_FILE_ERROR_OPTION_GID = "1218503183888429"
ISSUE_TYPE_DATA_ISSUE_OPTION_GID = "1218503183888430"
ISSUE_TYPE_INTEGRATION_ISSUE_OPTION_GID = "1218503183888437"

MONTH_NAMES = [
    'January','February','March','April','May','June',
    'July','August','September','October','November','December'
]

BATCH_SIZE = 25

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
DEDUP_TABLE = "yellow_row_asana_tasks_test"
BATCH_TABLE = "asana_batch_sections_test"
STANDARD_BATCH_SEQUENCE_TABLE = "standard_sla_batch_sequence_test"
STALE_RECREATION_LOG_TABLE = "stale_task_recreations_test"

TRUE_VALUES = {"true", "yes", "y", "1", "checked"}

COLUMN_ALIASES = {
    "billing_period": ["Billing Period Analyzed"],
    "hotel_name": ["Hotel", "Hotel Name"],
    "priority": ["Priority", "Data Priority"],
    "data_uploaded": ["Data Uploaded (Yes/No)"],
    "next_action": ["Next Action"],
    "send_by_date": ["Send by Date"],
    "data_automated": ["Data Automated"],
}

# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------


def get_credentials():
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    return Credentials.from_service_account_info(creds_dict, scopes=SHEETS_SCOPES)


def get_all_values_with_retry(ws):
    """Reads a worksheet's values, retrying with backoff on rate-limit
    errors instead of silently giving up. Unchanged from the original
    script - see its docstring for why this exists."""
    for attempt in range(SHEETS_MAX_RETRIES):
        try:
            values = ws.get_all_values()
            time.sleep(SHEETS_REQUEST_DELAY_SECONDS)
            return values
        except gspread.exceptions.APIError as e:
            status = getattr(e.response, "status_code", None)
            is_rate_limit = status == 429 or (
                e.response is not None and "RESOURCE_EXHAUSTED" in e.response.text
            )
            if not is_rate_limit:
                raise
            wait = (2 ** attempt) * 2
            print(f"Sheets API rate limited reading '{ws.title}', waiting {wait}s (attempt {attempt + 1}/{SHEETS_MAX_RETRIES})")
            time.sleep(wait)
    raise RuntimeError(f"Still rate limited reading '{ws.title}' after {SHEETS_MAX_RETRIES} retries")


def parse_utc_datetime(value):
    """Parses a timestamp string coming back from Supabase into a naive
    UTC datetime. Unchanged from the original script."""
    if value is None:
        return None
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return parsed


def parse_billing_period(value):
    """Returns the month from the SECOND date in a '{start} - {end}'
    range. Unchanged from the original script - Invoice Horizon's Billing
    Period Analyzed values use the same M.D.YY(YY) format."""
    if not value:
        return None
    matches = re.findall(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", value)
    if not matches:
        return None
    month, _day, year = matches[1] if len(matches) >= 2 else matches[0]
    year = int(year)
    if year < 100:
        year += 2000
    return f"{year:04d}-{int(month):02d}"


def find_col_index(headers, alias_key):
    def norm(s):
        return re.sub(r"\s+", " ", (s or "").strip()).lower()
    normalized = [norm(h) for h in headers]
    for alias in COLUMN_ALIASES[alias_key]:
        if norm(alias) in normalized:
            return normalized.index(norm(alias))
    return None


def find_latest_month_key(values_by_title):
    """Scans every worksheet's Billing Period Analyzed values and returns
    the MOST RECENT month_key actually found. Unchanged from the original
    script."""
    today = dt.date.today()
    earliest_plausible = month_key_shift(today, -1)
    latest_plausible = month_key_shift(today, 0)

    latest = None
    for values in values_by_title.values():
        if not values:
            continue
        headers = values[0]
        col_period = find_col_index(headers, "billing_period")
        if col_period is None:
            continue
        for row in values[1:]:
            if len(row) <= col_period:
                continue
            month_key = parse_billing_period(row[col_period])
            if month_key is None:
                continue
            if not (earliest_plausible <= month_key <= latest_plausible):
                continue
            if latest is None or month_key > latest:
                latest = month_key
    return latest


def find_latest_month_key_from_tab_names(worksheet_titles):
    """Determines the latest month by parsing TAB NAMES directly (e.g.
    'July 2026 - Media Brands'), matching the exact convention Apps
    Script's own getLatestSheetName() already relies on - rather than
    scanning cell values with a calendar-relative plausibility window.
    Invoice Horizon isn't in production yet, so real invoicing work can
    legitimately lag well behind the calendar while the dashboard is
    still being built out - a narrow calendar-relative window (as used
    by find_latest_month_key, above, inherited from the old script's
    different environment with years of messy legacy tabs) would
    incorrectly reject the real current tab in that situation. Invoice
    Horizon's tabs are reliably named this way, so this is both simpler
    and immune to that whole question entirely."""
    latest = None
    latest_sort_key = None
    for title in worksheet_titles:
        match = re.match(r'^(\w+)\s+(\d{4})\s*[-\u2013]\s*Media\s+Brands?$', title, re.IGNORECASE)
        if not match:
            continue
        month_name, year_str = match.group(1), match.group(2)
        month_lower = [m.lower() for m in MONTH_NAMES]
        if month_name.lower() not in month_lower:
            continue
        month_idx = month_lower.index(month_name.lower())
        year = int(year_str)
        sort_key = year * 100 + month_idx
        if latest_sort_key is None or sort_key > latest_sort_key:
            latest_sort_key = sort_key
            latest = f"{year:04d}-{month_idx + 1:02d}"
    return latest


def month_key_shift(base_date, months):
    total = base_date.year * 12 + (base_date.month - 1) + months
    year, month = divmod(total, 12)
    return f"{year:04d}-{month + 1:02d}"


def find_current_month_worksheet(values_by_title, target_month_key):
    for title, values in values_by_title.items():
        if not values:
            continue
        headers = values[0]
        col_period = find_col_index(headers, "billing_period")
        col_hotel = find_col_index(headers, "hotel_name")
        col_priority = find_col_index(headers, "priority")
        col_data_uploaded = find_col_index(headers, "data_uploaded")
        col_next_action = find_col_index(headers, "next_action")
        col_send_by = find_col_index(headers, "send_by_date")
        col_data_automated = find_col_index(headers, "data_automated")
        if col_period is None or col_hotel is None:
            continue
        for row in values[1:]:
            if len(row) <= col_period:
                continue
            if parse_billing_period(row[col_period]) == target_month_key:
                return (title, headers, col_period, col_hotel, col_priority,
                         col_data_uploaded, col_next_action, col_send_by, col_data_automated)
    return None


def is_truthy(value):
    return (value or "").strip().lower() in TRUE_VALUES


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


def fetch_all_project_tasks(project_gid):
    """Paginates through every TOP-LEVEL task currently in the project.
    Unchanged from the original script."""
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


def sweep_duplicate_hotel_tasks(tasks, dry_run=False):
    """Unchanged from the original script - see its docstring."""
    by_name = {}
    for t in tasks:
        by_name.setdefault(t["name"], []).append(t)

    any_swept = False
    for name, group in by_name.items():
        if len(group) < 2:
            continue
        group_sorted = sorted(group, key=lambda t: t["created_at"])
        keeper, duplicates = group_sorted[0], group_sorted[1:]

        if keeper["completed"]:
            print(f"'{name}': {len(duplicates)} duplicate(s) found, but the earliest task "
                  f"({keeper['gid']}) is already completed - leaving duplicates in place for "
                  f"manual review rather than assuming a completed hotel's extra task is a "
                  f"true duplicate.")
            continue

        any_swept = True
        for dup in duplicates:
            if dry_run:
                print(f"[DRY RUN] Would sweep duplicate task {dup['gid']} for '{name}' "
                      f"(keeping earliest task {keeper['gid']}, created {keeper['created_at']}).")
            else:
                delete_asana_task(dup["gid"])
                print(f"Swept duplicate task {dup['gid']} for '{name}' "
                      f"(kept earliest task {keeper['gid']}, created {keeper['created_at']}).")

    if not any_swept:
        print("Duplicate sweep: no open-and-duplicated hotel tasks found.")


def build_open_hotel_task_names(tasks):
    """Any hotel with at least one open task, regardless of which issue
    type or cycle created it - unchanged from the original script. This is
    what guarantees a hotel already being tracked never gets a second task
    piled on top while the first is still open, across all three issue
    types uniformly."""
    return {t["name"] for t in tasks if not t["completed"]}


def find_section_gid(sections, name):
    for s in sections:
        if s["name"].strip().lower() == name.strip().lower():
            return s["gid"]
    raise RuntimeError(
        f"No Asana section named '{name}' found in project {ASANA_PROJECT_GID}. "
        f"Check the exact section name and update the config constant if it differs."
    )


def create_standalone_task(hotel_name, month_key, section_gid, issue_type_option_gid,
                            due_at=None, data_automated=False):
    """Creates a hotel as a standalone TOP-LEVEL task directly in the
    given section - no parent, no batch container, no subtask nesting."""
    payload = {
        "data": {
            "name": hotel_name,
            "notes": f"Flagged (Data Status/Processing Status) on the Invoice Horizon sheet for {month_key}.",
            "projects": [ASANA_PROJECT_GID],
            "memberships": [{"project": ASANA_PROJECT_GID, "section": section_gid}],
        }
    }
    if due_at:
        payload["data"]["due_at"] = due_at
    custom_fields = {}
    if data_automated:
        custom_fields[DATA_AUTOMATED_FIELD_GID] = DATA_AUTOMATED_YES_OPTION_GID
    if issue_type_option_gid:
        custom_fields[ISSUE_TYPE_FIELD_GID] = issue_type_option_gid
    if custom_fields:
        payload["data"]["custom_fields"] = custom_fields
    data = asana_request("POST", "/tasks", json=payload, params={"opt_fields": "due_at,due_on,name"})
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


def asana_task_exists(task_gid):
    """Unchanged from the original script - see its docstring."""
    url = f"https://app.asana.com/api/1.0/tasks/{task_gid}"
    resp = requests.get(url, headers=asana_headers(), timeout=30)
    if resp.status_code == 404:
        return False
    resp.raise_for_status()
    return True


def log_stale_recreation(dedup_key, hotel_name, old_task_gid, month_key, due_day_group):
    """Unchanged from the original script - see its docstring."""
    payload = {
        "dedup_key": dedup_key,
        "hotel_name": hotel_name,
        "old_asana_task_gid": old_task_gid,
        "month_key": month_key,
        "due_day_group": due_day_group,
        "detected_at": dt.datetime.utcnow().isoformat(),
    }
    try:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/{STALE_RECREATION_LOG_TABLE}",
            headers=supabase_headers(),
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Warning: failed to log stale-recreation diagnostics for '{dedup_key}' "
              f"(continuing anyway - this is diagnostics-only, not the actual fix): {e}")


def delete_dedup_record(dedup_key):
    url = f"{SUPABASE_URL}/rest/v1/{DEDUP_TABLE}"
    params = {"dedup_key": f"eq.{dedup_key}"}
    resp = requests.delete(url, headers=supabase_headers(), params=params, timeout=30)
    resp.raise_for_status()


def already_actioned(dedup_key, dry_run=False):
    """Unchanged from the original script - see its docstring."""
    url = f"{SUPABASE_URL}/rest/v1/{DEDUP_TABLE}"
    params = {"dedup_key": f"eq.{dedup_key}",
              "select": "dedup_key,asana_task_gid,hotel_name,month_key,due_day_group"}
    resp = requests.get(url, headers=supabase_headers(), params=params, timeout=30)
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        return False

    task_gid = rows[0].get("asana_task_gid")
    if task_gid:
        try:
            task_still_exists = asana_task_exists(task_gid)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            print(f"Warning: could not verify Asana task {task_gid} for dedup_key "
                  f"'{dedup_key}' (HTTP {status}) - treating as still actioned rather "
                  f"than risk a duplicate or wrongly clearing a real record. If this "
                  f"keeps happening for the same task, investigate it directly.")
            return True
        if not task_still_exists:
            if dry_run:
                print(f"[DRY RUN] Dedup record for '{dedup_key}' points to Asana task "
                      f"{task_gid}, which no longer exists - would clear this stale "
                      f"record and treat the hotel as new (not deleting anything).")
            else:
                print(f"Dedup record for '{dedup_key}' points to Asana task {task_gid}, "
                      f"which no longer exists - clearing the stale record so this hotel "
                      f"can be re-actioned.")
                log_stale_recreation(
                    dedup_key,
                    rows[0].get("hotel_name"),
                    task_gid,
                    rows[0].get("month_key"),
                    rows[0].get("due_day_group"),
                )
                delete_dedup_record(dedup_key)
            return False

    return True


def already_actioned_with_legacy_fallback(sheet_title, target_month, hotel_name, dry_run=False):
    """Unchanged from the original script - see its docstring."""
    new_key = f"{sheet_title}:{hotel_name}"
    if already_actioned(new_key, dry_run=dry_run):
        return True
    legacy_key = f"{target_month}:{hotel_name}"
    return already_actioned(legacy_key, dry_run=dry_run)


def record_actioned(dedup_key, month_key, hotel_name, subtask_gid, due_day_group, sheet_title=None):
    payload = {
        "dedup_key": dedup_key,
        "month_key": month_key,
        "hotel_name": hotel_name,
        "asana_task_gid": subtask_gid,
        "due_day_group": due_day_group,
        "sheet_title": sheet_title,
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


def upsert_batch_state(month_key, due_day_group, batch_number, task_count, due_at=None):
    payload = {
        "month_key": month_key,
        "due_day_group": due_day_group,
        "batch_number": batch_number,
        "task_count": task_count,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    if due_at is not None:
        payload["due_at"] = due_at
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{BATCH_TABLE}",
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()


def get_next_standard_batch_due_at(sequence_key, dry_run=False):
    """48hr-batched groups ONLY (never the fixed-24h groups). Global PER
    SEQUENCE_KEY, not per due-day-group within it: the first batch under a
    given sequence_key gets now + 48h; every later batch under that SAME
    sequence_key gets the previous batch's due date + 24h, chained in
    creation order. File Error/Data Issue's 48hr SLA batches share one
    sequence_key (the plain month_key); Integration Issue's batches use a
    distinct sequence_key (month_key + ':integration') so the two never
    compress each other's deadlines - they're not competing for attention
    in the same Asana section.

    In dry_run mode this only reads existing state and never writes - safe
    to call repeatedly without advancing the real sequence."""
    url = f"{SUPABASE_URL}/rest/v1/{STANDARD_BATCH_SEQUENCE_TABLE}?sequence_key=eq.{sequence_key}"
    resp = requests.get(url, headers=supabase_headers(), timeout=30)
    resp.raise_for_status()
    rows = resp.json()

    if rows:
        last_due_at = parse_utc_datetime(rows[0]["last_due_at"])
        next_due_at = last_due_at + dt.timedelta(hours=24)
        next_sequence = rows[0]["sequence_number"] + 1
    else:
        next_due_at = dt.datetime.utcnow() + dt.timedelta(hours=48)
        next_sequence = 1

    if not dry_run:
        payload = {
            "sequence_key": sequence_key,
            "sequence_number": next_sequence,
            "last_due_at": next_due_at.isoformat(),
        }
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/{STANDARD_BATCH_SEQUENCE_TABLE}",
            headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()

    return next_due_at.replace(microsecond=0).isoformat() + "Z", next_sequence


def get_or_create_batch_due_at(month_key, due_day_group, count_needed, apply_staggered_due_date=False,
                                sequence_key=None):
    """Unchanged in mechanics from the original script - places an entire
    same-due-date cohort into one numbered batch, never split across two,
    with the same expiry/reuse rules. sequence_key defaults to month_key
    (File Error/Data Issue's shared 48hr SLA sequence) but can be
    overridden (Integration Issue passes its own distinct sequence_key)."""
    if sequence_key is None:
        sequence_key = month_key

    state = get_batch_state(month_key, due_day_group)

    is_expired = False
    if state and state.get("due_at"):
        due_at_dt = parse_utc_datetime(state["due_at"])
        is_expired = due_at_dt <= dt.datetime.utcnow()

    remaining = (BATCH_SIZE - state["task_count"]) if state else 0

    if state and not is_expired and remaining >= count_needed:
        new_count = state["task_count"] + count_needed
        upsert_batch_state(month_key, due_day_group, state["batch_number"], new_count, due_at=state.get("due_at"))
        return state.get("due_at")

    if state and is_expired:
        print(f"Batch #{state['batch_number']} for '{due_day_group}' has an expired due date "
              f"({state['due_at']}) with only {state['task_count']}/{BATCH_SIZE} filled - "
              f"closing it and starting a fresh batch rather than silently inheriting a stale deadline.")

    next_batch_number = (state["batch_number"] + 1) if state else 1

    due_at = None
    if apply_staggered_due_date:
        due_at, sequence_number = get_next_standard_batch_due_at(sequence_key)
        print(f"Staggered due date for batch #{next_batch_number} ('{due_day_group}'): "
              f"{due_at} (sequence #{sequence_number}, sequence_key={sequence_key})")

    upsert_batch_state(month_key, due_day_group, next_batch_number, count_needed, due_at=due_at)
    print(f"New batch #{next_batch_number} for due-day group '{due_day_group}' "
          f"({count_needed} hotels, due_at={due_at})")
    return due_at


# ---------------------------------------------------------------------------
# Write-back
# ---------------------------------------------------------------------------


def get_tracked_open_tasks():
    url = f"{SUPABASE_URL}/rest/v1/{DEDUP_TABLE}"
    params = {"select": "dedup_key,sheet_title,hotel_name,asana_task_gid"}
    resp = requests.get(url, headers=supabase_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_task_completion(task_gid):
    """Returns True/False, or None if the task no longer exists (404) -
    left alone here; already_actioned()'s existing stale-record cleanup
    handles that case on the next normal flagging pass, below."""
    url = f"https://app.asana.com/api/1.0/tasks/{task_gid}"
    params = {"opt_fields": "completed"}
    resp = requests.get(url, headers=asana_headers(), params=params, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()["data"].get("completed", False)


def get_worksheet(spreadsheet, sheet_title, cache):
    if sheet_title not in cache:
        cache[sheet_title] = spreadsheet.worksheet(sheet_title)
    return cache[sheet_title]


def write_back_resolved_hotel(worksheet, hotel_name, dry_run=False):
    """One uniform resolution, regardless of which issue type flagged this
    hotel: Data Uploaded = Yes, Next Action = Processing Invoice - the same
    default state any freshly-graduated hotel starts in. Lands it back in
    Work Queue, ready to work through normally (Mark as Sent is available
    the whole time regardless of Processing Status, so there's no need for
    a distinct "resolved" label)."""
    headers_row = worksheet.row_values(1)
    col_map = {h.strip(): i + 1 for i, h in enumerate(headers_row)}

    col_hotel = col_map.get("Hotel")
    if col_hotel is None:
        print(f"WARNING: 'Hotel' column not found in worksheet '{worksheet.title}' - cannot write back.")
        return False

    all_values = worksheet.get_all_values()
    target_row = None
    for i, row in enumerate(all_values[1:], start=2):
        if len(row) >= col_hotel and row[col_hotel - 1].strip() == hotel_name:
            target_row = i
            break

    if target_row is None:
        print(f"WARNING: Could not find row for '{hotel_name}' in worksheet '{worksheet.title}' - "
              f"it may have been moved or removed since the task was created.")
        return False

    today = dt.datetime.utcnow().strftime("%Y-%m-%d")
    updates = {
        "Data Uploaded (Yes/No)": "Yes",
        "Upload Date": today,
        "Next Action": "🧾 Processing Invoice",
        "Last Updated By": "Asana Integration Sync",
    }

    if dry_run:
        print(f"[DRY RUN] Would write back '{hotel_name}' in '{worksheet.title}': {updates}")
        return True

    for col_name, value in updates.items():
        col_idx = col_map.get(col_name)
        if col_idx is None:
            continue
        worksheet.update_cell(target_row, col_idx, value)

    print(f"Wrote back '{hotel_name}' in '{worksheet.title}': {updates}")
    return True


def process_write_backs(spreadsheet, dry_run=False):
    tracked = get_tracked_open_tasks()
    if not tracked:
        print("Write-back: no tracked tasks to check.")
        return

    worksheet_cache = {}
    resolved_count = 0

    for row in tracked:
        task_gid = row.get("asana_task_gid")
        if not task_gid:
            continue

        completed = get_task_completion(task_gid)
        if completed is None or not completed:
            continue

        sheet_title = row.get("sheet_title")
        hotel_name = row["hotel_name"]
        dedup_key = row["dedup_key"]

        if not sheet_title:
            print(f"WARNING: No sheet_title recorded for '{hotel_name}' (dedup_key={dedup_key}) - "
                  f"cannot write back. This hotel was likely flagged before the sheet_title column existed.")
            continue

        try:
            worksheet = get_worksheet(spreadsheet, sheet_title, worksheet_cache)
        except Exception as e:
            print(f"WARNING: Could not open worksheet '{sheet_title}' for '{hotel_name}': {e}")
            continue

        success = write_back_resolved_hotel(worksheet, hotel_name, dry_run=dry_run)
        if success:
            resolved_count += 1
            if not dry_run:
                delete_dedup_record(dedup_key)

    print(f"Write-back complete: {resolved_count} hotel(s) {'would be ' if dry_run else ''}resolved.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(dry_run=False, month_override=None, as_of_day_override=None):
    creds = get_credentials()

    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)

    # Write-back runs first on every invocation, so a hotel resolved in
    # Asana is cleared before this run re-evaluates the sheet for new flags.
    process_write_backs(spreadsheet, dry_run=dry_run)

    values_by_title = {}
    for ws in spreadsheet.worksheets():
        try:
            values_by_title[ws.title] = get_all_values_with_retry(ws)
        except Exception as e:
            print(f"Skipping worksheet '{ws.title}' after retries failed: {e}")

    if month_override:
        target_month = month_override
    else:
        target_month = find_latest_month_key_from_tab_names(values_by_title.keys())
        if target_month is None:
            print("Could not find ANY parseable billing period across the whole sheet - nothing to do.")
            return
        print(f"No --month override given - detected most recent billing period in the sheet: {target_month}")

    if as_of_day_override:
        today_day = as_of_day_override
    elif month_override:
        year, month = (int(x) for x in target_month.split("-"))
        today_day = calendar.monthrange(year, month)[1]
    else:
        today_day = dt.date.today().day

    if dry_run:
        print(f"[DRY RUN] Testing month={target_month}, simulated today_day={today_day}. "
              f"No Asana calls or Supabase writes will be made.\n")

    found = find_current_month_worksheet(values_by_title, target_month)
    if not found:
        print(f"No worksheet found with rows matching {target_month}.")
        return

    (sheet_title, headers, col_period, col_hotel, col_priority,
     col_data_uploaded, col_next_action, col_send_by, col_data_automated) = found
    data_rows = values_by_title[sheet_title][1:]
    print(f"Using worksheet '{sheet_title}' for {target_month}, {len(data_rows)} rows.")
    if col_data_uploaded is None:
        print("WARNING: 'Data Uploaded (Yes/No)' column was NOT found on this worksheet - "
              f"File Error rows can never be detected. Headers found: {headers}")
    if col_next_action is None:
        print("WARNING: 'Next Action' column was NOT found on this worksheet - "
              "Data Issue and Integration Issue rows can never be detected.")
    if col_send_by is None:
        print("WARNING: 'Send by Date' column was NOT found on this worksheet - "
              "every flagged row will route to the non-urgent lane regardless of actual urgency.")
    if col_priority is None:
        print("WARNING: 'Priority' column was NOT found on this worksheet - "
              "no File Error/Data Issue rows will be routed as standalone priority tasks via that path.")
    if col_data_automated is None:
        print("WARNING: 'Data Automated' column was NOT found on this worksheet - "
              "the Data Automated field will not be set on any task this run.")

    if not dry_run:
        sections = get_asana_sections()
        priority_section_gid = find_section_gid(sections, PRIORITY_SECTION_NAME)
        standard_section_gid = find_section_gid(sections, STANDARD_SECTION_NAME)
    else:
        priority_section_gid = "DRY_RUN_PRIORITY_SECTION"
        standard_section_gid = "DRY_RUN_STANDARD_SECTION"

    project_tasks = fetch_all_project_tasks(ASANA_PROJECT_GID)

    sweep_duplicate_hotel_tasks(project_tasks, dry_run=dry_run)

    # Hotels with an open task from ANY prior cycle/issue type - applies
    # uniformly across File Error, Data Issue, and Integration Issue, per
    # confirmed behavior: any flag with a matching currently-open task
    # never creates a second task.
    open_hotel_task_names = build_open_hotel_task_names(project_tasks)

    priority_flag_hotels = []   # (hotel_name, data_automated, issue_type_option_gid) - File Error/Data Issue, Priority=Yes
    groups = {}                  # due_day_group -> [(hotel_name, data_automated, issue_type_option_gid)] - File Error/Data Issue, not Priority
    integration_hotels = []      # (hotel_name, data_automated) - Integration Issue, ALWAYS this list regardless of Priority
    integration_due_day_group = None  # set once we know which lane (24h fixed vs batched) integration hotels fall into this run

    for row in data_rows:
        if len(row) <= col_period or len(row) <= col_hotel:
            continue
        hotel_name = row[col_hotel].strip()
        if not hotel_name:
            continue

        data_uploaded_value = (
            row[col_data_uploaded].strip() if (col_data_uploaded is not None and len(row) > col_data_uploaded) else ""
        )
        next_action_value = (
            row[col_next_action].strip() if (col_next_action is not None and len(row) > col_next_action) else ""
        )

       # Before
        is_file_error = data_uploaded_value.lower() == "error"
        is_integration_issue = "integration issue" in next_action_value.lower()
        is_data_issue = (not is_integration_issue) and "data issue" in next_action_value.lower()
        
        if not is_file_error and not is_data_issue and not is_integration_issue:
            continue  # not flagged - no action, by design
          # After
        data_uploaded_lower = data_uploaded_value.lower()
        is_file_error = data_uploaded_lower == "error"
        is_integration_issue = data_uploaded_lower == "integration issue"
        is_data_issue = "data issue" in next_action_value.lower()
        
        if not is_file_error and not is_data_issue and not is_integration_issue:
            continue  # not flagged (includes Delayed Billing - deliberately no Asana trigger)

        if is_file_error:
            issue_type_option_gid = ISSUE_TYPE_FILE_ERROR_OPTION_GID
        elif is_integration_issue:
            issue_type_option_gid = ISSUE_TYPE_INTEGRATION_ISSUE_OPTION_GID
        else:
            issue_type_option_gid = ISSUE_TYPE_DATA_ISSUE_OPTION_GID

        dedup_key = f"{sheet_title}:{hotel_name}"
        if already_actioned_with_legacy_fallback(sheet_title, target_month, hotel_name, dry_run=dry_run):
            continue

        if hotel_name in open_hotel_task_names:
            print(f"Skipping '{hotel_name}' - already has an open task in the project "
                  f"from a previous cycle; won't create another until that one is resolved.")
            continue

        data_automated_value = (
            is_truthy(row[col_data_automated]) if (col_data_automated is not None and len(row) > col_data_automated) else False
        )

        send_by_value = row[col_send_by].strip() if (col_send_by is not None and len(row) > col_send_by) else ""

        # Integration Issue: ALWAYS routes to the fixed Integrations
        # section, ignoring Priority entirely. Send by Date still governs
        # the due date (24h fixed vs batched) within that one destination.
        if is_integration_issue:
            this_due_day_group = "integration_overdue" if send_by_value else "integration_blank"
            integration_due_day_group = this_due_day_group
            integration_hotels.append((hotel_name, data_automated_value, this_due_day_group))
            continue

        # File Error / Data Issue: Priority=Yes bypasses due-date routing
        # entirely, straight to Priority regardless of Send by Date.
        priority_value = (
            row[col_priority].strip() if (col_priority is not None and len(row) > col_priority) else ""
        )
        if priority_value.lower() == "yes":
            priority_flag_hotels.append((hotel_name, data_automated_value, issue_type_option_gid))
            continue

        due_day_group = "overdue" if send_by_value else "blank"
        groups.setdefault(due_day_group, []).append((hotel_name, data_automated_value, issue_type_option_gid))

    if not priority_flag_hotels and not groups and not integration_hotels:
        print("No new flagged rows to action.")
        return

    # Pass 2a: File Error/Data Issue, Priority = Yes - standalone task
    # straight to Priority, due_at = creation time + 24h.
    if priority_flag_hotels:
        priority_due_at = (dt.datetime.utcnow() + dt.timedelta(hours=24)).replace(microsecond=0).isoformat() + "Z"
        if dry_run:
            names_only = [h for h, _, _ in priority_flag_hotels]
            print(f"[DRY RUN] Would create {len(priority_flag_hotels)} standalone task(s) "
                  f"in Priority (due_at={priority_due_at}): {names_only}")
        else:
            for hotel_name, data_automated_value, issue_type_option_gid in priority_flag_hotels:
                dedup_key = f"{sheet_title}:{hotel_name}"
                task_gid = create_standalone_task(
                    hotel_name, target_month, priority_section_gid, issue_type_option_gid,
                    due_at=priority_due_at, data_automated=data_automated_value,
                )
                record_actioned(dedup_key, target_month, hotel_name, task_gid, "priority_flag", sheet_title=sheet_title)
                print(f"Priority=Yes -> standalone task {task_gid} for '{hotel_name}' "
                      f"(Data Automated={data_automated_value}, due_at={priority_due_at})")

    # Pass 2b: File Error/Data Issue, everything else - grouped by whether
    # Send by Date is populated. Unchanged batching mechanics from the
    # original script.
    for due_day_group, hotel_names in groups.items():
        is_priority_group = due_day_group == "overdue"
        section_label = "Priority (Within 24hrs)" if is_priority_group else "48 hr SLA"
        target_section_gid = priority_section_gid if is_priority_group else standard_section_gid
        apply_staggered_due_date = not is_priority_group

        if dry_run:
            if is_priority_group:
                projected_due_at = (dt.datetime.utcnow() + dt.timedelta(hours=24)).replace(microsecond=0).isoformat() + "Z"
                print(f"[DRY RUN] Group '{due_day_group}' -> section '{section_label}', "
                      f"would create {len(hotel_names)} standalone task(s) with due_at={projected_due_at}: "
                      f"{[h for h, _, _ in hotel_names]}")
                continue

            state = get_batch_state(target_month, due_day_group)
            is_expired = False
            if state and state.get("due_at"):
                due_at_dt = parse_utc_datetime(state["due_at"])
                is_expired = due_at_dt <= dt.datetime.utcnow()
            remaining = (BATCH_SIZE - state["task_count"]) if state else 0
            count_needed = len(hotel_names)
            if state and not is_expired and remaining >= count_needed:
                action = f"REUSE batch #{state['batch_number']} (currently {state['task_count']}/{BATCH_SIZE}, room for {remaining})"
                due_note = f"(due_at stays {state.get('due_at')})"
            else:
                next_num = (state["batch_number"] + 1) if state else 1
                if state and is_expired:
                    action = f"NEW batch #{next_num} (previous EXPIRED at {state['due_at']}, was only {state['task_count']}/{BATCH_SIZE} full)"
                else:
                    action = f"NEW batch #{next_num}" if state else f"FIRST batch #{next_num}"
                projected_due_at, seq = get_next_standard_batch_due_at(target_month, dry_run=True)
                due_note = f"(would set due_at={projected_due_at}, sequence #{seq})"
            print(f"[DRY RUN] Group '{due_day_group}' -> section '{section_label}', {action} {due_note}, "
                  f"would create {count_needed} standalone task(s): {[h for h, _, _ in hotel_names]}")
            continue

        if is_priority_group:
            due_at = (dt.datetime.utcnow() + dt.timedelta(hours=24)).replace(microsecond=0).isoformat() + "Z"
        else:
            due_at = get_or_create_batch_due_at(
                target_month, due_day_group, len(hotel_names), apply_staggered_due_date=apply_staggered_due_date,
            )

        for hotel_name, data_automated_value, issue_type_option_gid in hotel_names:
            dedup_key = f"{sheet_title}:{hotel_name}"
            task_gid = create_standalone_task(
                hotel_name, target_month, target_section_gid, issue_type_option_gid,
                due_at=due_at, data_automated=data_automated_value,
            )
            record_actioned(dedup_key, target_month, hotel_name, task_gid, due_day_group, sheet_title=sheet_title)
            print(f"Group '{due_day_group}' -> standalone task {task_gid} (due_at={due_at}) for '{hotel_name}' "
                  f"(Data Automated={data_automated_value})")

    # Pass 2c: Integration Issue - ALWAYS the fixed Integrations section.
    # Send by Date still governs due_at (24h fixed vs batched), and
    # batched due dates use their OWN independent staggering sequence
    # (sequence_key = "{month_key}:integration") so they never compress
    # File Error/Data Issue's 48hr SLA deadlines, or vice versa.
    if integration_hotels:
        by_group = {}
        for hotel_name, data_automated_value, this_due_day_group in integration_hotels:
            by_group.setdefault(this_due_day_group, []).append((hotel_name, data_automated_value))

        for due_day_group, hotel_names in by_group.items():
            is_urgent = due_day_group == "integration_overdue"
            apply_staggered_due_date = not is_urgent
            integration_sequence_key = f"{target_month}:integration"

            if dry_run:
                if is_urgent:
                    projected_due_at = (dt.datetime.utcnow() + dt.timedelta(hours=24)).replace(microsecond=0).isoformat() + "Z"
                    print(f"[DRY RUN] Integration group '{due_day_group}' -> 'Transferred to Integrations', "
                          f"would create {len(hotel_names)} standalone task(s) with due_at={projected_due_at}: "
                          f"{[h for h, _ in hotel_names]}")
                else:
                    projected_due_at, seq = get_next_standard_batch_due_at(integration_sequence_key, dry_run=True)
                    print(f"[DRY RUN] Integration group '{due_day_group}' -> 'Transferred to Integrations', "
                          f"would batch {len(hotel_names)} standalone task(s) "
                          f"(would set due_at={projected_due_at}, sequence #{seq}, sequence_key={integration_sequence_key}): "
                          f"{[h for h, _ in hotel_names]}")
                continue

            if is_urgent:
                due_at = (dt.datetime.utcnow() + dt.timedelta(hours=24)).replace(microsecond=0).isoformat() + "Z"
            else:
                due_at = get_or_create_batch_due_at(
                    target_month, due_day_group, len(hotel_names),
                    apply_staggered_due_date=apply_staggered_due_date,
                    sequence_key=integration_sequence_key,
                )

            for hotel_name, data_automated_value in hotel_names:
                dedup_key = f"{sheet_title}:{hotel_name}"
                task_gid = create_standalone_task(
                    hotel_name, target_month, INTEGRATIONS_SECTION_GID, ISSUE_TYPE_INTEGRATION_ISSUE_OPTION_GID,
                    due_at=due_at, data_automated=data_automated_value,
                )
                record_actioned(dedup_key, target_month, hotel_name, task_gid, due_day_group, sheet_title=sheet_title)
                print(f"Integration Issue '{due_day_group}' -> standalone task {task_gid} (due_at={due_at}) "
                      f"for '{hotel_name}' (Data Automated={data_automated_value})")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                         help="No Asana calls or Supabase writes - just prints what would happen.")
    parser.add_argument("--month", type=str, default=None,
                         help="Override target month, e.g. 2026-06, to test against a past tab.")
    parser.add_argument("--as-of-day", type=int, default=None,
                         help="Simulate 'today' as this day-of-month, for testing overdue logic on a past month.")
    args = parser.parse_args()

    main(
        dry_run=args.dry_run,
        month_override=args.month,
        as_of_day_override=args.as_of_day,
    )
