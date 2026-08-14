"""
sync_yellow_rows_to_asana.py

Scans the CURRENT month's billing tab only (not historical tabs) for
rows checked in the "Flag to Innova" column. Unchecked/blank = no
action. Checked rows become a standalone TOP-LEVEL task directly in
one of two existing sections - NO parent, no subtask nesting at all:

- "Priority (Within 24hrs)" - due today or already passed.
- "48 hr SLA"                - everything else, regardless of how far out.
  (There is no third, lower-urgency destination - confirmed.)

--- Why standalone tasks, not batch-parent-with-subtasks ---
This used to nest hotels as subtasks under a shared batch/summary
parent (to consolidate notifications). That caused real, confirmed
confusion: an assignee-triggered Asana rule that moves a hotel to a
different section (e.g. Email Contact) changes that hotel's OWN section
membership, but never touches its pre-existing parent relationship - so
the hotel kept showing up under its old parent too, even though it also
correctly appeared in its new section. Every hotel is now a standalone
task from creation, so there's no parent for it to get stuck under.
Notification volume (the original reason for nesting) is being
addressed separately, as its own follow-up - not by nesting tasks.

--- How "current month" is determined ---
NOT a fixed calendar formula. This scans every tab, finds every Billing
Period Analyzed value that actually exists, and targets whichever month
is MOST RECENT. This replaced an earlier "today's month minus one"
assumption that broke the first time the team got ahead of the calendar
and started working next month's tab before the calendar actually
rolled over - a fixed offset can't account for a team running ahead or
behind schedule, but reading the sheet's own content always can.
--month overrides this entirely, for testing against a specific past
month regardless of what's most recent.

--- "Batch" is now pure Supabase bookkeeping, not an Asana object ---
Rows are still grouped by Priority Due Date first (rows due the same
day-of-month stay together; overdue rows share the group "overdue"),
still capped at 25 hotels per numbered batch, and same-due-date cohorts
still never split across two batch numbers, even partially - all of
that logic is unchanged. What changed is the ACTION taken once a cohort
is assigned to a batch number: instead of creating an Asana parent task
to hold it, the batch's due_at (for 48hr SLA - staggered, chained, see
get_next_standard_batch_due_at) is stamped directly onto each individual
hotel task. There is no Asana task anywhere representing "a batch" -
only Supabase's asana_batch_sections table knows batch numbers exist.

The actual due date value from the SHEET is still never written into
any Asana-visible field (task name or notes) - it's used only for
internal routing/batch-assignment and logged to Supabase. The due_at
that DOES appear on a task is the computed SLA deadline, not the raw
sheet value.

--- Why a checkbox instead of cell color ---
This used to detect flagged rows by background color (yellow vs green).
That was abandoned after two separate, never-fully-resolved mysteries:
a due-date column reading as empty despite visibly showing text, and a
"Flag to Innova" candidate row (Amanpuri) reading as an unrecognized
color despite being visibly yellow across the whole row, with values
that didn't match calibration against any tab on the sheet. A checkbox
reads as a plain TRUE/FALSE via the API - no RGB tolerance, no
effectiveFormat-vs-userEnteredFormat ambiguity, no color-matching at
all. The sheet can still be colored for humans scanning it visually;
the script no longer looks at color at all.

--- Confirmed decisions (see chat) ---
- "Due today" counts as overdue -> routes to Priority (Within 24hrs).
- Everything else -> "48 hr SLA", regardless of how far away the due
  date is. No third tier.
- This script does not set any due date on Priority tasks - an existing
  Asana rule on that section handles the SLA natively.
"""

import os
import re
import json
import time
import calendar
import datetime as dt

import requests
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SHEET_ID = "10osrvx4zsemAQy3rAci2tbV3cAzRBSM8ocecbnuw76I"
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

ASANA_TOKEN = os.environ["ASANA_PAT"]
ASANA_PROJECT_GID = "1207448572741662"  # Data Processing Requests
PRIORITY_SECTION_NAME = "Priority (Within 24hrs)"  # due today/overdue
# "Data Automated" mirrors the sheet's checkbox column. Asana has no
# native boolean/checkbox custom field type - this is the standard
# workaround: a single-select field with exactly ONE option. Selected =
# checked/true, left blank = unchecked/false.
DATA_AUTOMATED_FIELD_GID = "1217236216436084"
DATA_AUTOMATED_YES_OPTION_GID = "1217236216436085"
STANDARD_SECTION_NAME = "48 hr SLA"                # everything else
BATCH_SIZE = 25

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
DEDUP_TABLE = "yellow_row_asana_tasks"
BATCH_TABLE = "asana_batch_sections"
STANDARD_BATCH_SEQUENCE_TABLE = "standard_sla_batch_sequence"

TRUE_VALUES = {"true", "yes", "y", "1", "checked"}

COLUMN_ALIASES = {
    "billing_period": ["Billing Period Analyzed", "Period Being Analyzed"],
    "hotel_name": ["Hotel Name", "Hotel"],
    "priority_due_date": ["Priority Due Date"],
    "data_priority": ["Data Priority"],
    "flag_to_innova": ["Flag to Innova"],
    "data_automated": ["Data Automated"],
}

# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------


def get_credentials():
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    return Credentials.from_service_account_info(creds_dict, scopes=SHEETS_SCOPES)


def parse_utc_datetime(value):
    """Parses a timestamp string coming back from Supabase into a naive
    UTC datetime, so it stays comparable/addable with dt.datetime.utcnow()
    everywhere else in this script.

    Supabase's timestamptz columns come back suffixed like '+00:00' or
    '+00', NOT with a literal 'Z' - so a bare .replace('Z', '') does
    nothing to them, fromisoformat() parses the offset correctly and
    returns a TIMEZONE-AWARE datetime, and then comparing or adding that
    against utcnow() (naive) throws 'can't compare offset-naive and
    offset-aware datetimes'. This was a real, if latent, incident."""
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
    """Returns the month from the SECOND date in the cell (the end of a
    '{start} - {end}' range), always - not whichever date is
    chronologically later, and not the first. Billing periods are always
    written start-then-end by convention, so this is positional, not a
    comparison: even a malformed row where end < start should still be
    read by its end date, not whichever number happens to be bigger.
    Falls back to the only date present if there's just one."""
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


def find_latest_month_key(values_by_title):
    """Scans every worksheet's Billing Period Analyzed values and returns
    the MOST RECENT month_key actually found anywhere in the sheet -
    bounded to exactly two possible values: last month (the normal
    cadence - July's invoices are processed in August) or this month
    (the team started a few days early). Nothing else is physically
    possible, since invoicing can't run ahead of a month that hasn't
    happened yet, and can't run more than one month behind without
    something being badly wrong.

    This bound is what stops garbage data on an old or malformed tab
    from hijacking the result (a real incident: a 2020 tab's data
    parsed into '2038' and nearly became the target).

    This replaces a fixed 'always previous calendar month' assumption,
    which broke the first time the team got a few days ahead and started
    working this month's tab before the calendar rolled over. Letting
    the sheet's real content decide - within this tight, physically
    correct bound - means this follows wherever the team actually is,
    without trusting implausible outliers from years-old tabs."""
    today = dt.date.today()
    earliest_plausible = month_key_shift(today, -1)   # normal cadence: previous month
    latest_plausible = month_key_shift(today, 0)       # early start: this month, never beyond -
                                                        # invoicing can't run ahead of a month
                                                        # that hasn't happened yet

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
                continue  # implausibly old/future - almost certainly garbage
            if latest is None or month_key > latest:
                latest = month_key
    return latest


def month_key_shift(base_date, months):
    """Returns the 'YYYY-MM' key for base_date shifted by +/- months."""
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
        col_due = find_col_index(headers, "priority_due_date")
        col_data_priority = find_col_index(headers, "data_priority")
        col_flag = find_col_index(headers, "flag_to_innova")
        col_data_automated = find_col_index(headers, "data_automated")
        if col_period is None or col_hotel is None:
            continue
        for row in values[1:]:
            if len(row) <= col_period:
                continue
            if parse_billing_period(row[col_period]) == target_month_key:
                return title, headers, col_period, col_hotel, col_due, col_data_priority, col_flag, col_data_automated
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


def find_section_gid(sections, name):
    for s in sections:
        if s["name"].strip().lower() == name.strip().lower():
            return s["gid"]
    raise RuntimeError(
        f"No Asana section named '{name}' found in project {ASANA_PROJECT_GID}. "
        f"Check the exact section name and update the config constant if it differs."
    )


def create_standalone_task(hotel_name, month_key, section_gid, due_at=None, data_automated=False):
    """Creates a hotel as a standalone TOP-LEVEL task directly in the
    given section - no parent, no batch container, no subtask nesting
    at all. Replaces the earlier batch-parent-with-subtasks and
    priority-summary-with-subtasks designs, both retired because nesting
    caused real, confirmed confusion: hotels moved to a different
    section by an assignee-triggered rule kept visually showing up under
    their old parent too, since the rule changes a task's own section
    membership but never touches its pre-existing parent relationship.

    'Batch' is now purely a Supabase bookkeeping concept - which numbered
    batch a cohort belongs to, still governing the 25-cap, the never-
    split-same-due-date rule, and the staggered due-date chaining - with
    NO corresponding object in Asana at all. due_at (when given) is
    stamped directly onto this individual hotel task.

    data_automated mirrors the sheet's "Data Automated" checkbox onto
    the single-select workaround field - selecting its one option when
    True, leaving the field genuinely unset (not the field key omitted
    from the payload for readability, but no value assigned) when False,
    matching the sheet's own checked/unchecked semantics."""
    payload = {
        "data": {
            "name": hotel_name,
            "notes": f"Flagged via 'Flag to Innova' on the Curacity Billing Overview sheet for {month_key}.",
            "projects": [ASANA_PROJECT_GID],
            "memberships": [{"project": ASANA_PROJECT_GID, "section": section_gid}],
        }
    }
    if due_at:
        payload["data"]["due_at"] = due_at
    if data_automated:
        payload["data"]["custom_fields"] = {DATA_AUTOMATED_FIELD_GID: DATA_AUTOMATED_YES_OPTION_GID}
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


def already_actioned(dedup_key):
    """Uses params= so requests properly URL-encodes dedup_key - hotel
    names routinely contain commas and ampersands ('Resort & Suites',
    'Sky Rock Sedona, a Tribute Portfolio Hotel'), and an unencoded '&'
    in a manually-built URL string starts a NEW query parameter instead
    of being part of the value. That silently broke this exact check for
    every hotel with such a character in its name - it would always come
    back 'not found' even for genuinely already-actioned hotels, which is
    precisely how this produced systemic, not occasional, duplicates."""
    url = f"{SUPABASE_URL}/rest/v1/{DEDUP_TABLE}"
    params = {"dedup_key": f"eq.{dedup_key}", "select": "dedup_key"}
    resp = requests.get(url, headers=supabase_headers(), params=params, timeout=30)
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


def upsert_batch_state(month_key, due_day_group, batch_number, task_count, due_at=None):
    """Purely Supabase-side bookkeeping now - no batch_task_gid, since
    there's no longer an Asana object representing a batch at all. Still
    tracks exactly what it always did: how many hotels are in this
    numbered batch, and what due_at applies to it (for staggering and
    expiry-checking) - just nothing for a gid to point at anymore."""
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


def get_next_standard_batch_due_at(month_key, dry_run=False):
    """48hr SLA batches ONLY (never Priority - that has its own native-
    rule SLA already). Global per month, NOT per due-day-group: the
    first 48hr-SLA batch created this month gets now + 48h; every batch
    created after that gets the PREVIOUS batch's due date + 24h, chained
    in creation order regardless of which due-day-group triggered it.

    In dry_run mode this only reads existing state and never writes -
    safe to call repeatedly without advancing the real sequence."""
    url = f"{SUPABASE_URL}/rest/v1/{STANDARD_BATCH_SEQUENCE_TABLE}?month_key=eq.{month_key}"
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
            "month_key": month_key,
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


def get_or_create_batch_due_at(month_key, due_day_group, count_needed, apply_staggered_due_date=False):
    """Places an entire same-due-date cohort (count_needed new hotel
    tasks from this run) into one numbered batch - never split across
    two. Returns the due_at to stamp directly on each hotel task in this
    cohort (or None for Priority, which has its own native-rule SLA).

    'Batch' is PURE Supabase bookkeeping now - no Asana object at all.
    Every rule that applied when batches were Asana parent tasks still
    applies identically here, just without anything in Asana for it to
    point at:

    If the currently open batch has enough remaining room for ALL of
    count_needed, AND its due date (if any) hasn't already passed, this
    cohort joins it (same due_at as whatever's already in that batch).
    If not, a brand new batch number opens for the WHOLE cohort, even if
    that leaves the previous batch permanently short of 25. Same-due-
    date rows are never split across two batches, even partially.

    A batch whose due date has already passed is treated as CLOSED
    regardless of how few hotels it has - a low-volume batch that takes
    days to fill would otherwise let a hotel added on day 4 silently
    inherit a due date set back on day 1, possibly already expired.

    apply_staggered_due_date=True ONLY for 48hr SLA (never Priority) -
    see get_next_standard_batch_due_at for the 48h-then-chained-24h
    logic. Reusing an existing batch never recomputes its due_at - only
    set once, at that batch's creation."""
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
        due_at, sequence_number = get_next_standard_batch_due_at(month_key)
        print(f"Staggered due date for batch #{next_batch_number} ('{due_day_group}'): "
              f"{due_at} (sequence #{sequence_number})")

    upsert_batch_state(month_key, due_day_group, next_batch_number, count_needed, due_at=due_at)
    print(f"New batch #{next_batch_number} for due-day group '{due_day_group}' "
          f"({count_needed} hotels, due_at={due_at})")
    return due_at


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(dry_run=False, month_override=None, as_of_day_override=None):
    creds = get_credentials()

    import gspread
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)

    values_by_title = {}
    for ws in spreadsheet.worksheets():
        try:
            values_by_title[ws.title] = ws.get_all_values()
            time.sleep(1.1)
        except Exception as e:
            print(f"Skipping worksheet '{ws.title}': {e}")

    if month_override:
        target_month = month_override
    else:
        target_month = find_latest_month_key(values_by_title)
        if target_month is None:
            print("Could not find ANY parseable billing period across the whole sheet - nothing to do.")
            return
        print(f"No --month override given - detected most recent billing period in the sheet: {target_month}")

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

    found = find_current_month_worksheet(values_by_title, target_month)
    if not found:
        print(f"No worksheet found with rows matching {target_month}.")
        return

    sheet_title, headers, col_period, col_hotel, col_due, col_data_priority, col_flag, col_data_automated = found
    data_rows = values_by_title[sheet_title][1:]
    print(f"Using worksheet '{sheet_title}' for {target_month}, {len(data_rows)} rows.")
    if col_flag is None:
        print("WARNING: 'Flag to Innova' column was NOT found on this worksheet - "
              f"no rows can be actioned. Headers found: {headers}")
        return
    if col_due is None:
        print("WARNING: 'Priority Due Date' column was NOT found on this worksheet - "
              "every row will show due_day_group='blank' regardless of actual due dates. "
              f"Headers found: {headers}")
    if col_data_priority is None:
        print("WARNING: 'Data Priority' column was NOT found on this worksheet - "
              "no rows will be routed as standalone priority tasks.")
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

    # Pass 1: figure out which rows are new (Flag to Innova checked, not
    # yet actioned). Split into two paths:
    #  - Data Priority = Yes -> nested under a time-bucketed summary
    #    batch (accumulates until the 3pm ET rollover - see Pass 2a).
    #  - everything else -> grouped by due_day_group for nested batching,
    #    exactly as originally designed.
    priority_flag_hotels = []  # list of (hotel_name, data_automated)
    groups = {}  # due_day_group -> list of (hotel_name, data_automated)
    for row in data_rows:
        if len(row) <= max(col_period, col_hotel, col_flag):
            continue
        hotel_name = row[col_hotel].strip()
        if not hotel_name:
            continue

        if not is_truthy(row[col_flag]):
            continue  # not flagged - no action, by design

        # Keyed on sheet_title, not target_month: target_month is a single
        # global value (most recent billing period found ANYWHERE in the
        # sheet), reapplied to every row in this worksheet on every run.
        # If a row's own Billing Period Analyzed cell gets hand-corrected
        # (confirmed happening - see "this is an August file not a July
        # file"-style comments), target_month for that same row can differ
        # run-to-run even though nothing about the underlying invoice
        # changed, producing a new dedup_key and a duplicate task for a
        # hotel that was already actioned. sheet_title is the worksheet
        # tab itself, which doesn't change when a cell's label is fixed -
        # a hotel still gets a fresh key each real month (each month has
        # its own tab), so legitimate month-to-month re-actioning still
        # works exactly as before.
        dedup_key = f"{sheet_title}:{hotel_name}"
        if already_actioned(dedup_key):  # read-only either way, safe in dry-run
            continue

        data_automated_value = (
            is_truthy(row[col_data_automated]) if (col_data_automated is not None and len(row) > col_data_automated) else False
        )

        data_priority_value = (
            row[col_data_priority].strip() if (col_data_priority is not None and len(row) > col_data_priority) else ""
        )
        if data_priority_value.lower() == "yes":
            priority_flag_hotels.append((hotel_name, data_automated_value))
            continue

        due_value = row[col_due].strip() if (col_due is not None and len(row) > col_due) else ""
        due_day = parse_due_day(due_value)

        if due_day is not None and due_day <= today_day:
            due_day_group = "overdue"
        else:
            due_day_group = str(due_day) if due_day is not None else "blank"

        groups.setdefault(due_day_group, []).append((hotel_name, data_automated_value))

    if not priority_flag_hotels and not groups:
        print("No new flagged rows to action.")
        return

    # Pass 2a: Data Priority = Yes hotels - each one a standalone
    # top-level task directly in Priority, no parent at all. Reverted
    # from the earlier shared-summary-parent design after confirming
    # real, ongoing confusion: an assignee-triggered rule moving a hotel
    # to a different section (e.g. Email Contact) changes that hotel's
    # own section membership but never touches its pre-existing parent
    # relationship, so it kept showing up under its old Priority parent
    # too. Notification volume (the original reason for the shared
    # parent) is being addressed separately, as its own follow-up.
    if priority_flag_hotels:
        if dry_run:
            names_only = [h for h, _ in priority_flag_hotels]
            print(f"[DRY RUN] Would create {len(priority_flag_hotels)} standalone task(s) "
                  f"in Priority: {names_only}")
        else:
            for hotel_name, data_automated_value in priority_flag_hotels:
                dedup_key = f"{sheet_title}:{hotel_name}"  # see Pass 1 comment: keyed on the
                                                            # worksheet tab, not the mutable
                                                            # target_month label
                task_gid = create_standalone_task(hotel_name, target_month, priority_section_gid,
                                                   data_automated=data_automated_value)
                record_actioned(dedup_key, target_month, hotel_name, task_gid, due_day_group="data_priority_flag")
                print(f"Data Priority=Yes -> standalone task {task_gid} for '{hotel_name}' "
                      f"(Data Automated={data_automated_value})")

    # Pass 2b: everything else - each hotel becomes its own standalone
    # top-level task too, with due_at stamped directly on it. 'Batch' is
    # now PURE Supabase bookkeeping (25-cap, no-split-same-due-date,
    # staggered due-date chaining, expiry) - there is no Asana object
    # representing a batch anymore, by the same reasoning as Priority
    # above. Same-due-date cohorts still never split across two Supabase
    # batch numbers, and every hotel in the same batch still gets the
    # identical staggered due_at.
    for due_day_group, hotel_names in groups.items():
        section_label = "Priority (Within 24hrs)" if due_day_group == "overdue" else "48 hr SLA"
        target_section_gid = priority_section_gid if due_day_group == "overdue" else standard_section_gid
        apply_staggered_due_date = (due_day_group != "overdue")  # never for Priority

        if dry_run:
            state = get_batch_state(target_month, due_day_group)  # read-only, safe
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
                if apply_staggered_due_date:
                    projected_due_at, seq = get_next_standard_batch_due_at(target_month, dry_run=True)
                    due_note = f"(would set due_at={projected_due_at}, sequence #{seq})"
                else:
                    due_note = "(no due date - Priority has its own native-rule SLA)"
            print(f"[DRY RUN] Group '{due_day_group}' -> section '{section_label}', {action} {due_note}, "
                  f"would create {count_needed} standalone task(s): {[h for h, _ in hotel_names]}")
            continue

        due_at = get_or_create_batch_due_at(
            target_month, due_day_group, len(hotel_names), apply_staggered_due_date=apply_staggered_due_date
        )
        for hotel_name, data_automated_value in hotel_names:
            dedup_key = f"{sheet_title}:{hotel_name}"  # see Pass 1 comment: keyed on the
                                                        # worksheet tab, not the mutable
                                                        # target_month label
            task_gid = create_standalone_task(hotel_name, target_month, target_section_gid, due_at=due_at,
                                               data_automated=data_automated_value)
            record_actioned(dedup_key, target_month, hotel_name, task_gid, due_day_group)
            print(f"Group '{due_day_group}' -> standalone task {task_gid} (due_at={due_at}) for '{hotel_name}' "
                  f"(Data Automated={data_automated_value})")


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
