"""
sync_reports_sent_monthly.py

Tracks how many client performance reports were sent to hotels each
month, for the "Workload" dashboard section - source: the "CS Monthly
Report Schedule" Google Sheet.

--- Confirmed 2026-08-26 ---
"Sent to Hotel" is a real checkbox column, "Send Date" is a real date
column, and across the last three months' tabs (May/June/July 2026)
there are zero rows where the flag and date disagree (True-with-no-
date or False-with-a-date) - so unlike the billing sheet's free-text
Send Date problem, this data is clean enough to trust the flag+date
pair directly without a fallback path.

--- Why this scans every tab and buckets by the row's own Send Date,
    not by which tab a row lives on ---
Tab titles here are inconsistent in ways that make them unsafe to
parse: naming has drifted over years (AUGUST24 vs August 2025 vs July
2026, some with stray leading spaces), and a tab's own name doesn't
even match the month its Send Dates fall in - "July 2026 Report List"
contains rows sent in AUGUST (reports for July's performance go out
the following month, same processing-offset pattern as the billing
sheet). Rather than parse tab names or guess at an offset, this reads
Send Date directly - it's an actual date value, not a text label, so
grouping by it is exact with no guessing required. This also means no
"current tab" needs to be identified at all - every tab is scanned,
every row buckets itself by its own Send Date.

--- Column detection ---
The "Sent to Hotel" header has a manually-edited date suffix that
changes tab to tab ("Sent to Hotel\\n(Updated 8/XX)", "...(Updated
6/18)", etc.) - matched by prefix, not exact string, same reasoning as
COLUMN_ALIASES elsewhere in this pipeline. Column POSITION also isn't
stable - July's tab has an extra "Sent in Looker Studio format?"
column inserted before Send Date that May/June's tabs don't have - so
every column is located by its header, never by a fixed index.

Same current/closed lock pattern as every other monthly sync here:
current month recomputed daily, past months written once and left
alone unless force-recomputed via --month.
"""

import os
import re
import time
import datetime as dt

import gspread
import requests
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SHEET_ID = "1oOi2pPRG4ERdAxzKfOX-aMr94KB6tRQ7KcCg9oU5ODw"
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

SHEETS_REQUEST_DELAY_SECONDS = 1.1
SHEETS_MAX_RETRIES = 5

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
TABLE = "reports_sent_monthly"

TRAILING_MONTHS = 24

COLUMN_PREFIXES = {
    "sent_to_hotel": "sent to hotel",
    "send_date": "send date",
    "hotel": "hotel",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def normalize(s):
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def find_col_index(headers, prefix_key):
    prefix = COLUMN_PREFIXES[prefix_key]
    for i, h in enumerate(headers):
        if normalize(h).startswith(prefix):
            return i
    return None


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------


def get_credentials():
    import json
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    return Credentials.from_service_account_info(creds_dict, scopes=SHEETS_SCOPES)


def get_all_values_with_retry(ws):
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


def parse_cell_date(raw):
    """get_all_values() returns display strings, not typed dates -
    matching the pattern already used in sync_data_processing_metrics.py's
    parse_date(), since these sheets show dates the same loose way
    (M/D, M/D/YY, M/D/YYYY)."""
    if not raw or not str(raw).strip():
        return None
    raw = str(raw).strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def is_truthy(value):
    return normalize(value) in {"yes", "true", "y", "checked"}


def extract_reports_sent_by_month(spreadsheet, months_wanted):
    """Scans every tab, buckets each row with Sent to Hotel=True by its
    OWN Send Date's calendar month - not by which tab it's on. Returns
    {month_key: count}."""
    counts = {mk: 0 for mk in months_wanted}

    for ws in spreadsheet.worksheets():
        try:
            values = get_all_values_with_retry(ws)
        except Exception as e:
            print(f"SKIPPING worksheet '{ws.title}' after retries failed: {e}")
            continue
        if not values:
            print(f"'{ws.title}': no values at all, skipping.")
            continue

        headers = values[0]
        col_sent = find_col_index(headers, "sent_to_hotel")
        col_date = find_col_index(headers, "send_date")

        if col_sent is None or col_date is None:
            print(f"'{ws.title}': column detection FAILED - "
                  f"sent_to_hotel={'col ' + str(col_sent) if col_sent is not None else 'NOT FOUND'}, "
                  f"send_date={'col ' + str(col_date) if col_date is not None else 'NOT FOUND'}. "
                  f"Headers seen: {headers}")
            continue

        tab_true_count = 0
        tab_matched_count = 0
        for row in values[1:]:
            if len(row) <= max(col_sent, col_date):
                continue
            if not is_truthy(row[col_sent]):
                continue
            tab_true_count += 1
            send_date = parse_cell_date(row[col_date])
            if send_date is None:
                continue
            month_key = f"{send_date.year:04d}-{send_date.month:02d}"
            if month_key in counts:
                counts[month_key] += 1
                tab_matched_count += 1

        print(f"'{ws.title}': found columns OK (sent=col {col_sent}, date=col {col_date}). "
              f"{tab_true_count} rows with Sent to Hotel=True, {tab_matched_count} of those "
              f"parsed into a wanted month.")

    return counts


# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------


def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def fetch_closed_months():
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?select=month_key,status"
    resp = requests.get(url, headers=supabase_headers(), timeout=30)
    resp.raise_for_status()
    return {r["month_key"] for r in resp.json() if r["status"] == "closed"}


def upsert_month(month_key, reports_sent, status, dry_run=False):
    payload = {
        "month_key": month_key,
        "reports_sent": reports_sent,
        "status": status,
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    if dry_run:
        print(f"[DRY RUN] Would upsert {month_key} ({status}): {payload}")
        return
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    resp = requests.post(
        url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Upserted {month_key} ({status}): {payload}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def month_key_n_back(n):
    today = dt.date.today()
    month = today.month - n
    year = today.year
    while month <= 0:
        month += 12
        year -= 1
    return f"{year:04d}-{month:02d}"


def main(force_months=None, dry_run=False):
    force_months = set(force_months or [])
    current_month_key = month_key_n_back(0)
    wanted_months = [month_key_n_back(n) for n in range(TRAILING_MONTHS + 1)]
    already_closed = fetch_closed_months()

    creds = get_credentials()
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)

    counts = extract_reports_sent_by_month(spreadsheet, wanted_months)

    for month_key in wanted_months:
        if month_key != current_month_key and month_key in already_closed and month_key not in force_months:
            continue  # locked
        status = "current" if month_key == current_month_key else "closed"
        upsert_month(month_key, counts[month_key], status, dry_run=dry_run)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                         help="No Supabase writes - just prints what would happen.")
    parser.add_argument("--month", type=str, action="append", default=None,
                         help="Force-recompute a specific already-closed month (e.g. 2026-07). Repeatable.")
    args = parser.parse_args()

    main(force_months=args.month, dry_run=args.dry_run)
