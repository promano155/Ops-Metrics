"""
sync_data_processing_metrics.py

Pulls the hotel billing tracker tabs from the "Curacity Billing Overview"
Google Sheet, computes the "7 business day" data-processing SLA metrics
per billing month, and upserts the results into Supabase so the Lovable
dashboard can read them instead of relying on manual entry.

Run daily via GitHub Actions (see data-processing-sync.yml).

--- Design notes / assumptions (confirm these match reality before trusting numbers) ---
1. "Eligible files" = rows where the Data Uploaded flag is Yes/TRUE AND an
   Upload Date is present.
2. "Sent within 7 business days" = eligible rows where a Send Date is present
   AND the number of weekday-only business days between Upload Date and
   Send Date is <= 7. Business days = Mon-Fri, no holiday calendar applied.
   The upload day itself counts as day 1 of the window - confirmed
   2026-08-25 (a file uploaded Monday 8/3 and sent Tuesday 8/11 is within
   a 7-business-day SLA, not 8). If you observe federal holidays in your
   SLA, tell me and I'll add a holiday list.
3. "Total files sent" (previous-month closed card) = count of rows in that
   month where the Results Sent flag is Yes/TRUE, regardless of how long it
   took. This is a different, broader number than "sent within 7 days."
4. Column names have drifted across tabs over the years, so columns are
   matched by ALIAS, not fixed position. If a future tab renames a column
   again, add the new name to COLUMN_ALIASES below rather than touching the
   parsing logic.
5. Locking: the current REPORTING month (one month ahead of whichever
   billing period is actively being worked - see point 6) is recomputed
   and overwritten every run. Any earlier reporting month is written
   ONCE (status='closed') and never silently overwritten again, even if
   the underlying sheet is edited later. This protects numbers that have
   already been reported out. To force a recompute of a closed month,
   pass its REPORTING label (e.g. '2026-08') as a CLI arg, or delete its
   row from the Supabase table.
6. Billing period vs. reporting label: the tab this reads is named for
   its billing period (July's billing period tab has rows dated
   '7.1.26 - 7.31.26'), but the actual WORK of processing that billing
   period happens the following month (July's invoices are processed in
   August). This script reads and locates data by the billing period
   exactly as labeled in the sheet - that part is unchanged - but stores
   and displays the result under a REPORTING label one month later,
   since "how fast did we process files" is a statement about the month
   the work happened in, not the month being billed for.
"""

import os
import re
import json
import time
import datetime as dt
from dataclasses import dataclass, field

import gspread
import requests
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SHEET_ID = "10osrvx4zsemAQy3rAci2tbV3cAzRBSM8ocecbnuw76I"

SHEETS_REQUEST_DELAY_SECONDS = 1.1
SHEETS_MAX_RETRIES = 5

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
SUPABASE_TABLE = "data_processing_monthly_metrics"

GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

TRAILING_MONTHS = 24
BUSINESS_DAY_SLA = 7
MONTH_OFFSET = 1

COLUMN_ALIASES = {
    "billing_period": ["Billing Period Analyzed", "Period Being Analyzed"],
    "hotel_name": ["Hotel Name", "Hotel"],
    "data_uploaded_flag": ["Data Uploaded (Yes/No)", "Data Uploaded"],
    "upload_date": ["Upload Date"],
    "results_sent_flag": [
        "Results Sent?",
        "Invoice Sent (Yes/No)",
        "Invoice Sent?",
        "Invoice Sent",
    ],
    "send_date": ["Send Date", "Invoice Send Date"],
}

TRUE_VALUES = {"yes", "true", "y"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def normalize(s):
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def find_col_index(headers, alias_key):
    normalized_headers = [normalize(h) for h in headers]
    for alias in COLUMN_ALIASES[alias_key]:
        alias_norm = normalize(alias)
        if alias_norm in normalized_headers:
            return normalized_headers.index(alias_norm)
    return None


def is_truthy(value):
    return normalize(value) in TRUE_VALUES


def parse_date(value, reference_year):
    if not value or not str(value).strip():
        return None
    value = str(value).strip()
    formats_with_year = ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"]
    for fmt in formats_with_year:
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    m = re.match(r"^(\d{1,2})/(\d{1,2})$", value)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        try:
            return dt.date(reference_year, month, day)
        except ValueError:
            return None
    return None


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
    month = int(month)
    return f"{year:04d}-{month:02d}", year


def business_days_elapsed(start_date, end_date):
    """Count business days from start_date THROUGH end_date, INCLUSIVE
    of both ends - the upload day itself counts as day 1 of the SLA
    window. Confirmed 2026-08-25: a file uploaded Monday 8/3 and sent
    Tuesday 8/11 is within a 7-business-day SLA (8/3=1, 8/4=2, 8/5=3,
    8/6=4, 8/7=5, [weekend], 8/10=6, 8/11=7), not 8.

    Previously this started counting the day AFTER start_date, which
    silently extended every file's effective deadline by one business
    day and undercounted how many files were actually sent within the
    real 7-business-day window. Returns None if inputs are missing or
    out of order."""
    if start_date is None or end_date is None:
        return None
    if end_date < start_date:
        return None
    days = 0
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:  # Mon-Fri
            days += 1
        current += dt.timedelta(days=1)
    return days


@dataclass
class MonthAgg:
    eligible_files: int = 0
    sent_within_sla: int = 0
    total_files_sent: int = 0
    rows_seen: int = 0


# ---------------------------------------------------------------------------
# Google Sheets extraction
# ---------------------------------------------------------------------------


def get_gspread_client():
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


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


def extract_month_aggregates(spreadsheet, months_wanted):
    aggs = {mk: MonthAgg() for mk in months_wanted}

    for ws in spreadsheet.worksheets():
        try:
            values = get_all_values_with_retry(ws)
        except Exception as e:
            print(f"SKIPPING worksheet '{ws.title}' after retries failed: {e}")
            continue
        if not values:
            continue

        headers = values[0]
        col_period = find_col_index(headers, "billing_period")
        col_uploaded = find_col_index(headers, "data_uploaded_flag")
        col_upload_date = find_col_index(headers, "upload_date")
        col_sent_flag = find_col_index(headers, "results_sent_flag")
        col_send_date = find_col_index(headers, "send_date")

        if col_period is None or col_upload_date is None or col_send_date is None:
            continue

        for row in values[1:]:
            if len(row) <= max(col_period, col_upload_date, col_send_date):
                continue
            period_raw = row[col_period]
            parsed_period = parse_billing_period(period_raw)
            if not parsed_period:
                continue
            month_key, year = parsed_period
            if month_key not in aggs:
                continue

            agg = aggs[month_key]
            agg.rows_seen += 1

            uploaded_flag = is_truthy(row[col_uploaded]) if col_uploaded is not None else bool(row[col_upload_date])
            upload_date = parse_date(row[col_upload_date], year)
            sent_flag = is_truthy(row[col_sent_flag]) if col_sent_flag is not None else bool(row[col_send_date])
            send_date = parse_date(row[col_send_date], year)

            if uploaded_flag and upload_date:
                agg.eligible_files += 1
                if sent_flag and send_date:
                    elapsed = business_days_elapsed(upload_date, send_date)
                    if elapsed is not None and elapsed <= BUSINESS_DAY_SLA:
                        agg.sent_within_sla += 1

            if sent_flag:
                agg.total_files_sent += 1

    return aggs


def list_rows_for_billing_period(spreadsheet, target_billing_period):
    """Read-only diagnostic - makes NO Supabase calls. Walks every
    worksheet the same way extract_month_aggregates does, but instead of
    only tallying counts, returns one record per row whose Billing
    Period Analyzed matches target_billing_period, showing exactly which
    sheet tab it came from, its hotel name, upload/send dates, computed
    elapsed business days, and whether it counted as eligible / within
    SLA. Built specifically to let a human compare against known-correct
    numbers and point at the exact rows causing a discrepancy, rather
    than guessing at the cause from aggregate counts alone.

    Also flags duplicate (sheet-independent) hotel+date combinations
    that appear on MORE than one worksheet tab under the same billing
    period label - a real way overcounting can happen, since this
    script (by design, to survive tab-naming drift) buckets by the
    Billing Period Analyzed VALUE, not by which tab it's on."""
    records = []
    for ws in spreadsheet.worksheets():
        try:
            values = get_all_values_with_retry(ws)
        except Exception as e:
            print(f"SKIPPING worksheet '{ws.title}' after retries failed: {e}")
            continue
        if not values:
            continue

        headers = values[0]
        col_period = find_col_index(headers, "billing_period")
        col_hotel = find_col_index(headers, "hotel_name")
        col_uploaded = find_col_index(headers, "data_uploaded_flag")
        col_upload_date = find_col_index(headers, "upload_date")
        col_sent_flag = find_col_index(headers, "results_sent_flag")
        col_send_date = find_col_index(headers, "send_date")

        if col_period is None or col_upload_date is None or col_send_date is None:
            continue

        for row_num, row in enumerate(values[1:], start=2):
            if len(row) <= max(col_period, col_upload_date, col_send_date):
                continue
            parsed_period = parse_billing_period(row[col_period])
            if not parsed_period:
                continue
            month_key, year = parsed_period
            if month_key != target_billing_period:
                continue

            hotel_name = row[col_hotel].strip() if (col_hotel is not None and len(row) > col_hotel) else "(no hotel name column)"
            uploaded_flag = is_truthy(row[col_uploaded]) if col_uploaded is not None else bool(row[col_upload_date])
            upload_date = parse_date(row[col_upload_date], year)
            sent_flag = is_truthy(row[col_sent_flag]) if col_sent_flag is not None else bool(row[col_send_date])
            send_date = parse_date(row[col_send_date], year)

            eligible = bool(uploaded_flag and upload_date)
            elapsed = business_days_elapsed(upload_date, send_date) if (sent_flag and send_date) else None
            within_sla = eligible and elapsed is not None and elapsed <= BUSINESS_DAY_SLA

            records.append({
                "sheet_title": ws.title,
                "row_num": row_num,
                "hotel_name": hotel_name,
                "upload_date_raw": row[col_upload_date] if len(row) > col_upload_date else "",
                "upload_date_parsed": upload_date.isoformat() if upload_date else None,
                "send_date_raw": row[col_send_date] if len(row) > col_send_date else "",
                "send_date_parsed": send_date.isoformat() if send_date else None,
                "elapsed_business_days": elapsed,
                "eligible": eligible,
                "counted_within_sla": within_sla,
            })

    return records


def print_diagnostic_listing(records):
    within_sla_records = [r for r in records if r["counted_within_sla"]]
    print(f"\n{len(records)} total rows matched this billing period across all tabs.")
    print(f"{len(within_sla_records)} currently counted as 'within 7 business days'.\n")

    print("--- Rows counted as within SLA ---")
    for r in within_sla_records:
        print(f"  [{r['sheet_title']} row {r['row_num']}] {r['hotel_name']}: "
              f"uploaded {r['upload_date_raw']!r} -> {r['upload_date_parsed']}, "
              f"sent {r['send_date_raw']!r} -> {r['send_date_parsed']}, "
              f"elapsed={r['elapsed_business_days']}bd")

    # Duplicate detection: same hotel + same parsed send date appearing
    # on more than one sheet tab under this billing period - a real
    # mechanism for overcounting given this script buckets by billing
    # period VALUE, not by tab.
    seen = {}
    for r in within_sla_records:
        key = (r["hotel_name"], r["send_date_parsed"])
        seen.setdefault(key, []).append(r["sheet_title"])
    dupes = {k: v for k, v in seen.items() if len(v) > 1}
    if dupes:
        print("\n--- Possible duplicates (same hotel + send date on multiple tabs) ---")
        for (hotel, send_date), tabs in dupes.items():
            print(f"  {hotel} sent {send_date}: appears on tabs {tabs}")
    else:
        print("\nNo same-hotel-and-send-date duplicates found across tabs.")


# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------


def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def fetch_existing_month_keys(status_filter=None):
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?select=month_key,status"
    resp = requests.get(url, headers=supabase_headers(), timeout=30)
    resp.raise_for_status()
    rows = resp.json()
    if status_filter:
        rows = [r for r in rows if r["status"] == status_filter]
    return {r["month_key"] for r in rows}


def upsert_month(month_key, agg, status, dry_run=False):
    rate = (agg.sent_within_sla / agg.eligible_files * 100) if agg.eligible_files else None
    payload = {
        "month_key": month_key,
        "eligible_files": agg.eligible_files,
        "sent_within_7bd": agg.sent_within_sla,
        "rate_pct": round(rate, 1) if rate is not None else None,
        "total_files_sent": agg.total_files_sent,
        "status": status,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    if dry_run:
        print(f"[DRY RUN] Would upsert {month_key} ({status}): {payload}")
        return
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
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
    month = today.month - (n + MONTH_OFFSET)
    year = today.year
    while month <= 0:
        month += 12
        year -= 1
    return f"{year:04d}-{month:02d}"


def shift_month_key(month_key, n):
    year, month = (int(x) for x in month_key.split("-"))
    total = year * 12 + (month - 1) + n
    year, month = divmod(total, 12)
    return f"{year:04d}-{month + 1:02d}"


def main(force_months=None, dry_run=False):
    force_months = set(force_months or [])
    current_billing_period = month_key_n_back(0)
    wanted_billing_periods = [month_key_n_back(n) for n in range(TRAILING_MONTHS + 1)]

    already_closed = fetch_existing_month_keys(status_filter="closed")

    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SHEET_ID)

    aggs = extract_month_aggregates(spreadsheet, wanted_billing_periods)

    for billing_period in wanted_billing_periods:
        agg = aggs.get(billing_period)
        if agg is None or agg.rows_seen == 0:
            continue

        report_month_key = shift_month_key(billing_period, 1)

        if billing_period == current_billing_period:
            upsert_month(report_month_key, agg, status="current", dry_run=dry_run)
        elif report_month_key in already_closed and report_month_key not in force_months:
            continue
        else:
            upsert_month(report_month_key, agg, status="closed", dry_run=dry_run)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                         help="No Supabase writes - just prints what would happen.")
    parser.add_argument("--month", type=str, action="append", default=None,
                         help="Force-recompute a specific already-closed REPORTING month "
                              "(e.g. 2026-08). Repeatable.")
    parser.add_argument("--list-rows", type=str, default=None,
                         help="Diagnostic, read-only: list every row matching this BILLING "
                              "PERIOD (e.g. 2026-07, not the reporting month) across every "
                              "tab, showing what counted and why. No Supabase calls at all "
                              "in this mode - it doesn't even read the existing table.")
    args = parser.parse_args()

    if args.list_rows:
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(SHEET_ID)
        records = list_rows_for_billing_period(spreadsheet, args.list_rows)
        print_diagnostic_listing(records)
    else:
        main(force_months=args.month, dry_run=args.dry_run)
