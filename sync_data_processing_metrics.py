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
   If you observe federal holidays in your SLA, tell me and I'll add a
   holiday list.
3. "Total files sent" = count of rows in that billing period where the
   Results Sent flag is Yes/TRUE, regardless of how long it took. This is
   a deliberately different, ONGOING metric from "sent within 7 days" -
   confirmed directly, not a bug: it keeps climbing all month as backlog
   clears, even after that billing period's SLA window (point 5) has
   locked. See point 5 and patch_total_files_sent() for how it stays live
   independently of the frozen SLA fields.
4. Column names have drifted across tabs over the years, so columns are
   matched by ALIAS, not fixed position. If a future tab renames a column
   again, add the new name to COLUMN_ALIASES below rather than touching the
   parsing logic.
5. Locking: eligible_files/sent_within_7bd/rate_pct/status lock together,
   permanently, the moment sla_window_closed() says a billing period's
   7-business-day window has closed for every possible row in it (see
   point 6 and the FIXED note below for why this is NOT simply "once the
   calendar rolls to next month"). Once locked, those four fields are
   never silently overwritten again, even if the underlying sheet is
   edited later - this protects numbers that have already been reported
   out. total_files_sent is the one exception: it keeps being refreshed
   on every run via a narrow PATCH (patch_total_files_sent()), forever,
   because it's an intentionally ongoing count, not a frozen SLA outcome.
   To force a full recompute of an already-locked month (overriding the
   freeze on all four fields, not just total_files_sent), pass its
   REPORTING label (e.g. '2026-08') as a CLI arg, or delete its row from
   the Supabase table.
6. Billing period vs. reporting label: the tab this reads is named for
   its billing period (July's billing period tab has rows dated
   '7.1.26 - 7.31.26'), but the actual WORK of processing that billing
   period happens the following month (July's invoices are processed in
   August). This script reads and locates data by the billing period
   exactly as labeled in the sheet - that part is unchanged - but stores
   and displays the result under a REPORTING label one month later,
   since "how fast did we process files" is a statement about the month
   the work happened in, not the month being billed for.

--- RULED OUT: duplicate-row theory ---
Initially suspected upsert_month()'s merge-duplicates was matching against
an unrelated primary key and silently inserting duplicate rows per month,
causing nondeterministic reads. Confirmed via pg_get_constraintdef that
month_key IS the table's primary key - merge-duplicates was correctly
targeting it all along, so this was not the bug. on_conflict=month_key is
still added explicitly below since it's harmless and makes the intent
unambiguous, but it changes no actual behavior here.

--- FIXED: the lock was tied to the wrong clock ---
Previously a billing period stayed status='current' for the entire
calendar month it was being actively worked in (via MONTH_OFFSET), and
only locked once the calendar rolled to the NEXT month. But the actual
7-business-day SLA window for a billing period closes much earlier than
that: the latest possible Upload Date in, say, July is July 31, and 7
business days after that is around August 10 - yet the old logic kept
July's report recomputing and overwriting rate_pct/total_files_sent for
three more weeks after that, purely because the calendar hadn't rolled
into September. That's what produced a "closed" month's SLA % silently
drifting (91.4% -> 90.8%) with no code bug and no duplicate rows involved
- just real invoices continuing to work through the backlog after the
window had already functionally closed.

Fix: sla_window_closed() computes, per billing period, the actual date
by which every possible row in it must have resolved (period end + 7
business days), using the same business_days_elapsed() helper already
used for the row-level SLA math. A billing period locks as soon as that
date has passed, regardless of which calendar month we're in. This
replaces the old current_billing_period/MONTH_OFFSET-based lock
decision entirely - MONTH_OFFSET/month_key_n_back is still used to
build the list of billing periods to scan, just not to decide locking.

--- Locking eligible_files/sent_within_7bd/rate_pct is NOT the same as
locking the whole row ---
A first pass at this fix locked total_files_sent along with the SLA
trio, on the theory that "once locked, nothing on this row should
change." That's wrong for total_files_sent specifically - confirmed
directly, it's meant to be a running "invoices sent this billing period"
count that keeps growing indefinitely, independent of the SLA
determination. So once a month locks: upsert_month() (full row write) is
never called for it again; instead patch_total_files_sent() runs on
every subsequent execution, touching only total_files_sent and
updated_at. status, eligible_files, sent_within_7bd, and rate_pct are
frozen forever at whatever they were the moment the window closed.

--- CORRECTED: total_files_sent's grouping key ---
A second pass at this fix grouped total_files_sent by the CALENDAR MONTH
of each row's own Send Date, scanning every billing-period tab in the
24-month backfill window looking for matching dates - built on the
assumption that a late invoice might get its Send Date filled in on an
OLD tab after a newer one already exists. Confirmed directly - that
never happens on this sheet. Older tabs are frozen the moment a new one
is created; an invoice that wasn't sent while its tab was current does
NOT get updated retroactively - the billing period itself simply shifts
forward for that hotel instead (it becomes a fresh row in the new tab,
not an update to the old one). So there is no cross-tab "late send" to
find, and scanning for one only undercounted against what a single-tab
count would show. total_files_sent is now grouped the SAME way as
eligible_files/sent_within_sla: by billing period (see
extract_month_aggregates()), computed in the one worksheet scan.
"""

import os
import re
import json
import time
import calendar
import datetime as dt
from dataclasses import dataclass, field

import gspread
import requests
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SHEET_ID = "10osrvx4zsemAQy3rAci2tbV3cAzRBSM8ocecbnuw76I"

# The workbook has 40+ monthly tabs. Reading each one with get_all_values()
# in a tight loop reliably hits the Sheets API's per-minute read quota -
# this caused a real incident where most tabs were silently skipped and
# only one made it into Supabase. Every read is now retried with backoff
# on rate-limit errors, and throttled up front to avoid hitting the wall
# in the first place.
SHEETS_REQUEST_DELAY_SECONDS = 1.1
SHEETS_MAX_RETRIES = 5

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
SUPABASE_TABLE = "data_processing_monthly_metrics"

GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]  # raw JSON string secret
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

TRAILING_MONTHS = 24  # how far back to backfill/consider for the trend line
BUSINESS_DAY_SLA = 7
MONTH_OFFSET = 1  # the actively-worked tab is always the PREVIOUS calendar
                  # month, not the current one - confirmed with the team.
                  # e.g. in late July, the live/active tab is still June's.

# Column name aliases. Matching is case-insensitive and ignores surrounding
# whitespace. Add new observed variants here as the sheet evolves.
COLUMN_ALIASES = {
    "billing_period": ["Billing Period Analyzed", "Period Being Analyzed"],
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


HOTEL_NAME_ALIASES = ["Hotel Name", "Hotel"]


def find_hotel_col_index(headers):
    normalized_headers = [normalize(h) for h in headers]
    for alias in HOTEL_NAME_ALIASES:
        if normalize(alias) in normalized_headers:
            return normalized_headers.index(normalize(alias))
    return None


def is_truthy(value):
    return normalize(value) in TRUE_VALUES


def parse_date(value, reference_year):
    """Parses the sheet's loose date formats (M/D, M/D/YY, M/D/YYYY, etc).
    Sheet dates without a year (e.g. '7/1') are assumed to fall in
    reference_year, which the caller should set to the billing month's year.
    """
    if not value or not str(value).strip():
        return None
    value = str(value).strip()
    formats_with_year = ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"]
    for fmt in formats_with_year:
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    # No-year format like "7/1" or "7/8"
    m = re.match(r"^(\d{1,2})/(\d{1,2})$", value)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        try:
            return dt.date(reference_year, month, day)
        except ValueError:
            return None
    return None


def parse_billing_period(value):
    """'6.1.26 - 6.30.26' -> (month_key='2026-06', year=2026).

    Buckets by the SECOND date in the cell (the end of a '{start} - {end}'
    range), positionally - same rule as sync_yellow_rows_to_asana.py's
    parse_billing_period(). A multi-month range like '6.1.26 - 7.31.26'
    belongs to July, not June. This previously used re.search(), which
    only ever returned the FIRST date, so every multi-month row was
    bucketed into its start month and dropped from its real month's
    counts. Falls back to the only date present if there's just one."""
    if not value:
        return None
    matches = re.findall(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", value)
    if not matches:
        return None
    month, _day, year = matches[1] if len(matches) >= 2 else matches[0]
    year = int(year)
    if year < 100:
        year += 2000
    month = int(month)
    return f"{year:04d}-{month:02d}", year


_TAB_TITLE_MONTH_RE = re.compile(
    r"^\s*(january|february|march|april|may|june|july|august|september|"
    r"october|november|december)\s+(\d{4})\b",
    re.IGNORECASE,
)


def billing_period_from_tab_title(title):
    """'July 2026 - Media Brands' / 'July 2026' -> ('2026-07', 2026).
    Returns None for anything that doesn't START with a full month name
    and a 4-digit year - deliberately strict, so reference tabs and odd
    legacy names never match."""
    m = _TAB_TITLE_MONTH_RE.match(title or "")
    if not m:
        return None
    month = list(calendar.month_name).index(m.group(1).capitalize())
    year = int(m.group(2))
    return f"{year:04d}-{month:02d}", year


def resolve_billing_period(raw_period, hotel_name, tab_title):
    """Returns (parsed_period, inferred) for one row.

    Normal path: parse the row's own Billing Period Analyzed cell.
    Fallback: ONLY when that cell is completely BLANK and the row has a
    hotel name, use the tab title's month. Confirmed real case: 8 hotels
    on 'July 2026 - Media Brands' with a blank period cell were being
    silently dropped from every count. A cell that's filled in but
    unparseable is NOT inferred - that's a data error to fix, not guess
    around. Temporary by design: this pipeline moves to the new billing
    sheet, which has no free-text period column, after launch.

    inferred=True lets callers log every row that used the fallback, so
    it stays auditable rather than silent."""
    parsed = parse_billing_period(raw_period)
    if parsed:
        return parsed, False
    if (raw_period or "").strip() == "" and (hotel_name or "").strip():
        from_title = billing_period_from_tab_title(tab_title)
        if from_title:
            return from_title, True
    return None, False


def _nth_weekday_of_month(year, month, weekday, n):
    """The date of the nth occurrence of `weekday` (Monday=0...Sunday=6)
    in the given month/year. n=-1 means the LAST occurrence (used for
    Memorial Day, which is defined as the last Monday of May, not a
    fixed ordinal one)."""
    if n > 0:
        d = dt.date(year, month, 1)
        offset = (weekday - d.weekday()) % 7
        d += dt.timedelta(days=offset)
        d += dt.timedelta(weeks=n - 1)
        return d
    last_day = calendar.monthrange(year, month)[1]
    d = dt.date(year, month, last_day)
    offset = (d.weekday() - weekday) % 7
    d -= dt.timedelta(days=offset)
    return d


def _observed(holiday_date):
    """OPM's observed-date rule for a FIXED-date federal holiday: if it
    falls on a Saturday, federal offices observe it the preceding
    Friday; if it falls on a Sunday, the following Monday. Only applies
    to fixed-date holidays (New Year's, Juneteenth, Independence Day,
    Veterans Day, Christmas) - the floating ones (MLK Day, Presidents'
    Day, Memorial Day, Labor Day, Columbus Day, Thanksgiving) are
    already defined as a specific weekday and never need shifting."""
    if holiday_date.weekday() == 5:  # Saturday
        return holiday_date - dt.timedelta(days=1)
    if holiday_date.weekday() == 6:  # Sunday
        return holiday_date + dt.timedelta(days=1)
    return holiday_date


_FEDERAL_HOLIDAY_CACHE = {}


def federal_holidays(year):
    """The 11 standard US federal holidays for a given calendar year,
    with observed-date shifting applied to the fixed-date ones. This is
    the STANDARD OPM list only - no Curacity-specific additions (a
    shutdown week, day-after-Thanksgiving, etc.). If any should be
    layered on top, add them here explicitly with a comment explaining
    why, rather than folding them silently into this list."""
    if year not in _FEDERAL_HOLIDAY_CACHE:
        _FEDERAL_HOLIDAY_CACHE[year] = {
            _observed(dt.date(year, 1, 1)),         # New Year's Day
            _nth_weekday_of_month(year, 1, 0, 3),    # MLK Day (3rd Mon, Jan)
            _nth_weekday_of_month(year, 2, 0, 3),    # Presidents' Day (3rd Mon, Feb)
            _nth_weekday_of_month(year, 5, 0, -1),   # Memorial Day (last Mon, May)
            _observed(dt.date(year, 6, 19)),         # Juneteenth
            _observed(dt.date(year, 7, 4)),          # Independence Day
            _nth_weekday_of_month(year, 9, 0, 1),    # Labor Day (1st Mon, Sep)
            _nth_weekday_of_month(year, 10, 0, 2),   # Columbus Day (2nd Mon, Oct)
            _observed(dt.date(year, 11, 11)),        # Veterans Day
            _nth_weekday_of_month(year, 11, 3, 4),   # Thanksgiving (4th Thu, Nov)
            _observed(dt.date(year, 12, 25)),        # Christmas
        }
    return _FEDERAL_HOLIDAY_CACHE[year]


def _holidays_spanning(start_date, end_date):
    """Federal holidays for every calendar year touched by [start_date,
    end_date] - almost always one year, but a date range that crosses a
    Dec 31 -> Jan 1 boundary needs both."""
    holidays = set()
    for y in range(start_date.year, end_date.year + 1):
        holidays |= federal_holidays(y)
    return holidays


def business_days_elapsed(start_date, end_date):
    """Count weekday-only business days strictly after start_date through
    end_date inclusive, EXCLUDING the 11 standard US federal holidays
    (see federal_holidays()). Returns None if inputs are missing or out
    of order.

    This is used both for the row-level "was this sent within 7 business
    days" check AND for sla_window_closed()'s lock-timing calculation -
    a holiday within either date range now correctly adds a day, in both
    places, since they share this one implementation.

    IMPORTANT: this changes results for ANY date range that spans a
    federal holiday, compared to the weekends-only version this replaced.
    A month already frozen (status='closed') is NOT retroactively
    recomputed by this change - it stays exactly as reported, by design.
    If a recently-locked month's window overlapped a holiday under the
    OLD weekends-only logic, its frozen numbers were computed with an
    incomplete business-day definition; force-recompute it via the CLI
    month-key argument if that materially affected it - see the module
    docstring's locking section for how."""
    if start_date is None or end_date is None:
        return None
    if end_date < start_date:
        return None
    holidays = _holidays_spanning(start_date, end_date)
    days = 0
    current = start_date + dt.timedelta(days=1)
    while current <= end_date:
        if current.weekday() < 5 and current not in holidays:  # Mon-Fri, not a holiday
            days += 1
        current += dt.timedelta(days=1)
    return days


def sla_window_closed(billing_period, today=None):
    """True once the 7-business-day SLA window has definitively closed
    for EVERY possible row in this billing period, regardless of which
    calendar month we're currently in.

    The latest possible Upload Date for a billing period is the last
    calendar day of that period's month. Once BUSINESS_DAY_SLA business
    days have elapsed since then, no row from this billing period could
    still be legitimately "pending" inside its SLA window - each one has
    already been sent in time, sent late, or not sent at all, and none
    of those outcomes can change by waiting longer for THIS SLA
    determination specifically (a hotel uploading months-late backdated
    data for this period is a separate, accepted tradeoff - see the
    module docstring's second FIXED note).

    Reuses business_days_elapsed(), the same helper the row-level SLA
    math uses, rather than a second date-walking implementation."""
    if today is None:
        today = dt.date.today()
    year, month = (int(x) for x in billing_period.split("-"))
    last_day = calendar.monthrange(year, month)[1]
    period_end = dt.date(year, month, last_day)
    elapsed = business_days_elapsed(period_end, today)
    return elapsed is not None and elapsed >= BUSINESS_DAY_SLA


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
    """Reads a worksheet's values, retrying with backoff on rate-limit
    errors instead of silently giving up. This is the fix for a real
    incident where a plain try/except swallowed 429s across ~40 tabs and
    only one tab's data ever made it into Supabase."""
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


def relevant_worksheets(spreadsheet, cutoff_month_key):
    """Returns worksheets worth scanning: anything whose title looks like a
    month/year tab, without assuming an exact naming scheme (tab-naming has
    drifted over the years). We filter by content (Billing Period column),
    not by tab title, so this is robust to that drift."""
    return spreadsheet.worksheets()


def extract_month_aggregates(spreadsheet, billing_periods_wanted):
    """Scans all worksheets once, bucketing rows into MonthAgg by their
    actual Billing Period Analyzed value (not by tab name) - a single
    tab occasionally contains rows spanning more than one billing
    period, though in practice each tab represents one period, and
    older tabs are never touched again once a new one is created.
    Returns {billing_period_month_key: MonthAgg}.

    CORRECTED: total_files_sent is grouped the SAME way as
    eligible_files/sent_within_sla - by billing period (i.e. whichever
    tab that period's rows live in) - NOT by the calendar month of each
    row's own Send Date. An earlier version grouped total_files_sent by
    Send Date instead, scanning across every tab in the 24-month window
    looking for matching dates. That was built on a wrong assumption:
    that an invoice which missed being sent while its tab was current
    might get its Send Date filled in LATER, on that now-old tab, after
    a newer tab already exists. Confirmed directly - that never happens.
    Older tabs are frozen the moment a new one is created; an invoice
    that wasn't sent in time doesn't get chased down retroactively in
    its original tab, the billing period itself simply shifts forward
    for that hotel instead. So there is no cross-tab "late send" to find
    - scanning every tab for it only under-served the real (single-tab)
    count and needlessly re-read 40+ tabs' worth of already-frozen data.

    Reference year for parsing no-year date values (both Upload Date and
    Send Date) always comes from the row's own Billing Period Analyzed
    cell - neither date field is assumed to carry a year on its own."""
    aggs = {mk: MonthAgg() for mk in billing_periods_wanted}

    def _cell(row, idx):
        """Safe column access. A row can be legitimately shorter than
        the header row (trailing blank cells get dropped by
        get_all_values()) without any of its actually-populated columns
        being invalid - returns '' for a missing/out-of-range column
        instead of the caller having to skip the whole row."""
        if idx is None or idx >= len(row):
            return ""
        return row[idx]

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

        # A tab needs a Billing Period column to contribute anything -
        # everything is grouped off of it. Reference tabs (contact
        # directories, Go Live Fees, etc.) that lack it entirely are
        # skipped here.
        if col_period is None:
            continue
        # Beyond that, eligible_files/sent_within_sla and
        # total_files_sent have INDEPENDENT minimum requirements: a tab
        # missing Upload Date can still contribute a Results-Sent count,
        # and a tab missing a Sent flag/Send Date can still contribute
        # eligible_files. Only skip entirely if nothing usable is present.
        if col_upload_date is None and col_sent_flag is None and col_send_date is None:
            continue

        col_hotel = find_hotel_col_index(headers)
        inferred_count = 0

        for row in values[1:]:
            parsed_period, inferred = resolve_billing_period(
                _cell(row, col_period), _cell(row, col_hotel), ws.title
            )
            if not parsed_period:
                continue
            if inferred:
                inferred_count += 1
            billing_month_key, year = parsed_period
            if billing_month_key not in aggs:
                continue

            agg = aggs[billing_month_key]
            agg.rows_seen += 1

            uploaded_flag = is_truthy(_cell(row, col_uploaded)) if col_uploaded is not None else bool(_cell(row, col_upload_date))
            upload_date = parse_date(_cell(row, col_upload_date), year)
            sent_flag = is_truthy(_cell(row, col_sent_flag)) if col_sent_flag is not None else bool(_cell(row, col_send_date))
            send_date = parse_date(_cell(row, col_send_date), year)

            if uploaded_flag and upload_date:
                agg.eligible_files += 1
                if sent_flag and send_date:
                    elapsed = business_days_elapsed(upload_date, send_date)
                    if elapsed is not None and elapsed <= BUSINESS_DAY_SLA:
                        agg.sent_within_sla += 1

            if sent_flag:
                agg.total_files_sent += 1

        if inferred_count:
            print(f"[{ws.title}] {inferred_count} row(s) had a BLANK Billing Period Analyzed cell - "
                  f"used the tab title's month instead (see resolve_billing_period).")

    return aggs


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


def upsert_month(month_key, agg, total_files_sent, status, dry_run=False):
    """Full-row write. Only ever called for a month that is NOT yet
    SLA-locked (status='current') or the very first time a month
    transitions to 'closed' - both of those are legitimate moments to
    write everything at once. Never called again for an already-locked
    month; see patch_total_files_sent() for what happens to those.

    total_files_sent is passed in explicitly as its own parameter (even
    though callers currently always pass agg.total_files_sent) so this
    function's signature doesn't quietly assume where that number came
    from.

    dry_run=True prints the payload that WOULD be written and returns
    without making any Supabase call at all."""
    rate = (agg.sent_within_sla / agg.eligible_files * 100) if agg.eligible_files else None
    payload = {
        "month_key": month_key,
        "eligible_files": agg.eligible_files,
        "sent_within_7bd": agg.sent_within_sla,
        "rate_pct": round(rate, 1) if rate is not None else None,
        "total_files_sent": total_files_sent,
        "status": status,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    if dry_run:
        print(f"[DRY RUN] Would upsert {month_key} ({status}): {payload}")
        return
    # on_conflict=month_key: month_key is confirmed to be the table's
    # actual primary key (checked directly via pg_get_constraintdef), so
    # this is just making that explicit rather than fixing a real bug -
    # an earlier duplicate-row theory here was investigated and ruled out.
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?on_conflict=month_key"
    resp = requests.post(
        url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Upserted {month_key} ({status}): {payload}")


def patch_total_files_sent(month_key, total_files_sent, dry_run=False):
    """total_files_sent is a genuinely different metric from the SLA
    trio (eligible_files/sent_within_7bd/rate_pct): "how many invoices in
    THIS billing period's tab have Results Sent = Yes, right now" - a
    running count that keeps climbing as long as that tab is still being
    worked, independent of whether the SLA determination for the same
    period has already locked. Confirmed directly - this is not a bug to
    fix, it's the intended design.

    Once a month is SLA-locked, this is the ONLY field that should keep
    updating on its row. A full upsert_month() call here would also
    recompute and overwrite eligible_files/sent_within_7bd/rate_pct with
    a fresh value from today's sheet read - which might genuinely differ
    from what was locked in (e.g. a hotel backdating an upload weeks
    late) and would silently un-freeze exactly what sla_window_closed()
    exists to protect. This does a narrow PATCH touching only
    total_files_sent and updated_at, leaving status and the three SLA
    columns exactly as they were the moment this month locked.

    dry_run=True prints what WOULD be patched and returns without
    making any Supabase call at all."""
    if dry_run:
        print(f"[DRY RUN] Would patch total_files_sent for locked month {month_key}: {total_files_sent}")
        return
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?month_key=eq.{month_key}"
    payload = {
        "total_files_sent": total_files_sent,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    resp = requests.patch(url, headers=supabase_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    print(f"Patched total_files_sent for locked month {month_key}: {total_files_sent}")


def list_rows_for_billing_period(spreadsheet, billing_period):
    """Diagnostic ONLY - makes no Supabase calls at all, read-only
    against the sheet. Prints every row found (across all worksheets)
    whose Billing Period Analyzed cell matches billing_period, with
    enough detail to manually verify eligibility/SLA/sent-month
    determinations by eye against whatever Supabase ends up showing.
    billing_period is a BILLING PERIOD (e.g. '2026-07'), not a reporting
    label - August's reporting month is built from July's billing
    period, so pass '2026-07' to inspect what feeds August's row."""

    def _cell(row, idx):
        if idx is None or idx >= len(row):
            return ""
        return row[idx]

    found = 0
    inferred_total = 0
    unparsed_by_tab = {}            # tab title -> [(hotel, raw period)]
    unrecognized_sent_values = {}   # raw Results Sent value -> row count

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
        if col_period is None:
            continue
        col_uploaded = find_col_index(headers, "data_uploaded_flag")
        col_upload_date = find_col_index(headers, "upload_date")
        col_sent_flag = find_col_index(headers, "results_sent_flag")
        col_send_date = find_col_index(headers, "send_date")

        col_hotel = find_hotel_col_index(headers)

        tab_matches = 0
        unparsed_rows = []  # (hotel, raw period cell) - rows the real sync still drops

        for row in values[1:]:
            raw_period = _cell(row, col_period)
            parsed_period, inferred = resolve_billing_period(raw_period, _cell(row, col_hotel), ws.title)
            if not parsed_period:
                hotel_raw = _cell(row, col_hotel).strip()
                if hotel_raw:
                    unparsed_rows.append((hotel_raw, raw_period))
                continue
            if parsed_period[0] != billing_period:
                continue

            found += 1
            tab_matches += 1
            if inferred:
                inferred_total += 1
            year = parsed_period[1]
            hotel = _cell(row, col_hotel) or "(no hotel name column found)"

            uploaded_flag = is_truthy(_cell(row, col_uploaded)) if col_uploaded is not None else bool(_cell(row, col_upload_date))
            upload_date = parse_date(_cell(row, col_upload_date), year)
            sent_flag = is_truthy(_cell(row, col_sent_flag)) if col_sent_flag is not None else bool(_cell(row, col_send_date))
            send_date = parse_date(_cell(row, col_send_date), year)

            raw_sent = _cell(row, col_sent_flag) if col_sent_flag is not None else ""
            if raw_sent.strip() and not sent_flag and normalize(raw_sent) not in {"no", "n", "false"}:
                unrecognized_sent_values[raw_sent.strip()] = unrecognized_sent_values.get(raw_sent.strip(), 0) + 1

            elapsed = business_days_elapsed(upload_date, send_date) if (upload_date and send_date) else None
            eligible = bool(uploaded_flag and upload_date is not None)
            met_sla = bool(eligible and sent_flag and send_date is not None
                           and elapsed is not None and elapsed <= BUSINESS_DAY_SLA)

            print(
                f"[{ws.title}] {hotel} | uploaded={uploaded_flag} upload_date={upload_date} | "
                f"sent={sent_flag} send_date={send_date} | "
                f"elapsed_business_days={elapsed} | eligible={eligible} met_sla={met_sla} "
                f"| contributes_to_total_files_sent={sent_flag}"
                + (" | period INFERRED from tab title (cell blank)" if inferred else "")
            )

        # Only report unparseable rows for tabs that actually hold this
        # billing period - otherwise every old tab's junk would drown it out.
        if tab_matches and unparsed_rows:
            unparsed_by_tab[ws.title] = unparsed_rows

    print(f"\n{found} row(s) found for billing period {billing_period} "
          f"({inferred_total} of them via tab-title fallback for a blank period cell).")

    if unparsed_by_tab:
        total_unparsed = sum(len(v) for v in unparsed_by_tab.values())
        print(f"\nUNPARSEABLE BILLING PERIOD - {total_unparsed} row(s) with a hotel name, on tabs "
              f"that hold {billing_period}, whose Billing Period Analyzed cell is filled in but didn't "
              f"parse. The real sync drops these from every count - fix the cell on the sheet:")
        for title, rows in unparsed_by_tab.items():
            for hotel, raw in rows:
                print(f"  [{title}] {hotel} | raw period cell: {raw!r}")
    else:
        print("\nNo unparseable Billing Period Analyzed cells on the matching tab(s) "
              "(blank cells are handled by the tab-title fallback).")

    if unrecognized_sent_values:
        print(f"\nUNRECOGNIZED RESULTS-SENT VALUES - non-blank, not yes/true/y and not no/n/false, "
              f"so NOT counted in total_files_sent:")
        for raw, n in sorted(unrecognized_sent_values.items(), key=lambda kv: -kv[1]):
            print(f"  {raw!r}: {n} row(s)")
    else:
        print("No unrecognized Results Sent values among matching rows.")


def audit_columns(spreadsheet):
    """Diagnostic ONLY - no Supabase calls. For every worksheet, prints
    which of the four columns this script depends on (billing_period,
    upload_date, results_sent_flag, send_date) were actually recognized
    via COLUMN_ALIASES, and - for any tab missing one - the tab's raw
    header row.

    A tab whose Send Date or Results Sent column uses a header variant
    NOT in COLUMN_ALIASES silently contributes ZERO rows to
    total_files_sent (and to eligible_files/sent_within_sla too, if it's
    Upload Date or Billing Period that's unrecognized) - with no error,
    no warning, nothing. This is the single most likely explanation for
    a live count coming in lower than a manually-verified true count:
    the sheet's own column names have drifted across tabs over the
    years (see the module docstring), and COLUMN_ALIASES has to be
    updated by hand whenever that happens. Run this whenever a count
    looks short and check every "MISSING" line's raw headers against
    COLUMN_ALIASES to find the drifted name, then add it there."""
    total_missing_tabs = 0
    for ws in spreadsheet.worksheets():
        try:
            values = get_all_values_with_retry(ws)
        except Exception as e:
            print(f"[{ws.title}] SKIPPED after retries failed: {e}")
            continue
        if not values:
            print(f"[{ws.title}] EMPTY tab (no header row)")
            continue

        headers = values[0]
        row_count = len(values) - 1
        col_period = find_col_index(headers, "billing_period")
        col_upload_date = find_col_index(headers, "upload_date")
        col_sent_flag = find_col_index(headers, "results_sent_flag")
        col_send_date = find_col_index(headers, "send_date")

        missing = []
        if col_period is None:
            missing.append("billing_period")
        if col_upload_date is None:
            missing.append("upload_date")
        if col_sent_flag is None:
            missing.append("results_sent_flag")
        if col_send_date is None:
            missing.append("send_date")

        if not missing:
            print(f"[{ws.title}] OK - all 4 columns recognized ({row_count} data rows)")
        else:
            total_missing_tabs += 1
            print(f"[{ws.title}] MISSING {missing} ({row_count} data rows)")
            print(f"    raw headers: {headers}")

    print(f"\n{total_missing_tabs} tab(s) missing at least one recognized column.")


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
    """Shifts a 'YYYY-MM' key forward (or back, if n is negative) by n
    months. Used to convert a BILLING PERIOD (which tab/period was read
    from the sheet) into a REPORTING label (which month's throughput this
    counts toward on the dashboard) - these are deliberately different:
    July's billing period is processed in August, so July's billing-
    period data should be labeled and displayed as August's result."""
    year, month = (int(x) for x in month_key.split("-"))
    total = year * 12 + (month - 1) + n
    year, month = divmod(total, 12)
    return f"{year:04d}-{month + 1:02d}"


def main(force_months=None, dry_run=False, list_rows_billing_period=None, audit=False):
    if audit:
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(SHEET_ID)
        audit_columns(spreadsheet)
        return

    if list_rows_billing_period:
        # Diagnostic mode overrides everything else - read-only, no
        # Supabase calls, exits after printing. Matches the workflow's
        # own description: "Overrides dry_run/force_month when set."
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(SHEET_ID)
        list_rows_for_billing_period(spreadsheet, list_rows_billing_period)
        return

    force_months = set(force_months or [])  # these are REPORTING labels, e.g. '2026-08'
    wanted_billing_periods = [month_key_n_back(n) for n in range(TRAILING_MONTHS + 1)]

    already_closed = fetch_existing_month_keys(status_filter="closed")  # these are REPORTING labels too

    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(SHEET_ID)

    aggs = extract_month_aggregates(spreadsheet, wanted_billing_periods)

    for billing_period in wanted_billing_periods:
        agg = aggs.get(billing_period)
        if agg is None or agg.rows_seen == 0:
            continue  # no data found for this billing period in the sheet yet/anymore

        # The billing period itself is correct as read - only the LABEL
        # this gets stored/displayed under shifts forward one month.
        # Processing July's billing period happens in August, so this
        # data is August's throughput number, not July's.
        report_month_key = shift_month_key(billing_period, 1)

        if not sla_window_closed(billing_period):
            # Window still open for at least some rows in this billing
            # period - keep recomputing/overwriting daily. Deliberately
            # NOT tied to current_billing_period/calendar-month rollover
            # anymore - see sla_window_closed()'s docstring for why.
            upsert_month(report_month_key, agg, agg.total_files_sent, status="current", dry_run=dry_run)
        elif report_month_key in already_closed and report_month_key not in force_months:
            # SLA-locked: eligible_files/sent_within_7bd/rate_pct/status
            # must not change again. total_files_sent is still refreshed
            # on every run - the tab itself is frozen once superseded, so
            # this naturally stabilizes rather than needing a special
            # cutoff - via a narrow PATCH rather than a full upsert, so
            # the frozen SLA fields are never touched again.
            patch_total_files_sent(report_month_key, agg.total_files_sent, dry_run=dry_run)
        else:
            upsert_month(report_month_key, agg, agg.total_files_sent, status="closed", dry_run=dry_run)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("positional_months", nargs="*", default=[],
                         help="Backward-compatible bare month args, e.g. 2026-08 2026-07 - "
                              "treated identically to repeated --month flags.")
    parser.add_argument("--month", action="append", dest="force_months", default=[],
                         help="Force-recompute this already-closed REPORTING month (e.g. 2026-08), "
                              "overriding its freeze. Repeatable: --month 2026-08 --month 2026-01.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Print what would be written without writing it. Reads (fetching "
                              "existing month keys, reading the sheet) still happen normally.")
    parser.add_argument("--list-rows", dest="list_rows_billing_period", default=None,
                         help="Diagnostic only: print every row found for this BILLING PERIOD "
                              "(e.g. 2026-07 for August's reporting month), then exit. Read-only - "
                              "makes no Supabase calls. Overrides --dry-run/--month when set.")
    parser.add_argument("--audit-columns", action="store_true",
                         help="Diagnostic only: for every worksheet, print which required columns "
                              "were recognized and the raw header row for any tab missing one, then "
                              "exit. Read-only - makes no Supabase calls. Run this when a count looks "
                              "short - a drifted column header on some tab is the most likely cause.")
    args = parser.parse_args()

    all_force_months = list(args.force_months) + list(args.positional_months)
    main(
        force_months=all_force_months,
        dry_run=args.dry_run,
        list_rows_billing_period=args.list_rows_billing_period,
        audit=args.audit_columns,
    )
