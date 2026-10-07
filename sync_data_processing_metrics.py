"""
sync_data_processing_metrics.py

Pulls the hotel billing tracker tabs from the "Curacity Billing Overview"
Google Sheet, computes data-processing throughput metrics per billing
month, and upserts the results into Supabase so the Lovable dashboard
can read them instead of relying on manual entry.

Run daily via GitHub Actions (see data-processing-sync.yml).

--- REDEFINED 2026-10-07: binary per-file SLA pass/fail retired ---
Pia flagged that "sent within 7 business days" reads, on its face, as
"actual sent vs. available" - not as a per-file stopwatch that can make
a file "miss" independently of every other file. The old rate_pct /
sent_within_7bd pair *did* use a per-file clock (confirmed deliberately
on 2026-09-22 - see git history / prior docstring), but that per-file
framing was the actual source of confusion, not a misunderstanding of
it. Replaced with two metrics that don't make an individual pass/fail
claim at all:

  METRIC 1 - eligible_and_sent: a plain completion count. Of the files
  that are ELIGIBLE (see below, unchanged), how many have simply been
  marked Results Sent = Yes - no timing check on any individual file.
  Shown against eligible_files as a completion rate.

  METRIC 2 - avg_processing_days: the mean business-day gap between
  Upload Date and Send Date, across eligible-and-sent files only.
  Trend-only, descriptive - explicitly NOT a pass/fail test, and not
  tied to any goal/target in this script. Confirmed with Pia: this will
  get more precise once the new billing UI tracks "data became
  available" directly rather than inferring it from Upload Date.

rate_pct and sent_within_7bd (the old binary count and its percentage)
are RETIRED going forward: this script no longer computes or writes
them for current/closed-month rows (written explicitly as null so nothing
stale lingers looking current). Already-locked historical rows that used
the old definition are NOT recomputed or touched - by explicit decision,
not oversight. If older reporting needs the old-definition numbers, they
remain exactly as originally locked.

total_files_sent is unchanged - still every row in the billing period
with Results Sent = Yes, regardless of eligibility, and still refreshed
forever (see point 5 further down) since this is explicitly meant to be
trackable past the 7-day window, not an SLA-scoped number.

--- Design notes / assumptions (confirm these match reality before trusting numbers) ---
1. "Eligible files" (confirmed with Pia 2026-09-22) = rows where Data
   Uploaded is Yes/TRUE AND the Upload Date is on or before the
   eligibility cutoff: business day 7 of the PROCESSING month (see
   processing_deadline()). A file that wasn't there by then can't be
   held to the metric. Unchanged by the Oct 2026 redefinition above.
2. "eligible_and_sent" (Metric 1, see Oct 2026 note above) = eligible
   rows where Results Sent is Yes. No timing component at all.
3. "avg_processing_days" (Metric 2, see Oct 2026 note above) = mean of
   business_days_elapsed(upload_date, send_date) across rows that are
   both eligible and sent. Business days = Mon-Fri, EXCLUDING the 11
   standard US federal holidays (observed dates) AND Curacity's own
   closures (day after Thanksgiving, weekday Christmas Eve) - see
   federal_holidays(), curacity_closures() and business_days_elapsed().
   The holiday list is computed in-code, not pulled from the `holidays`
   pip package.
4. "Total files sent" = count of rows in that billing period where the
   Results Sent flag is Yes/TRUE, regardless of how long it took or
   whether the row was ever eligible. A deliberately different, ONGOING
   metric from eligible_and_sent - confirmed directly, not a bug: it
   keeps climbing all month as backlog clears, even after that billing
   period's eligible_and_sent (point 5) has locked. See point 5 and
   patch_live_metrics() for how it stays live independently of the
   frozen eligibility-scoped fields.
5. Column names have drifted across tabs over the years, so columns are
   matched by ALIAS, not fixed position. If a future tab renames a column
   again, add the new name to COLUMN_ALIASES below rather than touching the
   parsing logic.
6. Locking: eligible_files/eligible_and_sent/status lock together,
   permanently, the moment sla_window_closed() says a billing period's
   window has closed for every possible row in it (see point 7 and the
   FIXED note below for why this is NOT simply "once the calendar rolls
   to next month"). Once locked, those three fields are never silently
   overwritten again, even if the underlying sheet is edited later -
   this protects numbers that have already been reported out.
   total_files_sent and avg_processing_days are the two exceptions: both
   keep being refreshed on every run via a narrow PATCH
   (patch_live_metrics()), forever, because both are intentionally
   ongoing/descriptive rather than frozen reported outcomes. Note the
   Oct 2026 redefinition changes WHY eligible_and_sent locking matters
   (see that note) even though WHEN it locks is unchanged (same
   sla_window_closed() timing as before - revisit if that no longer
   makes sense now that there's no per-file clock backing it). To force
   a full recompute of an already-locked month (overriding the freeze on
   all three fields, not just the live ones), pass its REPORTING label
   (e.g. '2026-08') as a CLI arg, or delete its row from the Supabase
   table.
7. Billing period vs. reporting label: the tab this reads is named for
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
eligibility window for a billing period closes much earlier than that -
see processing_deadline(). Fix: sla_window_closed() computes, per
billing period, the actual date by which every possible row in it must
have resolved, using the same business_days_elapsed() helper already
used elsewhere. A billing period locks as soon as that date has passed,
regardless of which calendar month we're in. This replaces the old
current_billing_period/MONTH_OFFSET-based lock decision entirely -
MONTH_OFFSET/month_key_n_back is still used to build the list of billing
periods to scan, just not to decide locking.

--- Locking eligible_files/eligible_and_sent is NOT the same as locking
the whole row ---
total_files_sent and avg_processing_days are both meant to keep moving
indefinitely, independent of the eligibility-scoped determination. So
once a month locks: upsert_month() (full row write) is never called for
it again; instead patch_live_metrics() runs on every subsequent
execution, touching only total_files_sent, avg_processing_days, and
updated_at. status, eligible_files, and eligible_and_sent are frozen
forever at whatever they were the moment the window closed.

--- CORRECTED: total_files_sent's grouping key ---
total_files_sent is grouped by BILLING PERIOD (i.e. whichever tab that
period's rows live in) - NOT by the calendar month of each row's own
Send Date. An earlier version grouped it by Send Date instead, built on
a wrong assumption that a late invoice might get its Send Date filled in
on an OLD tab after a newer one already exists. Confirmed directly -
that never happens on this sheet. Older tabs are frozen the moment a new
one is created; an invoice that wasn't sent while its tab was current
does NOT get updated retroactively - the billing period itself simply
shifts forward for that hotel instead (it becomes a fresh row in the new
tab, not an update to the old one). So there is no cross-tab "late send"
to find, and scanning for one only undercounted against what a
single-tab count would show.
"""

import os
import re
import sys
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
BUSINESS_DAY_SLA = 7  # still used for the eligibility cutoff and lock timing -
                      # no longer used as a per-file pass/fail threshold
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


def parse_date(value, reference_year, reference_month=None):
    """Parses the sheet's loose date formats (M/D, M/D/YY, M/D/YYYY, etc).
    Sheet dates without a year (e.g. '7/1') are assumed to fall in
    reference_year, which the caller should set to the billing month's year.

    Year wrap: with reference_month (the billing period's month) given, a
    no-year date whose month is more than 6 months BEFORE the billing
    month is placed in the NEXT year. A December billing period is
    processed in January, so '1/5' on a December tab means Jan 5 of the
    following year, not 11 months before the period started.
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
        year = reference_year
        if reference_month is not None and (reference_month - month) > 6:
            year += 1
        try:
            return dt.date(year, month, day)
        except ValueError:
            return None
    return None


def parse_billing_period(value):
    """'6.1.26 - 6.30.26' -> (month_key='2026-06', year=2026).

    Buckets by the SECOND date in the cell (the end of a '{start} - {end}'
    range), positionally. A multi-month range like '6.1.26 - 7.31.26'
    belongs to July, not June. Falls back to the only date present if
    there's just one."""
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
    hotel name, use the tab title's month. A cell that's filled in but
    unparseable is NOT inferred - that's a data error to fix, not guess
    around.

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
    Veterans Day, Christmas) - the floating ones are already defined as
    a specific weekday and never need shifting."""
    if holiday_date.weekday() == 5:  # Saturday
        return holiday_date - dt.timedelta(days=1)
    if holiday_date.weekday() == 6:  # Sunday
        return holiday_date + dt.timedelta(days=1)
    return holiday_date


_FEDERAL_HOLIDAY_CACHE = {}


def federal_holidays(year):
    """The 11 standard US federal holidays for a given calendar year,
    with observed-date shifting applied to the fixed-date ones. Standard
    OPM list only - no Curacity-specific additions."""
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


_CURACITY_CLOSURE_CACHE = {}


def curacity_closures(year):
    """Curacity-specific business closures, layered ON TOP of the
    standard federal list:
      - Day after Thanksgiving (always a Friday).
      - Christmas Eve, Dec 24 - only when it falls on a weekday."""
    if year not in _CURACITY_CLOSURE_CACHE:
        closures = {
            _nth_weekday_of_month(year, 11, 3, 4) + dt.timedelta(days=1),  # Day after Thanksgiving
        }
        christmas_eve = dt.date(year, 12, 24)
        if christmas_eve.weekday() < 5:
            closures.add(christmas_eve)                                    # Christmas Eve (weekday only)
        _CURACITY_CLOSURE_CACHE[year] = closures
    return _CURACITY_CLOSURE_CACHE[year]


def _holidays_spanning(start_date, end_date):
    """Federal holidays for every calendar year touched by [start_date,
    end_date] - almost always one year, but a date range that crosses a
    Dec 31 -> Jan 1 boundary needs both."""
    holidays = set()
    for y in range(start_date.year, end_date.year + 1):
        holidays |= federal_holidays(y) | curacity_closures(y)
    return holidays


def business_days_elapsed(start_date, end_date):
    """Count weekday-only business days strictly after start_date through
    end_date inclusive, EXCLUDING the 11 standard US federal holidays
    plus Curacity's own closures. Returns None if inputs are missing or
    out of order.

    Used for: (a) processing_deadline()/sla_window_closed()'s cutoff and
    lock-timing math, and (b) per-row elapsed time feeding
    avg_processing_days (Metric 2) - purely descriptive there now, not a
    pass/fail test."""
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


def nth_business_day(year, month, n):
    """The nth business day (Mon-Fri, excluding federal holidays and
    Curacity closures) of the given calendar month, counting the 1st
    itself if it's a business day."""
    holidays = federal_holidays(year) | curacity_closures(year)
    d = dt.date(year, month, 1)
    count = 0
    while True:
        if d.weekday() < 5 and d not in holidays:
            count += 1
            if count == n:
                return d
        d += dt.timedelta(days=1)


def processing_deadline(billing_period):
    """The ELIGIBILITY deadline for a billing period: business day
    BUSINESS_DAY_SLA of the PROCESSING month (the month after the billing
    period - July's billing period is processed in August). e.g.
    '2026-07' -> Tue 2026-08-11. Unchanged by the Oct 2026 metric
    redefinition - this still gates what counts as "eligible" at all."""
    year, month = (int(x) for x in shift_month_key(billing_period, 1).split("-"))
    return nth_business_day(year, month, BUSINESS_DAY_SLA)


def add_business_days(start_date, n):
    """start_date shifted forward by n business days (same holiday and
    closure calendar as business_days_elapsed())."""
    current = start_date
    added = 0
    while added < n:
        current += dt.timedelta(days=1)
        if current.weekday() < 5 and current not in (federal_holidays(current.year) | curacity_closures(current.year)):
            added += 1
    return current


def evaluate_row_sla(billing_period, uploaded_flag, upload_date, sent_flag, send_date):
    """The ONE place eligibility and the two throughput metrics are
    computed for a single row, shared by the real sync and --list-rows
    so they can never disagree.

    REDEFINED 2026-10-07 (see module docstring's "REDEFINED" section):
      eligible          = Data Uploaded = Yes AND Upload Date on or
                           before the eligibility cutoff (business day 7
                           of the processing month). Unchanged.
      eligible_and_sent = eligible AND Results Sent = Yes. NO timing
                           check - this is Metric 1's per-row unit.
      elapsed           = business days from Upload Date to Send Date,
                           computed ONLY when eligible_and_sent is true.
                           Feeds Metric 2 (avg_processing_days) as a
                           trend input, never as a pass/fail gate.

    Returns (eligible, eligible_and_sent, cutoff, elapsed_business_days).
    elapsed is None whenever eligible_and_sent is False (nothing to
    measure yet), or when send_date is missing/unparseable despite the
    sent flag being set."""
    cutoff = processing_deadline(billing_period)
    eligible = bool(uploaded_flag and upload_date is not None and upload_date <= cutoff)
    eligible_and_sent = bool(eligible and sent_flag)
    elapsed = None
    if eligible_and_sent and upload_date and send_date:
        elapsed = business_days_elapsed(upload_date, send_date)
    return eligible, eligible_and_sent, cutoff, elapsed


def sla_window_closed(billing_period, today=None):
    """True once the eligible_files/eligible_and_sent pool for this
    billing period is treated as final and ready to lock: the day after
    (eligibility cutoff + BUSINESS_DAY_SLA business days).

    NOTE (Oct 2026): this timing is UNCHANGED from before the metric
    redefinition, but its justification has changed. It used to mark the
    point at which every individual file's personal 7-day pass/fail
    clock had necessarily expired. Now that eligible_and_sent has no
    per-file clock, this date instead marks "a reasonable grace period
    past the eligibility cutoff, after which we freeze the reported
    completion count for this period" - a policy choice carried forward
    unchanged rather than re-derived. Revisit this window length if it
    no longer reflects actual processing turnaround."""
    if today is None:
        today = dt.date.today()
    last_possible_grace_date = add_business_days(processing_deadline(billing_period), BUSINESS_DAY_SLA)
    return today > last_possible_grace_date


@dataclass
class MonthAgg:
    eligible_files: int = 0
    eligible_and_sent: int = 0
    total_files_sent: int = 0
    rows_seen: int = 0
    elapsed_days: list = field(default_factory=list)  # business days elapsed,
                                                        # eligible-and-sent rows
                                                        # only - feeds the
                                                        # avg_processing_days
                                                        # trend metric


# ---------------------------------------------------------------------------
# Google Sheets extraction
# ---------------------------------------------------------------------------


def get_gspread_client():
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_all_values_with_retry(ws):
    """Reads a worksheet's values, retrying with backoff on rate-limit
    errors instead of silently giving up."""
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
    """Returns worksheets worth scanning. We filter by content (Billing
    Period column), not by tab title, so this is robust to tab-naming
    drift."""
    return spreadsheet.worksheets()


def extract_month_aggregates(spreadsheet, billing_periods_wanted):
    """Scans all worksheets once, bucketing rows into MonthAgg by their
    actual Billing Period Analyzed value (not by tab name). Returns
    {billing_period_month_key: MonthAgg}.

    total_files_sent is grouped the SAME way as eligible_files/
    eligible_and_sent - by billing period (i.e. whichever tab that
    period's rows live in) - NOT by the calendar month of each row's own
    Send Date (see module docstring's "CORRECTED" section for why).

    Reference year for parsing no-year date values (both Upload Date and
    Send Date) always comes from the row's own Billing Period Analyzed
    cell."""
    aggs = {mk: MonthAgg() for mk in billing_periods_wanted}

    def _cell(row, idx):
        """Safe column access. A row can be legitimately shorter than
        the header row without any of its actually-populated columns
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
        # Beyond that, eligible_files/eligible_and_sent and
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
            ref_month = int(billing_month_key.split("-")[1])
            upload_date = parse_date(_cell(row, col_upload_date), year, ref_month)
            sent_flag = is_truthy(_cell(row, col_sent_flag)) if col_sent_flag is not None else bool(_cell(row, col_send_date))
            send_date = parse_date(_cell(row, col_send_date), year, ref_month)

            eligible, eligible_and_sent, _cutoff, elapsed = evaluate_row_sla(
                billing_month_key, uploaded_flag, upload_date, sent_flag, send_date
            )
            if eligible:
                agg.eligible_files += 1
            if eligible_and_sent:
                agg.eligible_and_sent += 1
                if elapsed is not None:
                    agg.elapsed_days.append(elapsed)

            if sent_flag:
                agg.total_files_sent += 1

        if inferred_count:
            print(f"[{ws.title}] {inferred_count} row(s) had a BLANK Billing Period Analyzed cell - "
                  f"used the tab title's month instead (see resolve_billing_period).")

    return aggs


def _avg(elapsed_days):
    if not elapsed_days:
        return None
    return round(sum(elapsed_days) / len(elapsed_days), 1)


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


def upsert_month(month_key, agg, total_files_sent, avg_processing_days, status, dry_run=False):
    """Full-row write. Only ever called for a month that is NOT yet
    locked (status='current') or the very first time a month transitions
    to 'closed' - both legitimate moments to write everything at once.
    Never called again for an already-locked month; see
    patch_live_metrics() for what happens to those.

    rate_pct and sent_within_7bd are explicitly written as null here -
    they are RETIRED (see module docstring's "REDEFINED" section) and
    this script no longer computes them for current/closed-month rows.
    Already-locked historical rows that still hold the old values are
    NOT touched by this function, since it's never called for them again.

    dry_run=True prints the payload that WOULD be written and returns
    without making any Supabase call at all."""
    payload = {
        "month_key": month_key,
        "eligible_files": agg.eligible_files,
        "eligible_and_sent": agg.eligible_and_sent,
        "sent_within_7bd": None,   # retired - see module docstring
        "rate_pct": None,          # retired - see module docstring
        "total_files_sent": total_files_sent,
        "avg_processing_days": avg_processing_days,
        "status": status,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    if dry_run:
        print(f"[DRY RUN] Would upsert {month_key} ({status}): {payload}")
        return
    # on_conflict=month_key: month_key is confirmed to be the table's
    # actual primary key (checked directly via pg_get_constraintdef).
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?on_conflict=month_key"
    resp = requests.post(
        url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Upserted {month_key} ({status}): {payload}")


def patch_live_metrics(month_key, total_files_sent, avg_processing_days, dry_run=False):
    """total_files_sent and avg_processing_days are the two metrics that
    keep moving indefinitely regardless of whether this month's
    eligibility-scoped fields (eligible_files/eligible_and_sent/status)
    have already locked. Confirmed directly for both: total_files_sent
    is meant to be trackable past the 7-day window, and avg_processing_days
    is explicitly trend-only with no freeze point.

    Once a month is locked, this is the ONLY function that should touch
    its row. A full upsert_month() call here would also recompute and
    overwrite eligible_files/eligible_and_sent/status with a fresh value
    from today's sheet read - which might genuinely differ from what was
    locked in (e.g. a hotel backdating an upload weeks late) and would
    silently un-freeze exactly what sla_window_closed() exists to
    protect. This does a narrow PATCH touching only total_files_sent,
    avg_processing_days, and updated_at, leaving status and the two
    eligibility fields exactly as they were the moment this month locked.

    dry_run=True prints what WOULD be patched and returns without making
    any Supabase call at all."""
    if dry_run:
        print(f"[DRY RUN] Would patch locked month {month_key}: "
              f"total_files_sent={total_files_sent}, avg_processing_days={avg_processing_days}")
        return
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?month_key=eq.{month_key}"
    payload = {
        "total_files_sent": total_files_sent,
        "avg_processing_days": avg_processing_days,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    resp = requests.patch(url, headers=supabase_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    print(f"Patched locked month {month_key}: {payload}")


def list_rows_for_billing_period(spreadsheet, billing_period):
    """Diagnostic ONLY - makes no Supabase calls at all, read-only
    against the sheet. Prints every row found (across all worksheets)
    whose Billing Period Analyzed cell matches billing_period, with
    enough detail to manually verify eligibility/eligible-and-sent/
    avg-processing-days determinations by eye against whatever Supabase
    ends up showing. billing_period is a BILLING PERIOD (e.g. '2026-07'),
    not a reporting label."""

    def _cell(row, idx):
        if idx is None or idx >= len(row):
            return ""
        return row[idx]

    found = 0
    inferred_total = 0
    not_yet_sent = []  # eligible rows with Results Sent not Yes - replaces the
                        # old "missed by reason" breakdown, which no longer
                        # applies since there's no timing-based miss condition
    unparsed_by_tab = {}            # tab title -> [(hotel, raw period)]
    unrecognized_sent_values = {}   # raw Results Sent value -> row count
    elapsed_days_found = []

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
            ref_month = int(parsed_period[0].split("-")[1])
            upload_date = parse_date(_cell(row, col_upload_date), year, ref_month)
            sent_flag = is_truthy(_cell(row, col_sent_flag)) if col_sent_flag is not None else bool(_cell(row, col_send_date))
            send_date = parse_date(_cell(row, col_send_date), year, ref_month)

            raw_sent = _cell(row, col_sent_flag) if col_sent_flag is not None else ""
            if raw_sent.strip() and not sent_flag and normalize(raw_sent) not in {"no", "n", "false"}:
                unrecognized_sent_values[raw_sent.strip()] = unrecognized_sent_values.get(raw_sent.strip(), 0) + 1

            eligible, eligible_and_sent, deadline, elapsed = evaluate_row_sla(
                parsed_period[0], uploaded_flag, upload_date, sent_flag, send_date
            )

            if eligible and not eligible_and_sent:
                raw_send = _cell(row, col_send_date).strip()
                not_yet_sent.append(f"{hotel} (upload {upload_date})")

            if elapsed is not None:
                elapsed_days_found.append(elapsed)

            print(
                f"[{ws.title}] {hotel} | uploaded={uploaded_flag} upload_date={upload_date} | "
                f"sent={sent_flag} send_date={send_date} | "
                f"elapsed_business_days={elapsed} | eligibility_cutoff={deadline} | "
                f"eligible={eligible} eligible_and_sent={eligible_and_sent} "
                f"| contributes_to_total_files_sent={sent_flag}"
                + (" | period INFERRED from tab title (cell blank)" if inferred else "")
            )

        if tab_matches and unparsed_rows:
            unparsed_by_tab[ws.title] = unparsed_rows

    print(f"\n{found} row(s) found for billing period {billing_period} "
          f"({inferred_total} of them via tab-title fallback for a blank period cell).")

    print(f"\nELIGIBLE BUT NOT YET SENT - {len(not_yet_sent)} row(s) "
          f"(no timing judgment here - just not marked Results Sent = Yes yet):")
    for h in not_yet_sent:
        print(f"    {h}")

    avg = _avg(elapsed_days_found)
    print(f"\nAVG PROCESSING DAYS (eligible-and-sent rows only): "
          f"{avg if avg is not None else 'n/a'} across {len(elapsed_days_found)} row(s)")

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
    which of the four columns this script depends on were actually
    recognized via COLUMN_ALIASES, and - for any tab missing one - the
    tab's raw header row.

    A tab whose Send Date or Results Sent column uses a header variant
    NOT in COLUMN_ALIASES silently contributes ZERO rows to
    total_files_sent (and to eligible_files/eligible_and_sent too, if
    it's Upload Date or Billing Period that's unrecognized) - with no
    error, no warning, nothing. Run this whenever a count looks short."""
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
    counts toward on the dashboard)."""
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
        # Supabase calls, exits after printing.
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

        report_month_key = shift_month_key(billing_period, 1)
        avg_processing_days = _avg(agg.elapsed_days)

        if not sla_window_closed(billing_period):
            # Window still open - keep recomputing/overwriting daily.
            upsert_month(report_month_key, agg, agg.total_files_sent, avg_processing_days,
                         status="current", dry_run=dry_run)
        elif report_month_key in already_closed and report_month_key not in force_months:
            # Locked: eligible_files/eligible_and_sent/status must not
            # change again. total_files_sent and avg_processing_days are
            # still refreshed on every run via a narrow PATCH.
            patch_live_metrics(report_month_key, agg.total_files_sent, avg_processing_days, dry_run=dry_run)
        else:
            upsert_month(report_month_key, agg, agg.total_files_sent, avg_processing_days,
                         status="closed", dry_run=dry_run)


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
                         help="Print what would be written without writing it.")
    parser.add_argument("--list-rows", dest="list_rows_billing_period", default=None,
                         help="Diagnostic only: print every row found for this BILLING PERIOD, "
                              "then exit. Read-only - makes no Supabase calls.")
    parser.add_argument("--audit-columns", action="store_true",
                         help="Diagnostic only: for every worksheet, print which required columns "
                              "were recognized and the raw header row for any tab missing one.")
    args = parser.parse_args()

    all_force_months = list(args.force_months) + list(args.positional_months)
    main(
        force_months=all_force_months,
        dry_run=args.dry_run,
        list_rows_billing_period=args.list_rows_billing_period,
        audit=args.audit_columns,
    )
