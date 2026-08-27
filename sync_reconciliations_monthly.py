"""
sync_reconciliations_monthly.py

Tracks reconciliation case volume - opened, completed, and backlog -
entirely from email. Replaces an earlier version of this script that
used HubSpot ticket_type = "Reconciliation Request" - confirmed
2026-08-26 that ticket type is actually CS support issues getting
resolved, NOT the same thing as the hotel reconciliation cycle this
metric is meant to track. That HubSpot-based data (and its tables) is
being retired; this is a full replacement, not an extension.

--- The two email signals this relies on ---
1. OPENED: "Reconciliation Request: {Hotel} for {Month} {Year}", from
   noreply@curacity.com, to operations@curacity.com. Sent once per
   hotel per month when the hotel submits their data. One thread per
   event - no fan-out.

2. COMPLETED: "Action Required: Finalize Your {Month} {Year} Results
   for {Hotel}", from operations@curacity.com, CC'd to
   customersuccess@curacity.com. The body opens with "We've reconciled
   your {Month} {Year} bookings for {Hotel}" - so the EXISTENCE of this
   email is the signal that Curacity's side of the reconciliation is
   done, even though the subject line is phrased as an ask to the
   hotel, not a "completed" announcement. Sent once per hotel per
   month, but FANNED OUT to multiple hotel contacts as separate
   messages in the SAME thread - confirmed a 5-recipient case where all
   5 messages share one threadId. Counting must dedupe by thread, not
   message, or this overcounts every case by however many contacts
   that hotel has on file.

Both confirmed live, 2026-08-26: searching with the exact "{Month}
{Year}" phrase embedded in the query (not a separate date filter) lets
Gmail's own search do the month scoping, which is more robust than
computing it from send date - a case labeled "July 2026" might
genuinely get sent a few days into August, and the label is what
matters for which cycle it belongs to, not the send timestamp.

--- What this computes ---
opened(month)     = distinct threads matching the opened pattern for
                     that month's label.
completed(month)  = distinct threads matching the completed pattern
                     for that month's label.
backlog           = for each of the last BACKLOG_LOOKBACK_MONTHS
                     months, every hotel with an opened thread but no
                     matching completed thread for that SAME month
                     label - a plain set difference, matched by
                     (normalized hotel name, month label). No
                     interpretation involved: a hotel either has a
                     matching completed thread for that specific cycle
                     or it doesn't.

--- Auth ---
Single-user OAuth (not domain-wide delegation) against Pia's own
mailbox, which already receives copies of both patterns as a member of
the operations team. Read-only scope (gmail.readonly). Refreshed via
a stored refresh token - GMAIL_OAUTH_CLIENT_ID, GMAIL_OAUTH_CLIENT_SECRET,
GMAIL_OAUTH_REFRESH_TOKEN.

Same current/closed lock pattern as every other monthly sync here for
the opened/completed table: current month recomputed daily, past
months written once and left alone unless force-recomputed via
--month. The backlog snapshot has no such lock - always fully
overwritten, live snapshot only.
"""

import os
import re
import html
import time
import calendar
import datetime as dt

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GMAIL_CLIENT_ID = os.environ["GMAIL_OAUTH_CLIENT_ID"]
GMAIL_CLIENT_SECRET = os.environ["GMAIL_OAUTH_CLIENT_SECRET"]
GMAIL_REFRESH_TOKEN = os.environ["GMAIL_OAUTH_REFRESH_TOKEN"]
GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"

REQUEST_DELAY_SECONDS = 0.2
MAX_RETRIES = 5
PAGE_SIZE = 100

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
MONTHLY_TABLE = "reconciliations_monthly"
BACKLOG_TABLE = "reconciliations_backlog"
BACKLOG_DETAIL_TABLE = "reconciliations_backlog_detail"

OPENED_FROM = "noreply@curacity.com"
COMPLETED_FROM = "operations@curacity.com"

OPENED_SUBJECT_RE = re.compile(r"^Reconciliation Request:\s*(.+?)\s+for\s+(\w+ \d{4})$")
COMPLETED_SUBJECT_RE = re.compile(r"^Action Required:\s*Finalize Your (\w+ \d{4}) Results for\s*(.+)$")

TRAILING_MONTHS = 24
BACKLOG_LOOKBACK_MONTHS = 6  # how far back to check for still-open cases

# ---------------------------------------------------------------------------
# Gmail auth
# ---------------------------------------------------------------------------


def get_access_token():
    resp = requests.post(GMAIL_TOKEN_URL, data={
        "client_id": GMAIL_CLIENT_ID,
        "client_secret": GMAIL_CLIENT_SECRET,
        "refresh_token": GMAIL_REFRESH_TOKEN,
        "grant_type": "refresh_token",
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def gmail_headers(access_token):
    return {"Authorization": f"Bearer {access_token}"}


# ---------------------------------------------------------------------------
# Gmail search
# ---------------------------------------------------------------------------


def gmail_list_messages(access_token, query, page_token=None):
    params = {"q": query, "maxResults": PAGE_SIZE}
    if page_token:
        params["pageToken"] = page_token
    for attempt in range(MAX_RETRIES):
        resp = requests.get(f"{GMAIL_API_BASE}/messages", headers=gmail_headers(access_token),
                             params=params, timeout=30)
        if resp.status_code == 429:
            wait = 2 ** attempt
            print(f"Gmail rate limited, waiting {wait}s (attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return resp.json()
    raise RuntimeError("Gmail search still rate limited after max retries")


def fetch_distinct_threads(access_token, query):
    """Paginates a Gmail search and returns {threadId: first_message_id},
    deduplicating fan-out messages within the same thread down to one
    representative message per thread - required because the completed
    pattern sends one message per hotel contact, all in the same
    thread."""
    threads = {}
    page_token = None
    while True:
        body = gmail_list_messages(access_token, query, page_token=page_token)
        for m in body.get("messages", []):
            tid = m["threadId"]
            if tid not in threads:
                threads[tid] = m["id"]
        page_token = body.get("nextPageToken")
        if not page_token:
            break
    return threads


def fetch_message_metadata(access_token, message_id):
    for attempt in range(MAX_RETRIES):
        resp = requests.get(
            f"{GMAIL_API_BASE}/messages/{message_id}",
            headers=gmail_headers(access_token),
            params={"format": "metadata", "metadataHeaders": ["Subject", "Date"]},
            timeout=30,
        )
        if resp.status_code == 429:
            wait = 2 ** attempt
            print(f"Gmail rate limited, waiting {wait}s (attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        body = resp.json()
        headers = {h["name"]: h["value"] for h in body.get("payload", {}).get("headers", [])}
        internal_ms = int(body["internalDate"])
        return {
            "subject": headers.get("Subject", ""),
            "date": dt.datetime.utcfromtimestamp(internal_ms / 1000),
        }
    raise RuntimeError(f"Gmail rate limited fetching metadata for {message_id} after max retries")


def normalize_hotel_name(name):
    """Unescapes HTML entities (subjects come back with &amp; etc. in
    some contexts) and normalizes whitespace/case for matching - NOT
    for display, only for the opened/completed set-difference join."""
    name = html.unescape(name or "").strip()
    name = re.sub(r"\s+", " ", name)
    return name.lower()


def month_label(year, month):
    return f"{calendar.month_name[month]} {year}"


def month_key_n_back(n, today=None):
    today = today or dt.date.today()
    month = today.month - n
    year = today.year
    while month <= 0:
        month += 12
        year -= 1
    return year, month


# ---------------------------------------------------------------------------
# Counts
# ---------------------------------------------------------------------------


def opened_threads_for_month(access_token, year, month):
    label = month_label(year, month)
    query = f'from:{OPENED_FROM} subject:"Reconciliation Request:" "for {label}"'
    return fetch_distinct_threads(access_token, query)


def completed_threads_for_month(access_token, year, month):
    label = month_label(year, month)
    query = f'from:{COMPLETED_FROM} subject:"Action Required: Finalize Your" "{label} Results"'
    return fetch_distinct_threads(access_token, query)


# ---------------------------------------------------------------------------
# Backlog
# ---------------------------------------------------------------------------


def compute_backlog(access_token, as_of=None):
    """For each of the last BACKLOG_LOOKBACK_MONTHS months, finds hotels
    with an opened thread but no completed thread for that same month
    label. Returns a list of backlog entries with hotel name, the
    month label they're stuck on, how old the opened thread is, and a
    link to it."""
    as_of = as_of or dt.date.today()
    entries = []

    for n in range(BACKLOG_LOOKBACK_MONTHS):
        year, month = month_key_n_back(n, today=as_of)
        label = month_label(year, month)

        opened_threads = opened_threads_for_month(access_token, year, month)
        completed_threads = completed_threads_for_month(access_token, year, month)

        # Only need hotel names from the COMPLETED side to build the
        # "already done" set - the opened side needs full metadata
        # anyway since backlog entries need subject/date/link from it.
        completed_hotels = set()
        for message_id in completed_threads.values():
            meta = fetch_message_metadata(access_token, message_id)
            match = COMPLETED_SUBJECT_RE.match(meta["subject"])
            if match:
                completed_hotels.add(normalize_hotel_name(match.group(2)))

        for thread_id, message_id in opened_threads.items():
            meta = fetch_message_metadata(access_token, message_id)
            match = OPENED_SUBJECT_RE.match(meta["subject"])
            if not match:
                continue  # subject didn't match the expected pattern - skip rather than guess
            hotel_raw = match.group(1)
            hotel_key = normalize_hotel_name(hotel_raw)
            if hotel_key in completed_hotels:
                continue  # already finalized for this month - not backlog

            opened_date = meta["date"].date()
            age_days = (as_of - opened_date).days
            entries.append({
                "thread_id": thread_id,
                "hotel_name": html.unescape(hotel_raw).strip(),
                "month_label": label,
                "opened_date": opened_date.isoformat(),
                "age_days": age_days,
                "url": f"https://mail.google.com/mail/u/0/#all/{thread_id}",
            })

    return entries


def bucket_backlog_by_age(entries):
    counts = {"0_30": 0, "31_60": 0, "61_90": 0, "90_plus": 0}
    for e in entries:
        age = e["age_days"]
        if age <= 30:
            counts["0_30"] += 1
        elif age <= 60:
            counts["31_60"] += 1
        elif age <= 90:
            counts["61_90"] += 1
        else:
            counts["90_plus"] += 1
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
    url = f"{SUPABASE_URL}/rest/v1/{MONTHLY_TABLE}?select=month_key,status"
    resp = requests.get(url, headers=supabase_headers(), timeout=30)
    resp.raise_for_status()
    return {r["month_key"] for r in resp.json() if r["status"] == "closed"}


def upsert_month(month_key, opened, completed, status, dry_run=False):
    payload = {
        "month_key": month_key,
        "reconciliations_opened": opened,
        "reconciliations_completed": completed,
        "status": status,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    if dry_run:
        print(f"[DRY RUN] Would upsert {month_key} ({status}): {payload}")
        return
    url = f"{SUPABASE_URL}/rest/v1/{MONTHLY_TABLE}"
    resp = requests.post(
        url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Upserted {month_key} ({status}): {payload}")


def upsert_backlog_snapshot(counts, dry_run=False):
    payload = {
        "id": 1,
        "age_0_30": counts["0_30"],
        "age_31_60": counts["31_60"],
        "age_61_90": counts["61_90"],
        "age_90_plus": counts["90_plus"],
        "total_open": sum(counts.values()),
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    if dry_run:
        print(f"[DRY RUN] Would upsert backlog snapshot: {payload}")
        return
    url = f"{SUPABASE_URL}/rest/v1/{BACKLOG_TABLE}"
    resp = requests.post(
        url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Upserted backlog snapshot: {payload}")


def replace_backlog_detail_rows(entries, dry_run=False):
    rows = [{
        "thread_id": e["thread_id"],
        "hotel_name": e["hotel_name"],
        "month_label": e["month_label"],
        "opened_date": e["opened_date"],
        "age_days": e["age_days"],
        "bucket": (
            "0_30" if e["age_days"] <= 30 else
            "31_60" if e["age_days"] <= 60 else
            "61_90" if e["age_days"] <= 90 else
            "90_plus"
        ),
        "url": e["url"],
        "updated_at": dt.datetime.utcnow().isoformat(),
    } for e in entries]

    if dry_run:
        print(f"[DRY RUN] Would replace {BACKLOG_DETAIL_TABLE} with {len(rows)} rows. Sample:")
        for row in rows[:5]:
            print(f"  {row}")
        return

    delete_url = f"{SUPABASE_URL}/rest/v1/{BACKLOG_DETAIL_TABLE}"
    resp = requests.delete(delete_url, headers=supabase_headers(), params={"thread_id": "not.is.null"}, timeout=30)
    resp.raise_for_status()

    if not rows:
        print("Backlog detail table cleared - no open reconciliations right now.")
        return

    insert_url = f"{SUPABASE_URL}/rest/v1/{BACKLOG_DETAIL_TABLE}"
    resp = requests.post(insert_url, headers=supabase_headers(), json=rows, timeout=30)
    resp.raise_for_status()
    print(f"Replaced {BACKLOG_DETAIL_TABLE} with {len(rows)} rows.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_backlog(access_token, dry_run=False):
    entries = compute_backlog(access_token)
    counts = bucket_backlog_by_age(entries)
    upsert_backlog_snapshot(counts, dry_run=dry_run)
    replace_backlog_detail_rows(entries, dry_run=dry_run)


def run_monthly(access_token, force_months=None, dry_run=False):
    force_months = set(force_months or [])
    current_year, current_month = month_key_n_back(0)
    current_month_key = f"{current_year:04d}-{current_month:02d}"
    already_closed = fetch_closed_months()

    for n in range(TRAILING_MONTHS + 1):
        year, month = month_key_n_back(n)
        month_key = f"{year:04d}-{month:02d}"

        if month_key != current_month_key and month_key in already_closed and month_key not in force_months:
            continue  # locked

        status = "current" if month_key == current_month_key else "closed"
        opened = len(opened_threads_for_month(access_token, year, month))
        completed = len(completed_threads_for_month(access_token, year, month))
        upsert_month(month_key, opened, completed, status, dry_run=dry_run)


def main(force_months=None, dry_run=False):
    access_token = get_access_token()
    run_backlog(access_token, dry_run=dry_run)
    run_monthly(access_token, force_months=force_months, dry_run=dry_run)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                         help="No Supabase writes - just prints what would happen.")
    parser.add_argument("--month", type=str, action="append", default=None,
                         help="Force-recompute a specific already-closed month (e.g. 2026-07). Repeatable.")
    args = parser.parse_args()

    main(force_months=args.month, dry_run=args.dry_run)
