"""
sync_hubspot_reconciliations_monthly.py

Tracks reconciliation case volume - opened and completed per month -
for the "Workload" section of the dashboard.

--- Why HubSpot, not email ---
Reconciliation cases arrive via an automated email ("Reconciliation
Request: {Hotel} for {Month} {Year}", from noreply@curacity.com) with
no equivalent structured "completed" email - confirmed by search,
2026-08-26. Inferring completion from email content/replies would mean
judging unstructured text, which is exactly the kind of interpretation
this pipeline deliberately avoids everywhere else (see: the Cowork-
based reporting approach that got abandoned for hallucinating).

HubSpot already has a real, structured ticket_type for this -
"Reconciliation Request" - with ordinary createdate/closed_date fields,
confirmed live with 230 closed tickets carrying real dates and hotel-
specific subjects. That's the same shape of data the existing ticket
SLA sync already handles, so this reuses that exact pattern rather than
inventing an email-parsing pipeline for a signal email can't actually
give cleanly.

--- What this counts ---
opened     = tickets with ticket_type = "Reconciliation Request" and
             createdate in the month.
completed  = tickets with ticket_type = "Reconciliation Request" and
             closed_date in the month, regardless of when they were
             opened (a case opened in June and closed in July counts
             as "completed" in July).

Team-wide only for now, no per-owner breakdown - the original ask was
just "reconciliations completed each month," not broken out by who
closed them. Easy to add an owner dimension later using the same
OWNERS dict pattern as sync_hubspot_ticket_sla.py if that's wanted.

Same current/closed lock pattern as every other monthly sync here:
current month recomputed daily, past months written once and left
alone unless force-recomputed via --month.
"""

import os
import time
import datetime as dt

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HUBSPOT_TOKEN = os.environ["HUBSPOT_PRIVATE_APP_TOKEN"]
HUBSPOT_SEARCH_URL = "https://api.hubapi.com/crm/v3/objects/tickets/search"

REQUEST_DELAY_SECONDS = 0.3
MAX_RETRIES = 5

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
TABLE = "hubspot_reconciliations_monthly"

TICKET_TYPE = "Reconciliation Request"

TRAILING_MONTHS = 24

# ---------------------------------------------------------------------------
# HubSpot
# ---------------------------------------------------------------------------


def hubspot_headers():
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }


def hubspot_search_total(filters):
    body = {
        "filterGroups": [{"filters": filters}],
        "properties": ["hs_object_id"],
        "limit": 1,
    }
    for attempt in range(MAX_RETRIES):
        resp = requests.post(HUBSPOT_SEARCH_URL, headers=hubspot_headers(), json=body, timeout=30)
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"Rate limited, waiting {retry_after}s (attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return resp.json()["total"]
    raise RuntimeError("HubSpot search still rate limited after max retries")


def month_bounds(month_key):
    year, month = (int(x) for x in month_key.split("-"))
    start = dt.date(year, month, 1)
    end = dt.date(year + (month == 12), (month % 12) + 1, 1) - dt.timedelta(days=1)
    return start, end


def to_millis(d):
    return int(dt.datetime.combine(d, dt.time.min, tzinfo=dt.timezone.utc).timestamp() * 1000)


def reconciliations_opened_in_month(month_key):
    start, end = month_bounds(month_key)
    filters = [
        {"propertyName": "ticket_type", "operator": "EQ", "value": TICKET_TYPE},
        {"propertyName": "createdate", "operator": "GTE", "value": str(to_millis(start))},
        {"propertyName": "createdate", "operator": "LTE", "value": str(to_millis(end) + 86_399_999)},
    ]
    return hubspot_search_total(filters)


def reconciliations_completed_in_month(month_key):
    start, end = month_bounds(month_key)
    filters = [
        {"propertyName": "ticket_type", "operator": "EQ", "value": TICKET_TYPE},
        {"propertyName": "closed_date", "operator": "GTE", "value": str(to_millis(start))},
        {"propertyName": "closed_date", "operator": "LTE", "value": str(to_millis(end) + 86_399_999)},
    ]
    return hubspot_search_total(filters)


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

    for month_key in wanted_months:
        if month_key != current_month_key and month_key in already_closed and month_key not in force_months:
            continue  # locked

        status = "current" if month_key == current_month_key else "closed"
        opened = reconciliations_opened_in_month(month_key)
        completed = reconciliations_completed_in_month(month_key)
        upsert_month(month_key, opened, completed, status, dry_run=dry_run)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                         help="No Supabase writes - just prints what would happen.")
    parser.add_argument("--month", type=str, action="append", default=None,
                         help="Force-recompute a specific already-closed month (e.g. 2026-07). Repeatable.")
    args = parser.parse_args()

    main(force_months=args.month, dry_run=args.dry_run)
