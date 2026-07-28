"""
sync_hubspot_ticket_sla.py

Replaces the manually-updated "HubSpot ticket SLA" tracker (previously
owned by Ben, who has since left) with a live pull from HubSpot.

--- Why the numbers here won't match the old screenshot ---
The old tracker's exact scope died with whoever set it up - there was no
documented rule for what counted. Rather than guess at a filter we can't
verify, this is a fresh, simple, documented definition going forward:

  SCOPE = every ticket owned by Javiana Pacheco, Lucas Berberian, or
  Victoria Camacho, closed in the given month, regardless of ticket type.

  SLA MET = HubSpot's own `time_to_close__met_sla` field (HubSpot's
  built-in calculation based on each ticket type's target close time).

If the team later decides this should be scoped to specific ticket types
(e.g. only Data Processing Request / Reconciliation / Invoice-related
tickets), update OWNER_IDS filtering logic accordingly - see the
commented-out TICKET_TYPE_FILTER block below for how to add that back in.

Same current/closed lock pattern as the other syncs: current month
recomputed daily, past months written once and left alone.
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

# This script fires 2 requests per owner per month (closed count + within-SLA
# count) across a 24-month backfill, which adds up fast - throttled and
# retried on 429s so a burst of rate limiting doesn't kill the whole run.
REQUEST_DELAY_SECONDS = 0.3
MAX_RETRIES = 5

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
TABLE = "hubspot_ticket_sla_monthly"

# Pulled live from the portal via search_owners - update here if these
# three change roles or someone new joins ops.
OWNERS = {
    79058582: "Javiana",
    1985678304: "Lucas",
    90048338: "Victoria",
}

# TICKET_TYPE_FILTER = [
#     "Data Processing Request", "Reconciliation Request",
#     "Data Automation Setup", "Data Automation Issue",
#     "Invoice Adjustment Request", "Invoice Error", "Invoice Escalation",
#     "Custom Invoice Request",
# ]
# To scope by type instead of just owner, add:
#   {"propertyName": "ticket_type", "operator": "IN", "values": TICKET_TYPE_FILTER}
# to the filters list in tickets_for_owner() below.

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


def owner_month_stats(owner_id, month_key):
    start, end = month_bounds(month_key)
    base_filters = [
        {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": str(owner_id)},
        {"propertyName": "closed_date", "operator": "GTE", "value": str(to_millis(start))},
        {"propertyName": "closed_date", "operator": "LTE", "value": str(to_millis(end) + 86_399_999)},
    ]
    total_closed = hubspot_search_total(base_filters)
    within_sla = hubspot_search_total(
        base_filters + [{"propertyName": "time_to_close__met_sla", "operator": "EQ", "value": "true"}]
    )
    return total_closed, within_sla


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
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?select=month_key,status&scope=eq.team"
    resp = requests.get(url, headers=supabase_headers(), timeout=30)
    resp.raise_for_status()
    return {r["month_key"] for r in resp.json() if r["status"] == "closed"}


def upsert_row(month_key, scope, tickets_closed, within_sla, status):
    sla_pct = round(within_sla / tickets_closed * 100, 1) if tickets_closed else None
    payload = {
        "month_key": month_key,
        "scope": scope,  # 'team' or one of 'Javiana' / 'Lucas' / 'Victoria'
        "tickets_closed": tickets_closed,
        "within_sla": within_sla,
        "sla_pct": sla_pct,
        "status": status,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    resp = requests.post(
        url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Upserted {month_key} / {scope} ({status}): {payload}")


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


def main(force_months=None):
    force_months = set(force_months or [])
    current_month_key = month_key_n_back(0)
    wanted_months = [month_key_n_back(n) for n in range(TRAILING_MONTHS + 1)]
    already_closed = fetch_closed_months()

    for month_key in wanted_months:
        if month_key != current_month_key and month_key in already_closed and month_key not in force_months:
            continue  # locked

        status = "current" if month_key == current_month_key else "closed"
        team_closed = 0
        team_within = 0

        for owner_id, owner_name in OWNERS.items():
            closed, within = owner_month_stats(owner_id, month_key)
            upsert_row(month_key, owner_name, closed, within, status)
            team_closed += closed
            team_within += within

        upsert_row(month_key, "team", team_closed, team_within, status)


if __name__ == "__main__":
    import sys
    main(force_months=sys.argv[1:])
