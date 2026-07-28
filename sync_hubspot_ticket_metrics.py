"""
sync_hubspot_ticket_metrics.py

Pulls support ticket volume and status from HubSpot and writes it to
Supabase for the Lovable dashboard's new "Support tickets" section.

Run daily via GitHub Actions, same workflow as the data processing sync.

--- What this covers ---
Your portal has one active pipeline with tickets in it: "Support Pipeline"
(id "0"). Stage IDs below were pulled live from the portal on 2026-07-28 -
if HubSpot support adds/renames a stage, update OPEN_STAGE_IDS /
STAGE_LABELS rather than the query logic.

  1  New Request
  2  In Progress
  3  CS Feedback Required
  4  Client Input Required
  1181474568  Long Term Request (No SLA)
  964383047   Completed   <- the only "closed" stage with any volume

--- Two different kinds of metric, handled differently ---
1. MONTHLY VOLUME (tickets_created, tickets_closed) - same lock pattern as
   the data processing sync: current calendar month recomputed daily,
   earlier months written once and left alone.
2. LIVE STATUS SNAPSHOT (how many open tickets are sitting in each stage
   right now) - this is inherently a "right now" number, not a per-month
   historical one, so there's no "closed month" concept for it. It's
   simply overwritten daily. See README for why this isn't forced into
   the same monthly-lock shape as the volume metric.
"""

import os
import datetime as dt

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HUBSPOT_TOKEN = os.environ["HUBSPOT_PRIVATE_APP_TOKEN"]
HUBSPOT_SEARCH_URL = "https://api.hubapi.com/crm/v3/objects/tickets/search"

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
VOLUME_TABLE = "hubspot_ticket_monthly_volume"
STATUS_TABLE = "hubspot_ticket_status_snapshot"

SUPPORT_PIPELINE_ID = "0"
STAGE_COMPLETED = "964383047"
OPEN_STAGE_IDS = ["1", "2", "3", "4", "1181474568"]
STAGE_LABELS = {
    "1": "New Request",
    "2": "In Progress",
    "3": "CS Feedback Required",
    "4": "Client Input Required",
    "1181474568": "Long Term Request (No SLA)",
}

TRAILING_MONTHS = 24

# ---------------------------------------------------------------------------
# HubSpot
# ---------------------------------------------------------------------------


def hubspot_headers():
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }


def hubspot_search_total(filter_groups):
    """Returns just the total count matching the filters (limit=1 -> we
    don't need the records themselves, just the count HubSpot reports)."""
    body = {
        "filterGroups": filter_groups,
        "properties": ["hs_object_id"],
        "limit": 1,
    }
    resp = requests.post(HUBSPOT_SEARCH_URL, headers=hubspot_headers(), json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()["total"]


def month_bounds(month_key):
    year, month = (int(x) for x in month_key.split("-"))
    start = dt.date(year, month, 1)
    end = (dt.date(year + (month == 12), (month % 12) + 1, 1) - dt.timedelta(days=1))
    return start, end


def to_millis(d):
    return int(dt.datetime.combine(d, dt.time.min, tzinfo=dt.timezone.utc).timestamp() * 1000)


def tickets_created_in_month(month_key):
    start, end = month_bounds(month_key)
    filter_groups = [{
        "filters": [
            {"propertyName": "hs_pipeline", "operator": "EQ", "value": SUPPORT_PIPELINE_ID},
            {"propertyName": "createdate", "operator": "GTE", "value": str(to_millis(start))},
            {"propertyName": "createdate", "operator": "LTE", "value": str(to_millis(end) + 86_399_999)},
        ]
    }]
    return hubspot_search_total(filter_groups)


def tickets_closed_in_month(month_key):
    start, end = month_bounds(month_key)
    filter_groups = [{
        "filters": [
            {"propertyName": "hs_pipeline", "operator": "EQ", "value": SUPPORT_PIPELINE_ID},
            {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": STAGE_COMPLETED},
            {"propertyName": "closed_date", "operator": "GTE", "value": str(to_millis(start))},
            {"propertyName": "closed_date", "operator": "LTE", "value": str(to_millis(end) + 86_399_999)},
        ]
    }]
    return hubspot_search_total(filter_groups)


def open_ticket_counts_by_stage():
    counts = {}
    for stage_id in OPEN_STAGE_IDS:
        filter_groups = [{
            "filters": [
                {"propertyName": "hs_pipeline", "operator": "EQ", "value": SUPPORT_PIPELINE_ID},
                {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": stage_id},
            ]
        }]
        counts[STAGE_LABELS[stage_id]] = hubspot_search_total(filter_groups)
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


def fetch_closed_volume_month_keys():
    url = f"{SUPABASE_URL}/rest/v1/{VOLUME_TABLE}?select=month_key,status"
    resp = requests.get(url, headers=supabase_headers(), timeout=30)
    resp.raise_for_status()
    return {r["month_key"] for r in resp.json() if r["status"] == "closed"}


def upsert_volume(month_key, created, closed, status):
    payload = {
        "month_key": month_key,
        "tickets_created": created,
        "tickets_closed": closed,
        "status": status,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    url = f"{SUPABASE_URL}/rest/v1/{VOLUME_TABLE}"
    resp = requests.post(
        url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Upserted ticket volume {month_key} ({status}): {payload}")


def overwrite_status_snapshot(stage_counts):
    # Single-row table, always overwritten: id fixed at 1.
    payload = {
        "id": 1,
        "open_by_stage": stage_counts,
        "total_open": sum(stage_counts.values()),
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    url = f"{SUPABASE_URL}/rest/v1/{STATUS_TABLE}"
    resp = requests.post(
        url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Overwrote status snapshot: {payload}")


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

    already_closed = fetch_closed_volume_month_keys()

    for month_key in wanted_months:
        if month_key != current_month_key and month_key in already_closed and month_key not in force_months:
            continue  # locked
        created = tickets_created_in_month(month_key)
        closed = tickets_closed_in_month(month_key)
        status = "current" if month_key == current_month_key else "closed"
        upsert_volume(month_key, created, closed, status)

    stage_counts = open_ticket_counts_by_stage()
    overwrite_status_snapshot(stage_counts)


if __name__ == "__main__":
    import sys
    main(force_months=sys.argv[1:])
