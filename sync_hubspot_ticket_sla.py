"""
sync_hubspot_ticket_sla.py

Two related things, one daily run, both scoped to tickets owned by
Javiana Pacheco, Lucas Berberian, or Victoria Camacho:

1. OPEN TICKET BUCKETS ("Currently sitting with") - a live snapshot,
   not a monthly metric. For every currently-open ticket owned by the
   three of them, buckets it by CURRENT pipeline stage into who is
   actually holding it up right now:

     with_ops      - New Request / In Progress - actively being worked,
                     nothing external blocking it
     cs_feedback   - CS Feedback Required
     tech          - Tech Action Required
     finance       - Finance Action Required
     media_brand   - Media Brand Feedback Required
     innova        - Innova Feedback Required (the vendor)
     hotel_client  - Client Input Required
     long_term     - Long Term Request (No SLA) - open-ended by design,
                     not "blocked" in the normal sense

   Content Request Rejected and Completed are terminal and excluded
   entirely (not open). One row per scope ('team' + each of the three
   names), overwritten every run - there's no historical "open right
   now as of last Tuesday" concept worth keeping.

2. MONTHLY SLA, RAW AND ADJUSTED - same current/closed lock pattern as
   the other syncs (current month recomputed daily, past months written
   once and left alone), but now computed per-ticket instead of via
   HubSpot's count-only search, because the adjusted number needs each
   ticket's own numbers to work with:

     within_sla            - unchanged from before: HubSpot's own
                              time_to_close__met_sla, wall-clock, no
                              adjustment. Kept as-is (not overwritten
                              with a redefinition) so nothing already
                              reported quietly changes meaning.
     within_sla_adjusted    - subtracts time the ticket spent sitting in
                              Tech / Finance / Media Brand / Innova
                              Feedback Required (pulled from HubSpot's
                              own hs_v2_cumulative_time_in_<stageId>
                              fields, which persist after the ticket
                              moves on or closes) from the elapsed
                              close time, then compares THAT against the
                              ticket's original SLA target duration
                              (hs_time_to_close_sla_at - createdate).
     eligible_for_adjusted  - count of closed tickets that actually HAD
                              an SLA target at all (hs_time_to_close_sla_at
                              present). Tickets with no target (e.g.
                              "Long Term Action (No SLA)" ticket type)
                              are excluded from the adjusted percentage's
                              denominator, same as they're already
                              excluded from HubSpot's own SLA field -
                              this is NOT the same denominator as
                              tickets_closed, so sla_pct_adjusted is
                              computed against eligible_for_adjusted,
                              not tickets_closed.

   CS Feedback Required and Client Input Required time is deliberately
   NOT subtracted for the adjusted metric - only the four stages above
   that represent another INTERNAL team (or the vendor) holding the
   ball. If that scope should widen, say so explicitly rather than
   assuming - this was a specific, scoped ask.

--- Why per-ticket fetch instead of count-only search now ---
The old version only ever asked HubSpot for two counts (total closed,
within-SLA) via the search endpoint's `total`. That's enough for the
raw number but can't produce the adjusted one - there's no way to ask
HubSpot's search API "give me the count where (elapsed - stage time) is
under X" server-side. So this version fetches each ticket's own fields
and computes both numbers in Python, the same way every other script in
this pipeline already handles anything that isn't a flat filter+count.

--- Why hs_v2_cumulative_time_in_<stageId>, not entered/exited timestamps ---
HubSpot already maintains a running total, in milliseconds, of time
spent in a given stage across the ticket's WHOLE life - including
stages it has since left, and it survives the ticket closing. That
means a ticket that bounced into Tech Action Required twice still gets
correctly summed, with no interval math needed on this end. Confirmed
this is actually populated on closed tickets (not just currently-open
ones) before relying on it.

--- Units ---
time_to_close, hs_time_to_close_sla_at, createdate, and every
hs_v2_cumulative_time_in_<stageId> field are all in the same units
HubSpot returns them in: epoch milliseconds for the dates, plain
milliseconds for the durations. Confirmed by cross-checking a real
ticket's time_to_close against its createdate/closed_date gap before
trusting the field.
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
PAGE_SIZE = 100

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
SLA_TABLE = "hubspot_ticket_sla_monthly"
OPEN_BUCKETS_TABLE = "hubspot_ticket_open_buckets"

OWNERS = {
    79058582: "Javiana",
    1985678304: "Lucas",
    90048338: "Victoria",
}

# Pulled live from the portal (Support Pipeline, id "0") on 2026-08-25.
# If HubSpot support adds/renames a stage, update these maps rather than
# touching the bucketing/adjustment logic below.
STAGE_COMPLETED = "964383047"
STAGE_CONTENT_REJECTED = "1062225450"  # terminal, excluded like Completed

BUCKET_BY_STAGE_ID = {
    "1": "with_ops",              # New Request
    "2": "with_ops",              # In Progress
    "3": "cs_feedback",           # CS Feedback Required
    "4": "hotel_client",          # Client Input Required
    "1008389482": "tech",         # Tech Action Required
    "1020326275": "finance",      # Finance Action Required
    "1061150809": "media_brand",  # Media Brand Feedback Required
    "1405980051": "innova",       # Innova Feedback Required
    "1181474568": "long_term",    # Long Term Request (No SLA)
}
BUCKET_NAMES = ["with_ops", "cs_feedback", "tech", "finance", "media_brand", "innova", "hotel_client", "long_term"]

# The four "another team/vendor is holding this" stages whose time gets
# subtracted for the adjusted SLA metric. Deliberately excludes CS
# Feedback Required and Client Input Required - narrower, explicitly-
# scoped ask, not "anything not with_ops."
OTHER_TEAM_STAGE_IDS = ["1008389482", "1020326275", "1061150809", "1405980051"]
OTHER_TEAM_TIME_FIELDS = [f"hs_v2_cumulative_time_in_{sid}" for sid in OTHER_TEAM_STAGE_IDS]

TRAILING_MONTHS = 24

# ---------------------------------------------------------------------------
# HubSpot
# ---------------------------------------------------------------------------


def hubspot_headers():
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }


def hubspot_search(filter_groups, properties, after=None):
    body = {
        "filterGroups": filter_groups,
        "properties": properties,
        "limit": PAGE_SIZE,
    }
    if after:
        body["after"] = after
    for attempt in range(MAX_RETRIES):
        resp = requests.post(HUBSPOT_SEARCH_URL, headers=hubspot_headers(), json=body, timeout=30)
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"Rate limited, waiting {retry_after}s (attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return resp.json()
    raise RuntimeError("HubSpot search still rate limited after max retries")


def fetch_all_tickets(filter_groups, properties):
    """Paginates a HubSpot ticket search to completion. Used for both the
    open-tickets pull and the per-month closed-tickets pull - anywhere we
    need each ticket's own fields, not just a count."""
    results = []
    after = None
    while True:
        body = hubspot_search(filter_groups, properties, after=after)
        results.extend(body.get("results", []))
        paging = body.get("paging", {}).get("next")
        if not paging:
            break
        after = paging["after"]
    return results


def to_millis(d):
    return int(dt.datetime.combine(d, dt.time.min, tzinfo=dt.timezone.utc).timestamp() * 1000)


def month_bounds(month_key):
    year, month = (int(x) for x in month_key.split("-"))
    start = dt.date(year, month, 1)
    end = dt.date(year + (month == 12), (month % 12) + 1, 1) - dt.timedelta(days=1)
    return start, end


def parse_hubspot_epoch_ms(value):
    """HubSpot returns these as either an ISO string or a millisecond
    epoch string depending on endpoint - normalize to int ms."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    value = str(value)
    if value.isdigit():
        return int(value)
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


# ---------------------------------------------------------------------------
# Open ticket buckets ("Currently sitting with")
# ---------------------------------------------------------------------------


def fetch_open_tickets_for_owners(owner_ids):
    filter_groups = [{
        "filters": [
            {"propertyName": "hubspot_owner_id", "operator": "IN", "values": [str(o) for o in owner_ids]},
            {"propertyName": "hs_pipeline_stage", "operator": "NOT_IN",
             "values": [STAGE_COMPLETED, STAGE_CONTENT_REJECTED]},
        ]
    }]
    return fetch_all_tickets(filter_groups, ["hubspot_owner_id", "hs_pipeline_stage"])


def compute_open_buckets(tickets):
    """Returns {owner_name_or_'team': {bucket: count}}. Stages not in
    BUCKET_BY_STAGE_ID (shouldn't happen given the NOT_IN filter above,
    but data drifts) are counted under 'unmapped' per scope so a new
    stage shows up as a visible gap instead of silently vanishing."""
    scopes = ["team"] + list(OWNERS.values())
    counts = {scope: {b: 0 for b in BUCKET_NAMES + ["unmapped"]} for scope in scopes}

    for t in tickets:
        props = t.get("properties", {})
        owner_id = props.get("hubspot_owner_id")
        owner_name = OWNERS.get(int(owner_id)) if owner_id else None
        if owner_name is None:
            continue  # not one of the three tracked owners
        stage_id = props.get("hs_pipeline_stage")
        bucket = BUCKET_BY_STAGE_ID.get(stage_id, "unmapped")
        counts["team"][bucket] += 1
        counts[owner_name][bucket] += 1

    return counts


def upsert_open_buckets(scope, bucket_counts):
    payload = {
        "scope": scope,
        "total_open": sum(bucket_counts[b] for b in BUCKET_NAMES + ["unmapped"]),
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    payload.update(bucket_counts)
    url = f"{SUPABASE_URL}/rest/v1/{OPEN_BUCKETS_TABLE}"
    resp = requests.post(
        url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Upserted open buckets for '{scope}': {payload}")


# ---------------------------------------------------------------------------
# Monthly SLA, raw + adjusted
# ---------------------------------------------------------------------------

CLOSED_TICKET_PROPERTIES = [
    "hubspot_owner_id", "createdate", "closed_date", "time_to_close",
    "time_to_close__met_sla", "hs_time_to_close_sla_at",
] + OTHER_TEAM_TIME_FIELDS


def fetch_closed_tickets_for_owner_month(owner_id, month_key):
    start, end = month_bounds(month_key)
    filter_groups = [{
        "filters": [
            {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": str(owner_id)},
            {"propertyName": "closed_date", "operator": "GTE", "value": str(to_millis(start))},
            {"propertyName": "closed_date", "operator": "LTE", "value": str(to_millis(end) + 86_399_999)},
        ]
    }]
    return fetch_all_tickets(filter_groups, CLOSED_TICKET_PROPERTIES)


def ticket_sla_outcome(props):
    """Returns (met_sla_raw_or_None, met_sla_adjusted_or_None,
    has_adjusted_target). met_sla_raw comes straight from HubSpot's own
    field - untouched. met_sla_adjusted is None when the ticket has no
    SLA target at all (e.g. Long Term Action (No SLA) ticket type),
    which excludes it from the adjusted denominator rather than counting
    it as a miss."""
    raw_value = props.get("time_to_close__met_sla")
    met_raw = None if raw_value is None else (raw_value == "true" or raw_value is True)

    sla_target_at = parse_hubspot_epoch_ms(props.get("hs_time_to_close_sla_at"))
    created_at = parse_hubspot_epoch_ms(props.get("createdate"))
    time_to_close = props.get("time_to_close")
    time_to_close = int(time_to_close) if time_to_close not in (None, "") else None

    if sla_target_at is None or created_at is None or time_to_close is None:
        return met_raw, None, False

    other_team_ms = 0
    for field in OTHER_TEAM_TIME_FIELDS:
        val = props.get(field)
        if val not in (None, ""):
            other_team_ms += int(val)

    adjusted_time_to_close = max(0, time_to_close - other_team_ms)
    sla_target_ms = sla_target_at - created_at
    met_adjusted = adjusted_time_to_close <= sla_target_ms
    return met_raw, met_adjusted, True


def aggregate_month(owner_id, owner_name, month_key):
    tickets = fetch_closed_tickets_for_owner_month(owner_id, month_key)
    tickets_closed = len(tickets)
    within_sla = 0
    within_sla_adjusted = 0
    eligible_for_adjusted = 0

    for t in tickets:
        props = t.get("properties", {})
        met_raw, met_adjusted, has_target = ticket_sla_outcome(props)
        if met_raw:
            within_sla += 1
        if has_target:
            eligible_for_adjusted += 1
            if met_adjusted:
                within_sla_adjusted += 1

    return {
        "owner_name": owner_name,
        "tickets_closed": tickets_closed,
        "within_sla": within_sla,
        "eligible_for_adjusted": eligible_for_adjusted,
        "within_sla_adjusted": within_sla_adjusted,
    }


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
    url = f"{SUPABASE_URL}/rest/v1/{SLA_TABLE}?select=month_key,status&scope=eq.team"
    resp = requests.get(url, headers=supabase_headers(), timeout=30)
    resp.raise_for_status()
    return {r["month_key"] for r in resp.json() if r["status"] == "closed"}


def upsert_sla_row(month_key, scope, agg, status):
    sla_pct = round(agg["within_sla"] / agg["tickets_closed"] * 100, 1) if agg["tickets_closed"] else None
    sla_pct_adjusted = (
        round(agg["within_sla_adjusted"] / agg["eligible_for_adjusted"] * 100, 1)
        if agg["eligible_for_adjusted"] else None
    )
    payload = {
        "month_key": month_key,
        "scope": scope,
        "tickets_closed": agg["tickets_closed"],
        "within_sla": agg["within_sla"],
        "sla_pct": sla_pct,
        "eligible_for_adjusted": agg["eligible_for_adjusted"],
        "within_sla_adjusted": agg["within_sla_adjusted"],
        "sla_pct_adjusted": sla_pct_adjusted,
        "status": status,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }
    url = f"{SUPABASE_URL}/rest/v1/{SLA_TABLE}"
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


def run_open_buckets(dry_run=False):
    tickets = fetch_open_tickets_for_owners(OWNERS.keys())
    counts = compute_open_buckets(tickets)
    if dry_run:
        print("[DRY RUN] Would upsert open buckets:")
        for scope, bucket_counts in counts.items():
            print(f"  {scope}: {bucket_counts}")
        return
    for scope, bucket_counts in counts.items():
        upsert_open_buckets(scope, bucket_counts)


def run_monthly_sla(force_months=None, dry_run=False):
    force_months = set(force_months or [])
    current_month_key = month_key_n_back(0)
    wanted_months = [month_key_n_back(n) for n in range(TRAILING_MONTHS + 1)]
    already_closed = fetch_closed_months()

    for month_key in wanted_months:
        if month_key != current_month_key and month_key in already_closed and month_key not in force_months:
            continue  # locked

        status = "current" if month_key == current_month_key else "closed"
        team_agg = {"tickets_closed": 0, "within_sla": 0, "eligible_for_adjusted": 0, "within_sla_adjusted": 0}

        for owner_id, owner_name in OWNERS.items():
            agg = aggregate_month(owner_id, owner_name, month_key)
            if dry_run:
                print(f"[DRY RUN] {month_key} / {owner_name} ({status}): {agg}")
            else:
                upsert_sla_row(month_key, owner_name, agg, status)
            for key in team_agg:
                team_agg[key] += agg[key]

        if dry_run:
            print(f"[DRY RUN] {month_key} / team ({status}): {team_agg}")
        else:
            upsert_sla_row(month_key, "team", team_agg, status)


def main(force_months=None, dry_run=False):
    run_open_buckets(dry_run=dry_run)
    run_monthly_sla(force_months=force_months, dry_run=dry_run)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                         help="No Supabase writes - just prints what would happen.")
    parser.add_argument("--month", type=str, action="append", default=None,
                         help="Force-recompute a specific already-closed month (e.g. 2026-07). Repeatable.")
    args = parser.parse_args()

    main(force_months=args.month, dry_run=args.dry_run)
