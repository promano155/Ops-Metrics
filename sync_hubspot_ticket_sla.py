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

  SLA MET = computed independently from raw timestamps
  (closed_date vs. hs_time_to_close_sla_at), NOT read from HubSpot's own
  time_to_close__met_sla or hs_time_to_close_sla_status fields. Those two
  HubSpot-calculated fields were found to disagree with each other on
  real tickets during the investigation that led to this rewrite (one
  said "completed on time," the other said the same ticket didn't meet
  SLA). Rather than pick one black box over another, this pipeline
  derives met/not-met itself from the raw dates, which is auditable and
  reproducible. See compute_met_sla() below for the exact formula,
  including the "adjusted for other-team time" version.

If the team later decides this should be scoped to specific ticket types
(e.g. only Data Processing Request / Reconciliation / Invoice-related
tickets), update OWNER_IDS filtering logic accordingly - see the
commented-out TICKET_TYPE_FILTER block below for how to add that back in.

Same current/closed lock pattern as the other syncs: current month
recomputed daily, past months written once and left alone.

--- New: "adjusted for other-team time" is now a real, shared formula ---
Previously the SLA % card and this script's "within_sla" count were two
independently-built things with no relationship to each other. They now
share one source of truth: STAGE_BUCKET_MAP (see below), which maps
every Support Pipeline stage to who currently owns the ticket. A ticket
that spent time in the tech / finance / media_brand / innova stages was
waiting on someone outside ops - compute_met_sla() pushes that ticket's
SLA deadline out by exactly that much time (pulled from HubSpot's own
per-stage cumulative-time properties) before checking whether it closed
in time, which is the "adjustment." A ticket with no hs_time_to_close_
sla_at at all (no SLA target applies) is excluded from the SLA
denominator entirely - this is the "eligible tickets" scoping already
visible on the dashboard card. hubspot_ticket_sla_monthly gains three
new, additive columns for this: eligible_closed, within_sla_adjusted,
sla_pct_adjusted. tickets_closed keeps its original meaning (every
closed ticket, eligible or not); within_sla/sla_pct are now computed
the same deterministic way but WITHOUT the other-team adjustment, so
both the raw and adjusted views stay available side by side. See the
ALTER TABLE statement in the delivery notes for the exact columns to add.

--- New: live "currently open" snapshot (Tickets Still Open panel) ---
Added to back a dashboard panel that shows, per person, how many
tickets are open RIGHT NOW and which team each one is currently
blocked on - distinct from (and not derived from) the
SLA-on-closed-tickets numbers above. This is a "right now" snapshot,
not a per-month historical metric, so it:
  - writes to its own tables (hubspot_ticket_open_buckets for per-scope
    counts, hubspot_ticket_open_detail for the per-ticket drill-down),
    never hubspot_ticket_sla_monthly - that table's past months are
    locked and must never be silently touched, and an
    always-overwritten live count has no business sharing a table with
    frozen numbers.
  - is always overwritten, never locked, same category as the
    open-by-stage snapshot in sync_hubspot_ticket_metrics.py.
The bucket columns (with_ops/cs_feedback/tech/finance/media_brand/
innova/hotel_client/long_term/unmapped) map to the Support Pipeline's
actual stage list - see the comment above STAGE_BUCKET_MAP for the
confirmed stage-id -> bucket mapping and the one deliberately-unmapped
stage. This was verified directly against HubSpot's own property
labels (search_properties), not guessed.
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

# Support Pipeline stage -> (bucket, human label). Confirmed directly
# against HubSpot's own property labels (search_properties), not guessed.
# Shared by both features in this script:
#   - the monthly SLA adjustment (compute_met_sla) uses OTHER_TEAM_BUCKETS
#     below to know which stages count as "waiting on another team"
#   - the live open-ticket snapshot (open_tickets_for_owner) uses the
#     full map to bucket every currently-open ticket
#
#   1           New Request                    -> with_ops
#   2           In Progress                     -> with_ops
#   3           CS Feedback Required             -> cs_feedback
#   4           Client Input Required            -> hotel_client
#   1008389482  Tech Action Required             -> tech
#   1020326275  Finance Action Required          -> finance
#   1061150809  Media Brand Feedback Required    -> media_brand
#   1405980051  Innova Feedback Required         -> innova
#   1181474568  Long Term Request (No SLA)       -> long_term
#   964383047   Completed                        -> (closed - tickets in
#                                                    this stage always
#                                                    have closed_date set,
#                                                    so they never reach
#                                                    this map via the
#                                                    open-ticket path, and
#                                                    the closed-ticket path
#                                                    doesn't bucket by
#                                                    stage at all)
#
# 1062225450 "Content Request Rejected" is deliberately left OUT of the
# map - it's a dead/terminal-ish state but doesn't set closed_date, so a
# ticket sitting there still shows as "open." Rather than guess whether
# that should count as with_ops, long_term, or something else, it falls
# into 'unmapped' so it's visible instead of silently misclassified. If
# the team decides this stage means something specific, add it here.
STAGE_BUCKET_MAP = {
    "1": ("with_ops", "New Request"),
    "2": ("with_ops", "In Progress"),
    "3": ("cs_feedback", "CS Feedback Required"),
    "4": ("hotel_client", "Client Input Required"),
    "1008389482": ("tech", "Tech Action Required"),
    "1020326275": ("finance", "Finance Action Required"),
    "1061150809": ("media_brand", "Media Brand Feedback Required"),
    "1405980051": ("innova", "Innova Feedback Required"),
    "1181474568": ("long_term", "Long Term Request (No SLA)"),
}

# Must match hubspot_ticket_open_buckets' columns exactly (minus scope/
# total_open/updated_at) - this list IS the table's bucket schema.
BUCKET_COLUMNS = [
    "with_ops", "cs_feedback", "tech", "finance",
    "media_brand", "innova", "hotel_client", "long_term", "unmapped",
]

# Which buckets count as "waiting on another team" for the SLA
# adjustment - i.e. NOT with_ops/cs_feedback/hotel_client/long_term.
# Derived from STAGE_BUCKET_MAP rather than listed separately, so the
# open-ticket bucketing and the SLA adjustment can never drift apart.
OTHER_TEAM_BUCKETS = {"tech", "finance", "media_brand", "innova"}
OTHER_TEAM_TIME_PROPS = [
    f"hs_v2_cumulative_time_in_{stage_id}"
    for stage_id, (bucket, _label) in STAGE_BUCKET_MAP.items()
    if bucket in OTHER_TEAM_BUCKETS
]

# ---------------------------------------------------------------------------
# HubSpot
# ---------------------------------------------------------------------------


def hubspot_headers():
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }


def hubspot_search_records(filters, properties, page_limit=100):
    """Paginated HubSpot ticket search returning full records (not just
    a count) via HubSpot's search 'after' cursor. Used everywhere in
    this script now - both the monthly SLA calc and the open-ticket
    snapshot need actual property values per ticket (closed_date,
    per-stage cumulative time, subject, etc.), not just a total.

    Sorted oldest-created-first as a sane default; callers that need a
    different order can re-sort the returned list."""
    results = []
    after = None
    while True:
        body = {
            "filterGroups": [{"filters": filters}],
            "properties": properties,
            "limit": page_limit,
            "sorts": [{"propertyName": "createdate", "direction": "ASCENDING"}],
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
            break
        else:
            raise RuntimeError("HubSpot search still rate limited after max retries")

        time.sleep(REQUEST_DELAY_SECONDS)
        payload = resp.json()
        results.extend(payload.get("results", []))

        next_page = payload.get("paging", {}).get("next")
        if not next_page:
            break
        after = next_page["after"]

    return results


def month_bounds(month_key):
    year, month = (int(x) for x in month_key.split("-"))
    start = dt.date(year, month, 1)
    end = dt.date(year + (month == 12), (month % 12) + 1, 1) - dt.timedelta(days=1)
    return start, end


def to_millis(d):
    return int(dt.datetime.combine(d, dt.time.min, tzinfo=dt.timezone.utc).timestamp() * 1000)


def parse_utc_datetime(value):
    """Same robust parser used elsewhere in this pipeline (see
    sync_yellow_rows_to_asana.py / sync_ops_task_tracker.py) - handles
    Supabase's and HubSpot's real timestamp formats ('+00', '+00:00',
    'Z', bare) and always returns a naive UTC datetime so it stays
    comparable with dt.datetime.utcnow()."""
    if value is None:
        return None
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return parsed


def compute_met_sla(props):
    """Returns (raw_met, adjusted_met, other_team_ms) for one closed
    ticket's properties, or (None, None, 0) if it has no SLA target at
    all (hs_time_to_close_sla_at missing) - such a ticket is excluded
    from the SLA denominator entirely rather than guessed either way.

    raw_met: closed_date <= hs_time_to_close_sla_at, computed directly
             from the raw dates. Deliberately NOT read from HubSpot's
             own time_to_close__met_sla or hs_time_to_close_sla_status
             fields - those two were found to disagree with each other
             on real tickets, so neither is trusted as ground truth here.

    adjusted_met: same comparison, but the deadline is pushed out by
                  other_team_ms - the total time this ticket spent in
                  an OTHER_TEAM_BUCKETS stage (tech/finance/media_brand/
                  innova), pulled from HubSpot's own per-stage
                  cumulative-time properties. Equivalent to pausing the
                  SLA clock while the ticket was waiting on someone
                  outside ops, computed after the fact since
                  hs_sla_pause_status isn't actually populated on any
                  ticket checked during this investigation.

    other_team_ms: the raw adjustment amount, in case a caller wants it
                   (e.g. for logging/debugging a specific ticket)."""
    closed_date = parse_utc_datetime(props.get("closed_date"))
    sla_due = parse_utc_datetime(props.get("hs_time_to_close_sla_at"))
    if closed_date is None or sla_due is None:
        return None, None, 0

    other_team_ms = sum(float(props.get(p) or 0) for p in OTHER_TEAM_TIME_PROPS)
    adjusted_due = sla_due + dt.timedelta(milliseconds=other_team_ms)

    raw_met = closed_date <= sla_due
    adjusted_met = closed_date <= adjusted_due
    return raw_met, adjusted_met, other_team_ms


def owner_month_stats(owner_id, month_key):
    """Returns (tickets_closed, eligible_closed, within_sla,
    within_sla_adjusted) for one owner/month.

    tickets_closed: every ticket closed in the month, regardless of
                     whether an SLA target applied to it.
    eligible_closed: the subset that actually had an SLA target
                      (hs_time_to_close_sla_at present) - this is the
                      "eligible tickets" denominator the SLA % card
                      should use, both raw and adjusted.
    within_sla / within_sla_adjusted: how many of the eligible tickets
                      met SLA, without and with the other-team
                      adjustment respectively. Both are out of
                      eligible_closed, not tickets_closed."""
    start, end = month_bounds(month_key)
    filters = [
        {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": str(owner_id)},
        {"propertyName": "closed_date", "operator": "GTE", "value": str(to_millis(start))},
        {"propertyName": "closed_date", "operator": "LTE", "value": str(to_millis(end) + 86_399_999)},
    ]
    properties = ["closed_date", "hs_time_to_close_sla_at"] + OTHER_TEAM_TIME_PROPS
    records = hubspot_search_records(filters, properties)

    tickets_closed = len(records)
    eligible_closed = 0
    within_sla = 0
    within_sla_adjusted = 0

    for r in records:
        raw_met, adjusted_met, _ = compute_met_sla(r.get("properties", {}))
        if raw_met is None:
            continue  # no SLA target on this ticket - not eligible, not counted either way
        eligible_closed += 1
        if raw_met:
            within_sla += 1
        if adjusted_met:
            within_sla_adjusted += 1

    return tickets_closed, eligible_closed, within_sla, within_sla_adjusted


# ---------------------------------------------------------------------------
# Currently-open snapshot (Tickets Still Open panel)
# ---------------------------------------------------------------------------
# Buckets every currently-open ticket by STAGE_BUCKET_MAP (defined in
# Config above, shared with the SLA adjustment). See that block's comment
# for the confirmed stage-id -> bucket mapping and the deliberately
# unmapped stage.

OPEN_BUCKETS_TABLE = "hubspot_ticket_open_buckets"     # per-scope bucket counts
OPEN_DETAIL_TABLE = "hubspot_ticket_open_detail"       # per-ticket rows for the drill-down


def open_tickets_for_owner(owner_id):
    """Every ticket currently owned by owner_id with no closed_date at
    all - i.e. genuinely still open right now. Returns
    [{ticket_id, subject, days_open, stage_id, bucket, stage_label}],
    oldest-created first."""
    filters = [
        {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": str(owner_id)},
        {"propertyName": "closed_date", "operator": "NOT_HAS_PROPERTY"},
    ]
    records = hubspot_search_records(
        filters, properties=["subject", "createdate", "hs_pipeline_stage"]
    )

    now = dt.datetime.utcnow()
    open_tickets = []
    for r in records:
        props = r.get("properties", {})
        created = parse_utc_datetime(props.get("createdate"))
        days_open = (now - created).days if created else None
        stage_id = props.get("hs_pipeline_stage")
        bucket, stage_label = STAGE_BUCKET_MAP.get(
            stage_id, ("unmapped", f"Unmapped stage ({stage_id})")
        )
        open_tickets.append({
            "ticket_id": r["id"],
            "subject": props.get("subject") or "(no subject)",
            "days_open": days_open,
            "stage_id": stage_id,
            "bucket": bucket,
            "stage_label": stage_label,
        })
    return open_tickets


def bucket_open_tickets(open_tickets):
    """Counts a list of {bucket, ...} dicts into BUCKET_COLUMNS, plus
    total_open. This drives hubspot_ticket_open_buckets' columns 1:1."""
    counts = {col: 0 for col in BUCKET_COLUMNS}
    for t in open_tickets:
        counts[t["bucket"]] += 1
    counts["total_open"] = len(open_tickets)
    return counts


def upsert_open_buckets(scope, open_tickets):
    """Single-row-per-scope table, always overwritten - same pattern as
    overwrite_status_snapshot() in sync_hubspot_ticket_metrics.py. scope
    is 'team' or one of the OWNERS names ('Javiana'/'Lucas'/'Victoria')."""
    counts = bucket_open_tickets(open_tickets)
    payload = {"scope": scope, "updated_at": dt.datetime.utcnow().isoformat(), **counts}
    url = f"{SUPABASE_URL}/rest/v1/{OPEN_BUCKETS_TABLE}"
    resp = requests.post(
        url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Upserted open-bucket snapshot for '{scope}': {counts}")


def replace_open_detail(scope, open_tickets):
    """hubspot_ticket_open_detail is per-TICKET (ticket_id is the primary
    key), not per-scope, so a plain upsert would leave a stale row behind
    forever once a ticket closes or changes owner - nothing would ever
    delete it. Instead: delete every existing detail row for this scope,
    then insert the current set fresh. Two requests instead of one, but
    correctness here (no ghost 'still open' tickets lingering in the
    drill-down) matters more than saving a round trip.

    Only called for individual people, not 'team' - a ticket belongs to
    exactly one owner, so a 'team' scope here would just duplicate every
    row already written under its owner's name."""
    delete_url = f"{SUPABASE_URL}/rest/v1/{OPEN_DETAIL_TABLE}"
    resp = requests.delete(
        delete_url, headers=supabase_headers(), params={"scope": f"eq.{scope}"}, timeout=30
    )
    resp.raise_for_status()

    if not open_tickets:
        print(f"Cleared open-detail rows for '{scope}' (0 currently open).")
        return

    rows = [
        {
            "ticket_id": t["ticket_id"],
            "scope": scope,
            "subject": t["subject"],
            "stage_bucket": t["bucket"],
            "stage_label": t["stage_label"],
            "days_open": t["days_open"],
            "updated_at": dt.datetime.utcnow().isoformat(),
        }
        for t in open_tickets
    ]
    insert_resp = requests.post(
        delete_url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=rows,
        timeout=30,
    )
    insert_resp.raise_for_status()
    print(f"Replaced open-detail rows for '{scope}': {len(rows)} open ticket(s).")


def sync_open_snapshots(dry_run=False):
    """Runs the live open-ticket snapshot (both the bucket-count table and
    the per-ticket detail table) for the team total plus each person.
    Always overwrites - nothing here is locked, and there's no month_key
    at all, since this is a 'right now' number. Safe to run every time
    this script runs. In dry-run mode, computes and prints everything but
    writes nothing to Supabase."""
    all_open = []
    for owner_id, owner_name in OWNERS.items():
        open_tickets = open_tickets_for_owner(owner_id)
        all_open.extend(open_tickets)
        if dry_run:
            print(f"[DRY RUN] '{owner_name}': {len(open_tickets)} open ticket(s), "
                  f"buckets={bucket_open_tickets(open_tickets)}")
        else:
            upsert_open_buckets(owner_name, open_tickets)
            replace_open_detail(owner_name, open_tickets)

    if dry_run:
        print(f"[DRY RUN] 'team': {len(all_open)} open ticket(s), "
              f"buckets={bucket_open_tickets(all_open)}")
    else:
        upsert_open_buckets("team", all_open)
        # No replace_open_detail("team", ...) - see its docstring: a
        # ticket belongs to one owner, so "team" detail rows would just
        # duplicate rows already written under that owner's name.


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


def upsert_row(month_key, scope, tickets_closed, eligible_closed, within_sla,
               within_sla_adjusted, status):
    # Both percentages are out of eligible_closed (tickets that actually
    # had an SLA target), not tickets_closed - see owner_month_stats().
    sla_pct = round(within_sla / eligible_closed * 100, 1) if eligible_closed else None
    sla_pct_adjusted = round(within_sla_adjusted / eligible_closed * 100, 1) if eligible_closed else None
    payload = {
        "month_key": month_key,
        "scope": scope,  # 'team' or one of 'Javiana' / 'Lucas' / 'Victoria'
        "tickets_closed": tickets_closed,
        "eligible_closed": eligible_closed,
        "within_sla": within_sla,
        "sla_pct": sla_pct,
        "within_sla_adjusted": within_sla_adjusted,
        "sla_pct_adjusted": sla_pct_adjusted,
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


def main(force_months=None, dry_run=False):
    force_months = set(force_months or [])
    current_month_key = month_key_n_back(0)
    wanted_months = [month_key_n_back(n) for n in range(TRAILING_MONTHS + 1)]
    already_closed = fetch_closed_months()

    for month_key in wanted_months:
        if month_key != current_month_key and month_key in already_closed and month_key not in force_months:
            continue  # locked

        status = "current" if month_key == current_month_key else "closed"
        team_closed = 0
        team_eligible = 0
        team_within = 0
        team_within_adjusted = 0

        for owner_id, owner_name in OWNERS.items():
            closed, eligible, within, within_adjusted = owner_month_stats(owner_id, month_key)
            upsert_row(month_key, owner_name, closed, eligible, within, within_adjusted, status)
            team_closed += closed
            team_eligible += eligible
            team_within += within
            team_within_adjusted += within_adjusted

        upsert_row(month_key, "team", team_closed, team_eligible, team_within,
                   team_within_adjusted, status)

    # Live "currently open" snapshot for the new Tickets Still Open panel -
    # its own table, no month_key, always overwritten. See
    # sync_open_snapshots()'s docstring and the ASSUMPTIONS note above
    # OPEN_BUCKETS_TABLE. Only this step honors --dry-run; the monthly
    # backfill above always writes, matching this script's existing
    # (pre-this-change) behavior so as not to change production semantics
    # for a step that wasn't part of this request.
    sync_open_snapshots(dry_run=dry_run)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("months", nargs="*",
                         help="Force-recompute specific already-closed months, e.g. 2026-08 2026-07.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Print what the open-ticket snapshot would write, without writing it. "
                              "Only affects the open-ticket snapshot step.")
    args = parser.parse_args()
    main(force_months=args.months, dry_run=args.dry_run)
