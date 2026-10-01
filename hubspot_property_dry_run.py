# HubSpot property cleanup - DRY RUN (read-only)
# Finds custom properties with no data on any record, minus integration fields.
# This script only READS from HubSpot. It never archives or changes anything.
#
# How to run in Google Colab:
#   1. Go to colab.research.google.com > New notebook
#   2. Paste this whole file into a cell and press the play button
#   3. Paste your private app token when asked (it is not saved or shown)
#   4. A CSV downloads when it's done

import requests, time, csv, getpass

TOKEN = getpass.getpass("Paste your HubSpot private app token: ")
HEADERS = {"Authorization": f"Bearer {pat-na1-988357be-19e3-49ff-85d9-3f2b1bba8f97}", "Content-Type": "application/json"}
BASE = "https://api.hubapi.com"

OBJECTS = ["companies", "contacts", "deals"]

# Integration fields - never archive (from the Curacity<>HubSpot Integration guide)
EXCLUDE_LABELS = {
    "lifecycle stage", "products", "currency", "hotel name", "monthly fee",
    "monthly fee start date", "min invoice threshold", "max invoice threshold",
    "invoice cap", "invoice cap type", "curacity select", "is curacity select",
    "attribution period", "attribution period days", "hotel status",
    "street address", "address", "city", "state", "zip", "contract start date",
    "contract expiration date", "number of rooms", "surface id",
    "exavault username", "hotel group", "parent", "parent (group)",
    "quickbooks customer id", "pms", "pms system", "rms", "rms system",
    "hotel description", "phone number", "website url",
    "booking file last uploaded", "last reconciled by hotel",
    "first name", "last name", "email", "job title",
}
# Labels containing these words get flagged for a manual look instead of listed
WATCH_WORDS = ["dmb", "roam", "curacity", "exavault", "surface", "invoice",
               "quickbooks", "webhook", "attribution", "hotel"]


def call(method, url, **kwargs):
    """Make a request, waiting and retrying if HubSpot rate-limits us."""
    for attempt in range(6):
        r = requests.request(method, url, headers=HEADERS, **kwargs)
        if r.status_code == 429:
            time.sleep(2 * (attempt + 1))
            continue
        return r
    return r


rows = []
for obj in OBJECTS:
    r = call("GET", f"{BASE}/crm/v3/properties/{obj}")
    if r.status_code != 200:
        print(f"Couldn't read {obj} properties ({r.status_code}): {r.text[:200]}")
        continue
    props = r.json().get("results", [])
    custom = [p for p in props
              if not p.get("hubspotDefined")
              and not p.get("calculated")
              and not p.get("modificationMetadata", {}).get("readOnlyValue")]
    print(f"{obj}: {len(props)} properties, {len(custom)} custom. Checking each for data...")

    for i, p in enumerate(custom, 1):
        label = p.get("label", "")
        name = p["name"]
        body = {"filterGroups": [{"filters": [{"propertyName": name, "operator": "HAS_PROPERTY"}]}],
                "limit": 1, "properties": ["hs_object_id"]}
        s = call("POST", f"{BASE}/crm/v3/objects/{obj}/search", json=body)
        time.sleep(0.25)  # stay under the search rate limit

        if s.status_code != 200:
            records, status = "", "CHECK MANUALLY (couldn't search this property)"
        else:
            records = s.json().get("total", 0)
            if records > 0:
                status = "Has data - keep"
            elif label.strip().lower() in EXCLUDE_LABELS:
                status = "Integration field - keep"
            elif any(w in label.lower() or w in name.lower() for w in WATCH_WORDS):
                status = "Empty, but name looks integration-related - review"
            else:
                status = "ARCHIVE CANDIDATE"

        rows.append({"object": obj, "label": label, "internal_name": name,
                     "group": p.get("groupName", ""), "records_with_value": records,
                     "created": p.get("createdAt", ""), "status": status})
        if i % 50 == 0:
            print(f"  ...{i}/{len(custom)}")

rows.sort(key=lambda x: (x["status"] != "ARCHIVE CANDIDATE", x["object"], x["label"]))
filename = "hubspot_property_dry_run.csv"
with open(filename, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["object"])
    w.writeheader()
    w.writerows(rows)

counts = {}
for r in rows:
    counts[r["status"]] = counts.get(r["status"], 0) + 1
print("\nDone. Summary:")
for k, v in counts.items():
    print(f"  {k}: {v}")

try:
    from google.colab import files
    files.download(filename)
except ImportError:
    print(f"Saved {filename}")
