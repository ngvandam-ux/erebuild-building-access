#!/usr/bin/env python3
"""
Enrich buildings with:
1. Ramsey County Socrata taxpayer names (167K parcels with owner info)
2. St Paul Commercial Certificate of Occupancy (4,111 commercial buildings)

Matches by normalized street address to existing buildings in Supabase.
Inserts owner/taxpayer as contacts, and C of O property name + occupancy type.
"""
import json, re, time
from urllib.request import urlopen, Request
from urllib.error import HTTPError
from urllib.parse import quote

SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg"

CON_KEYS = ["building_id", "name", "role", "phone", "email", "notes", "source"]

def norm_addr(addr):
    """Normalize address for matching"""
    if not addr:
        return ""
    a = addr.upper().strip()
    # Remove unit/suite/apt
    a = re.sub(r'\s+(UNIT|STE|SUITE|APT|#)\s*\S*', '', a)
    # Standardize directionals
    a = re.sub(r'\bNORTH\b', 'N', a)
    a = re.sub(r'\bSOUTH\b', 'S', a)
    a = re.sub(r'\bEAST\b', 'E', a)
    a = re.sub(r'\bWEST\b', 'W', a)
    # Standardize street types
    a = re.sub(r'\bSTREET\b', 'ST', a)
    a = re.sub(r'\bAVENUE\b', 'AVE', a)
    a = re.sub(r'\bBOULEVARD\b', 'BLVD', a)
    a = re.sub(r'\bDRIVE\b', 'DR', a)
    a = re.sub(r'\bLANE\b', 'LN', a)
    a = re.sub(r'\bPLACE\b', 'PL', a)
    a = re.sub(r'\bROAD\b', 'RD', a)
    a = re.sub(r'\bCOURT\b', 'CT', a)
    a = re.sub(r'\bCIRCLE\b', 'CIR', a)
    a = re.sub(r'\bPARKWAY\b', 'PKWY', a)
    a = re.sub(r'\bTERRACE\b', 'TER', a)
    a = re.sub(r'\bHIGHWAY\b', 'HWY', a)
    # Remove extra spaces
    a = re.sub(r'\s+', ' ', a).strip()
    return a

def norm_row(row):
    return {k: row.get(k) for k in CON_KEYS}

def supabase_post(rows):
    if not rows: return 0
    total = 0
    for i in range(0, len(rows), 200):
        batch = rows[i:i+200]
        data = json.dumps(batch).encode()
        req = Request(f"{SUPABASE_URL}/rest/v1/ba_contacts", data=data, headers={
            "apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json", "Prefer": "resolution=ignore-duplicates,return=minimal"
        }, method="POST")
        try:
            urlopen(req, timeout=30)
            total += len(batch)
        except HTTPError as e:
            err = e.read().decode()[:200]
            if "Empty or invalid" not in err:
                print(f"  Insert err: {err}")
        time.sleep(0.2)
    return total

# ====================================================
# Step 1: Load existing buildings from Supabase
# ====================================================
print("Loading existing buildings from Supabase...")
buildings = []
last_id = 0
while True:
    url = f"{SUPABASE_URL}/rest/v1/ba_buildings?select=id,address,city&id=gt.{last_id}&order=id.asc&limit=1000"
    req = Request(url, headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"})
    resp = json.loads(urlopen(req, timeout=30).read())
    if not resp:
        break
    buildings.extend(resp)
    last_id = resp[-1]["id"]
    if len(resp) < 1000:
        break
print(f"  Loaded {len(buildings)} buildings")

# Build address lookup: norm_addr+city -> building_id
addr_map = {}
for b in buildings:
    addr = b.get("address", "")
    city = (b.get("city", "") or "").upper().strip()
    key = norm_addr(addr) + "|" + city
    if key and key != "|":
        addr_map[key] = b["id"]
print(f"  Built address index with {len(addr_map)} entries")

# ====================================================
# Step 2: Ramsey County Socrata - Taxpayer Names
# ====================================================
print("\n--- RAMSEY COUNTY TAXPAYER ENRICHMENT ---")
ramsey_contacts = []
offset = 0
page_size = 5000
total_fetched = 0
skip_names = {"", "UNKNOWN", "UNASSIGNED", "N/A", "NONE"}

# Cities in Ramsey County
ramsey_cities = {"ST PAUL", "MAPLEWOOD", "ROSEVILLE", "ARDEN HILLS", "SHOREVIEW",
                 "WHITE BEAR LAKE", "NEW BRIGHTON", "MOUNDS VIEW", "NORTH ST PAUL",
                 "NORTH SAINT PAUL", "LITTLE CANADA", "VADNAIS HEIGHTS", "GEM LAKE",
                 "WHITE BEAR", "FALCON HEIGHTS", "LAUDERDALE", "ST ANTHONY",
                 "SAINT PAUL", "SAINT ANTHONY", "SPRING LAKE PARK", "BLAINE"}

while True:
    url = (f"https://opendata.ramseycountymn.gov/resource/dmvd-tktr.json?"
           f"$where=taxpayer_name IS NOT NULL AND tax_year='2019'"
           f"&$limit={page_size}&$offset={offset}"
           f"&$select=parcel_id,street_address,city,zip_code,taxpayer_name,legal_party_role")
    try:
        req = Request(url, headers={"Accept": "application/json"})
        resp = json.loads(urlopen(req, timeout=60).read())
    except Exception as e:
        print(f"  Error at offset {offset}: {e}")
        break

    if not resp:
        break

    for r in resp:
        name = (r.get("taxpayer_name", "") or "").strip()
        if not name or name.upper() in skip_names:
            continue
        addr = r.get("street_address", "")
        city = (r.get("city", "") or "").upper().strip()

        # Skip residential-sounding individual names for single-family
        # Keep LLC, INC, CORP, TRUST, CO, ASSOC, MGMT, MANAGEMENT, LP, LLP, etc.
        name_upper = name.upper()
        is_entity = any(kw in name_upper for kw in [
            "LLC", "INC", "CORP", "TRUST", "CO ", "ASSOC", "MGMT", "MANAGEMENT",
            " LP", "LLP", "LTD", "PARTNERS", "PROPERTIES", "REALTY", "HOUSING",
            "CHURCH", "SCHOOL", "CITY OF", "COUNTY", "STATE OF", "UNIVERSITY",
            "HOSPITAL", "CLINIC", "FOUNDATION", "INSTITUTE", "CENTER", "COMPANY"
        ])

        key = norm_addr(addr) + "|" + city
        bid = addr_map.get(key)

        if bid and is_entity:
            ramsey_contacts.append(norm_row({
                "building_id": bid,
                "name": name.title(),
                "role": (r.get("legal_party_role", "") or "Owner").title(),
                "phone": None,
                "email": None,
                "source": "ramsey-county-taxpayer",
                "notes": f"Parcel {r.get('parcel_id', '')}"
            }))

    total_fetched += len(resp)
    offset += page_size
    if len(resp) < page_size:
        break
    print(f"  Fetched {total_fetched} Ramsey records, {len(ramsey_contacts)} matched...")
    time.sleep(0.3)

print(f"  Total Ramsey records scanned: {total_fetched}")
print(f"  Matched entity taxpayers to buildings: {len(ramsey_contacts)}")

# Insert Ramsey contacts
ramsey_inserted = supabase_post(ramsey_contacts)
print(f"  Inserted: {ramsey_inserted}")

# ====================================================
# Step 3: St Paul Commercial C of O
# ====================================================
print("\n--- ST PAUL COMMERCIAL C of O ENRICHMENT ---")
cofo_contacts = []
cofo_offset = 0
total_cofo = 0

while True:
    url = (f"https://services1.arcgis.com/9meaaHE3uiba0zr8/arcgis/rest/services/"
           f"Certificate_of_Occupancy_-_Commercial/FeatureServer/1/query?"
           f"where=1%3D1&outFields=PROPNAME,ADDRESS,PRIMOCCTYPE,SUB_TYPE,STATUS,PIN,LATITUE,LONGITUTE"
           f"&f=json&resultRecordCount=2000&resultOffset={cofo_offset}")
    try:
        resp = json.loads(urlopen(Request(url), timeout=30).read())
    except Exception as e:
        print(f"  Error at offset {cofo_offset}: {e}")
        break

    features = resp.get("features", [])
    if not features:
        break

    for feat in features:
        a = feat.get("attributes", {})
        prop_name = (a.get("PROPNAME", "") or "").strip()
        address = (a.get("ADDRESS", "") or "").strip()
        occ_type = (a.get("PRIMOCCTYPE", "") or "").strip()
        sub_type = (a.get("SUB_TYPE", "") or "").strip()
        status = (a.get("STATUS", "") or "").strip()

        if not prop_name or not address:
            continue

        # Match to existing buildings by address
        key = norm_addr(address) + "|ST PAUL"
        bid = addr_map.get(key)

        if bid:
            role_parts = []
            if occ_type:
                role_parts.append(occ_type)
            if sub_type:
                role_parts.append(sub_type)

            cofo_contacts.append(norm_row({
                "building_id": bid,
                "name": prop_name.title(),
                "role": " / ".join(role_parts) if role_parts else "Commercial Occupant",
                "phone": None,
                "email": None,
                "source": "stpaul-cofo-commercial",
                "notes": f"Status: {status}, PIN: {a.get('PIN', '')}"
            }))

    total_cofo += len(features)
    cofo_offset += len(features)
    if not resp.get("exceededTransferLimit", False):
        break
    time.sleep(0.5)

print(f"  Total C of O records: {total_cofo}")
print(f"  Matched to buildings: {len(cofo_contacts)}")

cofo_inserted = supabase_post(cofo_contacts)
print(f"  Inserted: {cofo_inserted}")

# ====================================================
# Summary
# ====================================================
results = {
    "ramsey_county": {
        "records_scanned": total_fetched,
        "entity_matches": len(ramsey_contacts),
        "contacts_inserted": ramsey_inserted
    },
    "stpaul_cofo": {
        "records_fetched": total_cofo,
        "matches": len(cofo_contacts),
        "contacts_inserted": cofo_inserted
    }
}
with open("ramsey_stpaul_results.json", "w") as f:
    json.dump(results, f, indent=2)

print(f"\n{'='*60}")
print(f"ENRICHMENT COMPLETE")
print(f"  Ramsey taxpayers matched:  {len(ramsey_contacts):,}")
print(f"  St Paul C of O matched:   {len(cofo_contacts):,}")
print(f"  Total new contacts:        {len(ramsey_contacts) + len(cofo_contacts):,}")
print(f"  Inserted to Supabase:      {ramsey_inserted + cofo_inserted:,}")
