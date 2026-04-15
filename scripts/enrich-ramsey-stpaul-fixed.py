#!/usr/bin/env python3
"""
Enrich buildings with:
1. Ramsey County Socrata taxpayer names (167K parcels with owner info)
2. St Paul Commercial Certificate of Occupancy (4,111 commercial buildings)

Matches by normalized street address to existing buildings in Supabase.
Inserts owner/taxpayer as contacts, and C of O property name + occupancy type.
"""
import json, re, time, subprocess

SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg"

CON_KEYS = ["building_id", "name", "role", "phone", "email", "notes", "source"]

def norm_city(city):
    """Normalize city name to canonical uppercase form for matching"""
    if not city:
        return ""
    c = city.upper().strip()
    # Normalize Saint/St variants
    c = re.sub(r'\bST\b\.?\s+', 'SAINT ', c)
    c = re.sub(r'\bST\.\s*', 'SAINT ', c)
    # Handle "NORTH ST PAUL" -> "NORTH SAINT PAUL"
    c = re.sub(r'\s+', ' ', c).strip()
    return c

def norm_addr(addr):
    """Normalize address for matching"""
    if not addr:
        return ""
    a = addr.upper().strip()
    # Remove leading zeros like "0  " or "0 "
    a = re.sub(r'^0+\s+', '', a)
    # Collapse multiple spaces
    a = re.sub(r'\s+', ' ', a)
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

def curl_get(url, headers=None):
    """Use curl to fetch URL, avoids Python urllib issues with special chars"""
    cmd = ['curl', '-s', '--max-time', '60', url]
    if headers:
        for k, v in headers.items():
            cmd += ['-H', f'{k}: {v}']
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"curl failed: {result.stderr}")
    return json.loads(result.stdout)

def supabase_post(rows):
    if not rows: return 0
    total = 0
    for i in range(0, len(rows), 200):
        batch = rows[i:i+200]
        data = json.dumps(batch)
        cmd = [
            'curl', '-s', '-X', 'POST',
            f'{SUPABASE_URL}/rest/v1/ba_contacts',
            '-H', f'apikey: {SUPABASE_KEY}',
            '-H', f'Authorization: Bearer {SUPABASE_KEY}',
            '-H', 'Content-Type: application/json',
            '-H', 'Prefer: resolution=ignore-duplicates,return=minimal',
            '-d', data
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  Insert err: {result.stderr[:200]}")
        elif result.stdout and "error" in result.stdout.lower():
            err = result.stdout[:200]
            if "Empty or invalid" not in err:
                print(f"  Insert err: {err}")
        total += len(batch)
        time.sleep(0.2)
    return total

# ====================================================
# Step 1: Load existing buildings from Supabase
# ====================================================
print("Loading existing buildings from Supabase...")
buildings = []
offset = 0
while True:
    resp = curl_get(
        f"{SUPABASE_URL}/rest/v1/ba_buildings?select=id,address,city&limit=1000&offset={offset}",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    )
    if not resp:
        break
    buildings.extend(resp)
    offset += len(resp)
    if len(resp) < 1000:
        break
print(f"  Loaded {len(buildings)} buildings")

# Build address lookup: norm_addr+norm_city -> building_id
addr_map = {}
for b in buildings:
    addr = b.get("address", "")
    city = norm_city(b.get("city", "") or "")
    key = norm_addr(addr) + "|" + city
    if key and key != "|":
        addr_map[key] = b["id"]
print(f"  Built address index with {len(addr_map)} entries")

# Debug: show a few Saint Paul entries
saint_paul_keys = [k for k in addr_map if 'SAINT PAUL' in k][:5]
print(f"  Sample Saint Paul keys: {saint_paul_keys}")

# ====================================================
# Step 2: Ramsey County Socrata - Taxpayer Names
# ====================================================
print("\n--- RAMSEY COUNTY TAXPAYER ENRICHMENT ---")
ramsey_contacts = []
offset = 0
page_size = 5000
total_fetched = 0
skip_names = {"", "UNKNOWN", "UNASSIGNED", "N/A", "NONE"}

while True:
    url = (f"https://opendata.ramseycountymn.gov/resource/dmvd-tktr.json"
           f"?$where=tax_year=%272019%27"
           f"&$limit={page_size}&$offset={offset}"
           f"&$select=parcel_id,street_address,city,zip_code,taxpayer_name,legal_party_role")
    try:
        resp = curl_get(url, headers={"Accept": "application/json"})
    except Exception as e:
        print(f"  Error at offset {offset}: {e}")
        break

    if not resp:
        break

    for r in resp:
        name = (r.get("taxpayer_name", "") or "").strip()
        if not name or name.upper() in skip_names:
            continue
        addr = r.get("street_address", "") or ""
        city_raw = (r.get("city", "") or "").upper().strip()
        city = norm_city(city_raw)

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
    if offset % 25000 == 0:
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
        resp = curl_get(url)
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

        # Match to existing buildings by address - St Paul C of O is all in Saint Paul
        key = norm_addr(address) + "|SAINT PAUL"
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
