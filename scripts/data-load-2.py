#!/usr/bin/env python3
"""
Second data load pass — Ramsey County commercial parcels + HUD multifamily.
"""

import json
import time
import urllib.request
import urllib.parse
import ssl

SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg"

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fetch_json(url, headers=None):
    req = urllib.request.Request(url)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt == 2:
                print(f"  FAILED: {e}")
                return None
            time.sleep(2)

def supabase_headers(prefer=""):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    return h

def supabase_insert(table, rows):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = supabase_headers("return=minimal")
    total = len(rows)
    inserted = 0
    for i in range(0, total, 500):
        batch = rows[i:i+500]
        body = json.dumps(batch).encode()
        req = urllib.request.Request(url, data=body, method="POST")
        for k, v in headers.items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=120) as resp:
                inserted += len(batch)
                print(f"  Inserted {inserted}/{total} to {table}")
        except urllib.error.HTTPError as e:
            err = e.read().decode() if e.fp else ""
            print(f"  Error at batch {i//500}: {e.code} — {err[:300]}")
        time.sleep(0.3)
    return inserted

def normalize_address(addr):
    if not addr:
        return ""
    a = addr.upper().strip()
    for old, new in [(" STREET", " ST"), (" AVENUE", " AVE"), (" BOULEVARD", " BLVD"),
                     (" DRIVE", " DR"), (" ROAD", " RD"), (" LANE", " LN"),
                     (" COURT", " CT"), (" PLACE", " PL"), (" CIRCLE", " CIR"),
                     (" NORTH", " N"), (" SOUTH", " S"), (" EAST", " E"), (" WEST", " W"),
                     (" NORTHEAST", " NE"), (" NORTHWEST", " NW"),
                     (" SOUTHEAST", " SE"), (" SOUTHWEST", " SW"),
                     (".", ""), (",", "")]:
        a = a.replace(old, new)
    return a

# ─── Fetch existing buildings ───────────────────────────────────────────────
print("Fetching existing buildings for dedup...")
existing = []
offset = 0
while True:
    url = f"{SUPABASE_URL}/rest/v1/ba_buildings?select=id,address,city&limit=1000&offset={offset}"
    data = fetch_json(url, supabase_headers())
    if not data:
        break
    existing.extend(data)
    if len(data) < 1000:
        break
    offset += 1000

existing_addrs = set()
addr_to_id = {}
for b in existing:
    norm = normalize_address(b["address"])
    existing_addrs.add(norm)
    addr_to_id[norm] = b["id"]

print(f"  Existing: {len(existing)} buildings\n")

# ═══════════════════════════════════════════════════════════════════════════════
# 1. HUD MULTIFAMILY — Twin Cities metro
# ═══════════════════════════════════════════════════════════════════════════════
print("="*70)
print("HUD MULTIFAMILY — Twin Cities Metro")
print("="*70)

# Use bbox for Twin Cities (in Web Mercator)
# Twin Cities metro: lat 44.7-45.3, lng -93.8 to -92.8
# Web Mercator approx: x -10441000 to -10330000, y 5584000 to 5665000

hud_url = "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/Multifamily_Properties_Assisted/FeatureServer/0/query"

all_hud = []
offset = 0
while True:
    params = urllib.parse.urlencode({
        "geometry": "-10441000,5584000,-10330000,5665000",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "102100",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "PROPERTY_NAME_TEXT,STD_ADDR,STD_CITY,STD_ZIP5,TOTAL_UNIT_COUNT,TOTAL_ASSISTED_UNIT_COUNT,PROPERTY_CATEGORY_NAME,MGMT_AGENT_ORG_NAME,MGMT_CONTACT_FULL_NAME,MGMT_CONTACT_MAIN_PHN_NBR,MGMT_CONTACT_EMAIL_TEXT,PROPERTY_ON_SITE_PHONE_NUMBER,STATE2KX",
        "outSR": "4326",
        "returnGeometry": "true",
        "resultRecordCount": 2000,
        "resultOffset": offset,
        "f": "json",
    })
    print(f"  Fetching offset {offset}...")
    data = fetch_json(f"{hud_url}?{params}")
    if not data or "features" not in data:
        print(f"  Error or no data: {json.dumps(data or {})[:300]}")
        break
    features = data["features"]
    if not features:
        break
    all_hud.extend(features)
    print(f"  Got {len(features)} (total: {len(all_hud)})")
    if len(features) < 2000:
        break
    offset += 2000
    time.sleep(0.5)

print(f"  Total HUD metro features: {len(all_hud)}")

hud_buildings = []
hud_contacts = []
seen_new = set()

for f in all_hud:
    a = f.get("attributes", {})
    g = f.get("geometry", {})
    addr = (a.get("STD_ADDR") or "").strip()
    city = (a.get("STD_CITY") or "").strip()
    if not addr or not g.get("y") or not g.get("x"):
        continue
    
    norm = normalize_address(addr)
    if norm in existing_addrs or norm in seen_new:
        continue
    seen_new.add(norm)
    
    bldg = {
        "name": a.get("PROPERTY_NAME_TEXT"),
        "address": addr,
        "city": city,
        "state": "MN",
        "zip": a.get("STD_ZIP5"),
        "lat": round(float(g["y"]), 6),
        "lng": round(float(g["x"]), 6),
        "building_type": "apartment",
        "unit_count": a.get("TOTAL_UNIT_COUNT"),
        "data_source": "hud",
        "source_id": f"hud-{a.get('PROPERTY_NAME_TEXT', '')[:30]}",
    }
    hud_buildings.append(bldg)
    
    # Management contact
    mgmt = a.get("MGMT_CONTACT_FULL_NAME") or a.get("MGMT_AGENT_ORG_NAME")
    if mgmt:
        hud_contacts.append({
            "_addr": addr,
            "role": "property-manager",
            "name": mgmt,
            "phone": a.get("MGMT_CONTACT_MAIN_PHN_NBR"),
            "email": a.get("MGMT_CONTACT_EMAIL_TEXT"),
            "source": "hud",
        })
    
    # On-site phone
    onsite_phone = a.get("PROPERTY_ON_SITE_PHONE_NUMBER")
    if onsite_phone and onsite_phone != (a.get("MGMT_CONTACT_MAIN_PHN_NBR") or ""):
        hud_contacts.append({
            "_addr": addr,
            "role": "leasing-office",
            "name": a.get("PROPERTY_NAME_TEXT"),
            "phone": onsite_phone,
            "source": "hud",
        })

print(f"  New HUD buildings: {len(hud_buildings)}")
print(f"  New HUD contacts: {len(hud_contacts)}")

if hud_buildings:
    inserted = supabase_insert("ba_buildings", hud_buildings)
    print(f"  Inserted: {inserted}")
    
    # Refresh addr_to_id
    time.sleep(1)
    offset = 0
    updated = []
    while True:
        url = f"{SUPABASE_URL}/rest/v1/ba_buildings?select=id,address&limit=1000&offset={offset}"
        data = fetch_json(url, supabase_headers())
        if not data:
            break
        updated.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    for b in updated:
        addr_to_id[normalize_address(b["address"])] = b["id"]
        existing_addrs.add(normalize_address(b["address"]))

# Insert HUD contacts
hud_contact_rows = []
for c in hud_contacts:
    addr = c.pop("_addr", "")
    bid = addr_to_id.get(normalize_address(addr))
    if not bid:
        continue
    hud_contact_rows.append({
        "building_id": bid,
        "role": c["role"],
        "name": c.get("name"),
        "phone": c.get("phone"),
        "email": c.get("email"),
        "source": "hud",
        "confidence": "verified",
        "verified_count": 1,
        "contributed_by": "data-import",
        "created_at": "2026-04-14T00:00:00Z",
    })

if hud_contact_rows:
    print(f"\n  Inserting {len(hud_contact_rows)} HUD contacts...")
    supabase_insert("ba_contacts", hud_contact_rows)


# ═══════════════════════════════════════════════════════════════════════════════
# 2. RAMSEY COUNTY COMMERCIAL PARCELS — get addresses with lat/lng
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("RAMSEY COUNTY COMMERCIAL PARCELS")
print("="*70)

ramsey_url = "https://services2.arcgis.com/hQZvdtFxRzJpMtdS/arcgis/rest/services/Parcel_Commercial_Characteristics/FeatureServer/8/query"

# Check what fields are available
print("  Checking fields...")
data = fetch_json(f"https://services2.arcgis.com/hQZvdtFxRzJpMtdS/arcgis/rest/services/Parcel_Commercial_Characteristics/FeatureServer/8?f=json")
if data and 'fields' in data:
    useful_fields = []
    for f in data['fields']:
        name = f['name'].lower()
        if any(k in name for k in ['addr', 'city', 'zip', 'owner', 'name', 'unit', 'year', 'type', 'use', 'class', 'sqft', 'area', 'story', 'build']):
            useful_fields.append(f['name'])
            print(f"  Field: {f['name']} ({f['type']})")
    
    # Fetch a sample
    params = urllib.parse.urlencode({
        "where": "1=1",
        "outFields": ",".join(useful_fields[:15]),
        "returnGeometry": "true",
        "outSR": "4326",
        "resultRecordCount": 3,
        "f": "json",
    })
    sample = fetch_json(f"{ramsey_url}?{params}")
    if sample and 'features' in sample:
        print("\n  Sample record:")
        for k, v in sample['features'][0]['attributes'].items():
            if v is not None:
                print(f"    {k}: {v}")
        g = sample['features'][0].get('geometry', {})
        if g:
            print(f"    GEOMETRY: lat={g.get('y')}, lng={g.get('x')}")
else:
    print("  Could not read layer metadata")

# Now fetch ALL Ramsey commercial parcels
print("\n  Fetching all Ramsey commercial parcels...")
# Get the fields we need
out_fields = "HOUSE_NO,STREET_NAME,STREET_DIR,STREET_TYPE,CITY_NAME,ZIP,OWNER_NAME,YEAR_BUILT,NUM_OF_UNITS,TOTAL_BLDG_AREA,CLASS_DESCRIPTION"

all_ramsey = []
offset = 0
while True:
    params = urllib.parse.urlencode({
        "where": "1=1",
        "outFields": out_fields,
        "returnGeometry": "true",
        "outSR": "4326",
        "resultRecordCount": 2000,
        "resultOffset": offset,
        "f": "json",
    })
    print(f"  Fetching offset {offset}...")
    data = fetch_json(f"{ramsey_url}?{params}")
    if not data or "features" not in data:
        if data and 'error' in data:
            print(f"  Error: {data['error']}")
        break
    features = data["features"]
    if not features:
        break
    all_ramsey.extend(features)
    print(f"  Got {len(features)} (total: {len(all_ramsey)})")
    if len(features) < 2000:
        break
    offset += 2000
    time.sleep(0.5)

print(f"\n  Total Ramsey parcels fetched: {len(all_ramsey)}")

# Filter to multi-unit or commercial — skip single-family residential
ramsey_buildings = []
ramsey_seen = set()

for f in all_ramsey:
    a = f.get("attributes", {})
    g = f.get("geometry", {})
    
    if not g or not g.get("y") or not g.get("x"):
        continue
    
    # Build address
    house = str(a.get("HOUSE_NO") or "").strip()
    street = (a.get("STREET_NAME") or "").strip()
    sdir = (a.get("STREET_DIR") or "").strip()
    stype = (a.get("STREET_TYPE") or "").strip()
    
    if not house or not street:
        continue
    
    addr_parts = [house]
    if sdir:
        addr_parts.append(sdir)
    addr_parts.append(street)
    if stype:
        addr_parts.append(stype)
    addr = " ".join(addr_parts)
    
    city = (a.get("CITY_NAME") or "Saint Paul").strip()
    
    norm = normalize_address(addr)
    if norm in existing_addrs or norm in ramsey_seen:
        continue
    ramsey_seen.add(norm)
    
    units = a.get("NUM_OF_UNITS")
    desc = (a.get("CLASS_DESCRIPTION") or "").lower()
    year = a.get("YEAR_BUILT")
    sqft = a.get("TOTAL_BLDG_AREA")
    
    # Classify type
    if units and units >= 3:
        btype = "apartment"
    elif "office" in desc:
        btype = "office"
    elif "retail" in desc or "store" in desc:
        btype = "retail"
    elif "industrial" in desc or "warehouse" in desc:
        btype = "industrial"
    elif "hospital" in desc or "medical" in desc:
        btype = "hospital"
    elif "school" in desc or "education" in desc:
        btype = "school"
    elif "government" in desc or "public" in desc:
        btype = "government"
    else:
        btype = "commercial"
    
    bldg = {
        "address": addr,
        "city": city,
        "state": "MN",
        "zip": a.get("ZIP"),
        "lat": round(float(g["y"]), 6),
        "lng": round(float(g["x"]), 6),
        "building_type": btype,
        "unit_count": units if units and units > 1 else None,
        "year_built": int(year) if year and year > 1800 else None,
        "sqft": int(sqft) if sqft and sqft > 0 else None,
        "owner_name": a.get("OWNER_NAME"),
        "data_source": "ramsey-gis",
    }
    ramsey_buildings.append(bldg)

print(f"  New Ramsey buildings to insert: {len(ramsey_buildings)}")

if ramsey_buildings:
    inserted = supabase_insert("ba_buildings", ramsey_buildings)
    print(f"  Inserted: {inserted}")
    
    # Add owner contacts for Ramsey buildings
    time.sleep(1)
    offset = 0
    updated = []
    while True:
        url = f"{SUPABASE_URL}/rest/v1/ba_buildings?select=id,address,owner_name&data_source=eq.ramsey-gis&limit=1000&offset={offset}"
        data = fetch_json(url, supabase_headers())
        if not data:
            break
        updated.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    
    ramsey_contacts = []
    for b in updated:
        if b.get("owner_name"):
            ramsey_contacts.append({
                "building_id": b["id"],
                "role": "owner",
                "name": b["owner_name"],
                "source": "ramsey-gis",
                "confidence": "unverified",
                "verified_count": 0,
                "contributed_by": "data-import",
                "created_at": "2026-04-14T00:00:00Z",
            })
    
    if ramsey_contacts:
        print(f"\n  Inserting {len(ramsey_contacts)} Ramsey owner contacts...")
        supabase_insert("ba_contacts", ramsey_contacts)


# ═══════════════════════════════════════════════════════════════════════════════
# FINAL COUNTS
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("FINAL COUNTS")
print("="*70)

for table in ["ba_buildings", "ba_contacts", "ba_building_notes"]:
    url = f"{SUPABASE_URL}/rest/v1/{table}?select=id&limit=0"
    req = urllib.request.Request(url)
    for k, v in supabase_headers("count=exact").items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            cr = resp.getheader("content-range")
            count = cr.split("/")[-1] if cr else "?"
            print(f"  {table}: {count}")
    except Exception as e:
        print(f"  {table}: error — {e}")

print("\nDone!")
