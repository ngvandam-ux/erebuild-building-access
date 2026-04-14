#!/usr/bin/env python3
"""
Data Load 4 — MetroGIS Regional Parcel Data (6 counties)

Loads non-residential parcels from Anoka, Carver, Dakota, Ramsey, Scott, 
and Washington counties via the MetroGIS unified parcel service.

Source: https://arcgis.metc.state.mn.us/data1/rest/services/parcels/Parcel_Points/FeatureServer
Coordinate system: EPSG:26915 (UTM Zone 15N) — request outSR=4326 for direct lat/lng
"""

import json
import time
import urllib.request
import urllib.parse
import urllib.error
import ssl
import sys
from collections import defaultdict

# ─── Config ──────────────────────────────────────────────────────────────────
SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg"

METRO_BASE = "https://arcgis.metc.state.mn.us/data1/rest/services/parcels/Parcel_Points/FeatureServer"

# County layers: {layer_id: (county_name, use_class_filter)}
# Most counties use text USECLASS1 like "3a COMMERCIAL PREFERENTIAL"  
# Washington uses numeric codes (300=commercial, 400=apartment)
COUNTIES = {
    0: ("Anoka", "text"),
    1: ("Carver", "text"),
    2: ("Dakota", "text"),
    4: ("Ramsey", "text"),
    5: ("Scott", "text"),
    6: ("Washington", "numeric"),
}

# Text-based filter (Anoka, Carver, Dakota, Ramsey, Scott)
TEXT_WHERE = "(USECLASS1 LIKE '%COMMERCIAL%' OR USECLASS1 LIKE '%APARTMENT%' OR USECLASS1 LIKE '%INDUSTRIAL%')"

# Numeric filter for Washington (300s = commercial/industrial, 400s = apartment, 500s = utility)
NUMERIC_WHERE = "(USECLASS1 LIKE '3%' OR USECLASS1 LIKE '4%' OR USECLASS1 LIKE '5%')"

OUT_FIELDS = "ANUMBER,ST_PRE_DIR,ST_NAME,ST_POS_TYP,ST_POS_DIR,ZIP,CTU_NAME,CO_NAME,OWNER_NAME,YEAR_BUILT,NUM_UNITS,EMV_TOTAL,FIN_SQ_FT,USECLASS1,DWELL_TYPE,HOMESTEAD,TAX_NAME"

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def fetch_json(url, timeout=60):
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Mozilla/5.0")
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt == 2:
                print(f"  FAILED: {e}")
                return None
            time.sleep(2)


def normalize_address(addr):
    if not addr: return ""
    a = addr.upper().strip()
    for old, new in [(" STREET", " ST"), (" AVENUE", " AVE"), (" BOULEVARD", " BLVD"),
                     (" DRIVE", " DR"), (" ROAD", " RD"), (" LANE", " LN"),
                     (" COURT", " CT"), (" PLACE", " PL"), (" CIRCLE", " CIR"),
                     (" NORTH", " N"), (" SOUTH", " S"), (" EAST", " E"), (" WEST", " W"),
                     (".", ""), (",", "")]:
        a = a.replace(old, new)
    return a


def map_building_type(useclass, dwell_type=""):
    """Map MetroGIS USECLASS1 to our building_type."""
    uc = (useclass or "").upper()
    dt = (dwell_type or "").upper()
    
    if "APARTMENT" in uc or "APARTMENT" in dt:
        return "apartment"
    elif "COMMERCIAL" in uc or uc.startswith("3"):
        return "commercial"
    elif "INDUSTRIAL" in uc:
        return "industrial"
    elif uc.startswith("4"):
        return "apartment"
    elif uc.startswith("5"):
        return "utility"
    elif "DUPLEX" in dt or "TRIPLEX" in dt or "TOWNHOUSE" in dt:
        return "apartment"
    return "commercial"


def build_address(attrs):
    """Build a formatted address from MetroGIS fields."""
    parts = []
    num = attrs.get("ANUMBER")
    if num:
        parts.append(str(num))
    pre_dir = attrs.get("ST_PRE_DIR")
    if pre_dir:
        parts.append(str(pre_dir))
    name = attrs.get("ST_NAME")
    if name:
        parts.append(str(name))
    post_type = attrs.get("ST_POS_TYP")
    if post_type:
        parts.append(str(post_type))
    post_dir = attrs.get("ST_POS_DIR")
    if post_dir:
        parts.append(str(post_dir))
    return " ".join(p.strip() for p in parts if p and str(p).strip())


def supabase_headers(prefer=""):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}
    if prefer: h["Prefer"] = prefer
    return h


def supabase_insert(table, rows):
    """Insert rows in batches of 500."""
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
                if inserted % 2000 == 0 or inserted >= total:
                    print(f"    Inserted {inserted}/{total}")
        except urllib.error.HTTPError as e:
            err = e.read().decode() if e.fp else ""
            print(f"    Batch error at {i}: {e.code} — {err[:200]}")
        time.sleep(0.2)
    
    return inserted


# ═══════════════════════════════════════════════════════════════════════════════
# FETCH DATA
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_county(layer_id, county_name, filter_type):
    """Fetch all non-residential parcels from a county."""
    where = TEXT_WHERE if filter_type == "text" else NUMERIC_WHERE
    
    all_features = []
    offset = 0
    
    while True:
        params = urllib.parse.urlencode({
            "where": where,
            "outFields": OUT_FIELDS,
            "resultRecordCount": 2000,
            "resultOffset": offset,
            "orderByFields": "OBJECTID",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "json",
        })
        url = f"{METRO_BASE}/{layer_id}/query?{params}"
        data = fetch_json(url)
        
        if not data or "features" not in data or not data["features"]:
            break
        
        features = data["features"]
        all_features.extend(features)
        
        if len(all_features) % 5000 < 2000:
            print(f"    {county_name}: {len(all_features)} records...")
        
        if len(features) < 2000:
            break
        offset += 2000
        time.sleep(0.3)
    
    print(f"  {county_name}: {len(all_features)} total")
    return all_features


def process_county(features, county_name):
    """Convert features to building rows."""
    buildings = []
    skip_addr = skip_coord = 0
    
    for feat in features:
        a = feat.get("attributes", {})
        g = feat.get("geometry", {})
        
        addr = build_address(a)
        if not addr:
            skip_addr += 1
            continue
        
        lat = g.get("y")
        lng = g.get("x")
        if not lat or not lng:
            skip_coord += 1
            continue
        
        # Sanity check
        if not (44.0 < lat < 46.0 and -95.0 < lng < -92.0):
            skip_coord += 1
            continue
        
        useclass = a.get("USECLASS1") or ""
        dwell = a.get("DWELL_TYPE") or ""
        bt = map_building_type(useclass, dwell)
        
        city = (a.get("CTU_NAME") or "").strip()
        zip_code = str(a.get("ZIP") or "").strip()
        if zip_code and len(zip_code) < 5:
            zip_code = zip_code.zfill(5)
        
        year_built = a.get("YEAR_BUILT")
        if not year_built or year_built == 0:
            year_built = None
        
        num_units = a.get("NUM_UNITS")
        if not num_units or num_units == 0:
            num_units = None
        
        emv = a.get("EMV_TOTAL")
        if not emv or emv == 0:
            emv = None
        
        sqft = a.get("FIN_SQ_FT")
        if not sqft or sqft == 0:
            sqft = None
        
        county = a.get("CO_NAME") or county_name
        
        buildings.append({
            "address": addr,
            "city": city,
            "state": "MN",
            "zip": zip_code or None,
            "lat": round(lat, 6),
            "lng": round(lng, 6),
            "building_type": bt,
            "unit_count": num_units,
            "owner_name": (a.get("OWNER_NAME") or "").strip() or None,
            "taxpayer_name": (a.get("TAX_NAME") or "").strip() or None,
            "data_source": f"metrogis-{county.lower().replace(' ', '-')}",
            "source_id": f"metro-{county_name.lower()}-{addr.replace(' ', '-')}",
            "year_built": year_built,
            "estimated_value": emv,
            "sqft": sqft,
        })
    
    return buildings, skip_addr, skip_coord


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("="*70)
    print("Data Load 4 — MetroGIS Regional Parcels (6 counties)")
    print("="*70)
    
    # 1. Fetch all county data
    all_buildings = []
    type_counts = defaultdict(int)
    county_counts = defaultdict(int)
    
    for layer_id, (county_name, filter_type) in COUNTIES.items():
        print(f"\n  Fetching {county_name}...")
        features = fetch_county(layer_id, county_name, filter_type)
        buildings, skip_a, skip_c = process_county(features, county_name)
        print(f"    Processed: {len(buildings)} (skipped: {skip_a} no addr, {skip_c} no coords)")
        
        for b in buildings:
            type_counts[b["building_type"]] += 1
            county_counts[b["city"]] += 1
        
        all_buildings.extend(buildings)
    
    print(f"\n  Total processed: {len(all_buildings)}")
    print(f"\n  By type:")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"    {t}: {c}")
    
    # 2. Fetch existing for dedup
    print(f"\n{'='*70}")
    print("FETCHING EXISTING DATA FOR DEDUP")
    print("="*70)
    
    existing = []
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/ba_buildings?select=id,address,city,lat,lng&limit=1000&offset={offset}"
        req = urllib.request.Request(url)
        for k, v in supabase_headers().items():
            req.add_header(k, v)
        with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
            data = json.loads(resp.read())
        if not data: break
        existing.extend(data)
        if len(data) < 1000: break
        offset += 1000
    
    print(f"  Existing buildings: {len(existing)}")
    
    # 3. Deduplicate
    print(f"\n{'='*70}")
    print("DEDUPLICATION")
    print("="*70)
    
    existing_addrs = set()
    existing_latlng = set()
    for b in existing:
        existing_addrs.add(normalize_address(b.get("address", "")))
        lat, lng = b.get("lat"), b.get("lng")
        if lat and lng:
            existing_latlng.add(f"{round(float(lat),4)}_{round(float(lng),4)}")
    
    seen = set()
    new_buildings = []
    skip_existing = skip_dup = 0
    
    for b in all_buildings:
        norm = normalize_address(b["address"])
        if norm in existing_addrs:
            skip_existing += 1; continue
        if norm in seen:
            skip_dup += 1; continue
        lk = f"{round(b['lat'],4)}_{round(b['lng'],4)}"
        if lk in existing_latlng:
            skip_existing += 1; continue
        seen.add(norm)
        existing_latlng.add(lk)  # prevent self-proximity dupes too
        new_buildings.append(b)
    
    print(f"  New after dedup: {len(new_buildings)}")
    print(f"  Skipped (existing): {skip_existing}, (self-dup): {skip_dup}")
    
    # 4. Insert
    print(f"\n{'='*70}")
    print("INSERTING")
    print("="*70)
    
    if new_buildings:
        print(f"  Inserting {len(new_buildings)} buildings...")
        inserted = supabase_insert("ba_buildings", new_buildings)
        print(f"  Total inserted: {inserted}")
    else:
        print("  No new buildings to insert")
    
    # 5. Final counts
    print(f"\n{'='*70}")
    print("FINAL COUNTS")
    print("="*70)
    
    for table in ["ba_buildings", "ba_contacts"]:
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
    
    # City breakdown
    all_b = []
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/ba_buildings?select=city&limit=1000&offset={offset}"
        req = urllib.request.Request(url)
        for k, v in supabase_headers().items(): req.add_header(k, v)
        with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
            data = json.loads(resp.read())
        if not data: break
        all_b.extend(data)
        if len(data) < 1000: break
        offset += 1000
    
    cc = defaultdict(int)
    for b in all_b:
        cc[b.get("city", "?")] += 1
    
    print(f"\n  Top 25 cities (of {len(cc)}):")
    for city, c in sorted(cc.items(), key=lambda x: -x[1])[:25]:
        print(f"    {city}: {c}")
    
    print("\nDone!")


if __name__ == "__main__":
    main()
