#!/usr/bin/env python3
"""
Data Load 3 — Hennepin County Assessing Parcel Data 2025

Loads all non-residential parcels from Hennepin County into the Building Access
Supabase DB. Includes apartments, commercial, industrial, mixed use, medical,
fraternity, multi-unit residential, and vacant commercial/industrial land.

Key challenges:
- X/Y coords are Minnesota State Plane South (EPSG:26915) — need pyproj or manual conversion
- ~27K records total — paginate in batches of 2000
- Deduplicate against existing 5,837 buildings
"""

import json
import math
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

HENNEPIN_BASE = "https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/Assessing_Department_Parcel_Data_2025/FeatureServer/0/query"

# Property types we want (everything non-single-family-residential)
TARGET_TYPES = [
    "APARTMENT",
    "COMMERCIAL",
    "COMMERCIAL RAILROAD",
    "INDUSTRIAL",
    "MEDICAL CARE FACILITY",
    "FRATERNITY/SORORITY HOUSING",
    "MIXED USE - COMMERCIAL IND/APARTMENT",
    "MIXED USE - COMMERCIAL IND/RES 1 UNIT",
    "MIXED USE - COMMERCIAL IND/RES 2 UNITS",
    "MIXED USE - COMMERCIAL IND/RES 3 UNITS",
    "RESIDENTIAL 2 UNITS",
    "RESIDENTIAL 3 UNITS",
    "UTILITY",
    "VACANT LAND - APARTMENT",
    "VACANT LAND - COMMERCIAL",
    "VACANT LAND - INDUSTRIAL",
]

# SSL context
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


# ─── Hennepin County Coords to Lat/Lng conversion ─────────────────────────────
# Hennepin County uses NAD 1983 HARN Adj MN Hennepin Feet (ESRI:103734)
# We use pyproj for accurate conversion to WGS84 lat/lng

from pyproj import Transformer

# Initialize transformer once (thread-safe, reusable)
_transformer = Transformer.from_crs("ESRI:103734", "EPSG:4326", always_xy=True)

def hennepin_to_latlon(x, y):
    """
    Convert Hennepin County coordinates (ESRI:103734, US Survey Feet) to WGS84 lat/lng.
    X = Easting, Y = Northing in US Survey Feet
    """
    lng, lat = _transformer.transform(float(x), float(y))
    return round(lat, 6), round(lng, 6)


def fetch_json(url, headers=None, timeout=60):
    """Fetch JSON from URL with retries."""
    req = urllib.request.Request(url)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt == 2:
                print(f"  FAILED after 3 attempts: {e}")
                return None
            print(f"  Retry {attempt+1}: {e}")
            time.sleep(2)


def supabase_headers(prefer=""):
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h


def supabase_insert(table, rows):
    """Insert rows to Supabase in batches of 500."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = supabase_headers("return=minimal")
    
    total = len(rows)
    batch_size = 500
    inserted = 0
    
    for i in range(0, total, batch_size):
        batch = rows[i:i+batch_size]
        body = json.dumps(batch).encode()
        req = urllib.request.Request(url, data=body, method="POST")
        for k, v in headers.items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=120) as resp:
                inserted += len(batch)
                if inserted % 2000 == 0 or inserted == total:
                    print(f"  Inserted {inserted}/{total} to {table}")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode() if e.fp else ""
            print(f"  Insert error at batch {i//batch_size}: {e.code} — {err_body[:300]}")
        time.sleep(0.2)
    
    return inserted


def supabase_patch(table, rows, match_col):
    """Update existing rows by matching on a column, in batches."""
    headers = supabase_headers("return=minimal")
    updated = 0
    
    for row in rows:
        match_val = row.pop(match_col)
        if not row:  # nothing to update
            continue
        url = f"{SUPABASE_URL}/rest/v1/{table}?{match_col}=eq.{urllib.parse.quote(str(match_val))}"
        body = json.dumps(row).encode()
        req = urllib.request.Request(url, data=body, method="PATCH")
        for k, v in headers.items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
                updated += 1
        except Exception as e:
            pass  # skip individual failures
        
        if updated % 500 == 0 and updated > 0:
            print(f"  Updated {updated} rows...")
            time.sleep(0.5)
    
    return updated


def normalize_address(addr):
    """Normalize address for dedup comparison."""
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


def map_building_type(property_type):
    """Map Hennepin PROPERTYTYPE to our building_type categories."""
    pt = (property_type or "").upper()
    if "APARTMENT" in pt:
        return "apartment"
    elif "COMMERCIAL" in pt:
        return "commercial"
    elif "INDUSTRIAL" in pt:
        return "industrial"
    elif "MIXED USE" in pt:
        return "mixed-use"
    elif "MEDICAL" in pt:
        return "medical"
    elif "FRATERNITY" in pt:
        return "apartment"  # student housing = apartment for access purposes
    elif "UTILITY" in pt:
        return "utility"
    elif "RESIDENTIAL 2 UNITS" in pt or "RESIDENTIAL 3 UNITS" in pt:
        return "apartment"
    elif "VACANT LAND" in pt:
        return "vacant-land"
    return "commercial"


def map_city_from_neighborhood(neighborhood, community):
    """Map Hennepin neighborhood/community to city. Most are Minneapolis but some are suburbs."""
    # The Hennepin parcels all have MUNI code — we'll use that
    return "Minneapolis"  # default, we'll override with MUNI lookup


# Hennepin County MUNI codes to city names
MUNI_CODES = {
    "01": "Bloomington", "02": "Brooklyn Center", "03": "Brooklyn Park",
    "04": "Champlin", "05": "Crystal", "06": "Dayton", "07": "Deephaven",
    "08": "Eden Prairie", "09": "Edina", "10": "Excelsior", "11": "Golden Valley",
    "12": "Greenfield", "13": "Greenwood", "14": "Hanover", "15": "Hassan",
    "16": "Hopkins", "17": "Independence", "18": "Long Lake", "19": "Loretto",
    "20": "Maple Grove", "21": "Maple Plain", "22": "Medicine Lake",
    "23": "Medina", "24": "Minnetonka", "25": "Minneapolis", "26": "Minnetonka Beach",
    "27": "Minnetrista", "28": "Mound", "29": "New Hope", "30": "Orono",
    "31": "Osseo", "32": "Plymouth", "33": "Richfield", "34": "Robbinsdale",
    "35": "Rogers", "36": "Shorewood", "37": "Spring Park", "38": "St. Anthony",
    "39": "St. Bonifacius", "40": "St. Louis Park", "41": "Tonka Bay",
    "42": "Wayzata", "43": "Woodland", "44": "Corcoran", "45": "Fort Snelling",
}


# ═══════════════════════════════════════════════════════════════════════════════
# FETCH HENNEPIN DATA
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_hennepin_parcels():
    """Fetch all non-residential parcels from Hennepin County."""
    print("\n" + "="*70)
    print("1. HENNEPIN COUNTY ASSESSING PARCELS 2025")
    print("="*70)
    
    # Build WHERE clause for all target types
    type_conditions = " OR ".join([f"PROPERTYTYPE='{t}'" for t in TARGET_TYPES])
    where = f"({type_conditions})"
    
    all_features = []
    offset = 0
    batch_size = 1000  # ArcGIS server cap
    
    while True:
        params = urllib.parse.urlencode({
            "where": where,
            "outFields": "ADDRESSFORMATTED,OWNERNAME,PROPERTYTYPE,BUILDINGUSE,YEARBUILT,TOTALUNITS,TOTALVALUE,ABOVEGROUNDAREA,STORIES,NUMBEROFBUILDINGS,CONSTRUCTIONTYPE,PRIMARYHEATING,NEIGHBORHOOD,COMMUNITY,MUNI,ZIP1,X,Y,TOTALBEDROOMS,TOTALBATHROOMS,HOUSE_NO,ADRSTR,UNITNO,LANDVALUE,BUILDINGVALUE,ISEXEMPT,HOMESTEAD",
            "resultRecordCount": batch_size,
            "resultOffset": offset,
            "orderByFields": "ObjectId",
            "f": "json",
        })
        url = f"{HENNEPIN_BASE}?{params}"
        print(f"  Fetching offset {offset}...", end=" ", flush=True)
        data = fetch_json(url)
        
        if not data or "features" not in data:
            print(f"No features at offset {offset}")
            break
        
        features = data["features"]
        if not features:
            print("empty")
            break
        
        all_features.extend(features)
        print(f"got {len(features)} (total: {len(all_features)})")
        
        if len(features) < batch_size:
            break
        offset += batch_size
        time.sleep(0.3)
    
    print(f"\n  Total Hennepin non-residential parcels fetched: {len(all_features)}")
    return all_features


def process_hennepin(features):
    """Convert Hennepin features to building rows."""
    buildings = []
    skipped_no_addr = 0
    skipped_no_coords = 0
    coord_errors = 0
    
    type_counts = defaultdict(int)
    city_counts = defaultdict(int)
    
    for f in features:
        a = f.get("attributes", {})
        
        addr = (a.get("ADDRESSFORMATTED") or "").strip()
        if not addr:
            skipped_no_addr += 1
            continue
        
        # Convert state plane to lat/lng
        x = a.get("X")
        y = a.get("Y")
        if not x or not y or x == 0 or y == 0:
            skipped_no_coords += 1
            continue
        
        try:
            lat, lng = hennepin_to_latlon(float(x), float(y))
            # Sanity check — should be roughly in Twin Cities metro
            if not (44.5 < lat < 45.5 and -94.5 < lng < -92.5):
                coord_errors += 1
                continue
        except Exception:
            coord_errors += 1
            continue
        
        property_type = a.get("PROPERTYTYPE") or ""
        building_type = map_building_type(property_type)
        
        # City from MUNI code
        muni = str(a.get("MUNI") or "25").zfill(2)
        city = MUNI_CODES.get(muni, "Minneapolis")
        
        zip_code = str(a.get("ZIP1") or "").strip()
        if zip_code and len(zip_code) < 5:
            zip_code = zip_code.zfill(5)
        
        total_units = a.get("TOTALUNITS") or None
        if total_units == 0:
            total_units = None
        
        year_built = a.get("YEARBUILT") or None
        if year_built == 0:
            year_built = None
        
        # Map to existing DB columns:
        # total_value -> estimated_value, building_area -> sqft
        estimated_value = a.get("TOTALVALUE") or None
        if estimated_value == 0:
            estimated_value = None
        
        sqft = a.get("ABOVEGROUNDAREA") or None
        if sqft == 0:
            sqft = None
        
        building = {
            "address": addr,
            "city": city,
            "state": "MN",
            "zip": zip_code or None,
            "lat": lat,
            "lng": lng,
            "building_type": building_type,
            "unit_count": total_units,
            "owner_name": a.get("OWNERNAME") or None,
            "owner_address": None,
            "taxpayer_name": None,
            "data_source": "hennepin-assessor",
            "source_id": f"hennepin-{a.get('HOUSE_NO','')}-{(a.get('ADRSTR') or '').strip()}",
            "year_built": year_built,
            "estimated_value": estimated_value,
            "sqft": sqft,
        }
        
        buildings.append(building)
        type_counts[building_type] += 1
        city_counts[city] += 1
    
    print(f"\n  Processed: {len(buildings)} buildings")
    print(f"  Skipped (no address): {skipped_no_addr}")
    print(f"  Skipped (no coords): {skipped_no_coords}")
    print(f"  Skipped (coord errors): {coord_errors}")
    print(f"\n  By building type:")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"    {t}: {c}")
    print(f"\n  By city (top 15):")
    for city, c in sorted(city_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"    {city}: {c}")
    
    return buildings


# ═══════════════════════════════════════════════════════════════════════════════
# CHECK IF DB HAS NEW COLUMNS
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_columns_exist():
    """
    The ba_buildings table may not have year_built, total_value, building_area, 
    stories, property_subtype, neighborhood columns yet. We need to add them.
    
    Try inserting a test row — if it fails on unknown columns, we'll strip those fields.
    """
    print("\n  Checking which columns exist in ba_buildings...")
    
    # Try fetching one row to see what columns exist
    url = f"{SUPABASE_URL}/rest/v1/ba_buildings?select=*&limit=1"
    data = fetch_json(url, supabase_headers())
    
    if data and len(data) > 0:
        existing_cols = set(data[0].keys())
        print(f"  Existing columns: {sorted(existing_cols)}")
        return existing_cols
    
    return set()


def strip_unknown_columns(buildings, known_cols):
    """Remove columns from building dicts that don't exist in the DB."""
    new_cols = {"year_built", "total_value", "building_area", "stories", "property_subtype", "neighborhood"}
    missing_cols = new_cols - known_cols
    
    if missing_cols:
        print(f"\n  ⚠ Columns not in DB (will be stripped): {missing_cols}")
        for b in buildings:
            for col in missing_cols:
                b.pop(col, None)
    else:
        print(f"\n  ✓ All new columns exist in DB")
    
    return buildings


# ═══════════════════════════════════════════════════════════════════════════════
# EXISTING DATA + DEDUP
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_existing_buildings():
    """Fetch existing buildings for dedup."""
    print("\n" + "="*70)
    print("2. FETCHING EXISTING DATA FOR DEDUP")
    print("="*70)
    
    all_buildings = []
    offset = 0
    
    while True:
        url = f"{SUPABASE_URL}/rest/v1/ba_buildings?select=id,address,city,lat,lng,year_built,owner_name,unit_count&limit=1000&offset={offset}"
        data = fetch_json(url, supabase_headers())
        if not data:
            break
        all_buildings.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    
    print(f"  Existing buildings: {len(all_buildings)}")
    return all_buildings


def deduplicate(new_buildings, existing_buildings):
    """Remove duplicates — match by normalized address OR by lat/lng proximity."""
    print("\n" + "="*70)
    print("3. DEDUPLICATION")
    print("="*70)
    
    # Build index from existing
    existing_addr_index = set()
    existing_by_addr = {}
    existing_latlng = {}
    
    for b in existing_buildings:
        norm = normalize_address(b.get("address", ""))
        existing_addr_index.add(norm)
        existing_by_addr[norm] = b
        # Round lat/lng for proximity check
        lat = b.get("lat")
        lng = b.get("lng")
        if lat and lng:
            key = f"{round(float(lat), 4)}_{round(float(lng), 4)}"
            existing_latlng[key] = b
    
    # Also deduplicate within new data itself
    seen_new = set()
    unique_buildings = []
    skipped_existing = 0
    skipped_self_dup = 0
    
    for b in new_buildings:
        norm = normalize_address(b["address"])
        
        # Skip if address matches existing
        if norm in existing_addr_index:
            skipped_existing += 1
            continue
        
        # Skip self-duplicates
        if norm in seen_new:
            skipped_self_dup += 1
            continue
        
        # Proximity check (within ~11m)
        latlng_key = f"{round(b['lat'], 4)}_{round(b['lng'], 4)}"
        if latlng_key in existing_latlng:
            skipped_existing += 1
            continue
        
        seen_new.add(norm)
        unique_buildings.append(b)
    
    print(f"  New buildings after dedup: {len(unique_buildings)}")
    print(f"  Skipped (existing match): {skipped_existing}")
    print(f"  Skipped (self-duplicate): {skipped_self_dup}")
    
    return unique_buildings


# ═══════════════════════════════════════════════════════════════════════════════
# ENRICH EXISTING RECORDS
# ═══════════════════════════════════════════════════════════════════════════════

def build_enrichment_updates(new_features, existing_buildings):
    """
    For existing records missing year_built, owner_name, or unit_count, 
    try to fill them from Hennepin data by address match.
    """
    print("\n" + "="*70)
    print("4. ENRICHMENT — Fill gaps in existing records")
    print("="*70)
    
    # Build lookup from new data by normalized address
    hennepin_by_addr = {}
    for f in new_features:
        a = f.get("attributes", {})
        addr = (a.get("ADDRESSFORMATTED") or "").strip()
        if addr:
            hennepin_by_addr[normalize_address(addr)] = a
    
    updates = []
    enriched_year = 0
    enriched_owner = 0
    enriched_units = 0
    
    for b in existing_buildings:
        norm = normalize_address(b.get("address", ""))
        hennepin = hennepin_by_addr.get(norm)
        if not hennepin:
            continue
        
        patch = {"id": b["id"]}
        
        # Fill missing year_built
        if not b.get("year_built"):
            yb = hennepin.get("YEARBUILT")
            if yb and yb > 0:
                patch["year_built"] = yb
                enriched_year += 1
        
        # Fill missing owner_name
        if not b.get("owner_name"):
            on = hennepin.get("OWNERNAME")
            if on:
                patch["owner_name"] = on
                enriched_owner += 1
        
        # Fill missing unit_count
        if not b.get("unit_count"):
            uc = hennepin.get("TOTALUNITS")
            if uc and uc > 0:
                patch["unit_count"] = uc
                enriched_units += 1
        
        if len(patch) > 1:  # more than just the id
            updates.append(patch)
    
    print(f"  Records to enrich: {len(updates)}")
    print(f"    year_built fills: {enriched_year}")
    print(f"    owner_name fills: {enriched_owner}")
    print(f"    unit_count fills: {enriched_units}")
    
    return updates


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("="*70)
    print("Building Access — Data Load 3: Hennepin County Assessor 2025")
    print("="*70)
    
    # 1. Fetch Hennepin data
    features = fetch_hennepin_parcels()
    
    # 2. Process into building records
    buildings = process_hennepin(features)
    
    # 3. Fetch existing data
    existing = fetch_existing_buildings()
    
    # 4. Deduplicate
    new_buildings = deduplicate(buildings, existing)
    
    # 5. Insert new buildings
    print("\n" + "="*70)
    print("5. INSERTING NEW BUILDINGS")
    print("="*70)
    
    if new_buildings:
        print(f"\n  Inserting {len(new_buildings)} new buildings...")
        inserted = supabase_insert("ba_buildings", new_buildings)
        print(f"  Buildings inserted: {inserted}")
    else:
        print("  No new buildings to insert")
    
    # 6. Enrich existing records
    enrichment_updates = build_enrichment_updates(features, existing)
    
    if enrichment_updates:
        print(f"\n  Enriching {len(enrichment_updates)} existing records...")
        enriched = 0
        headers = supabase_headers("return=minimal")
        
        for update in enrichment_updates:
            bid = update.pop("id")
            url = f"{SUPABASE_URL}/rest/v1/ba_buildings?id=eq.{bid}"
            body = json.dumps(update).encode()
            req = urllib.request.Request(url, data=body, method="PATCH")
            for k, v in headers.items():
                req.add_header(k, v)
            try:
                with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
                    enriched += 1
            except Exception:
                pass
            
            if enriched % 200 == 0 and enriched > 0:
                print(f"  Enriched {enriched}/{len(enrichment_updates)}")
                time.sleep(0.3)
        
        print(f"  Total enriched: {enriched}")
    
    # 8. Final counts
    print("\n" + "="*70)
    print("6. FINAL COUNTS")
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
    
    # City breakdown
    print("\n  City breakdown:")
    all_buildings = []
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/ba_buildings?select=city&limit=1000&offset={offset}"
        data = fetch_json(url, supabase_headers())
        if not data:
            break
        all_buildings.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    
    city_counts = defaultdict(int)
    for b in all_buildings:
        city_counts[b.get("city", "Unknown")] += 1
    
    for city, c in sorted(city_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"    {city}: {c}")
    
    print("\nDone!")


if __name__ == "__main__":
    main()
