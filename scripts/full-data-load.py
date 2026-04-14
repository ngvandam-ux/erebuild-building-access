#!/usr/bin/env python3
"""
Full data load for Building Access — pulls from all open-source data sources,
deduplicates, and upserts to Supabase.

Sources:
1. Minneapolis Active Rental Licenses (ArcGIS) — 3+ unit buildings
2. HUD Multifamily Housing (data.hud.gov) — Minneapolis metro
3. Existing data in Supabase (for dedup)
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

# SSL context to avoid cert issues
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fetch_json(url, headers=None):
    """Fetch JSON from URL with retries."""
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
                print(f"  FAILED after 3 attempts: {e}")
                return None
            print(f"  Retry {attempt+1}: {e}")
            time.sleep(2)

def post_json(url, data, headers=None):
    """POST JSON to URL."""
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            body_err = e.read().decode() if e.fp else ""
            if attempt == 2:
                print(f"  POST FAILED: {e.code} {body_err[:300]}")
                return None
            print(f"  Retry POST {attempt+1}: {e.code}")
            time.sleep(2)
        except Exception as e:
            if attempt == 2:
                print(f"  POST FAILED: {e}")
                return None
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

def supabase_upsert(table, rows, conflict_cols):
    """Upsert rows to Supabase in batches of 500."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = supabase_headers(f"resolution=merge-duplicates,return=minimal")
    headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
    
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
                print(f"  Upserted {inserted}/{total} to {table}")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode() if e.fp else ""
            print(f"  Upsert error at batch {i//batch_size}: {e.code} — {err_body[:300]}")
            # Try one-by-one for this batch
            for row in batch:
                body2 = json.dumps([row]).encode()
                req2 = urllib.request.Request(url, data=body2, method="POST")
                for k, v in headers.items():
                    req2.add_header(k, v)
                try:
                    with urllib.request.urlopen(req2, context=ctx, timeout=30) as r2:
                        inserted += 1
                except:
                    pass
            print(f"  Recovered, total upserted: {inserted}/{total}")
        time.sleep(0.3)
    
    return inserted

def supabase_insert(table, rows):
    """Insert rows to Supabase in batches of 500 (no upsert)."""
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
                print(f"  Inserted {inserted}/{total} to {table}")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode() if e.fp else ""
            print(f"  Insert error at batch {i//batch_size}: {e.code} — {err_body[:300]}")
        time.sleep(0.3)
    
    return inserted


# ═══════════════════════════════════════════════════════════════════════════════
# 1. MINNEAPOLIS ACTIVE RENTAL LICENSES
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_mpls_rental_licenses():
    """Fetch ALL Minneapolis rental licenses with 3+ units from ArcGIS."""
    print("\n" + "="*70)
    print("1. MINNEAPOLIS ACTIVE RENTAL LICENSES (3+ units)")
    print("="*70)
    
    base_url = "https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/Active_Rental_Licenses/FeatureServer/0/query"
    
    all_features = []
    offset = 0
    batch_size = 2000
    
    while True:
        params = urllib.parse.urlencode({
            "where": "licensedUnits > 2",
            "outFields": "*",
            "resultRecordCount": batch_size,
            "resultOffset": offset,
            "f": "json",
            "orderByFields": "OBJECTID",
        })
        url = f"{base_url}?{params}"
        print(f"  Fetching offset {offset}...")
        data = fetch_json(url)
        
        if not data or "features" not in data:
            print(f"  No features returned at offset {offset}")
            break
        
        features = data["features"]
        if not features:
            break
        
        all_features.extend(features)
        print(f"  Got {len(features)} features (total: {len(all_features)})")
        
        if len(features) < batch_size:
            break
        offset += batch_size
        time.sleep(0.5)
    
    print(f"\n  Total Minneapolis rental licenses (3+ units): {len(all_features)}")
    return all_features


def classify_building_type(units, addr=""):
    """Classify building type based on unit count."""
    addr_lower = (addr or "").lower()
    if units and units >= 3:
        return "apartment"
    return "commercial"


def process_mpls_licenses(features):
    """Convert Minneapolis rental license features to building + contact rows."""
    buildings = []
    contacts = []
    
    # Deduplicate by address (multiple licenses at same address)
    seen_addresses = {}
    
    for f in features:
        a = f.get("attributes", {})
        addr = (a.get("address") or "").strip()
        if not addr:
            continue
        
        lat = a.get("latitude")
        lng = a.get("longitude")
        if not lat or not lng:
            continue
        
        # Deduplicate by address — keep the one with more units
        units = a.get("licensedUnits") or 0
        if addr in seen_addresses:
            if units <= seen_addresses[addr]["units"]:
                continue
        
        building = {
            "address": addr,
            "city": "Minneapolis",
            "state": "MN",
            "zip": a.get("ownerZip") or None,
            "lat": round(float(lat), 6),
            "lng": round(float(lng), 6),
            "building_type": classify_building_type(units, addr),
            "unit_count": units if units else None,
            "owner_name": a.get("ownerName") or None,
            "owner_address": None,
            "taxpayer_name": None,
            "data_source": "rental-license",
            "source_id": a.get("licenseNumber") or None,
        }
        
        # Build owner address
        parts = []
        if a.get("ownerAddress1"):
            parts.append(a["ownerAddress1"])
        if a.get("ownerCity"):
            parts.append(f"{a['ownerCity']}, {a.get('ownerState', '')} {a.get('ownerZip', '')}")
        if parts:
            building["owner_address"] = ", ".join(parts)
        
        seen_addresses[addr] = {"units": units, "building": building}
        
        # Contact from applicant (the property manager / owner on the license)
        applicant_name = a.get("applicantName")
        applicant_email = a.get("applicantEmail")
        applicant_phone = a.get("applicantPhone")
        owner_name = a.get("ownerName")
        owner_email = a.get("ownerEmail")
        owner_phone = a.get("ownerPhone")
        
        # Owner contact
        if owner_name or owner_email or owner_phone:
            contacts.append({
                "_addr_key": addr,
                "role": "owner",
                "name": owner_name,
                "phone": owner_phone,
                "email": owner_email,
                "source": "rental-license",
                "confidence": "verified",
                "verified_count": 1,
            })
        
        # Applicant contact (if different from owner)
        if applicant_name and applicant_name != owner_name:
            contacts.append({
                "_addr_key": addr,
                "role": "property-manager",
                "name": applicant_name,
                "phone": applicant_phone,
                "email": applicant_email,
                "source": "rental-license",
                "confidence": "verified",
                "verified_count": 1,
            })
    
    buildings = [v["building"] for v in seen_addresses.values()]
    print(f"  Processed: {len(buildings)} unique buildings, {len(contacts)} contacts")
    return buildings, contacts


# ═══════════════════════════════════════════════════════════════════════════════
# 2. HUD MULTIFAMILY HOUSING
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_hud_multifamily():
    """Fetch HUD multifamily properties for Minnesota metro area."""
    print("\n" + "="*70)
    print("2. HUD MULTIFAMILY HOUSING — Minnesota")
    print("="*70)
    
    # HUD data API for multifamily properties
    url = "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/Multifamily_Properties_Assisted/FeatureServer/0/query"
    
    all_features = []
    offset = 0
    batch_size = 2000
    
    while True:
        params = urllib.parse.urlencode({
            "where": "STATE_CODE='MN'",
            "outFields": "*",
            "resultRecordCount": batch_size,
            "resultOffset": offset,
            "f": "json",
        })
        full_url = f"{url}?{params}"
        print(f"  Fetching offset {offset}...")
        data = fetch_json(full_url)
        
        if not data or "features" not in data:
            break
        
        features = data["features"]
        if not features:
            break
        
        all_features.extend(features)
        print(f"  Got {len(features)} features (total: {len(all_features)})")
        
        if len(features) < batch_size:
            break
        offset += batch_size
        time.sleep(0.5)
    
    # Filter to Twin Cities metro area (roughly lat 44.7-45.3, lng -93.8 to -92.8)
    metro_features = []
    for f in all_features:
        geom = f.get("geometry", {})
        lat = geom.get("y")
        lng = geom.get("x")
        if lat and lng and 44.7 < lat < 45.3 and -93.8 < lng < -92.8:
            metro_features.append(f)
    
    print(f"  Total MN HUD properties: {len(all_features)}")
    print(f"  Twin Cities metro: {len(metro_features)}")
    return metro_features


def process_hud(features):
    """Convert HUD features to building + contact rows."""
    buildings = []
    contacts = []
    
    for f in features:
        a = f.get("attributes", {})
        geom = f.get("geometry", {})
        
        addr = (a.get("PROPERTY_STREET") or "").strip()
        if not addr:
            continue
        
        lat = geom.get("y")
        lng = geom.get("x")
        if not lat or not lng:
            continue
        
        city = (a.get("PROPERTY_CITY") or "").strip()
        state = (a.get("STATE_CODE") or "MN").strip()
        zip_code = (a.get("PROPERTY_ZIP") or "").strip()
        
        building = {
            "address": addr,
            "city": city,
            "state": state,
            "zip": zip_code or None,
            "lat": round(float(lat), 6),
            "lng": round(float(lng), 6),
            "building_type": "apartment",
            "unit_count": a.get("TOTAL_UNITS") or a.get("ASSISTED_UNITS") or None,
            "owner_name": a.get("OWNER_ORGANIZATION_NAME") or None,
            "owner_address": None,
            "taxpayer_name": None,
            "data_source": "hud",
            "source_id": str(a.get("HUB_NAME_TEXT", "")) + "-" + str(a.get("OBJECTID", "")),
        }
        
        buildings.append(building)
        
        # Management agent as contact
        mgmt_name = a.get("MGMT_AGENT_ORG_NAME")
        mgmt_phone = a.get("MGMT_AGENT_PHONE_NUM")
        mgmt_email = a.get("MGMT_AGENT_EMAIL")
        
        if mgmt_name or mgmt_phone:
            contacts.append({
                "_addr_key": addr,
                "role": "property-manager",
                "name": mgmt_name,
                "phone": mgmt_phone,
                "email": mgmt_email,
                "source": "hud",
                "confidence": "verified",
                "verified_count": 1,
            })
        
        # Owner as contact
        owner_name = a.get("OWNER_ORGANIZATION_NAME")
        owner_phone = a.get("OWNER_PHONE")
        owner_email = a.get("OWNER_EMAIL")
        
        if owner_name and owner_name != mgmt_name:
            contacts.append({
                "_addr_key": addr,
                "role": "owner",
                "name": owner_name,
                "phone": owner_phone,
                "email": owner_email,
                "source": "hud",
                "confidence": "verified",
                "verified_count": 1,
            })
    
    print(f"  Processed: {len(buildings)} buildings, {len(contacts)} contacts")
    return buildings, contacts


# ═══════════════════════════════════════════════════════════════════════════════
# 3. FETCH EXISTING DATA FROM SUPABASE (for dedup)
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_existing_buildings():
    """Fetch all existing buildings from Supabase for dedup."""
    print("\n" + "="*70)
    print("3. FETCHING EXISTING SUPABASE DATA")
    print("="*70)
    
    all_buildings = []
    offset = 0
    batch_size = 1000
    
    while True:
        url = f"{SUPABASE_URL}/rest/v1/ba_buildings?select=id,address,city,lat,lng&limit={batch_size}&offset={offset}"
        data = fetch_json(url, supabase_headers())
        if not data:
            break
        all_buildings.extend(data)
        if len(data) < batch_size:
            break
        offset += batch_size
    
    print(f"  Existing buildings: {len(all_buildings)}")
    
    # Also fetch existing contacts
    all_contacts = []
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/ba_contacts?select=id,building_id,name,phone,email,source&limit={batch_size}&offset={offset}"
        data = fetch_json(url, supabase_headers())
        if not data:
            break
        all_contacts.extend(data)
        if len(data) < batch_size:
            break
        offset += batch_size
    
    print(f"  Existing contacts: {len(all_contacts)}")
    return all_buildings, all_contacts


def normalize_address(addr):
    """Normalize address for dedup comparison."""
    if not addr:
        return ""
    a = addr.upper().strip()
    # Common abbreviations
    for old, new in [(" STREET", " ST"), (" AVENUE", " AVE"), (" BOULEVARD", " BLVD"),
                     (" DRIVE", " DR"), (" ROAD", " RD"), (" LANE", " LN"),
                     (" COURT", " CT"), (" PLACE", " PL"), (" CIRCLE", " CIR"),
                     (" NORTH", " N"), (" SOUTH", " S"), (" EAST", " E"), (" WEST", " W"),
                     (" NORTHEAST", " NE"), (" NORTHWEST", " NW"),
                     (" SOUTHEAST", " SE"), (" SOUTHWEST", " SW"),
                     (".", ""), (",", "")]:
        a = a.replace(old, new)
    return a


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    print("Building Access — Full Data Load")
    print("================================\n")
    
    # 1. Minneapolis rental licenses
    mpls_features = fetch_mpls_rental_licenses()
    mpls_buildings, mpls_contacts = process_mpls_licenses(mpls_features)
    
    # 2. HUD Multifamily
    hud_features = fetch_hud_multifamily()
    hud_buildings, hud_contacts = process_hud(hud_features)
    
    # 3. Existing data
    existing_buildings, existing_contacts = fetch_existing_buildings()
    
    # Build dedup index from existing data
    existing_addr_index = set()
    existing_id_by_addr = {}
    for b in existing_buildings:
        norm = normalize_address(b.get("address", ""))
        existing_addr_index.add(norm)
        existing_id_by_addr[norm] = b["id"]
    
    # ─── Merge & deduplicate ─────────────────────────────────────────────────
    print("\n" + "="*70)
    print("4. DEDUPLICATION & MERGE")
    print("="*70)
    
    # Combine all new buildings
    all_new_buildings = mpls_buildings + hud_buildings
    
    # Deduplicate: skip if address already exists in Supabase
    new_buildings = []
    addr_to_source = {}  # Track which buildings we're inserting (for contact linking)
    skipped = 0
    
    # Also deduplicate within the new data itself
    seen_new_addrs = set()
    
    for b in all_new_buildings:
        norm = normalize_address(b["address"])
        
        if norm in existing_addr_index:
            skipped += 1
            continue
        
        if norm in seen_new_addrs:
            skipped += 1
            continue
        
        seen_new_addrs.add(norm)
        addr_to_source[b["address"]] = b
        new_buildings.append(b)
    
    print(f"  Total new buildings to insert: {len(new_buildings)}")
    print(f"  Skipped (already in Supabase or dupes): {skipped}")
    
    # ─── Insert buildings ────────────────────────────────────────────────────
    print("\n" + "="*70)
    print("5. UPSERTING TO SUPABASE")
    print("="*70)
    
    if new_buildings:
        print(f"\n  Inserting {len(new_buildings)} new buildings...")
        inserted = supabase_insert("ba_buildings", new_buildings)
        print(f"  Buildings inserted: {inserted}")
    else:
        print("  No new buildings to insert")
    
    # ─── Now fetch all buildings again to get IDs for contact linking ─────────
    print("\n  Fetching updated building list for contact linking...")
    time.sleep(1)
    
    all_buildings_now = []
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/ba_buildings?select=id,address&limit=1000&offset={offset}"
        data = fetch_json(url, supabase_headers())
        if not data:
            break
        all_buildings_now.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    
    print(f"  Total buildings now: {len(all_buildings_now)}")
    
    # Build address → building_id lookup
    addr_to_id = {}
    for b in all_buildings_now:
        norm = normalize_address(b["address"])
        addr_to_id[norm] = b["id"]
    
    # Build existing contacts index for dedup
    existing_contact_keys = set()
    for c in existing_contacts:
        key = f"{c.get('building_id')}|{normalize_address(c.get('name',''))}|{c.get('source','')}"
        existing_contact_keys.add(key)
    
    # ─── Prepare contacts ────────────────────────────────────────────────────
    all_new_contacts = mpls_contacts + hud_contacts
    contacts_to_insert = []
    
    for c in all_new_contacts:
        addr_key = c.pop("_addr_key", "")
        norm = normalize_address(addr_key)
        building_id = addr_to_id.get(norm)
        
        if not building_id:
            continue
        
        # Dedup check
        contact_key = f"{building_id}|{normalize_address(c.get('name',''))}|{c.get('source','')}"
        if contact_key in existing_contact_keys:
            continue
        existing_contact_keys.add(contact_key)
        
        contacts_to_insert.append({
            "building_id": building_id,
            "role": c.get("role", "other"),
            "name": c.get("name"),
            "phone": c.get("phone"),
            "email": c.get("email"),
            "source": c.get("source", "community"),
            "confidence": c.get("confidence", "unverified"),
            "verified_count": c.get("verified_count", 0),
            "contributed_by": "data-import",
            "created_at": "2026-04-14T00:00:00Z",
        })
    
    print(f"\n  New contacts to insert: {len(contacts_to_insert)}")
    
    if contacts_to_insert:
        inserted_contacts = supabase_insert("ba_contacts", contacts_to_insert)
        print(f"  Contacts inserted: {inserted_contacts}")
    
    # ─── Final counts ────────────────────────────────────────────────────────
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
    
    print("\nDone!")


if __name__ == "__main__":
    main()
