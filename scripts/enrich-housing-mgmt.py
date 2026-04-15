#!/usr/bin/env python3
"""
Enrich building contacts with property management company data from:
1. MHFA (Minnesota Housing Finance Agency) HTC monitoring list - 778 properties
2. HUD LIHTC (Low-Income Housing Tax Credit) database - 1,114 MN properties

Matches housing projects to existing buildings by address+city and lat/lng proximity.
Inserts management company contacts with phone, email, and contact name.
"""

import json
import re
import math
import urllib.request
import urllib.parse
import time
import sys

SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg"

def supabase_get(table, params):
    """GET from Supabase REST API with pagination."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{urllib.parse.urlencode(params, doseq=True)}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    })
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())

def supabase_post(table, rows):
    """POST to Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    data = json.dumps(rows).encode()
    req = urllib.request.Request(url, data=data, method="POST", headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  POST error {e.code}: {body[:200]}")
        return e.code

def normalize_addr(addr):
    """Normalize address for matching."""
    if not addr:
        return ""
    addr = str(addr).upper().strip()
    # Remove unit/apt/suite
    addr = re.sub(r'\s*(APT|UNIT|STE|SUITE|#)\s*\S*', '', addr)
    # Normalize directionals
    for full, abbr in [('NORTH','N'), ('SOUTH','S'), ('EAST','E'), ('WEST','W'),
                       ('NORTHEAST','NE'), ('NORTHWEST','NW'), ('SOUTHEAST','SE'), ('SOUTHWEST','SW')]:
        addr = re.sub(r'\b' + full + r'\b', abbr, addr)
    # Normalize street types
    for full, abbr in [('STREET','ST'), ('AVENUE','AVE'), ('BOULEVARD','BLVD'), ('DRIVE','DR'),
                       ('ROAD','RD'), ('LANE','LN'), ('COURT','CT'), ('PLACE','PL'),
                       ('CIRCLE','CIR'), ('TRAIL','TRL'), ('PARKWAY','PKWY'), ('WAY','WAY')]:
        addr = re.sub(r'\b' + full + r'\b', abbr, addr)
    # Remove extra whitespace
    addr = re.sub(r'\s+', ' ', addr).strip()
    return addr

def normalize_city(city):
    """Normalize city name."""
    if not city:
        return ""
    city = str(city).upper().strip()
    # Common abbreviations
    city = city.replace('ST.', 'ST').replace('ST ', 'SAINT ')
    city = re.sub(r'\s+', ' ', city).strip()
    return city

def haversine_m(lat1, lon1, lat2, lon2):
    """Distance in meters between two lat/lng points."""
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def load_all_buildings():
    """Load all buildings from Supabase with cursor pagination."""
    print("Loading all buildings from Supabase...")
    all_buildings = []
    last_id = 0
    while True:
        batch = supabase_get("ba_buildings", {
            "select": "id,name,address,city,state,lat,lng",
            "id": f"gt.{last_id}",
            "order": "id.asc",
            "limit": 1000,
        })
        if not batch:
            break
        all_buildings.extend(batch)
        last_id = batch[-1]["id"]
        if len(batch) < 1000:
            break
    print(f"  Loaded {len(all_buildings)} buildings")
    return all_buildings

def load_existing_contacts():
    """Load existing contacts to avoid duplicates."""
    print("Loading existing contacts...")
    contacts = []
    last_id = 0
    while True:
        batch = supabase_get("ba_contacts", {
            "select": "id,building_id,name,source",
            "id": f"gt.{last_id}",
            "order": "id.asc",
            "limit": 1000,
        })
        if not batch:
            break
        contacts.extend(batch)
        last_id = batch[-1]["id"]
        if len(batch) < 1000:
            break
    print(f"  Loaded {len(contacts)} existing contacts")
    return contacts

def main():
    # Load buildings
    buildings = load_all_buildings()
    
    # Build lookup indexes
    addr_city_to_bids = {}  # "NORMALIZED_ADDR|CITY" -> [building_ids]
    city_to_buildings = {}  # "CITY" -> [buildings with lat/lng]
    
    for b in buildings:
        norm_a = normalize_addr(b.get("address"))
        norm_c = normalize_city(b.get("city"))
        if norm_a and norm_c:
            key = f"{norm_a}|{norm_c}"
            addr_city_to_bids.setdefault(key, []).append(b["id"])
        if norm_c and b.get("lat") and b.get("lng"):
            city_to_buildings.setdefault(norm_c, []).append(b)
    
    print(f"  Address+city index: {len(addr_city_to_bids)} unique addresses")
    print(f"  City index: {len(city_to_buildings)} cities")
    
    # Load existing contacts to check for duplicates
    existing = load_existing_contacts()
    existing_keys = set()
    for c in existing:
        if c.get("building_id") and c.get("source"):
            existing_keys.add(f"{c['building_id']}|{c['source']}")
        # Also track building_id + name to avoid dups
        if c.get("building_id") and c.get("name"):
            existing_keys.add(f"{c['building_id']}|{c['name']}")
    
    # ========== MHFA DATA ==========
    print("\n=== Processing MHFA HTC Data ===")
    mhfa = json.load(open("/tmp/mhfa_htc.json"))
    
    mhfa_matched = 0
    mhfa_contacts = []
    mhfa_skipped_dup = 0
    mhfa_no_match = 0
    
    for rec in mhfa:
        mgmt = str(rec.get("Management Company") or "").strip()
        contact = str(rec.get("Mgmt Company Contact") or "").strip()
        phone = str(rec.get("Mgmt Company Contact Phone") or "").strip()
        email = str(rec.get("Mgmt Company Contact Email") or "").strip()
        addr = str(rec.get("Address 1") or "").strip()
        city = str(rec.get("City") or "").strip()
        prop_name = str(rec.get("Property Name") or "").strip()
        
        if not mgmt and not contact:
            continue
        
        # Clean phone - remove .0 suffix from Excel
        if phone.endswith('.0'):
            phone = phone[:-2]
        # Format phone
        if phone and len(phone) == 10 and phone.isdigit():
            phone = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
        
        # Skip "Multiple Building Addresses" - try city match
        matched_bids = []
        
        if addr and "Multiple" not in addr:
            norm_a = normalize_addr(addr)
            norm_c = normalize_city(city)
            key = f"{norm_a}|{norm_c}"
            if key in addr_city_to_bids:
                matched_bids = addr_city_to_bids[key]
        
        if not matched_bids:
            mhfa_no_match += 1
            continue
        
        for bid in matched_bids:
            dup_key = f"{bid}|mhfa-htc"
            if dup_key in existing_keys:
                mhfa_skipped_dup += 1
                continue
            existing_keys.add(dup_key)
            
            # Build notes with company info
            notes_parts = []
            if mgmt:
                notes_parts.append(f"Mgmt Co: {mgmt}")
            if prop_name:
                notes_parts.append(f"MHFA HTC: {prop_name}")
            
            contact_rec = {
                "building_id": bid,
                "name": contact if contact else mgmt,
                "role": "property-manager",
                "title": "Property Management" if contact else None,
                "phone": phone if phone else None,
                "email": email if email else None,
                "source": "mhfa-htc",
                "notes": "; ".join(notes_parts) if notes_parts else None,
            }
            # Remove None values
            contact_rec = {k: v for k, v in contact_rec.items() if v is not None}
            mhfa_contacts.append(contact_rec)
            mhfa_matched += 1
    
    print(f"  MHFA matched to buildings: {mhfa_matched}")
    print(f"  MHFA skipped (duplicate): {mhfa_skipped_dup}")
    print(f"  MHFA no address match: {mhfa_no_match}")
    
    # ========== LIHTC DATA ==========
    print("\n=== Processing HUD LIHTC Data ===")
    lihtc = json.load(open("/tmp/lihtc_mn.json"))
    
    lihtc_addr_matched = 0
    lihtc_geo_matched = 0
    lihtc_contacts = []
    lihtc_skipped_dup = 0
    lihtc_no_match = 0
    
    for rec in lihtc:
        company = str(rec.get("COMPANY") or "").strip()
        contact = str(rec.get("CONTACT") or "").strip()
        phone = str(rec.get("CO_TEL") or "").strip()
        addr = str(rec.get("PROJ_ADD") or "").strip()
        city = str(rec.get("PROJ_CTY") or "").strip()
        proj_name = str(rec.get("PROJECT") or "").strip()
        lat = rec.get("LATITUDE")
        lng = rec.get("LONGITUDE")
        
        if not company and not contact:
            continue
        
        # Try address match first
        matched_bids = []
        
        if addr:
            norm_a = normalize_addr(addr)
            norm_c = normalize_city(city)
            key = f"{norm_a}|{norm_c}"
            if key in addr_city_to_bids:
                matched_bids = addr_city_to_bids[key]
                lihtc_addr_matched += len(matched_bids)
        
        # If no address match, try lat/lng proximity (within 100m)
        if not matched_bids and lat and lng and str(lat) != 'None' and str(lng) != 'None':
            try:
                flat = float(lat)
                flng = float(lng)
                norm_c = normalize_city(city)
                if norm_c in city_to_buildings:
                    for b in city_to_buildings[norm_c]:
                        dist = haversine_m(flat, flng, float(b["lat"]), float(b["lng"]))
                        if dist < 100:
                            matched_bids.append(b["id"])
                            lihtc_geo_matched += 1
            except (ValueError, TypeError):
                pass
        
        if not matched_bids:
            lihtc_no_match += 1
            continue
        
        for bid in matched_bids:
            # Skip if already has MHFA contact (which is newer/better)
            dup_key_mhfa = f"{bid}|mhfa-htc"
            dup_key_lihtc = f"{bid}|hud-lihtc"
            if dup_key_mhfa in existing_keys or dup_key_lihtc in existing_keys:
                lihtc_skipped_dup += 1
                continue
            existing_keys.add(dup_key_lihtc)
            
            notes_parts = []
            if company:
                notes_parts.append(f"Mgmt Co: {company}")
            if proj_name:
                notes_parts.append(f"LIHTC: {proj_name}, {rec.get('N_UNITS','')} units")
            
            contact_rec = {
                "building_id": bid,
                "name": contact if contact else company,
                "role": "property-manager",
                "title": "Property Management" if contact else None,
                "phone": phone if phone else None,
                "source": "hud-lihtc",
                "notes": "; ".join(notes_parts) if notes_parts else None,
            }
            contact_rec = {k: v for k, v in contact_rec.items() if v is not None}
            lihtc_contacts.append(contact_rec)
    
    print(f"  LIHTC address-matched: {lihtc_addr_matched}")
    print(f"  LIHTC geo-matched: {lihtc_geo_matched}")
    print(f"  LIHTC skipped (dup/MHFA already): {lihtc_skipped_dup}")
    print(f"  LIHTC no match: {lihtc_no_match}")
    print(f"  LIHTC contacts to insert: {len(lihtc_contacts)}")
    
    # ========== INSERT ALL ==========
    all_contacts = mhfa_contacts + lihtc_contacts
    print(f"\n=== Inserting {len(all_contacts)} total contacts ===")
    
    if not all_contacts:
        print("No contacts to insert!")
        return
    
    # Ensure all rows have identical keys
    all_keys = set()
    for c in all_contacts:
        all_keys.update(c.keys())
    for c in all_contacts:
        for k in all_keys:
            if k not in c:
                c[k] = None
    
    # Insert in batches
    batch_size = 200
    inserted = 0
    for i in range(0, len(all_contacts), batch_size):
        batch = all_contacts[i:i+batch_size]
        status = supabase_post("ba_contacts", batch)
        if status in (200, 201):
            inserted += len(batch)
            print(f"  Inserted batch {i//batch_size+1}: {len(batch)} contacts (total: {inserted})")
        else:
            print(f"  FAILED batch {i//batch_size+1}")
            # Try one by one to find problem rows
            for j, c in enumerate(batch):
                s = supabase_post("ba_contacts", [c])
                if s in (200, 201):
                    inserted += 1
                else:
                    print(f"    Failed row: {c.get('name')} at {c.get('building_id')}")
        time.sleep(0.3)
    
    # Summary
    results = {
        "mhfa_matched": mhfa_matched,
        "lihtc_contacts": len(lihtc_contacts),
        "total_inserted": inserted,
        "mhfa_with_phone": sum(1 for c in mhfa_contacts if c.get("phone")),
        "mhfa_with_email": sum(1 for c in mhfa_contacts if c.get("email")),
        "lihtc_with_phone": sum(1 for c in lihtc_contacts if c.get("phone")),
    }
    
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"MHFA contacts inserted:  {mhfa_matched}")
    print(f"  with phone: {results['mhfa_with_phone']}")
    print(f"  with email: {results['mhfa_with_email']}")
    print(f"LIHTC contacts inserted: {len(lihtc_contacts)}")
    print(f"  with phone: {results['lihtc_with_phone']}")
    print(f"Total inserted:          {inserted}")
    print(f"{'='*50}")
    
    json.dump(results, open("/home/user/workspace/building-access/scripts/housing_mgmt_results.json", "w"), indent=2)
    print("Results saved to housing_mgmt_results.json")

if __name__ == "__main__":
    main()
