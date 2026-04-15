#!/usr/bin/env python3
"""
Enrich building contacts with MN DLI Elevator and Boiler/Pressure Vessel data.

Elevator data: 50K records with building owner names + addresses for buildings with elevators.
Boiler data: 168K records with building owner names + addresses for buildings with boilers.

These are large commercial/apartment buildings — exactly what we want.
Match by address+city to existing buildings, insert owner contacts.
"""

import csv
import json
import re
import urllib.request
import urllib.parse
import time

SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg"

def supabase_get(table, params):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{urllib.parse.urlencode(params, doseq=True)}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    })
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())

def supabase_post(table, rows):
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
                       ('CIRCLE','CIR'), ('TRAIL','TRL'), ('PARKWAY','PKWY')]:
        addr = re.sub(r'\b' + full + r'\b', abbr, addr)
    addr = re.sub(r'\s+', ' ', addr).strip()
    return addr

def normalize_city(city):
    if not city:
        return ""
    city = str(city).upper().strip()
    city = city.replace('ST.', 'ST').replace('ST ', 'SAINT ')
    city = re.sub(r'\s+', ' ', city).strip()
    return city

def load_all_buildings():
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
    print("Loading existing contacts...")
    contacts = []
    last_id = 0
    while True:
        batch = supabase_get("ba_contacts", {
            "select": "id,building_id,source",
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

def process_elevator_data(addr_city_to_bids, existing_keys):
    """Process elevator data and return contacts to insert."""
    print("\n=== Processing Elevator Data ===")
    contacts = []
    matched = 0
    skipped_dup = 0
    no_match = 0
    
    # Deduplicate by location — multiple elevators per building, keep best record
    location_records = {}  # addr|city -> best record
    
    with open('/tmp/elevator_data.csv', encoding='latin-1', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            addr = row.get('SiteAddr', '').strip()
            city = row.get('Location_City', '').strip()
            owner = row.get('Owner_Name', '').strip()
            
            if not addr or not city or not owner:
                continue
            
            norm_a = normalize_addr(addr)
            norm_c = normalize_city(city)
            key = f"{norm_a}|{norm_c}"
            
            # Keep the record with the most info
            if key not in location_records:
                location_records[key] = row
            else:
                # Prefer records with more floors or business name
                existing = location_records[key]
                if (row.get('Floors', '') > existing.get('Floors', '') or
                    (row.get('BusinessName', '').strip() and not existing.get('BusinessName', '').strip())):
                    location_records[key] = row
    
    print(f"  Unique elevator locations: {len(location_records)}")
    
    for key, row in location_records.items():
        if key not in addr_city_to_bids:
            no_match += 1
            continue
        
        owner = row.get('Owner_Name', '').strip()
        owner_addr = row.get('Owner_Address', '').strip()
        owner_city = row.get('Owner_Cty', '').strip()
        owner_state = row.get('Owner_State', '').strip()
        biz_name = row.get('BusinessName', '').strip()
        bldg_use = row.get('Building_Use', '').strip()
        floors = row.get('Floors', '').strip()
        
        for bid in addr_city_to_bids[key]:
            dup_key = f"{bid}|dli-elevator"
            if dup_key in existing_keys:
                skipped_dup += 1
                continue
            existing_keys.add(dup_key)
            
            # Build notes
            notes_parts = []
            if biz_name and biz_name != owner:
                notes_parts.append(f"Business: {biz_name}")
            if bldg_use:
                notes_parts.append(f"Type: {bldg_use}")
            if floors:
                notes_parts.append(f"{floors} floors")
            if owner_addr:
                notes_parts.append(f"Owner addr: {owner_addr}, {owner_city} {owner_state}")
            
            contact = {
                "building_id": bid,
                "name": owner,
                "role": "building-owner",
                "source": "dli-elevator",
                "notes": "; ".join(notes_parts) if notes_parts else None,
            }
            contact = {k: v for k, v in contact.items() if v is not None}
            contacts.append(contact)
            matched += 1
    
    print(f"  Matched to buildings: {matched}")
    print(f"  Skipped (duplicate): {skipped_dup}")
    print(f"  No match: {no_match}")
    return contacts

def process_boiler_data(addr_city_to_bids, existing_keys):
    """Process boiler data and return contacts to insert."""
    print("\n=== Processing Boiler Data ===")
    contacts = []
    matched = 0
    skipped_dup = 0
    no_match = 0
    
    # Deduplicate by location — many boilers/vessels per building
    location_records = {}
    
    with open('/tmp/boiler/BPV_SNAP_04_14_2026.csv', encoding='latin-1', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            addr = row.get('Location_Address1', '').strip()
            city = row.get('Location_City', '').strip()
            owner = row.get('Owner_Name', '').strip()
            
            if not addr or not city or not owner:
                continue
            
            norm_a = normalize_addr(addr)
            norm_c = normalize_city(city)
            key = f"{norm_a}|{norm_c}"
            
            if key not in location_records:
                location_records[key] = row
    
    print(f"  Unique boiler locations: {len(location_records)}")
    
    for key, row in location_records.items():
        if key not in addr_city_to_bids:
            no_match += 1
            continue
        
        owner = row.get('Owner_Name', '').strip()
        owner_addr = row.get('Owner_Address1', '').strip()
        owner_city = row.get('Owner_City', '').strip()
        owner_state = row.get('Owner_State', '').strip()
        biz_name = row.get('Business_Name', '').strip()
        
        for bid in addr_city_to_bids[key]:
            # Skip if we already have elevator owner for this building
            dup_key_elv = f"{bid}|dli-elevator"
            dup_key_bpv = f"{bid}|dli-boiler"
            if dup_key_bpv in existing_keys or dup_key_elv in existing_keys:
                skipped_dup += 1
                continue
            existing_keys.add(dup_key_bpv)
            
            notes_parts = []
            if biz_name and biz_name != owner:
                notes_parts.append(f"Business: {biz_name}")
            if owner_addr:
                notes_parts.append(f"Owner addr: {owner_addr}, {owner_city} {owner_state}")
            
            contact = {
                "building_id": bid,
                "name": owner,
                "role": "building-owner",
                "source": "dli-boiler",
                "notes": "; ".join(notes_parts) if notes_parts else None,
            }
            contact = {k: v for k, v in contact.items() if v is not None}
            contacts.append(contact)
            matched += 1
    
    print(f"  Matched to buildings: {matched}")
    print(f"  Skipped (dup or elevator already): {skipped_dup}")
    print(f"  No match: {no_match}")
    return contacts

def main():
    buildings = load_all_buildings()
    
    # Build address+city index
    addr_city_to_bids = {}
    for b in buildings:
        norm_a = normalize_addr(b.get("address"))
        norm_c = normalize_city(b.get("city"))
        if norm_a and norm_c:
            key = f"{norm_a}|{norm_c}"
            addr_city_to_bids.setdefault(key, []).append(b["id"])
    print(f"  Address+city index: {len(addr_city_to_bids)} unique addresses")
    
    # Load existing contacts
    existing = load_existing_contacts()
    existing_keys = set()
    for c in existing:
        if c.get("building_id") and c.get("source"):
            existing_keys.add(f"{c['building_id']}|{c['source']}")
    
    # Process both sources
    elv_contacts = process_elevator_data(addr_city_to_bids, existing_keys)
    bpv_contacts = process_boiler_data(addr_city_to_bids, existing_keys)
    
    all_contacts = elv_contacts + bpv_contacts
    print(f"\n=== Total contacts to insert: {len(all_contacts)} ===")
    
    if not all_contacts:
        print("No contacts to insert!")
        return
    
    # Ensure uniform keys
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
            print(f"  Batch {i//batch_size+1}: {len(batch)} inserted (total: {inserted})")
        else:
            print(f"  FAILED batch {i//batch_size+1}, trying individually...")
            for c in batch:
                s = supabase_post("ba_contacts", [c])
                if s in (200, 201):
                    inserted += 1
                else:
                    print(f"    Failed: {c.get('name')} at bid {c.get('building_id')}")
        time.sleep(0.3)
    
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"Elevator owner contacts: {len(elv_contacts)} inserted")
    print(f"Boiler owner contacts:   {len(bpv_contacts)} inserted")
    print(f"Total inserted:          {inserted}")
    print(f"{'='*50}")
    
    results = {
        "elevator_contacts": len(elv_contacts),
        "boiler_contacts": len(bpv_contacts),
        "total_inserted": inserted,
    }
    json.dump(results, open("/home/user/workspace/building-access/scripts/dli_elevator_boiler_results.json", "w"), indent=2)
    print("Results saved to dli_elevator_boiler_results.json")

if __name__ == "__main__":
    main()
