#!/usr/bin/env python3
"""
Round 2: Match MHFA "Multiple Building Addresses" entries by property name to building name,
and by city proximity. These are the large apartment complexes the user wants most.
"""

import json
import re
import math
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

def normalize_name(name):
    """Normalize property/building name for fuzzy matching."""
    if not name:
        return ""
    name = str(name).upper().strip()
    # Remove common suffixes
    name = re.sub(r'\s*(APARTMENTS?|APTS?|TOWNHOMES?|TOWNHOUSES?|RESIDENCES?|HOUSING|COMPLEX|VILLAGE|MANOR|ESTATE|PLACE|COURT|COURTS|SQUARE|CENTER|CENTRE|PLAZA|PARK|GARDENS?|TERRACE|TOWERS?|COMMONS?|HOMES?|SENIOR|LIVING|LLC|INC|LP)\s*', ' ', name)
    # Remove parenthetical
    name = re.sub(r'\(.*?\)', '', name)
    # Remove fka/aka
    name = re.sub(r'\b(FKA|AKA)\b.*', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def main():
    # Load buildings
    print("Loading buildings...")
    buildings = []
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
        buildings.extend(batch)
        last_id = batch[-1]["id"]
        if len(batch) < 1000:
            break
    print(f"  {len(buildings)} buildings loaded")

    # Build city -> buildings with names index
    city_buildings = {}  # CITY -> [{id, name, norm_name, lat, lng}]
    for b in buildings:
        city = str(b.get("city","")).upper().strip()
        if city and b.get("name"):
            entry = {
                "id": b["id"],
                "name": b["name"],
                "norm_name": normalize_name(b["name"]),
                "lat": b.get("lat"),
                "lng": b.get("lng"),
            }
            city_buildings.setdefault(city, []).append(entry)
    
    # Load existing contacts
    print("Loading existing contacts...")
    existing = []
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
        existing.extend(batch)
        last_id = batch[-1]["id"]
        if len(batch) < 1000:
            break
    
    existing_keys = set()
    for c in existing:
        if c.get("building_id") and c.get("source"):
            existing_keys.add(f"{c['building_id']}|{c['source']}")
    print(f"  {len(existing)} existing contacts, {len(existing_keys)} unique keys")
    
    # Load MHFA data
    mhfa = json.load(open("/tmp/mhfa_htc.json"))
    
    # Focus on "Multiple Building Addresses" and unmatched single addresses
    contacts_to_insert = []
    name_matched = 0
    no_match = 0
    skipped_dup = 0
    
    for rec in mhfa:
        mgmt = str(rec.get("Management Company") or "").strip()
        contact = str(rec.get("Mgmt Company Contact") or "").strip()
        phone = str(rec.get("Mgmt Company Contact Phone") or "").strip()
        email = str(rec.get("Mgmt Company Contact Email") or "").strip()
        city = str(rec.get("City") or "").strip().upper()
        prop_name = str(rec.get("Property Name") or "").strip()
        
        if not mgmt and not contact:
            continue
        if not phone and not email:
            continue
        
        # Clean phone
        if phone.endswith('.0'):
            phone = phone[:-2]
        if phone and len(phone) == 10 and phone.isdigit():
            phone = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
        
        # Try name matching within same city
        norm_prop = normalize_name(prop_name)
        if not norm_prop or city not in city_buildings:
            no_match += 1
            continue
        
        # Find buildings with matching name in same city
        matched_bids = []
        for b in city_buildings[city]:
            if not b["norm_name"]:
                continue
            # Check if normalized names share significant words
            prop_words = set(norm_prop.split())
            bldg_words = set(b["norm_name"].split())
            if not prop_words or not bldg_words:
                continue
            
            # Calculate word overlap
            common = prop_words & bldg_words
            # Need at least 1 significant word match (>2 chars)
            sig_common = [w for w in common if len(w) > 2]
            
            if sig_common:
                # Check that common words are substantial part of property name
                coverage = len(sig_common) / max(len([w for w in prop_words if len(w) > 2]), 1)
                if coverage >= 0.5:  # At least half the significant words match
                    matched_bids.append(b["id"])
        
        if not matched_bids:
            no_match += 1
            continue
        
        for bid in matched_bids:
            dup_key = f"{bid}|mhfa-htc-name"
            dup_key2 = f"{bid}|mhfa-htc"
            if dup_key in existing_keys or dup_key2 in existing_keys:
                skipped_dup += 1
                continue
            existing_keys.add(dup_key)
            
            notes_parts = []
            if mgmt:
                notes_parts.append(f"Mgmt Co: {mgmt}")
            if prop_name:
                notes_parts.append(f"MHFA HTC: {prop_name}")
            
            contact_rec = {
                "building_id": bid,
                "name": contact if contact else mgmt,
                "role": "property-manager",
                "phone": phone if phone else None,
                "email": email if email else None,
                "source": "mhfa-htc-name",
                "notes": "; ".join(notes_parts) if notes_parts else None,
            }
            contact_rec = {k: v for k, v in contact_rec.items() if v is not None}
            contacts_to_insert.append(contact_rec)
            name_matched += 1
    
    print(f"\nName-matched contacts: {name_matched}")
    print(f"Skipped duplicates: {skipped_dup}")
    print(f"No match: {no_match}")
    
    if not contacts_to_insert:
        print("No new contacts to insert.")
        return
    
    # Ensure uniform keys
    all_keys = set()
    for c in contacts_to_insert:
        all_keys.update(c.keys())
    for c in contacts_to_insert:
        for k in all_keys:
            if k not in c:
                c[k] = None
    
    # Insert
    print(f"\nInserting {len(contacts_to_insert)} contacts...")
    batch_size = 200
    inserted = 0
    for i in range(0, len(contacts_to_insert), batch_size):
        batch = contacts_to_insert[i:i+batch_size]
        status = supabase_post("ba_contacts", batch)
        if status in (200, 201):
            inserted += len(batch)
            print(f"  Batch {i//batch_size+1}: {len(batch)} inserted (total: {inserted})")
        else:
            print(f"  FAILED batch {i//batch_size+1}")
            for c in batch:
                s = supabase_post("ba_contacts", [c])
                if s in (200, 201):
                    inserted += 1
        time.sleep(0.3)
    
    with_phone = sum(1 for c in contacts_to_insert if c.get("phone"))
    with_email = sum(1 for c in contacts_to_insert if c.get("email"))
    print(f"\n{'='*50}")
    print(f"Round 2 MHFA Name-Match Summary")
    print(f"{'='*50}")
    print(f"Total inserted:  {inserted}")
    print(f"  with phone:    {with_phone}")
    print(f"  with email:    {with_email}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
