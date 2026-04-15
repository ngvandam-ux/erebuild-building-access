#!/usr/bin/env python3
"""
Data Load 6: OpenStreetMap Overpass API → ba_contacts
Source: OSM Overpass - all elements with phone tag in Twin Cities metro
Match: by address to existing ba_buildings
"""

import requests
import json
import re
import time
from collections import Counter

SUPABASE_URL = 'https://jvecvoqzyxsicsrrpvyu.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg'
headers_write = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}
headers_read = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}'
}

def normalize_address(addr):
    if not addr:
        return ''
    addr = addr.upper().strip()
    addr = re.sub(r'\s*(APT|UNIT|STE|SUITE|#)\s*\S*$', '', addr)
    addr = re.sub(r'\bNORTH\b', 'N', addr)
    addr = re.sub(r'\bSOUTH\b', 'S', addr)
    addr = re.sub(r'\bEAST\b', 'E', addr)
    addr = re.sub(r'\bWEST\b', 'W', addr)
    addr = re.sub(r'\bAVENUE\b', 'AVE', addr)
    addr = re.sub(r'\bSTREET\b', 'ST', addr)
    addr = re.sub(r'\bBOULEVARD\b', 'BLVD', addr)
    addr = re.sub(r'\bDRIVE\b', 'DR', addr)
    addr = re.sub(r'\bLANE\b', 'LN', addr)
    addr = re.sub(r'\bPLACE\b', 'PL', addr)
    addr = re.sub(r'\bCOURT\b', 'CT', addr)
    addr = re.sub(r'\bROAD\b', 'RD', addr)
    addr = re.sub(r'\bPARKWAY\b', 'PKWY', addr)
    addr = re.sub(r'\bCIRCLE\b', 'CIR', addr)
    addr = re.sub(r'\s+', ' ', addr).strip()
    return addr

def fetch_osm_data():
    """Fetch all OSM elements with phone in Twin Cities metro, split into quadrants"""
    # Split into 4 quadrants to avoid timeout
    quadrants = [
        (44.65, -93.70, 44.95, -93.25, "SW"),
        (44.65, -93.25, 44.95, -92.80, "SE"),
        (44.95, -93.70, 45.25, -93.25, "NW"),
        (44.95, -93.25, 45.25, -92.80, "NE"),
    ]
    
    all_elements = []
    seen_ids = set()
    
    for s, w, n, e, label in quadrants:
        query = f'''
[out:json][timeout:90];
(
  node["phone"]({s},{w},{n},{e});
  way["phone"]({s},{w},{n},{e});
);
out center tags;
'''
        print(f"  Querying {label} quadrant...")
        try:
            r = requests.post('https://overpass-api.de/api/interpreter', data={'data': query}, timeout=120)
            if r.ok:
                data = r.json()
                elements = data.get('elements', [])
                new = 0
                for elem in elements:
                    eid = f"{elem['type']}/{elem['id']}"
                    if eid not in seen_ids:
                        seen_ids.add(eid)
                        all_elements.append(elem)
                        new += 1
                print(f"    {label}: {new} new elements")
            else:
                print(f"    {label}: error {r.status_code}")
        except Exception as ex:
            print(f"    {label}: {ex}")
        time.sleep(2)  # Rate limiting
    
    print(f"  Total: {len(all_elements)} unique OSM elements")
    return all_elements

def fetch_all_buildings():
    """Fetch all buildings from Supabase for address matching (paging by 1000)"""
    all_buildings = []
    last_id = 0
    batch_size = 1000
    
    while True:
        r = requests.get(f'{SUPABASE_URL}/rest/v1/ba_buildings', params={
            'select': 'id,address,city',
            'id': f'gt.{last_id}',
            'order': 'id.asc',
            'limit': batch_size
        }, headers=headers_read)
        
        if not r.ok:
            print(f"    Fetch error: {r.status_code}")
            break
        
        batch = r.json()
        if not batch:
            break
        all_buildings.extend(batch)
        last_id = batch[-1]['id']
        if len(batch) < batch_size:
            break
        if len(all_buildings) % 10000 == 0:
            print(f"    {len(all_buildings)} buildings loaded...")
    
    return all_buildings

def fetch_existing_osm_contacts():
    """Get building_ids that already have OSM contacts"""
    existing = set()
    offset = 0
    while True:
        r = requests.get(f'{SUPABASE_URL}/rest/v1/ba_contacts', params={
            'select': 'building_id',
            'source': 'eq.osm-overpass',
            'limit': 5000,
            'offset': offset
        }, headers=headers_read)
        if not r.ok or not r.json():
            break
        for c in r.json():
            existing.add(c['building_id'])
        if len(r.json()) < 5000:
            break
        offset += 5000
    return existing

def main():
    print("=== Data Load 6: OpenStreetMap Overpass → Contacts ===\n")
    
    # 1. Fetch OSM data
    print("1. Fetching OSM data from Overpass API...")
    elements = fetch_osm_data()
    
    # 2. Fetch buildings
    print("\n2. Fetching all buildings from DB...")
    buildings = fetch_all_buildings()
    print(f"  {len(buildings)} buildings loaded")
    
    # Build address index
    addr_to_building = {}
    for b in buildings:
        norm = normalize_address(b['address'])
        if norm and b.get('city'):
            key = f"{norm}|{b['city'].upper()}"
            if key not in addr_to_building:
                addr_to_building[key] = b
            # Also index without city for flexible matching
            if norm not in addr_to_building:
                addr_to_building[norm] = b
    
    # 3. Check existing
    print("\n3. Checking existing OSM contacts...")
    existing_bids = fetch_existing_osm_contacts()
    print(f"  {len(existing_bids)} buildings already have OSM contacts")
    
    # 4. Match and create contacts
    print("\n4. Matching OSM elements to buildings...")
    new_contacts = []
    matched = 0
    unmatched = 0
    no_address = 0
    stats = Counter()
    
    for elem in elements:
        tags = elem.get('tags', {})
        
        # Build address from OSM tags
        house_num = tags.get('addr:housenumber', '')
        street = tags.get('addr:street', '')
        city = tags.get('addr:city', '')
        
        if not house_num or not street:
            no_address += 1
            continue
        
        osm_addr = f"{house_num} {street}"
        norm_addr = normalize_address(osm_addr)
        
        # Try with city first, then without
        building = None
        if city:
            key = f"{norm_addr}|{city.upper()}"
            building = addr_to_building.get(key)
        if not building:
            building = addr_to_building.get(norm_addr)
        
        if not building:
            unmatched += 1
            continue
        
        bid = building['id']
        if bid in existing_bids:
            continue
        
        existing_bids.add(bid)
        matched += 1
        
        name = tags.get('name', '')
        phone = tags.get('phone', tags.get('contact:phone', ''))
        email = tags.get('email', tags.get('contact:email', ''))
        website = tags.get('website', tags.get('contact:website', ''))
        
        if not phone and not email:
            continue
        
        # Determine role based on OSM tags
        amenity = tags.get('amenity', '')
        office = tags.get('office', '')
        building_tag = tags.get('building', '')
        shop = tags.get('shop', '')
        
        role = 'building-contact'
        if amenity in ('school', 'college', 'university'):
            role = 'school-office'
        elif amenity in ('hospital', 'clinic', 'doctors', 'dentist'):
            role = 'front-desk'
        elif amenity in ('place_of_worship', 'community_centre'):
            role = 'building-contact'
        elif office:
            role = 'office-contact'
        elif shop:
            role = 'store-contact'
        
        btype = amenity or office or shop or building_tag
        stats[btype] += 1
        
        notes_parts = []
        if website:
            notes_parts.append(f"Web: {website}")
        if btype:
            notes_parts.append(f"OSM type: {btype}")
        
        new_contacts.append({
            'building_id': bid,
            'role': role,
            'name': name if name else None,
            'phone': phone if phone else None,
            'email': email if email else None,
            'source': 'osm-overpass',
            'confidence': 'community',
            'notes': '; '.join(notes_parts) if notes_parts else None
        })
    
    print(f"  Matched: {matched} | Unmatched: {unmatched} | No address: {no_address}")
    print(f"  New contacts: {len(new_contacts)}")
    print(f"  Top types: {stats.most_common(10)}")
    
    # 5. Insert
    if new_contacts:
        print(f"\n5. Inserting {len(new_contacts)} contacts...")
        total = 0
        for i in range(0, len(new_contacts), 500):
            batch = new_contacts[i:i+500]
            r = requests.post(f'{SUPABASE_URL}/rest/v1/ba_contacts', headers=headers_write, json=batch)
            if r.ok:
                total += len(batch)
                print(f"  Inserted {total}/{len(new_contacts)}")
            else:
                print(f"  Error: {r.status_code} {r.text[:100]}")
            time.sleep(0.2)
        print(f"  Done: {total} contacts inserted")
    
    print("\n=== Done! ===")

if __name__ == '__main__':
    main()
