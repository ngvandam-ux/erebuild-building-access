#!/usr/bin/env python3
"""
Data Load 5: Minneapolis Active Rental Licenses → ba_contacts
Source: Minneapolis Open Data - Active Rental Licenses ArcGIS API
Fields: ownerName, ownerPhone, ownerEmail, applicantName, applicantPhone, applicantEmail
Match: by address to existing ba_buildings in Minneapolis
"""

import requests
import json
import re
import time
from collections import Counter

SUPABASE_URL = 'https://jvecvoqzyxsicsrrpvyu.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg'
MPLS_RENTAL_URL = 'https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/Active_Rental_Licenses/FeatureServer/0'

headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}

def normalize_address(addr):
    """Normalize address for matching"""
    if not addr:
        return ''
    addr = addr.upper().strip()
    # Remove unit/apt suffixes for matching
    addr = re.sub(r'\s*(APT|UNIT|STE|SUITE|#)\s*\S*$', '', addr)
    # Normalize directionals
    addr = re.sub(r'\bNORTH\b', 'N', addr)
    addr = re.sub(r'\bSOUTH\b', 'S', addr)
    addr = re.sub(r'\bEAST\b', 'E', addr)
    addr = re.sub(r'\bWEST\b', 'W', addr)
    # Normalize street types
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
    # Remove extra spaces
    addr = re.sub(r'\s+', ' ', addr).strip()
    return addr

def fetch_all_rental_licenses():
    """Fetch all Minneapolis Active Rental Licenses with contact info"""
    all_records = []
    offset = 0
    batch_size = 2000
    
    while True:
        r = requests.get(f'{MPLS_RENTAL_URL}/query', params={
            'where': "(ownerPhone IS NOT NULL AND ownerPhone <> '') OR (ownerEmail IS NOT NULL AND ownerEmail <> '') OR (applicantPhone IS NOT NULL AND applicantPhone <> '')",
            'outFields': 'ownerName,ownerPhone,ownerEmail,ownerAddress1,ownerCity,ownerState,ownerZip,applicantName,applicantPhone,applicantEmail,applicantAddress1,applicantCity,address,licensedUnits,latitude,longitude',
            'resultOffset': offset,
            'resultRecordCount': batch_size,
            'f': 'json'
        }, timeout=30)
        
        if not r.ok:
            print(f'  API error at offset {offset}: {r.status_code}')
            break
            
        data = r.json()
        features = data.get('features', [])
        if not features:
            break
            
        all_records.extend([f['attributes'] for f in features])
        print(f'  Fetched {len(all_records)} rental license records...')
        
        if len(features) < batch_size:
            break
        offset += batch_size
        time.sleep(0.3)
    
    return all_records

def fetch_mpls_buildings():
    """Fetch all Minneapolis buildings from Supabase"""
    all_buildings = []
    offset = 0
    batch_size = 1000
    
    while True:
        r = requests.get(f'{SUPABASE_URL}/rest/v1/ba_buildings', params={
            'select': 'id,address,city,building_type,unit_count',
            'city': 'eq.Minneapolis',
            'limit': batch_size,
            'offset': offset
        }, headers={
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}'
        })
        
        if not r.ok or not r.json():
            break
        
        batch = r.json()
        all_buildings.extend(batch)
        if len(batch) < batch_size:
            break
        offset += batch_size
    
    return all_buildings

def fetch_existing_contacts():
    """Fetch all existing contact building_ids to avoid duplicates"""
    existing = set()
    offset = 0
    batch_size = 1000
    
    while True:
        r = requests.get(f'{SUPABASE_URL}/rest/v1/ba_contacts', params={
            'select': 'building_id,source',
            'source': 'eq.mpls-rental-license',
            'limit': batch_size,
            'offset': offset
        }, headers={
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}'
        })
        
        if not r.ok or not r.json():
            break
        
        batch = r.json()
        for c in batch:
            existing.add(c['building_id'])
        if len(batch) < batch_size:
            break
        offset += batch_size
    
    return existing

def insert_contacts(contacts):
    """Insert contacts in batches"""
    total = 0
    batch_size = 500
    
    for i in range(0, len(contacts), batch_size):
        batch = contacts[i:i+batch_size]
        r = requests.post(
            f'{SUPABASE_URL}/rest/v1/ba_contacts',
            headers=headers,
            json=batch
        )
        if r.ok:
            total += len(batch)
            print(f'  Inserted {total}/{len(contacts)} contacts')
        else:
            print(f'  Error inserting batch: {r.status_code} - {r.text[:200]}')
        time.sleep(0.2)
    
    return total

def main():
    print("=== Data Load 5: Minneapolis Active Rental Licenses → Contacts ===\n")
    
    # 1. Fetch rental license data
    print("1. Fetching Minneapolis rental license data...")
    licenses = fetch_all_rental_licenses()
    print(f"   Got {len(licenses)} licenses with contact info\n")
    
    # 2. Fetch existing Minneapolis buildings
    print("2. Fetching Minneapolis buildings from DB...")
    buildings = fetch_mpls_buildings()
    print(f"   Got {len(buildings)} buildings\n")
    
    # 3. Build address lookup index
    print("3. Building address lookup index...")
    addr_to_building = {}
    for b in buildings:
        norm = normalize_address(b['address'])
        if norm:
            # Store first match (could be multiple buildings at same address)
            if norm not in addr_to_building:
                addr_to_building[norm] = b
    print(f"   {len(addr_to_building)} unique addresses indexed\n")
    
    # 4. Check existing contacts to avoid dupes
    print("4. Checking existing contacts...")
    existing_contact_bids = fetch_existing_contacts()
    print(f"   {len(existing_contact_bids)} buildings already have mpls-rental-license contacts\n")
    
    # 5. Match and create contacts
    print("5. Matching licenses to buildings and creating contacts...")
    new_contacts = []
    matched = 0
    unmatched = 0
    skipped_existing = 0
    stats = Counter()
    
    for lic in licenses:
        lic_addr = normalize_address(lic.get('address', ''))
        if not lic_addr:
            continue
        
        building = addr_to_building.get(lic_addr)
        if not building:
            unmatched += 1
            continue
        
        bid = building['id']
        if bid in existing_contact_bids:
            skipped_existing += 1
            continue
        
        matched += 1
        existing_contact_bids.add(bid)  # Prevent dupes within this run
        
        owner_name = lic.get('ownerName', '').strip() if lic.get('ownerName') else None
        owner_phone = lic.get('ownerPhone', '').strip() if lic.get('ownerPhone') else None
        owner_email = lic.get('ownerEmail', '').strip() if lic.get('ownerEmail') else None
        app_name = lic.get('applicantName', '').strip() if lic.get('applicantName') else None
        app_phone = lic.get('applicantPhone', '').strip() if lic.get('applicantPhone') else None
        app_email = lic.get('applicantEmail', '').strip() if lic.get('applicantEmail') else None
        
        # Add owner contact if has name + (phone or email)
        if owner_name and (owner_phone or owner_email):
            owner_addr_parts = [lic.get('ownerAddress1',''), lic.get('ownerCity',''), lic.get('ownerState',''), lic.get('ownerZip','')]
            owner_addr_str = ' '.join(p.strip() for p in owner_addr_parts if p and p.strip())
            new_contacts.append({
                'building_id': bid,
                'role': 'property-owner',
                'name': owner_name,
                'phone': owner_phone,
                'email': owner_email,
                'source': 'mpls-rental-license',
                'confidence': 'official',
                'notes': f"Owner address: {owner_addr_str}" if owner_addr_str else None
            })
            stats['owner'] += 1
        
        # Add applicant/manager if different from owner
        if app_name and app_name != owner_name and (app_phone or app_email):
            new_contacts.append({
                'building_id': bid,
                'role': 'property-manager',
                'name': app_name,
                'phone': app_phone,
                'email': app_email,
                'source': 'mpls-rental-license',
                'confidence': 'official',
                'notes': None
            })
            stats['manager'] += 1
        elif app_name and app_name == owner_name and app_phone != owner_phone:
            # Same person, different phone - add note about alt number
            pass  # Already captured as owner
    
    print(f"   Matched: {matched} | Unmatched: {unmatched} | Skipped (existing): {skipped_existing}")
    print(f"   New contacts to insert: {len(new_contacts)}")
    print(f"   Breakdown: {dict(stats)}\n")
    
    # 6. Insert contacts
    if new_contacts:
        print("6. Inserting contacts into ba_contacts...")
        inserted = insert_contacts(new_contacts)
        print(f"   Inserted {inserted} contacts\n")
    else:
        print("6. No new contacts to insert\n")
    
    # 7. Skip individual owner_name updates (too slow per-record)
    # Owner names are already on most buildings from assessor data
    update_count = 0
    
    print("=== Done! ===")
    print(f"Summary:")
    print(f"  Rental licenses processed: {len(licenses)}")
    print(f"  Buildings matched: {matched}")
    print(f"  Contacts created: {len(new_contacts)}")
    print(f"  Owner names updated: {update_count}")

if __name__ == '__main__':
    main()
