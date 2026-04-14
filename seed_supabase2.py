#!/usr/bin/env python3
"""Seed Supabase ba_* tables from SQLite data.
Version 2: Uses sequential ID mapping since buildings are inserted in order.
"""

import sqlite3
import json
import requests
import time
import sys

SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co"
ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg"

HEADERS = {
    "apikey": ANON_KEY,
    "Authorization": f"Bearer {ANON_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

DB_PATH = "/home/user/workspace/building-access/data.db"
BATCH_SIZE = 200

def get_all_supabase_buildings():
    """Fetch all buildings from Supabase, paginating if needed."""
    all_buildings = []
    offset = 0
    limit = 1000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/ba_buildings"
        resp = requests.get(url, headers={**HEADERS, "Prefer": "return=representation"}, 
                           params={"select": "id,address,lat,lng", 
                                   "order": "id.asc", 
                                   "limit": limit, 
                                   "offset": offset})
        if resp.status_code != 200:
            print(f"ERROR fetching buildings at offset {offset}: {resp.status_code}")
            break
        batch = resp.json()
        all_buildings.extend(batch)
        print(f"  Fetched {len(all_buildings)} buildings so far...")
        if len(batch) < limit:
            break
        offset += limit
    return all_buildings

def get_sqlite_buildings():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, address, lat, lng FROM buildings ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def check_existing_buildings():
    """Check if buildings already exist."""
    url = f"{SUPABASE_URL}/rest/v1/ba_buildings"
    resp = requests.get(url, headers={**HEADERS, "Prefer": "count=exact"},
                       params={"limit": 0})
    cr = resp.headers.get("Content-Range", "*/0")
    count = int(cr.split("/")[1]) if "/" in cr else 0
    return count

def delete_existing_data():
    """Delete all existing ba_ data to start fresh."""
    print("Deleting existing data...")
    
    # Delete contacts first (foreign key)
    url = f"{SUPABASE_URL}/rest/v1/ba_contacts"
    resp = requests.delete(url, headers={**HEADERS, "Prefer": "return=minimal"},
                          params={"id": "gte.0"})
    print(f"  Delete contacts: {resp.status_code}")
    
    # Delete notes
    url = f"{SUPABASE_URL}/rest/v1/ba_building_notes"
    resp = requests.delete(url, headers={**HEADERS, "Prefer": "return=minimal"},
                          params={"id": "gte.0"})
    print(f"  Delete notes: {resp.status_code}")
    
    # Delete buildings
    url = f"{SUPABASE_URL}/rest/v1/ba_buildings"
    resp = requests.delete(url, headers={**HEADERS, "Prefer": "return=minimal"},
                          params={"id": "gte.0"})
    print(f"  Delete buildings: {resp.status_code}")

def insert_batch(table, rows):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    resp = requests.post(url, headers=HEADERS, json=rows)
    return resp

def seed_buildings():
    """Insert all buildings and return mapping of sqlite_id -> supabase_id."""
    print("Seeding ba_buildings...")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    sqlite_rows = conn.execute("SELECT * FROM buildings ORDER BY id").fetchall()
    conn.close()
    
    sqlite_id_order = [dict(r)["id"] for r in sqlite_rows]
    rows_to_insert = []
    for r in sqlite_rows:
        d = dict(r)
        d.pop("id")
        rows_to_insert.append(d)
    
    print(f"  Total: {len(rows_to_insert)} rows")
    
    all_inserted = []
    for i in range(0, len(rows_to_insert), BATCH_SIZE):
        batch = rows_to_insert[i:i+BATCH_SIZE]
        resp = insert_batch("ba_buildings", batch)
        if resp.status_code in (200, 201):
            data = resp.json()
            all_inserted.extend(data)
            print(f"  Batch {i//BATCH_SIZE + 1}: inserted {len(data)} rows (IDs {data[0]['id']} - {data[-1]['id']})")
        else:
            print(f"  Batch {i//BATCH_SIZE + 1} ERROR: {resp.status_code} - {resp.text[:300]}")
            return None, None
        time.sleep(0.05)
    
    # Build ID map: sqlite_id -> supabase_id
    # Inserted rows are in same order as sqlite rows (ordered by sqlite id)
    id_map = {}
    for sqlite_id, supabase_row in zip(sqlite_id_order, all_inserted):
        id_map[sqlite_id] = supabase_row["id"]
    
    print(f"  Done: {len(all_inserted)} buildings inserted, {len(id_map)} ID mappings built")
    return all_inserted, id_map

def seed_contacts(id_map):
    print("Seeding ba_contacts...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM contacts").fetchall()
    conn.close()
    
    mapped_rows = []
    skipped = 0
    for row in rows:
        d = dict(row)
        d.pop("id")
        old_building_id = d["building_id"]
        new_building_id = id_map.get(old_building_id)
        if new_building_id is None:
            print(f"  SKIP contact (no building mapping for building_id={old_building_id})")
            skipped += 1
            continue
        d["building_id"] = new_building_id
        mapped_rows.append(d)
    
    print(f"  Total: {len(mapped_rows)} rows (skipped {skipped})")
    
    inserted = 0
    for i in range(0, len(mapped_rows), BATCH_SIZE):
        batch = mapped_rows[i:i+BATCH_SIZE]
        resp = insert_batch("ba_contacts", batch)
        if resp.status_code in (200, 201):
            data = resp.json()
            inserted += len(data)
            print(f"  Batch {i//BATCH_SIZE + 1}: inserted {len(data)} contacts")
        else:
            print(f"  Batch {i//BATCH_SIZE + 1} ERROR: {resp.status_code} - {resp.text[:300]}")
        time.sleep(0.05)
    
    print(f"  Done: {inserted} contacts inserted")
    return inserted

def seed_notes(id_map):
    print("Seeding ba_building_notes...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM building_notes").fetchall()
    conn.close()
    
    mapped_rows = []
    skipped = 0
    for row in rows:
        d = dict(row)
        d.pop("id")
        old_building_id = d["building_id"]
        new_building_id = id_map.get(old_building_id)
        if new_building_id is None:
            print(f"  SKIP note (no building mapping for building_id={old_building_id})")
            skipped += 1
            continue
        d["building_id"] = new_building_id
        mapped_rows.append(d)
    
    print(f"  Total: {len(mapped_rows)} notes (skipped {skipped})")
    
    if not mapped_rows:
        print("  No notes to insert")
        return 0
    
    resp = insert_batch("ba_building_notes", mapped_rows)
    if resp.status_code in (200, 201):
        data = resp.json()
        print(f"  Done: {len(data)} notes inserted")
        return len(data)
    else:
        print(f"  ERROR: {resp.status_code} - {resp.text[:300]}")
        return 0

def main():
    print("=" * 60)
    print("Supabase Building Access Seed Script v2")
    print("=" * 60)
    
    # Check current state
    existing_count = check_existing_buildings()
    print(f"Current ba_buildings count: {existing_count}")
    
    if existing_count > 0:
        print(f"\nExisting data found ({existing_count} buildings). Deleting and re-seeding...")
        delete_existing_data()
        time.sleep(1)
        
        # Reset sequences
        print("Waiting for deletion to complete...")
        time.sleep(2)
    
    # Seed buildings
    print()
    building_rows, id_map = seed_buildings()
    if building_rows is None:
        print("FAILED to seed buildings. Aborting.")
        sys.exit(1)
    
    # Seed contacts
    print()
    contacts_count = seed_contacts(id_map)
    
    # Seed notes  
    print()
    notes_count = seed_notes(id_map)
    
    # Final verification
    print("\n" + "=" * 60)
    print("VERIFYING FINAL COUNTS...")
    
    for table in ["ba_buildings", "ba_contacts", "ba_building_notes"]:
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        resp = requests.get(url, headers={**HEADERS, "Prefer": "count=exact"}, params={"limit": 0})
        cr = resp.headers.get("Content-Range", "*/0")
        count = cr.split("/")[1] if "/" in cr else "?"
        print(f"  {table}: {count} rows")
    
    print("\n" + "=" * 60)
    print("SEED COMPLETE")
    print(f"  Buildings: {len(building_rows)}")
    print(f"  Contacts:  {contacts_count}")
    print(f"  Notes:     {notes_count}")
    print("=" * 60)
    
    return {
        "buildings": len(building_rows),
        "contacts": contacts_count,
        "notes": notes_count
    }

if __name__ == "__main__":
    result = main()
