#!/usr/bin/env python3
"""Seed Supabase ba_* tables from SQLite data."""

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

def get_rows(table, exclude_id=True):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(f"SELECT * FROM {table}")
    rows = []
    for row in cursor.fetchall():
        d = dict(row)
        if exclude_id:
            d.pop("id", None)
        rows.append(d)
    conn.close()
    return rows

def insert_batch(table, rows):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    resp = requests.post(url, headers=HEADERS, json=rows)
    return resp

def seed_buildings():
    print("Seeding ba_buildings...")
    rows = get_rows("buildings")
    print(f"  Total: {len(rows)} rows")
    inserted_ids = []
    
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        resp = insert_batch("ba_buildings", batch)
        if resp.status_code in (200, 201):
            data = resp.json()
            inserted_ids.extend([r["id"] for r in data])
            print(f"  Batch {i//BATCH_SIZE + 1}: inserted {len(data)} rows (IDs {data[0]['id']} - {data[-1]['id']})")
        else:
            print(f"  Batch {i//BATCH_SIZE + 1} ERROR: {resp.status_code} - {resp.text[:200]}")
            return None
        time.sleep(0.1)
    
    print(f"  Done: {len(inserted_ids)} buildings inserted")
    return inserted_ids

def get_building_id_map():
    """Map old SQLite IDs to new Supabase IDs by reading buildings back."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Get original buildings ordered by their sqlite rowid
    sqlite_buildings = conn.execute("SELECT id, address, lat, lng FROM buildings ORDER BY id").fetchall()
    conn.close()
    
    # Get Supabase buildings
    url = f"{SUPABASE_URL}/rest/v1/ba_buildings"
    resp = requests.get(url, headers=HEADERS, params={"select": "id,address,lat,lng", "order": "id.asc", "limit": 2000})
    if resp.status_code != 200:
        print(f"Failed to get buildings: {resp.status_code} - {resp.text[:200]}")
        return {}
    
    supabase_buildings = resp.json()
    print(f"  Got {len(supabase_buildings)} Supabase buildings vs {len(sqlite_buildings)} SQLite buildings")
    
    # Build mapping: (address, lat, lng) -> supabase_id
    supabase_map = {}
    for b in supabase_buildings:
        key = (b["address"], b["lat"], b["lng"])
        supabase_map[key] = b["id"]
    
    # Map sqlite_id -> supabase_id
    id_map = {}
    for b in sqlite_buildings:
        key = (b["address"], b["lat"], b["lng"])
        if key in supabase_map:
            id_map[b["id"]] = supabase_map[key]
        else:
            print(f"  WARNING: No match for building ID {b['id']}: {b['address']}")
    
    print(f"  Mapped {len(id_map)} of {len(sqlite_buildings)} building IDs")
    return id_map

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
        old_id = d.pop("id")
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
            print(f"  Batch {i//BATCH_SIZE + 1}: inserted {len(data)} rows")
        else:
            print(f"  Batch {i//BATCH_SIZE + 1} ERROR: {resp.status_code} - {resp.text[:200]}")
        time.sleep(0.1)
    
    print(f"  Done: {inserted} contacts inserted")
    return inserted

def seed_notes(id_map):
    print("Seeding ba_building_notes...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM building_notes").fetchall()
    conn.close()
    
    mapped_rows = []
    for row in rows:
        d = dict(row)
        old_id = d.pop("id")
        old_building_id = d["building_id"]
        new_building_id = id_map.get(old_building_id)
        if new_building_id is None:
            print(f"  SKIP note (no building mapping for building_id={old_building_id})")
            continue
        d["building_id"] = new_building_id
        mapped_rows.append(d)
    
    if not mapped_rows:
        print("  No notes to insert")
        return 0
    
    resp = insert_batch("ba_building_notes", mapped_rows)
    if resp.status_code in (200, 201):
        data = resp.json()
        print(f"  Done: {len(data)} notes inserted")
        return len(data)
    else:
        print(f"  ERROR: {resp.status_code} - {resp.text[:200]}")
        return 0

def main():
    print("=" * 50)
    print("Supabase Building Access Seed Script")
    print("=" * 50)
    
    # Check current state
    count_headers = {**HEADERS, "Prefer": "count=exact"}
    count_resp = requests.get(f"{SUPABASE_URL}/rest/v1/ba_buildings", 
                              headers=count_headers,
                              params={"limit": 0})
    content_range = count_resp.headers.get("Content-Range", "")
    print(f"Current ba_buildings count: {content_range}")
    
    # Seed buildings
    building_ids = seed_buildings()
    if building_ids is None:
        print("FAILED to seed buildings. Aborting.")
        sys.exit(1)
    
    # Build ID map
    print("\nBuilding ID mapping...")
    id_map = get_building_id_map()
    
    if not id_map:
        print("FAILED to build ID map. Aborting contacts/notes seeding.")
        sys.exit(1)
    
    # Seed contacts
    print()
    contacts_count = seed_contacts(id_map)
    
    # Seed notes
    print()
    notes_count = seed_notes(id_map)
    
    print("\n" + "=" * 50)
    print("SEED COMPLETE")
    print(f"  Buildings: {len(building_ids)}")
    print(f"  Contacts:  {contacts_count}")
    print(f"  Notes:     {notes_count}")
    print("=" * 50)

if __name__ == "__main__":
    main()
