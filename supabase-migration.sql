-- Building Access Tool - Supabase Migration
-- Run this in the Supabase SQL Editor if automatic migration fails

CREATE TABLE IF NOT EXISTS ba_buildings (
  id BIGSERIAL PRIMARY KEY,
  name TEXT,
  address TEXT NOT NULL,
  city TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'MN',
  zip TEXT,
  lat DOUBLE PRECISION NOT NULL,
  lng DOUBLE PRECISION NOT NULL,
  building_type TEXT,
  unit_count INTEGER,
  year_built INTEGER,
  sqft INTEGER,
  owner_name TEXT,
  owner_address TEXT,
  taxpayer_name TEXT,
  estimated_value INTEGER,
  data_source TEXT,
  source_id TEXT,
  last_data_sync TEXT
);

CREATE TABLE IF NOT EXISTS ba_contacts (
  id BIGSERIAL PRIMARY KEY,
  building_id BIGINT NOT NULL REFERENCES ba_buildings(id),
  role TEXT NOT NULL,
  name TEXT,
  phone TEXT,
  email TEXT,
  title TEXT,
  notes TEXT,
  source TEXT,
  confidence TEXT DEFAULT 'unverified',
  verified_count INTEGER DEFAULT 0,
  contributed_by TEXT,
  created_at TEXT,
  last_verified TEXT
);

CREATE TABLE IF NOT EXISTS ba_building_notes (
  id BIGSERIAL PRIMARY KEY,
  building_id BIGINT NOT NULL REFERENCES ba_buildings(id),
  note_type TEXT NOT NULL,
  content TEXT NOT NULL,
  contributed_by TEXT,
  upvotes INTEGER DEFAULT 0,
  created_at TEXT,
  last_verified TEXT
);

-- RLS policies: allow public read, authenticated write
ALTER TABLE ba_buildings ENABLE ROW LEVEL SECURITY;
ALTER TABLE ba_contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE ba_building_notes ENABLE ROW LEVEL SECURITY;

-- Public read for all
CREATE POLICY "ba_buildings_read" ON ba_buildings FOR SELECT USING (true);
CREATE POLICY "ba_contacts_read" ON ba_contacts FOR SELECT USING (true);
CREATE POLICY "ba_building_notes_read" ON ba_building_notes FOR SELECT USING (true);

-- Anon can also write (community tool, open source)
CREATE POLICY "ba_buildings_insert" ON ba_buildings FOR INSERT WITH CHECK (true);
CREATE POLICY "ba_contacts_insert" ON ba_contacts FOR INSERT WITH CHECK (true);
CREATE POLICY "ba_contacts_delete" ON ba_contacts FOR DELETE USING (true);
CREATE POLICY "ba_building_notes_insert" ON ba_building_notes FOR INSERT WITH CHECK (true);
CREATE POLICY "ba_contacts_update" ON ba_contacts FOR UPDATE USING (true);
CREATE POLICY "ba_building_notes_update" ON ba_building_notes FOR UPDATE USING (true);
