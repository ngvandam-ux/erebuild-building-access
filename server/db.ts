import { drizzle } from "drizzle-orm/better-sqlite3";
import Database from "better-sqlite3";
import * as schema from "@shared/schema";

const sqlite = new Database("./data.db");

// Enable WAL mode for better concurrent read performance
sqlite.pragma("journal_mode = WAL");

// Push schema (create tables if they don't exist)
sqlite.exec(`
  CREATE TABLE IF NOT EXISTS buildings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    address TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'MN',
    zip TEXT,
    lat REAL NOT NULL,
    lng REAL NOT NULL,
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

  CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    building_id INTEGER NOT NULL,
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

  CREATE TABLE IF NOT EXISTS building_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    building_id INTEGER NOT NULL,
    note_type TEXT NOT NULL,
    content TEXT NOT NULL,
    contributed_by TEXT,
    upvotes INTEGER DEFAULT 0,
    created_at TEXT,
    last_verified TEXT
  );
`);

export const db = drizzle(sqlite, { schema });
