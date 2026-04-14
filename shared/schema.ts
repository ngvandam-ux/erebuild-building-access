import { sqliteTable, text, integer, real } from "drizzle-orm/sqlite-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// Buildings - community shared, seeded from public data sources
export const buildings = sqliteTable("buildings", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  name: text("name"), // e.g. "Riverdale Apartments"
  address: text("address").notNull(),
  city: text("city").notNull(),
  state: text("state").notNull().default("MN"),
  zip: text("zip"),
  lat: real("lat").notNull(),
  lng: real("lng").notNull(),
  buildingType: text("building_type"), // apartment, commercial, office, industrial, school, hospital, government, retail, mixed-use
  unitCount: integer("unit_count"),
  yearBuilt: integer("year_built"),
  sqft: integer("sqft"),
  ownerName: text("owner_name"),
  ownerAddress: text("owner_address"),
  taxpayerName: text("taxpayer_name"),
  estimatedValue: integer("estimated_value"),
  dataSource: text("data_source"), // metrogis, hud, rental-license, community
  sourceId: text("source_id"), // external ID for dedup
  lastDataSync: text("last_data_sync"),
});

// Contacts - community contributed, multiple per building
export const contacts = sqliteTable("contacts", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  buildingId: integer("building_id").notNull(),
  role: text("role").notNull(), // on-site-super, property-manager, leasing-office, owner, maintenance, other
  name: text("name"),
  phone: text("phone"),
  email: text("email"),
  title: text("title"),
  notes: text("notes"),
  source: text("source"), // hud, rental-license, google-places, skip-trace, community
  confidence: text("confidence").default("unverified"), // verified, unverified, stale
  verifiedCount: integer("verified_count").default(0),
  contributedBy: text("contributed_by"),
  createdAt: text("created_at"),
  lastVerified: text("last_verified"),
});

// Building notes - field intel from crews
export const buildingNotes = sqliteTable("building_notes", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  buildingId: integer("building_id").notNull(),
  noteType: text("note_type").notNull(), // access-code, riser-location, parking, entrance, general, gate-code, lockbox
  content: text("content").notNull(),
  contributedBy: text("contributed_by"),
  upvotes: integer("upvotes").default(0),
  createdAt: text("created_at"),
  lastVerified: text("last_verified"),
});

// Insert schemas
export const insertBuildingSchema = createInsertSchema(buildings).omit({ id: true });
export const insertContactSchema = createInsertSchema(contacts).omit({ id: true });
export const insertBuildingNoteSchema = createInsertSchema(buildingNotes).omit({ id: true });

// Types
export type Building = typeof buildings.$inferSelect;
export type InsertBuilding = z.infer<typeof insertBuildingSchema>;
export type Contact = typeof contacts.$inferSelect;
export type InsertContact = z.infer<typeof insertContactSchema>;
export type BuildingNote = typeof buildingNotes.$inferSelect;
export type InsertBuildingNote = z.infer<typeof insertBuildingNoteSchema>;
