import { buildings, contacts, buildingNotes, type Building, type InsertBuilding, type Contact, type InsertContact, type BuildingNote, type InsertBuildingNote } from "@shared/schema";
import { db } from "./db";
import { eq, like, and, or, sql } from "drizzle-orm";

export interface IStorage {
  // Buildings
  getBuildings(bounds?: { north: number; south: number; east: number; west: number }, typeFilter?: string): Building[];
  getBuilding(id: number): Building | undefined;
  searchBuildings(query: string): Building[];
  createBuilding(data: InsertBuilding): Building;
  bulkCreateBuildings(data: InsertBuilding[]): number;

  // Contacts
  getContactsByBuilding(buildingId: number): Contact[];
  createContact(data: InsertContact): Contact;
  verifyContact(id: number): Contact | undefined;
  deleteContact(id: number): void;

  // Building Notes
  getNotesByBuilding(buildingId: number): BuildingNote[];
  createNote(data: InsertBuildingNote): BuildingNote;
  upvoteNote(id: number): BuildingNote | undefined;
  deleteNote(id: number): void;

  // Stats
  getStats(): { buildings: number; contacts: number; notes: number };
}

export class DatabaseStorage implements IStorage {
  getBuildings(bounds?: { north: number; south: number; east: number; west: number }, typeFilter?: string): Building[] {
    let query = db.select().from(buildings);
    if (bounds) {
      query = query.where(
        and(
          sql`${buildings.lat} >= ${bounds.south}`,
          sql`${buildings.lat} <= ${bounds.north}`,
          sql`${buildings.lng} >= ${bounds.west}`,
          sql`${buildings.lng} <= ${bounds.east}`,
        )
      ) as any;
    }
    if (typeFilter && typeFilter !== "all") {
      query = query.where(eq(buildings.buildingType, typeFilter)) as any;
    }
    return query.all();
  }

  getBuilding(id: number): Building | undefined {
    return db.select().from(buildings).where(eq(buildings.id, id)).get();
  }

  searchBuildings(query: string): Building[] {
    const q = `%${query}%`;
    return db.select().from(buildings).where(
      or(
        like(buildings.address, q),
        like(buildings.name, q),
        like(buildings.ownerName, q),
        like(buildings.city, q),
      )
    ).limit(50).all();
  }

  createBuilding(data: InsertBuilding): Building {
    return db.insert(buildings).values(data).returning().get();
  }

  bulkCreateBuildings(data: InsertBuilding[]): number {
    if (data.length === 0) return 0;
    // Insert in batches of 100
    let count = 0;
    for (let i = 0; i < data.length; i += 100) {
      const batch = data.slice(i, i + 100);
      db.insert(buildings).values(batch).run();
      count += batch.length;
    }
    return count;
  }

  getContactsByBuilding(buildingId: number): Contact[] {
    return db.select().from(contacts).where(eq(contacts.buildingId, buildingId)).all();
  }

  createContact(data: InsertContact): Contact {
    return db.insert(contacts).values({
      ...data,
      createdAt: new Date().toISOString(),
    }).returning().get();
  }

  verifyContact(id: number): Contact | undefined {
    const existing = db.select().from(contacts).where(eq(contacts.id, id)).get();
    if (!existing) return undefined;
    db.update(contacts).set({
      verifiedCount: (existing.verifiedCount || 0) + 1,
      confidence: "verified",
      lastVerified: new Date().toISOString(),
    }).where(eq(contacts.id, id)).run();
    return db.select().from(contacts).where(eq(contacts.id, id)).get();
  }

  deleteContact(id: number): void {
    db.delete(contacts).where(eq(contacts.id, id)).run();
  }

  getNotesByBuilding(buildingId: number): BuildingNote[] {
    return db.select().from(buildingNotes).where(eq(buildingNotes.buildingId, buildingId)).all();
  }

  createNote(data: InsertBuildingNote): BuildingNote {
    return db.insert(buildingNotes).values({
      ...data,
      createdAt: new Date().toISOString(),
    }).returning().get();
  }

  upvoteNote(id: number): BuildingNote | undefined {
    const existing = db.select().from(buildingNotes).where(eq(buildingNotes.id, id)).get();
    if (!existing) return undefined;
    db.update(buildingNotes).set({
      upvotes: (existing.upvotes || 0) + 1,
    }).where(eq(buildingNotes.id, id)).run();
    return db.select().from(buildingNotes).where(eq(buildingNotes.id, id)).get();
  }

  deleteNote(id: number): void {
    db.delete(buildingNotes).where(eq(buildingNotes.id, id)).run();
  }

  getStats(): { buildings: number; contacts: number; notes: number } {
    const b = db.select({ count: sql<number>`count(*)` }).from(buildings).get();
    const c = db.select({ count: sql<number>`count(*)` }).from(contacts).get();
    const n = db.select({ count: sql<number>`count(*)` }).from(buildingNotes).get();
    return {
      buildings: b?.count || 0,
      contacts: c?.count || 0,
      notes: n?.count || 0,
    };
  }
}

export const storage = new DatabaseStorage();
