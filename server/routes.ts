import type { Express } from "express";
import type { Server } from "http";
import { storage } from "./storage";
import { insertBuildingSchema, insertContactSchema, insertBuildingNoteSchema } from "@shared/schema";

export async function registerRoutes(server: Server, app: Express) {
  // Get buildings within map bounds
  app.get("/api/buildings", (req, res) => {
    const { north, south, east, west, type } = req.query;
    let bounds;
    if (north && south && east && west) {
      bounds = {
        north: parseFloat(north as string),
        south: parseFloat(south as string),
        east: parseFloat(east as string),
        west: parseFloat(west as string),
      };
    }
    const buildings = storage.getBuildings(bounds, type as string);
    res.json(buildings);
  });

  // Search buildings
  app.get("/api/buildings/search", (req, res) => {
    const { q } = req.query;
    if (!q || typeof q !== "string") {
      return res.json([]);
    }
    const results = storage.searchBuildings(q);
    res.json(results);
  });

  // Get single building with contacts and notes
  app.get("/api/buildings/:id", (req, res) => {
    const id = parseInt(req.params.id);
    const building = storage.getBuilding(id);
    if (!building) {
      return res.status(404).json({ message: "Building not found" });
    }
    const contacts = storage.getContactsByBuilding(id);
    const notes = storage.getNotesByBuilding(id);
    res.json({ building, contacts, notes });
  });

  // Seed buildings from MetroGIS API (proxy to avoid CORS)
  app.get("/api/seed/metrogis", async (req, res) => {
    const { layer = "3", count = "200" } = req.query;
    try {
      // Query non-residential parcels from MetroGIS
      const url = `https://arcgis.metc.state.mn.us/data1/rest/services/parcels/Parcel_Points/FeatureServer/${layer}/query?where=USECLASS1+LIKE+%27%25Apartment%25%27+OR+USECLASS1+LIKE+%27%25Commercial%25%27+OR+USECLASS1+LIKE+%27%25Industrial%25%27+OR+USECLASS1+LIKE+%27%25Exempt%25%27&outFields=ANUMBER,ST_NAME,ST_POS_TYP,ST_PRE_DIR,CTU_NAME,ZIP,OWNER_NAME,TAX_NAME,USECLASS1,DWELL_TYPE,NUM_UNITS,EMV_TOTAL,YEAR_BUILT,FIN_SQ_FT&returnGeometry=true&outSR=4326&resultRecordCount=${count}&f=json`;
      const response = await fetch(url);
      const data = await response.json();

      if (!data.features || data.features.length === 0) {
        return res.json({ seeded: 0, message: "No features returned" });
      }

      const newBuildings = data.features
        .filter((f: any) => f.geometry && f.attributes)
        .map((f: any) => {
          const a = f.attributes;
          const parts = [a.ANUMBER, a.ST_PRE_DIR, a.ST_NAME, a.ST_POS_TYP].filter(Boolean);
          return {
            address: parts.join(" "),
            city: a.CTU_NAME || "Unknown",
            state: "MN",
            zip: a.ZIP || null,
            lat: f.geometry.y,
            lng: f.geometry.x,
            buildingType: classifyUseCode(a.USECLASS1),
            unitCount: a.NUM_UNITS || null,
            yearBuilt: a.YEAR_BUILT || null,
            sqft: a.FIN_SQ_FT || null,
            ownerName: a.OWNER_NAME || null,
            taxpayerName: a.TAX_NAME || null,
            estimatedValue: a.EMV_TOTAL || null,
            dataSource: "metrogis",
            sourceId: `metrogis-${layer}-${a.ANUMBER}-${a.ST_NAME}`,
            lastDataSync: new Date().toISOString(),
          };
        });

      const count_inserted = storage.bulkCreateBuildings(newBuildings);
      res.json({ seeded: count_inserted });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  // Seed from HUD multifamily
  app.get("/api/seed/hud", async (req, res) => {
    try {
      const url = `https://egis.hud.gov/arcgis/rest/services/cpdmaps/HudMfProps/MapServer/1/query?where=STD_ST%3D%27MN%27+AND+CBSA_NM+LIKE+%27%25Minneapolis%25%27&outFields=PROPERTY_NAME_TEXT,STD_ADDR,STD_CITY,STD_ST,STD_ZIP5,LAT,LON,TOTAL_UNIT_COUNT,PROPERTY_ON_SITE_PHONE_NUMBER,MGMT_AGENT_ORG_NAME,MGMT_CONTACT_FULL_NAME,MGMT_CONTACT_MAIN_PHN_NBR,MGMT_CONTACT_EMAIL_TEXT,MGMT_CONTACT_INDV_TITLE_TEXT,PROPERTY_CATEGORY_NAME&returnGeometry=false&resultRecordCount=500&f=json`;
      const response = await fetch(url);
      const data = await response.json();

      if (!data.features || data.features.length === 0) {
        return res.json({ seeded: 0, contacts: 0 });
      }

      let buildingsSeeded = 0;
      let contactsSeeded = 0;

      for (const f of data.features) {
        const a = f.attributes;
        if (!a.LAT || !a.LON) continue;

        const building = storage.createBuilding({
          name: a.PROPERTY_NAME_TEXT || null,
          address: a.STD_ADDR || "Unknown",
          city: a.STD_CITY || "Unknown",
          state: "MN",
          zip: a.STD_ZIP5 || null,
          lat: a.LAT,
          lng: a.LON,
          buildingType: "apartment",
          unitCount: a.TOTAL_UNIT_COUNT || null,
          dataSource: "hud",
          sourceId: `hud-${a.PROPERTY_NAME_TEXT}`,
          lastDataSync: new Date().toISOString(),
        });
        buildingsSeeded++;

        // Add on-site phone contact
        if (a.PROPERTY_ON_SITE_PHONE_NUMBER) {
          storage.createContact({
            buildingId: building.id,
            role: "leasing-office",
            phone: a.PROPERTY_ON_SITE_PHONE_NUMBER,
            source: "hud",
            confidence: "unverified",
          });
          contactsSeeded++;
        }

        // Add management contact
        if (a.MGMT_CONTACT_FULL_NAME || a.MGMT_CONTACT_MAIN_PHN_NBR) {
          storage.createContact({
            buildingId: building.id,
            role: "property-manager",
            name: a.MGMT_CONTACT_FULL_NAME || null,
            phone: a.MGMT_CONTACT_MAIN_PHN_NBR || null,
            email: a.MGMT_CONTACT_EMAIL_TEXT || null,
            title: a.MGMT_CONTACT_INDV_TITLE_TEXT || null,
            notes: a.MGMT_AGENT_ORG_NAME ? `Mgmt Co: ${a.MGMT_AGENT_ORG_NAME}` : null,
            source: "hud",
            confidence: "unverified",
          });
          contactsSeeded++;
        }
      }

      res.json({ seeded: buildingsSeeded, contacts: contactsSeeded });
    } catch (err: any) {
      res.status(500).json({ message: err.message });
    }
  });

  // Add contact
  app.post("/api/contacts", (req, res) => {
    const parsed = insertContactSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ message: parsed.error.message });
    }
    const contact = storage.createContact(parsed.data);
    res.json(contact);
  });

  // Verify contact
  app.post("/api/contacts/:id/verify", (req, res) => {
    const id = parseInt(req.params.id);
    const contact = storage.verifyContact(id);
    if (!contact) return res.status(404).json({ message: "Contact not found" });
    res.json(contact);
  });

  // Delete contact
  app.delete("/api/contacts/:id", (req, res) => {
    const id = parseInt(req.params.id);
    storage.deleteContact(id);
    res.json({ ok: true });
  });

  // Add building note
  app.post("/api/notes", (req, res) => {
    const parsed = insertBuildingNoteSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ message: parsed.error.message });
    }
    const note = storage.createNote(parsed.data);
    res.json(note);
  });

  // Upvote note
  app.post("/api/notes/:id/upvote", (req, res) => {
    const id = parseInt(req.params.id);
    const note = storage.upvoteNote(id);
    if (!note) return res.status(404).json({ message: "Note not found" });
    res.json(note);
  });

  // Stats
  app.get("/api/stats", (_req, res) => {
    res.json(storage.getStats());
  });
}

function classifyUseCode(useClass: string | null): string {
  if (!useClass) return "commercial";
  const u = useClass.toLowerCase();
  if (u.includes("apartment")) return "apartment";
  if (u.includes("commercial") || u.includes("retail")) return "commercial";
  if (u.includes("industrial")) return "industrial";
  if (u.includes("office")) return "office";
  if (u.includes("exempt")) return "government";
  if (u.includes("hospital") || u.includes("medical")) return "hospital";
  if (u.includes("school") || u.includes("education")) return "school";
  return "commercial";
}
