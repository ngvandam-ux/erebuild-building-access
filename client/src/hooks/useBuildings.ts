import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { supabase } from "@/lib/supabase";

// ─── Types (match Supabase table columns) ───────────────────────────────────
export interface Building {
  id: number;
  name: string | null;
  address: string;
  city: string;
  state: string;
  zip: string | null;
  lat: number;
  lng: number;
  building_type: string | null;
  unit_count: number | null;
  year_built: number | null;
  sqft: number | null;
  owner_name: string | null;
  owner_address: string | null;
  taxpayer_name: string | null;
  estimated_value: number | null;
  data_source: string | null;
  source_id: string | null;
  last_data_sync: string | null;
}

// Lightweight version for map/list (skip heavy fields)
const LIST_FIELDS = "id,name,address,city,state,lat,lng,building_type,unit_count,data_source";

export interface Contact {
  id: number;
  building_id: number;
  role: string;
  name: string | null;
  phone: string | null;
  email: string | null;
  title: string | null;
  notes: string | null;
  source: string | null;
  confidence: string | null;
  verified_count: number;
  contributed_by: string | null;
  created_at: string | null;
  last_verified: string | null;
}

export interface BuildingNote {
  id: number;
  building_id: number;
  note_type: string;
  content: string;
  contributed_by: string | null;
  upvotes: number;
  created_at: string | null;
  last_verified: string | null;
}

export interface MapBounds {
  north: number;
  south: number;
  east: number;
  west: number;
}

export interface BuildingDetail {
  building: Building;
  contacts: Contact[];
  notes: BuildingNote[];
}

export interface Stats {
  buildings: number;
  contacts: number;
  notes: number;
}

// ─── Hooks ──────────────────────────────────────────────────────────────────

// Fetch buildings within map bounds + optional type filter
// Only selects lightweight fields to minimize payload
export function useMapBuildings(bounds: MapBounds | null, type?: string) {
  // Round bounds to 3 decimal places (~100m) to stabilize the query key
  // This prevents refetches on tiny sub-pixel map movements
  const stableBounds = bounds ? {
    north: Math.round(bounds.north * 1000) / 1000,
    south: Math.round(bounds.south * 1000) / 1000,
    east: Math.round(bounds.east * 1000) / 1000,
    west: Math.round(bounds.west * 1000) / 1000,
  } : null;

  return useQuery<Building[]>({
    queryKey: ["buildings", stableBounds, type],
    queryFn: async () => {
      let query = supabase
        .from("ba_buildings")
        .select(LIST_FIELDS)
        .limit(5000);

      if (stableBounds) {
        // Add a small buffer (~0.002 deg ≈ 200m) so markers don't pop at edges
        query = query
          .gte("lat", stableBounds.south - 0.002)
          .lte("lat", stableBounds.north + 0.002)
          .gte("lng", stableBounds.west - 0.002)
          .lte("lng", stableBounds.east + 0.002);
      }

      if (type && type !== "all") {
        query = query.eq("building_type", type);
      }

      const { data, error } = await query;
      if (error) throw error;
      return (data ?? []) as Building[];
    },
    enabled: true,
    staleTime: 30_000,
    // Keep previous data visible while refetching on pan
    placeholderData: (prev) => prev,
  });
}

// Search buildings by text — smart multi-field search
// Searches building name, address, city, zip, owner, taxpayer AND contact names/notes
export function useSearchBuildings(query: string) {
  return useQuery<Building[]>({
    queryKey: ["buildings/search", query],
    queryFn: async () => {
      const q = query.trim();
      if (!q) return [];

      // Phase 1: Search buildings table directly (name, address, city, zip, owner, taxpayer)
      const buildingSearch = supabase
        .from("ba_buildings")
        .select(LIST_FIELDS)
        .or(`name.ilike.%${q}%,address.ilike.%${q}%,city.ilike.%${q}%,zip.ilike.%${q}%,owner_name.ilike.%${q}%,taxpayer_name.ilike.%${q}%`)
        .limit(200);

      // Phase 2: Search contacts table (name, notes) to find building_ids
      const contactSearch = supabase
        .from("ba_contacts")
        .select("building_id")
        .or(`name.ilike.%${q}%,notes.ilike.%${q}%`)
        .limit(500);

      const [bResult, cResult] = await Promise.all([
        buildingSearch,
        contactSearch,
      ]);

      if (bResult.error) throw bResult.error;

      // Merge: start with building matches
      const seen = new Set<number>();
      const merged: Building[] = [];
      for (const b of (bResult.data ?? []) as Building[]) {
        if (!seen.has(b.id)) {
          seen.add(b.id);
          merged.push(b);
        }
      }

      // Add buildings from contact matches (if not already in results)
      if (!cResult.error && cResult.data && cResult.data.length > 0) {
        const contactBids = [...new Set(cResult.data.map((c) => c.building_id))];
        const missingBids = contactBids.filter((id) => !seen.has(id));

        if (missingBids.length > 0) {
          // Fetch those buildings in batches of 50
          for (let i = 0; i < missingBids.length && merged.length < 300; i += 50) {
            const batch = missingBids.slice(i, i + 50);
            const { data: extra } = await supabase
              .from("ba_buildings")
              .select(LIST_FIELDS)
              .in("id", batch);
            if (extra) {
              for (const b of extra as Building[]) {
                if (!seen.has(b.id)) {
                  seen.add(b.id);
                  merged.push(b);
                }
              }
            }
          }
        }
      }

      return merged;
    },
    enabled: query.length >= 2,
    staleTime: 30_000,
  });
}

// Fetch a single building with all contacts and notes
export function useBuildingDetail(id: number | null) {
  return useQuery<BuildingDetail>({
    queryKey: ["buildings", id],
    queryFn: async () => {
      const [bRes, cRes, nRes] = await Promise.all([
        supabase.from("ba_buildings").select("*").eq("id", id!).single(),
        supabase.from("ba_contacts").select("*").eq("building_id", id!).order("verified_count", { ascending: false }),
        supabase.from("ba_building_notes").select("*").eq("building_id", id!).order("upvotes", { ascending: false }),
      ]);
      if (bRes.error) throw bRes.error;
      return {
        building: bRes.data,
        contacts: cRes.data ?? [],
        notes: nRes.data ?? [],
      };
    },
    enabled: id !== null,
    staleTime: 10_000,
  });
}

// Global stats
export function useStats() {
  return useQuery<Stats>({
    queryKey: ["stats"],
    queryFn: async () => {
      const [b, c, n] = await Promise.all([
        supabase.from("ba_buildings").select("id", { count: "exact", head: true }),
        supabase.from("ba_contacts").select("id", { count: "exact", head: true }),
        supabase.from("ba_building_notes").select("id", { count: "exact", head: true }),
      ]);
      return {
        buildings: b.count ?? 0,
        contacts: c.count ?? 0,
        notes: n.count ?? 0,
      };
    },
    staleTime: 60_000,
  });
}

// Add a contact to a building
export function useAddContact() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (data: Record<string, unknown>) => {
      const { data: result, error } = await supabase
        .from("ba_contacts")
        .insert({
          building_id: data.buildingId,
          role: data.role,
          name: data.name || null,
          phone: data.phone || null,
          email: data.email || null,
          title: data.title || null,
          notes: data.notes || null,
          source: "community",
          confidence: "unverified",
          verified_count: 0,
          contributed_by: (data.contributedBy as string) || "anonymous",
          created_at: new Date().toISOString(),
        })
        .select()
        .single();
      if (error) throw error;
      return result;
    },
    onSuccess: (_data, variables) => {
      qc.invalidateQueries({ queryKey: ["buildings", variables.buildingId] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

// Verify a contact
export function useVerifyContact() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async ({ id, buildingId }: { id: number; buildingId: number }) => {
      // Fetch current count, then increment
      const { data: current } = await supabase
        .from("ba_contacts")
        .select("verified_count")
        .eq("id", id)
        .single();
      const newCount = (current?.verified_count ?? 0) + 1;
      const { data, error } = await supabase
        .from("ba_contacts")
        .update({
          verified_count: newCount,
          confidence: "verified",
          last_verified: new Date().toISOString(),
        })
        .eq("id", id)
        .select()
        .single();
      if (error) throw error;
      return { data, buildingId };
    },
    onSuccess: (result) => {
      qc.invalidateQueries({ queryKey: ["buildings", result.buildingId] });
    },
  });
}

// Add a note to a building
export function useAddNote() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (data: Record<string, unknown>) => {
      const { data: result, error } = await supabase
        .from("ba_building_notes")
        .insert({
          building_id: data.buildingId,
          note_type: data.noteType,
          content: data.content,
          contributed_by: (data.contributedBy as string) || "anonymous",
          upvotes: 0,
          created_at: new Date().toISOString(),
        })
        .select()
        .single();
      if (error) throw error;
      return result;
    },
    onSuccess: (_data, variables) => {
      qc.invalidateQueries({ queryKey: ["buildings", variables.buildingId] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

// Upvote a note
export function useUpvoteNote() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async ({ id, buildingId }: { id: number; buildingId: number }) => {
      const { data: current } = await supabase
        .from("ba_building_notes")
        .select("upvotes")
        .eq("id", id)
        .single();
      const newCount = (current?.upvotes ?? 0) + 1;
      const { data, error } = await supabase
        .from("ba_building_notes")
        .update({ upvotes: newCount })
        .eq("id", id)
        .select()
        .single();
      if (error) throw error;
      return { data, buildingId };
    },
    onSuccess: (result) => {
      qc.invalidateQueries({ queryKey: ["buildings", result.buildingId] });
    },
  });
}

// ─── Contact level lookup ────────────────────────────────────────────────────
// Returns a Map of building_id → contact level for quick O(1) lookups
// "has-contact" = has phone or email, "name-only" = only owner name, null = nothing
export type ContactLevel = "has-contact" | "name-only" | null;

export function useContactLevels() {
  return useQuery<Set<number>>({
    queryKey: ["contact-levels"],
    queryFn: async () => {
      // Only fetch building_ids that have a phone or email
      // This is the set of buildings with "real" contact info
      const hasContactSet = new Set<number>();

      let offset = 0;
      const batchSize = 5000;
      while (true) {
        const { data, error } = await supabase
          .from("ba_contacts")
          .select("building_id")
          .not("phone", "is", null)
          .range(offset, offset + batchSize - 1);
        if (error) throw error;
        if (!data || data.length === 0) break;

        for (const c of data) {
          hasContactSet.add(c.building_id);
        }

        if (data.length < batchSize) break;
        offset += batchSize;
      }

      // Also get buildings with email (not null)
      offset = 0;
      while (true) {
        const { data, error } = await supabase
          .from("ba_contacts")
          .select("building_id")
          .not("email", "is", null)
          .range(offset, offset + batchSize - 1);
        if (error) throw error;
        if (!data || data.length === 0) break;

        for (const c of data) {
          hasContactSet.add(c.building_id);
        }

        if (data.length < batchSize) break;
        offset += batchSize;
      }

      return hasContactSet;
    },
    staleTime: 120_000,
  });
}

// Fetch data source counts for settings popover
export interface DataSourceCount {
  source: string;
  count: number;
}

export function useDataSourceCounts() {
  return useQuery<DataSourceCount[]>({
    queryKey: ["data-source-counts"],
    queryFn: async () => {
      const sources = ["hennepin-assessor", "metrogis-6-counties", "metrogis-original", "rental-license", "hud", "community"];
      const results: DataSourceCount[] = [];
      for (const source of sources) {
        const { count } = await supabase
          .from("ba_buildings")
          .select("id", { count: "exact", head: true })
          .eq("data_source", source);
        if (count && count > 0) {
          results.push({ source, count });
        }
      }
      return results.sort((a, b) => b.count - a.count);
    },
    staleTime: 120_000,
  });
}

// Update an existing contact (limited fields: name, phone, email, notes)
export function useUpdateContact() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async ({
      id,
      buildingId,
      updates,
    }: {
      id: number;
      buildingId: number;
      updates: { name?: string | null; phone?: string | null; email?: string | null; notes?: string | null };
    }) => {
      const { data, error } = await supabase
        .from("ba_contacts")
        .update(updates)
        .eq("id", id)
        .select()
        .single();
      if (error) throw error;
      return { data, buildingId };
    },
    onSuccess: (result) => {
      qc.invalidateQueries({ queryKey: ["buildings", result.buildingId] });
    },
  });
}

// Update an existing note (limited fields: content, note_type)
export function useUpdateNote() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async ({
      id,
      buildingId,
      updates,
    }: {
      id: number;
      buildingId: number;
      updates: { content?: string; note_type?: string };
    }) => {
      const { data, error } = await supabase
        .from("ba_building_notes")
        .update(updates)
        .eq("id", id)
        .select()
        .single();
      if (error) throw error;
      return { data, buildingId };
    },
    onSuccess: (result) => {
      qc.invalidateQueries({ queryKey: ["buildings", result.buildingId] });
    },
  });
}
