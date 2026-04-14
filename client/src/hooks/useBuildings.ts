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
export function useMapBuildings(bounds: MapBounds | null, type?: string) {
  return useQuery<Building[]>({
    queryKey: ["buildings", bounds, type],
    queryFn: async () => {
      let query = supabase
        .from("ba_buildings")
        .select("*")
        .limit(5000);

      if (bounds) {
        query = query
          .gte("lat", bounds.south)
          .lte("lat", bounds.north)
          .gte("lng", bounds.west)
          .lte("lng", bounds.east);
      }

      if (type && type !== "all") {
        query = query.eq("building_type", type);
      }

      const { data, error } = await query;
      if (error) throw error;
      return data ?? [];
    },
    enabled: true,
    staleTime: 30_000,
  });
}

// Search buildings by text
export function useSearchBuildings(query: string) {
  return useQuery<Building[]>({
    queryKey: ["buildings/search", query],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ba_buildings")
        .select("*")
        .or(`name.ilike.%${query}%,address.ilike.%${query}%,owner_name.ilike.%${query}%`)
        .limit(50);
      if (error) throw error;
      return data ?? [];
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

// Fetch data source counts for settings popover
export interface DataSourceCount {
  source: string;
  count: number;
}

export function useDataSourceCounts() {
  return useQuery<DataSourceCount[]>({
    queryKey: ["data-source-counts"],
    queryFn: async () => {
      // We query distinct data_source values and count per source
      const sources = ["metrogis", "rental-license", "hud", "community"];
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
