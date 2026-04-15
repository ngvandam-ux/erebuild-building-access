import { useQuery } from "@tanstack/react-query";
import { erebuildSupabase } from "@/lib/erebuild-supabase";

// ─── Types ─────────────────────────────────────────────────────────────────
export interface OfficeLocation {
  name: string;
  address: string;
  lat: number;
  lng: number;
  city: string;
  county: string;
  state: string;
}

export interface GeoFence {
  center: { lat: number; lng: number };
  radiusMiles: number;
  offices: OfficeLocation[];
  /** Bounding box for the max allowed map extent */
  maxBounds: {
    north: number;
    south: number;
    east: number;
    west: number;
  };
}

// ─── Constants ─────────────────────────────────────────────────────────────
const RADIUS_MILES = 200;
// Degrees per mile at mid-latitudes (~45°N)
const DEG_PER_MILE_LAT = 1 / 69.0;
const DEG_PER_MILE_LNG = 1 / 49.0; // at ~45°N: cos(45°) ≈ 0.707, 69 * 0.707 ≈ 49

/**
 * Fetch office locations from Erebuild's system_settings table
 * and calculate a centroid + bounding box for the geo-fence.
 */
export function useGeoFence() {
  return useQuery<GeoFence | null>({
    queryKey: ["geo-fence"],
    queryFn: async () => {
      const { data, error } = await erebuildSupabase
        .from("system_settings")
        .select("value")
        .eq("key", "office_locations")
        .single();

      if (error || !data?.value) return null;

      let offices: OfficeLocation[];
      try {
        offices = JSON.parse(data.value);
      } catch {
        return null;
      }

      if (!offices.length) return null;

      // Calculate centroid of all office locations
      const sumLat = offices.reduce((s, o) => s + o.lat, 0);
      const sumLng = offices.reduce((s, o) => s + o.lng, 0);
      const center = {
        lat: sumLat / offices.length,
        lng: sumLng / offices.length,
      };

      // Calculate max bounds as a box ~RADIUS_MILES from the centroid
      const maxBounds = {
        north: center.lat + RADIUS_MILES * DEG_PER_MILE_LAT,
        south: center.lat - RADIUS_MILES * DEG_PER_MILE_LAT,
        east: center.lng + RADIUS_MILES * DEG_PER_MILE_LNG,
        west: center.lng - RADIUS_MILES * DEG_PER_MILE_LNG,
      };

      return {
        center,
        radiusMiles: RADIUS_MILES,
        offices,
        maxBounds,
      };
    },
    staleTime: 300_000, // 5 minutes
  });
}
