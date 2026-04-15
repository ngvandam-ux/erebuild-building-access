import { useEffect, useRef, useState } from "react";
import L from "leaflet";
import "leaflet.markercluster";
import { Map as MapIcon, Satellite } from "lucide-react";
import type { Building } from "@/hooks/useBuildings";
import type { MapBounds } from "@/hooks/useBuildings";

// ─── Cached DivIcon pool (avoid creating new icons on every render) ──────────
const iconCache = new Map<string, L.DivIcon>();

function markerClass(type: string | null | undefined): string {
  switch (type) {
    case "apartment":  return "marker-apartment";
    case "commercial": return "marker-commercial";
    case "office":     return "marker-office";
    case "industrial": return "marker-industrial";
    case "government": return "marker-government";
    case "school":     return "marker-school";
    case "hospital":   return "marker-hospital";
    case "retail":     return "marker-retail";
    case "mixed-use":  return "marker-mixed-use";
    default:           return "marker-default";
  }
}

function getCachedIcon(
  type: string | null | undefined,
  selected: boolean,
  hasContact: boolean
): L.DivIcon {
  const key = `${type ?? "default"}-${selected ? "s" : "n"}-${hasContact ? "c" : "x"}`;
  let icon = iconCache.get(key);
  if (!icon) {
    const contactClass = hasContact ? " has-contact" : "";
    const selClass = selected ? " selected" : "";
    icon = L.divIcon({
      className: "",
      html: `<div class="map-marker ${markerClass(type)}${selClass}${contactClass}"></div>`,
      iconSize: hasContact ? [18, 18] : [12, 12],
      iconAnchor: hasContact ? [9, 9] : [6, 6],
    });
    iconCache.set(key, icon);
  }
  return icon;
}

type TileMode = "street" | "satellite";

// ─── Main map component ──────────────────────────────────────────────────────
interface BuildingMapProps {
  buildings: Building[];
  selectedBuilding: Building | null;
  contactSet?: Set<number>;
  onBoundsChange: (bounds: MapBounds) => void;
  onSelectBuilding: (building: Building) => void;
}

export default function BuildingMap({
  buildings,
  selectedBuilding,
  contactSet,
  onBoundsChange,
  onSelectBuilding,
}: BuildingMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const clusterGroupRef = useRef<L.MarkerClusterGroup | null>(null);
  const markersRef = useRef<Map<number, L.Marker>>(new Map());
  const buildingMapRef = useRef<Map<number, Building>>(new Map());
  const tileLayersRef = useRef<L.TileLayer[]>([]);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const onBoundsChangeRef = useRef(onBoundsChange);
  const onSelectBuildingRef = useRef(onSelectBuilding);
  const selectedIdRef = useRef<number | null>(null);
  const prevSelectedIdRef = useRef<number | null>(null);
  const contactSetRef = useRef<Set<number>>(new Set());
  const [tileMode, setTileMode] = useState<TileMode>("street");

  // Keep refs current
  useEffect(() => { onBoundsChangeRef.current = onBoundsChange; }, [onBoundsChange]);
  useEffect(() => { onSelectBuildingRef.current = onSelectBuilding; }, [onSelectBuilding]);
  useEffect(() => { contactSetRef.current = contactSet ?? new Set(); }, [contactSet]);

  // ── Initialize map once ──
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = L.map(containerRef.current, {
      center: [44.9778, -93.265],
      zoom: 12,
      zoomControl: true,
    });

    const streetLayer = L.tileLayer(
      "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
      {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: "abcd",
        maxZoom: 20,
      }
    );

    streetLayer.addTo(map);
    tileLayersRef.current = [streetLayer];

    const clusterGroup = L.markerClusterGroup({
      maxClusterRadius: (zoom: number) => {
        // More aggressive clustering when zoomed out, loose when zoomed in
        if (zoom <= 10) return 120;
        if (zoom <= 12) return 80;
        if (zoom <= 14) return 60;
        return 40;
      },
      spiderfyOnMaxZoom: true,
      showCoverageOnHover: false,
      zoomToBoundsOnClick: true,
      disableClusteringAtZoom: 17,
      chunkedLoading: true,
      chunkInterval: 200,
      chunkDelay: 50,
      animate: false,
      iconCreateFunction: (cluster: L.MarkerCluster) => {
        const count = cluster.getChildCount();
        let size = "small";
        let px = 36;
        if (count > 100) { size = "large"; px = 48; }
        else if (count > 30) { size = "medium"; px = 42; }

        return L.divIcon({
          html: `<div class="cluster-inner">${count}</div>`,
          className: `marker-cluster marker-cluster-${size}`,
          iconSize: L.point(px, px),
        });
      },
    });

    clusterGroup.addTo(map);
    clusterGroupRef.current = clusterGroup;

    const emitBounds = () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = setTimeout(() => {
        const b = map.getBounds();
        onBoundsChangeRef.current({
          north: b.getNorth(),
          south: b.getSouth(),
          east: b.getEast(),
          west: b.getWest(),
        });
      }, 400);  // longer debounce to reduce query thrash
    };

    map.on("moveend", emitBounds);
    map.on("zoomend", emitBounds);
    map.whenReady(() => emitBounds());

    mapRef.current = map;

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      map.remove();
      mapRef.current = null;
      clusterGroupRef.current = null;
      markersRef.current.clear();
      buildingMapRef.current.clear();
    };
  }, []);

  // ── Switch tiles ──
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    for (const layer of tileLayersRef.current) {
      map.removeLayer(layer);
    }
    tileLayersRef.current = [];

    if (tileMode === "street") {
      const street = L.tileLayer(
        "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
        {
          attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
          subdomains: "abcd",
          maxZoom: 20,
        }
      );
      street.addTo(map);
      tileLayersRef.current = [street];
    } else {
      const sat = L.tileLayer(
        "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        { attribution: "Tiles &copy; Esri", maxZoom: 19 }
      );
      const roads = L.tileLayer(
        "https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Transportation/MapServer/tile/{z}/{y}/{x}",
        { maxZoom: 19 }
      );
      const labels = L.tileLayer(
        "https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}",
        { maxZoom: 19 }
      );
      sat.addTo(map);
      roads.addTo(map);
      labels.addTo(map);
      tileLayersRef.current = [sat, roads, labels];
    }
  }, [tileMode]);

  // ── Update markers when buildings change (the ONLY marker effect) ──
  useEffect(() => {
    const clusterGroup = clusterGroupRef.current;
    if (!clusterGroup) return;

    const prev = markersRef.current;
    const bMap = buildingMapRef.current;
    const currentIds = new Set(buildings.map((b) => b.id));
    const selId = selectedBuilding?.id ?? null;

    // Remove stale markers in batch
    const toRemove: L.Marker[] = [];
    for (const [id, marker] of prev.entries()) {
      if (!currentIds.has(id)) {
        toRemove.push(marker);
        prev.delete(id);
        bMap.delete(id);
      }
    }
    if (toRemove.length > 0) {
      clusterGroup.removeLayers(toRemove);
    }

    // Add new markers in batch
    const toAdd: L.Marker[] = [];
    for (const building of buildings) {
      bMap.set(building.id, building);
      const existing = prev.get(building.id);

      if (!existing) {
        const hasContact = contactSetRef.current.has(building.id);
        const marker = L.marker([building.lat, building.lng], {
          icon: getCachedIcon(building.building_type, building.id === selId, hasContact),
        });

        // Use a closure-free click handler that reads from ref
        marker.on("click", () => {
          const b = buildingMapRef.current.get(building.id);
          if (b) onSelectBuildingRef.current(b);
        });

        const label = building.name ?? building.address;
        marker.bindTooltip(label, { direction: "top", offset: [0, -8] });

        prev.set(building.id, marker);
        toAdd.push(marker);
      }
    }

    if (toAdd.length > 0) {
      clusterGroup.addLayers(toAdd);
    }
  }, [buildings]);  // only re-run when the buildings array changes, NOT on selection

  // ── Update marker icons when contactSet changes (e.g. data loads) ──
  useEffect(() => {
    if (!contactSet || contactSet.size === 0) return;
    const prev = markersRef.current;
    const bMap = buildingMapRef.current;
    const selId = selectedIdRef.current;

    for (const [id, marker] of prev.entries()) {
      const building = bMap.get(id);
      if (!building) continue;
      const hasContact = contactSet.has(id);
      const isSel = id === selId;
      marker.setIcon(getCachedIcon(building.building_type, isSel, hasContact));
    }
  }, [contactSet]);

  // ── Lightweight selection highlight (only touches 2 markers: old + new) ──
  useEffect(() => {
    const prev = markersRef.current;
    const bMap = buildingMapRef.current;
    const newId = selectedBuilding?.id ?? null;
    const oldId = selectedIdRef.current;

    if (newId === oldId) return;

    // Un-highlight old
    if (oldId !== null) {
      const oldMarker = prev.get(oldId);
      const oldBuilding = bMap.get(oldId);
      if (oldMarker && oldBuilding) {
        const oldHasContact = contactSetRef.current.has(oldId);
        oldMarker.setIcon(getCachedIcon(oldBuilding.building_type, false, oldHasContact));
      }
    }

    // Highlight new
    if (newId !== null) {
      const newMarker = prev.get(newId);
      const newBuilding = bMap.get(newId);
      if (newMarker && newBuilding) {
        const newHasContact = contactSetRef.current.has(newId);
        newMarker.setIcon(getCachedIcon(newBuilding.building_type, true, newHasContact));
      }
    }

    selectedIdRef.current = newId;
  }, [selectedBuilding?.id]);

  // ── Fly to selected building ──
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !selectedBuilding) return;
    if (selectedBuilding.id === prevSelectedIdRef.current) return;
    prevSelectedIdRef.current = selectedBuilding.id;
    map.flyTo([selectedBuilding.lat, selectedBuilding.lng], Math.max(map.getZoom(), 15), {
      duration: 0.8,
    });
  }, [selectedBuilding]);

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      <div
        ref={containerRef}
        style={{ width: "100%", height: "100%" }}
        data-testid="leaflet-map"
      />

      {/* Map legend */}
      <div
        style={{
          position: "absolute",
          bottom: 28,
          left: 10,
          zIndex: 1000,
          background: "rgba(10, 16, 30, 0.85)",
          backdropFilter: "blur(4px)",
          borderRadius: 4,
          padding: "6px 10px",
          display: "flex",
          alignItems: "center",
          gap: 12,
          fontSize: 10,
          color: "#E8E4DC",
          fontFamily: "var(--font-sans)",
          boxShadow: "0 1px 4px rgba(0,0,0,0.3)",
        }}
        data-testid="map-legend"
      >
        <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <span
            style={{
              width: 10,
              height: 10,
              borderRadius: "50%",
              border: "2px solid #4ADE80",
              background: "#D4A547",
              boxShadow: "0 0 0 2px rgba(74,222,128,0.35)",
              display: "inline-block",
            }}
          />
          Has Contact
        </span>
        <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <span
            style={{
              width: 8,
              height: 8,
              borderRadius: "50%",
              border: "1.5px solid rgba(255,255,255,0.5)",
              background: "#4A4844",
              opacity: 0.6,
              display: "inline-block",
            }}
          />
          No Contact
        </span>
      </div>

      <div className="tile-toggle" data-testid="tile-toggle">
        <button
          className={`tile-toggle-btn${tileMode === "street" ? " active" : ""}`}
          onClick={() => setTileMode("street")}
          title="Street map"
          data-testid="button-tile-street"
        >
          <MapIcon className="w-3.5 h-3.5" />
          <span>Street</span>
        </button>
        <button
          className={`tile-toggle-btn${tileMode === "satellite" ? " active" : ""}`}
          onClick={() => setTileMode("satellite")}
          title="Satellite"
          data-testid="button-tile-satellite"
        >
          <Satellite className="w-3.5 h-3.5" />
          <span>Satellite</span>
        </button>
      </div>
    </div>
  );
}
