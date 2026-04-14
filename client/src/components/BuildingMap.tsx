import { useEffect, useRef, useState } from "react";
import L from "leaflet";
import "leaflet.markercluster";
import { Map as MapIcon, Satellite } from "lucide-react";
import type { Building } from "@/hooks/useBuildings";
import type { MapBounds } from "@/hooks/useBuildings";

// ─── Helper: building type → marker CSS class ───────────────────────────────
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

function createDivIcon(type: string | null | undefined, selected: boolean): L.DivIcon {
  return L.divIcon({
    className: "",
    html: `<div class="map-marker ${markerClass(type)}${selected ? " selected" : ""}"></div>`,
    iconSize: [14, 14],
    iconAnchor: [7, 7],
  });
}

type TileMode = "street" | "satellite";

// ─── Main map component (pure Leaflet + markercluster) ───────────────────────
interface BuildingMapProps {
  buildings: Building[];
  selectedBuilding: Building | null;
  onBoundsChange: (bounds: MapBounds) => void;
  onSelectBuilding: (building: Building) => void;
}

export default function BuildingMap({
  buildings,
  selectedBuilding,
  onBoundsChange,
  onSelectBuilding,
}: BuildingMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const clusterGroupRef = useRef<L.MarkerClusterGroup | null>(null);
  const markersRef = useRef<Map<number, L.Marker>>(new Map());
  const tileLayersRef = useRef<L.TileLayer[]>([]);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const onBoundsChangeRef = useRef(onBoundsChange);
  const onSelectBuildingRef = useRef(onSelectBuilding);
  const [tileMode, setTileMode] = useState<TileMode>("street");

  // Keep refs current
  useEffect(() => { onBoundsChangeRef.current = onBoundsChange; }, [onBoundsChange]);
  useEffect(() => { onSelectBuildingRef.current = onSelectBuilding; }, [onSelectBuilding]);

  // ── Initialize map once ──
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = L.map(containerRef.current, {
      center: [44.9778, -93.265],
      zoom: 12,
      zoomControl: true,
    });

    // Default: CARTO Voyager (clean street map)
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

    // ── Create MarkerClusterGroup with Erebuild-branded styling ──
    const clusterGroup = L.markerClusterGroup({
      maxClusterRadius: 50,
      spiderfyOnMaxZoom: true,
      showCoverageOnHover: false,
      zoomToBoundsOnClick: true,
      disableClusteringAtZoom: 17,
      chunkedLoading: true,
      chunkInterval: 100,
      chunkDelay: 20,
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
      }, 300);
    };

    map.on("moveend", emitBounds);
    map.on("zoomend", emitBounds);

    // Emit initial bounds after map is ready
    map.whenReady(() => emitBounds());

    mapRef.current = map;

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      map.remove();
      mapRef.current = null;
      clusterGroupRef.current = null;
      markersRef.current.clear();
    };
  }, []);

  // ── Switch tiles when tileMode changes ──
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    // Remove existing tile layers
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
      // Satellite + road/label overlays
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

  // ── Update markers when buildings change ──
  useEffect(() => {
    const clusterGroup = clusterGroupRef.current;
    if (!clusterGroup) return;

    const prev = markersRef.current;
    const currentIds = new Set(buildings.map((b) => b.id));

    // Remove stale markers
    const toRemove: L.Marker[] = [];
    for (const [id, marker] of prev.entries()) {
      if (!currentIds.has(id)) {
        toRemove.push(marker);
        prev.delete(id);
      }
    }
    if (toRemove.length > 0) {
      clusterGroup.removeLayers(toRemove);
    }

    // Add new markers (batch for performance)
    const toAdd: L.Marker[] = [];
    for (const building of buildings) {
      const isSelected = building.id === selectedBuilding?.id;
      const existing = prev.get(building.id);

      if (existing) {
        existing.setIcon(createDivIcon(building.building_type, isSelected));
      } else {
        const marker = L.marker([building.lat, building.lng], {
          icon: createDivIcon(building.building_type, isSelected),
        });

        marker.on("click", () => onSelectBuildingRef.current(building));

        const label = building.name ?? building.address;
        marker.bindTooltip(label, {
          direction: "top",
          offset: [0, -8],
        });

        prev.set(building.id, marker);
        toAdd.push(marker);
      }
    }

    if (toAdd.length > 0) {
      clusterGroup.addLayers(toAdd);
    }
  }, [buildings, selectedBuilding?.id]);

  // ── Update icon when selection changes (without full re-render) ──
  useEffect(() => {
    const prev = markersRef.current;
    for (const [id, marker] of prev.entries()) {
      const building = buildings.find((b) => b.id === id);
      if (building) {
        marker.setIcon(createDivIcon(building.building_type, id === selectedBuilding?.id));
      }
    }
  }, [selectedBuilding?.id, buildings]);

  // ── Fly to selected building ──
  const prevSelectedIdRef = useRef<number | null>(null);
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

      {/* ── Tile layer toggle ── */}
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
