import { useEffect, useRef, useCallback } from "react";
import L from "leaflet";
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

// ─── Main map component (pure Leaflet, no react-leaflet hooks) ───────────────
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
  const markersRef = useRef<Map<number, L.Marker>>(new Map());
  const layerGroupRef = useRef<L.LayerGroup | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const onBoundsChangeRef = useRef(onBoundsChange);
  const onSelectBuildingRef = useRef(onSelectBuilding);

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

    // Esri World Imagery — satellite hybrid with road/label overlay, no API key
    L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}", {
      attribution: 'Tiles &copy; Esri',
      maxZoom: 19,
    }).addTo(map);

    // Road + label overlay on top of satellite
    L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Transportation/MapServer/tile/{z}/{y}/{x}", {
      maxZoom: 19,
    }).addTo(map);
    L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}", {
      maxZoom: 19,
    }).addTo(map);

    const layerGroup = L.layerGroup().addTo(map);
    layerGroupRef.current = layerGroup;

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
      markersRef.current.clear();
    };
  }, []);

  // ── Update markers when buildings change ──
  useEffect(() => {
    const map = mapRef.current;
    const layerGroup = layerGroupRef.current;
    if (!map || !layerGroup) return;

    const prev = markersRef.current;
    const currentIds = new Set(buildings.map((b) => b.id));

    // Remove stale markers
    for (const [id, marker] of prev.entries()) {
      if (!currentIds.has(id)) {
        layerGroup.removeLayer(marker);
        prev.delete(id);
      }
    }

    // Add/update markers
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

        layerGroup.addLayer(marker);
        prev.set(building.id, marker);
      }
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
    <div
      ref={containerRef}
      style={{ width: "100%", height: "100%" }}
      data-testid="leaflet-map"
    />
  );
}
