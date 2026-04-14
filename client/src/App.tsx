import { useState, useCallback } from "react";
import { Switch, Route, Router } from "wouter";
import { useHashLocation } from "wouter/use-hash-location";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { useIsMobile } from "@/hooks/use-mobile";
import { List, Map } from "lucide-react";
import type { Building } from "@/hooks/useBuildings";
import type { MapBounds } from "@/hooks/useBuildings";
import Sidebar from "@/components/Sidebar";
import BuildingMap from "@/components/BuildingMap";

// ─── Main View: full-screen split layout ────────────────────────────────────
function MainView() {
  const [selectedBuilding, setSelectedBuilding] = useState<Building | null>(null);
  const [mapBounds, setMapBounds] = useState<MapBounds | null>(null);
  const [mapBuildings, setMapBuildings] = useState<Building[]>([]);
  const [mobileView, setMobileView] = useState<"map" | "list">("map");
  const isMobile = useIsMobile();

  const handleSelectBuilding = useCallback((building: Building | null) => {
    setSelectedBuilding(building);
    // On mobile, selecting a building from list → show map
    if (building) setMobileView("map");
  }, []);

  const handleBoundsChange = useCallback((bounds: MapBounds) => {
    setMapBounds(bounds);
  }, []);

  const handleMapMarkerClick = useCallback((building: Building) => {
    setSelectedBuilding(building);
    // On mobile, clicking a marker → show list/detail
    setMobileView("list");
  }, []);

  const handleMapBuildings = useCallback((buildings: Building[]) => {
    setMapBuildings(buildings);
  }, []);

  // ── Mobile layout: toggled panels (both mounted, visibility toggled) ──
  if (isMobile) {
    return (
      <div className="flex flex-col h-screen w-full overflow-hidden">
        <div className="flex-1 min-h-0 relative">
          {/* Map — always mounted so it emits bounds */}
          <div
            className={`absolute inset-0 ${mobileView === "map" ? "z-10" : "z-0 pointer-events-none"}`}
            data-testid="map-container"
          >
            <BuildingMap
              buildings={mapBuildings}
              selectedBuilding={selectedBuilding}
              onBoundsChange={handleBoundsChange}
              onSelectBuilding={handleMapMarkerClick}
            />
          </div>
          {/* List — always mounted so it keeps querying */}
          <div
            className={`absolute inset-0 ${mobileView === "list" ? "z-10" : "z-0 pointer-events-none opacity-0"}`}
            data-testid="sidebar"
          >
            <Sidebar
              bounds={mapBounds}
              selectedBuilding={selectedBuilding}
              onSelectBuilding={handleSelectBuilding}
              onMapBuildings={handleMapBuildings}
            />
          </div>
        </div>

        {/* Mobile toggle bar */}
        <div className="flex shrink-0 border-t border-border bg-sidebar z-20">
          <button
            onClick={() => setMobileView("map")}
            className={`flex-1 flex items-center justify-center gap-1.5 py-2.5 text-xs font-medium transition-colors ${
              mobileView === "map"
                ? "text-primary"
                : "text-muted-foreground"
            }`}
            data-testid="button-mobile-map"
          >
            <Map className="w-4 h-4" />
            Map
          </button>
          <div className="w-px bg-border" />
          <button
            onClick={() => setMobileView("list")}
            className={`flex-1 flex items-center justify-center gap-1.5 py-2.5 text-xs font-medium transition-colors ${
              mobileView === "list"
                ? "text-primary"
                : "text-muted-foreground"
            }`}
            data-testid="button-mobile-list"
          >
            <List className="w-4 h-4" />
            Buildings
          </button>
        </div>
      </div>
    );
  }

  // ── Desktop layout: side-by-side ──
  return (
    <div className="flex h-screen w-full overflow-hidden">
      <div
        className="flex-none w-[380px] flex flex-col min-h-0 overflow-hidden border-r border-border"
        data-testid="sidebar"
      >
        <Sidebar
          bounds={mapBounds}
          selectedBuilding={selectedBuilding}
          onSelectBuilding={handleSelectBuilding}
          onMapBuildings={handleMapBuildings}
        />
      </div>

      <div className="flex-1 min-w-0 relative" data-testid="map-container">
        <BuildingMap
          buildings={mapBuildings}
          selectedBuilding={selectedBuilding}
          onBoundsChange={handleBoundsChange}
          onSelectBuilding={handleMapMarkerClick}
        />
      </div>
    </div>
  );
}

function AppRouter() {
  return (
    <Switch>
      <Route path="/" component={MainView} />
      <Route component={MainView} />
    </Switch>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Router hook={useHashLocation}>
          <AppRouter />
        </Router>
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
