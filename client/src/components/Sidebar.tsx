import { useState, useCallback, useRef } from "react";
import { Search, Building2, Settings, X, Loader2 } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  useMapBuildings,
  useSearchBuildings,
  useStats,
  type MapBounds,
} from "@/hooks/useBuildings";
import type { Building } from "@/hooks/useBuildings";
import BuildingDetail from "./BuildingDetail";


// ─── Filter chip config ───────────────────────────────────────────────────
const FILTER_CHIPS = [
  { value: "all",        label: "All" },
  { value: "apartment",  label: "Apartment" },
  { value: "commercial", label: "Commercial" },
  { value: "industrial", label: "Industrial" },
  { value: "office",     label: "Office" },
  { value: "government", label: "Government" },
  { value: "school",     label: "School" },
  { value: "hospital",   label: "Hospital" },
];

// ─── Building type dot colors ─────────────────────────────────────────────
const TYPE_DOT: Record<string, string> = {
  apartment:  "bg-[#D4A547]",
  commercial: "bg-[#5B9BD5]",
  office:     "bg-[#9B8EC4]",
  industrial: "bg-[#7C8A96]",
  government: "bg-[#4ADE80]",
  school:     "bg-[#34D399]",
  hospital:   "bg-[#F87171]",
  retail:     "bg-[#C17A2E]",
  "mixed-use":"bg-[#C084FC]",
};

// ─── Settings Popover (data info) ────────────────────────────────────────
function SeedSettings() {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button
          size="icon"
          variant="ghost"
          data-testid="button-settings"
          title="Settings"
        >
          <Settings className="w-4 h-4" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-64 p-3 space-y-3" align="end">
        <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">
          Data Sources
        </p>

        <div className="space-y-1 text-xs text-muted-foreground">
          <p>MetroGIS — Hennepin &amp; Ramsey counties</p>
          <p>HUD Multifamily — Minneapolis metro</p>
          <p>Community contributions</p>
        </div>
      </PopoverContent>
    </Popover>
  );
}

// ─── Building list item ────────────────────────────────────────────────────
interface BuildingRowProps {
  building: Building;
  selected: boolean;
  onClick: () => void;
}

function BuildingRow({ building, selected, onClick }: BuildingRowProps) {
  const dotClass = TYPE_DOT[building.building_type ?? ""] ?? "bg-slate-400";
  const displayName = building.name ?? building.address;
  const subLine = building.name ? building.address : null;

  return (
    <button
      className={`w-full text-left px-3 py-2.5 rounded-md transition-colors hover-elevate ${
        selected
          ? "bg-accent text-foreground"
          : "text-foreground"
      }`}
      onClick={onClick}
      data-testid={`row-building-${building.id}`}
    >
      <div className="flex items-start gap-2.5 min-w-0">
        {/* Type dot */}
        <span className={`mt-1.5 w-2.5 h-2.5 rounded-full shrink-0 ${dotClass}`} />
        <div className="min-w-0 flex-1">
          <p className="text-sm font-medium truncate leading-tight">{displayName}</p>
          {subLine && (
            <p className="text-xs text-muted-foreground truncate">{subLine}</p>
          )}
          <p className="text-xs text-muted-foreground truncate">
            {building.city}, {building.state}
          </p>
          <div className="flex flex-wrap gap-1.5 mt-1">
            {building.building_type && (
              <span className="text-[10px] text-muted-foreground capitalize">
                {building.building_type}
              </span>
            )}
            {building.unit_count && (
              <span className="text-[10px] text-muted-foreground">
                · {building.unit_count} units
              </span>
            )}
          </div>
        </div>
      </div>
    </button>
  );
}

// ─── Stats bar ────────────────────────────────────────────────────────────
function StatsBar() {
  const { data: stats } = useStats();

  return (
    <div
      className="flex items-center justify-around py-2 px-3 border-t border-border text-center shrink-0"
      data-testid="stats-bar"
    >
      <div>
        <p className="text-sm font-semibold" data-testid="stat-buildings">
          {stats?.buildings?.toLocaleString() ?? "—"}
        </p>
        <p className="text-[10px] text-muted-foreground">Buildings</p>
      </div>
      <div className="w-px h-6 bg-border" />
      <div>
        <p className="text-sm font-semibold" data-testid="stat-contacts">
          {stats?.contacts?.toLocaleString() ?? "—"}
        </p>
        <p className="text-[10px] text-muted-foreground">Contacts</p>
      </div>
      <div className="w-px h-6 bg-border" />
      <div>
        <p className="text-sm font-semibold" data-testid="stat-notes">
          {stats?.notes?.toLocaleString() ?? "—"}
        </p>
        <p className="text-[10px] text-muted-foreground">Notes</p>
      </div>
    </div>
  );
}

// ─── Main Sidebar ─────────────────────────────────────────────────────────
interface SidebarProps {
  bounds: MapBounds | null;
  selectedBuilding: Building | null;
  onSelectBuilding: (building: Building | null) => void;
  onMapBuildings: (buildings: Building[]) => void;
}

export default function Sidebar({
  bounds,
  selectedBuilding,
  onSelectBuilding,
  onMapBuildings,
}: SidebarProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [activeFilter, setActiveFilter] = useState("all");
  const searchDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [debouncedSearch, setDebouncedSearch] = useState("");

  const handleSearchChange = useCallback((val: string) => {
    setSearchQuery(val);
    if (searchDebounceRef.current) clearTimeout(searchDebounceRef.current);
    searchDebounceRef.current = setTimeout(() => setDebouncedSearch(val), 300);
  }, []);

  // Decide whether to show search results or map-bounded list
  const isSearching = debouncedSearch.length >= 2;

  const { data: mapBuildings, isLoading: mapLoading } = useMapBuildings(
    isSearching ? null : bounds,
    isSearching ? undefined : activeFilter
  );

  const { data: searchResults, isLoading: searchLoading } = useSearchBuildings(
    isSearching ? debouncedSearch : ""
  );

  // Propagate map buildings up so parent can render markers
  const buildings = isSearching ? (searchResults ?? []) : (mapBuildings ?? []);
  const isLoading = isSearching ? searchLoading : mapLoading;

  // Notify parent of building list changes for map markers
  // (we do this via an effect-like callback — just expose them via the already-passed prop)
  // The parent handles marker rendering from `buildings` passed here via onMapBuildings
  // We call it inline when buildings change (not ideal but avoids an effect)
  const prevBuildingsRef = useRef<Building[]>([]);
  if (buildings !== prevBuildingsRef.current) {
    prevBuildingsRef.current = buildings;
    // Schedule for next microtask to avoid setState during render
    Promise.resolve().then(() => onMapBuildings(buildings));
  }

  return (
    <div className="flex flex-col h-full bg-sidebar border-r border-sidebar-border">

      {/* ── Header ── */}
      <div className="flex items-center justify-between gap-2 px-3 py-3 border-b border-sidebar-border shrink-0">
        <div className="flex items-center gap-2">
          {/* Erebuild logo */}
          <svg
            width="28"
            height="28"
            viewBox="0 0 44 36"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
            className="shrink-0"
            aria-label="Erebuild"
          >
            <path d="M22 2L10 34H18L22 22L26 34H34L22 2Z" fill="#D4A547" />
            <line x1="8" y1="20" x2="36" y2="20" stroke="#E8E4DC" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
          <div>
            <p className="text-xs font-bold leading-none tracking-tight text-foreground">
              Building Access
            </p>
            <p className="text-[10px] text-muted-foreground leading-none mt-0.5">Twin Cities</p>
          </div>
        </div>
        <SeedSettings />
      </div>

      {/* ── When a building is selected, show detail panel ── */}
      {selectedBuilding ? (
        <div className="flex flex-col flex-1 min-h-0">
          <BuildingDetail
            building={selectedBuilding}
            onBack={() => onSelectBuilding(null)}
          />
        </div>
      ) : (
        <>
          {/* ── Search bar ── */}
          <div className="px-3 pt-3 pb-2 shrink-0">
            <div className="relative flex items-center">
              <Search className="absolute left-2.5 w-3.5 h-3.5 text-muted-foreground pointer-events-none" />
              <Input
                type="search"
                value={searchQuery}
                onChange={(e) => handleSearchChange(e.target.value)}
                placeholder="Search by address or owner..."
                className="pl-8 pr-8 text-xs h-9"
                data-testid="input-building-search"
              />
              {searchQuery && (
                <button
                  className="absolute right-2.5 text-muted-foreground"
                  onClick={() => { setSearchQuery(""); setDebouncedSearch(""); }}
                  data-testid="button-clear-search"
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              )}
            </div>
          </div>

          {/* ── Filter chips ── */}
          {!isSearching && (
            <div className="px-3 pb-2 shrink-0">
              <div className="flex gap-1 flex-wrap">
                {FILTER_CHIPS.map((chip) => (
                  <button
                    key={chip.value}
                    onClick={() => setActiveFilter(chip.value)}
                    className={`text-[11px] px-2 py-0.5 rounded-full border transition-colors ${
                      activeFilter === chip.value
                        ? "bg-primary text-primary-foreground border-primary"
                        : "bg-transparent text-muted-foreground border-border hover:border-primary/50"
                    }`}
                    data-testid={`chip-filter-${chip.value}`}
                  >
                    {chip.label}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* ── Results label ── */}
          <div className="px-3 pb-1 shrink-0 flex items-center justify-between">
            <span className="text-[11px] text-muted-foreground">
              {isSearching
                ? `Results for "${searchQuery}"`
                : `In current view`}
            </span>
            {isLoading ? (
              <Loader2 className="w-3 h-3 animate-spin text-muted-foreground" />
            ) : (
              <span className="text-[11px] text-muted-foreground" data-testid="text-result-count">
                {buildings.length}
              </span>
            )}
          </div>

          {/* ── Building list ── */}
          <ScrollArea className="flex-1 min-h-0 sidebar-scroll">
            <div className="px-2 pb-2 space-y-0.5">
              {isLoading ? (
                <>
                  {[...Array(5)].map((_, i) => (
                    <div key={i} className="px-3 py-2.5">
                      <Skeleton className="h-4 w-4/5 mb-1.5 rounded" />
                      <Skeleton className="h-3 w-3/5 rounded" />
                    </div>
                  ))}
                </>
              ) : buildings.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-10 text-center px-4">
                  <Building2 className="w-8 h-8 text-muted-foreground/40 mb-2" />
                  <p className="text-sm text-muted-foreground">No buildings found</p>
                  <p className="text-xs text-muted-foreground/70 mt-1">
                    {isSearching
                      ? "Try a different search term"
                      : "Zoom in or pan the map, or use the settings menu to seed data"}
                  </p>
                </div>
              ) : (
                buildings.map((building) => (
                  <BuildingRow
                    key={building.id}
                    building={building}
                    selected={selectedBuilding?.id === building.id}
                    onClick={() => onSelectBuilding(building)}
                  />
                ))
              )}
            </div>
          </ScrollArea>

          {/* ── Stats bar ── */}
          <StatsBar />
        </>
      )}
    </div>
  );
}
