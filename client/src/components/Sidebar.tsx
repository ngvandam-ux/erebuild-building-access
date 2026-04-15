import { useState, useCallback, useRef, useMemo, memo } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { Search, Building2, Settings, X, Loader2, ArrowUpDown, ChevronDown, Phone, Mail } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  useMapBuildings,
  useSearchBuildings,
  useStats,
  useDataSourceCounts,
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

// ─── Sort options ─────────────────────────────────────────────────────────
type SortKey = "name" | "units" | "type" | "source" | "contact";

const SORT_OPTIONS: { value: SortKey; label: string }[] = [
  { value: "name",    label: "Name" },
  { value: "units",   label: "Units" },
  { value: "type",    label: "Type" },
  { value: "source",  label: "Data Source" },
  { value: "contact", label: "Contact Info" },
];

function sortBuildings(buildings: Building[], sortKey: SortKey, contactSet?: Set<number>): Building[] {
  const sorted = [...buildings];
  switch (sortKey) {
    case "name":
      return sorted.sort((a, b) => {
        const nameA = (a.name ?? a.address).toLowerCase();
        const nameB = (b.name ?? b.address).toLowerCase();
        return nameA.localeCompare(nameB);
      });
    case "units":
      return sorted.sort((a, b) => (b.unit_count ?? 0) - (a.unit_count ?? 0));
    case "type":
      return sorted.sort((a, b) => {
        const typeA = (a.building_type ?? "zzz").toLowerCase();
        const typeB = (b.building_type ?? "zzz").toLowerCase();
        return typeA.localeCompare(typeB);
      });
    case "source":
      return sorted.sort((a, b) => {
        const srcA = (a.data_source ?? "zzz").toLowerCase();
        const srcB = (b.data_source ?? "zzz").toLowerCase();
        return srcA.localeCompare(srcB);
      });
    case "contact":
      return sorted.sort((a, b) => {
        const aHas = contactSet?.has(a.id) ? 1 : 0;
        const bHas = contactSet?.has(b.id) ? 1 : 0;
        return bHas - aHas; // buildings with contacts first
      });
    default:
      return sorted;
  }
}

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

// ─── Data source labels ──────────────────────────────────────────────────
const SOURCE_LABELS: Record<string, string> = {
  "hennepin-assessor":  "Hennepin Assessor 2025",
  "metrogis-6-counties":"MetroGIS (6 Counties)",
  "metrogis":           "MetroGIS Original",
  "metrogis-original":  "MetroGIS Original",
  "rental-license":     "Mpls Rental Licenses",
  "hud":                "HUD Multifamily",
  "community":          "Community Contributions",
};

// ─── Settings Popover (live data info) ──────────────────────────────────
function DataSettings() {
  const { data: sourceCounts, isLoading } = useDataSourceCounts();
  const { data: stats } = useStats();

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button
          size="icon"
          variant="ghost"
          data-testid="button-settings"
          title="Data Sources"
        >
          <Settings className="w-4 h-4" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-72 p-3 space-y-3" align="end">
        <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">
          Open Data Sources
        </p>

        {isLoading ? (
          <div className="space-y-2">
            <Skeleton className="h-3 w-full" />
            <Skeleton className="h-3 w-4/5" />
            <Skeleton className="h-3 w-3/5" />
          </div>
        ) : (
          <div className="space-y-1.5">
            {sourceCounts?.map((sc) => (
              <div key={sc.source} className="flex items-center justify-between text-xs">
                <span className="text-muted-foreground truncate mr-2">
                  {SOURCE_LABELS[sc.source] ?? sc.source}
                </span>
                <span className="text-foreground font-medium tabular-nums shrink-0">
                  {sc.count.toLocaleString()}
                </span>
              </div>
            ))}
          </div>
        )}

        <div className="border-t border-border pt-2 mt-2">
          <div className="flex items-center justify-between text-xs">
            <span className="text-muted-foreground">Total Buildings</span>
            <span className="text-foreground font-semibold tabular-nums">
              {stats?.buildings?.toLocaleString() ?? "—"}
            </span>
          </div>
          <div className="flex items-center justify-between text-xs mt-1">
            <span className="text-muted-foreground">Total Contacts</span>
            <span className="text-foreground font-semibold tabular-nums">
              {stats?.contacts?.toLocaleString() ?? "—"}
            </span>
          </div>
        </div>

        <p className="text-[10px] text-muted-foreground/70 pt-1">
          Shared across all Erebuild users
        </p>
      </PopoverContent>
    </Popover>
  );
}

// ─── Sort dropdown ────────────────────────────────────────────────────────
function SortDropdown({ sortKey, onSortChange }: { sortKey: SortKey; onSortChange: (k: SortKey) => void }) {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          className="flex items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground transition-colors"
          data-testid="button-sort"
        >
          <ArrowUpDown className="w-3 h-3" />
          <span>{SORT_OPTIONS.find(o => o.value === sortKey)?.label ?? "Sort"}</span>
          <ChevronDown className="w-2.5 h-2.5" />
        </button>
      </PopoverTrigger>
      <PopoverContent className="w-36 p-1" align="end">
        {SORT_OPTIONS.map((opt) => (
          <button
            key={opt.value}
            onClick={() => onSortChange(opt.value)}
            className={`w-full text-left text-xs px-2.5 py-1.5 rounded-sm transition-colors ${
              sortKey === opt.value
                ? "bg-primary text-primary-foreground"
                : "text-foreground hover:bg-accent"
            }`}
            data-testid={`sort-option-${opt.value}`}
          >
            {opt.label}
          </button>
        ))}
      </PopoverContent>
    </Popover>
  );
}

// ─── Building list item (memoized) ──────────────────────────────────────────
interface BuildingRowProps {
  building: Building;
  selected: boolean;
  hasContact: boolean;
  onClick: () => void;
}

const BuildingRow = memo(function BuildingRow({ building, selected, hasContact, onClick }: BuildingRowProps) {
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
        <span className={`mt-1.5 w-2.5 h-2.5 rounded-full shrink-0 ${dotClass}`} />
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-1.5">
            <p className="text-sm font-medium truncate leading-tight flex-1 min-w-0">{displayName}</p>
            {hasContact && (
              <span className="shrink-0 flex items-center gap-0.5 text-[#4ADE80]" title="Has phone/email contact">
                <Phone className="w-2.5 h-2.5" />
              </span>
            )}
          </div>
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
});

// ─── Virtualized building list ────────────────────────────────────────────
function VirtualBuildingList({
  buildings,
  selectedId,
  contactSet,
  onSelect,
}: {
  buildings: Building[];
  selectedId: number | null;
  contactSet?: Set<number>;
  onSelect: (b: Building) => void;
}) {
  const parentRef = useRef<HTMLDivElement>(null);

  const virtualizer = useVirtualizer({
    count: buildings.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 76, // approx row height
    overscan: 10,
  });

  return (
    <div
      ref={parentRef}
      className="flex-1 min-h-0 overflow-y-auto sidebar-scroll"
      data-testid="building-list-scroll"
    >
      <div
        style={{
          height: virtualizer.getTotalSize(),
          width: "100%",
          position: "relative",
        }}
      >
        <div
          className="px-2"
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            width: "100%",
            transform: `translateY(${virtualizer.getVirtualItems()[0]?.start ?? 0}px)`,
          }}
        >
          {virtualizer.getVirtualItems().map((virtualRow) => {
            const building = buildings[virtualRow.index];
            return (
              <div
                key={building.id}
                data-index={virtualRow.index}
                ref={virtualizer.measureElement}
              >
                <BuildingRow
                  building={building}
                  selected={building.id === selectedId}
                  hasContact={contactSet?.has(building.id) ?? false}
                  onClick={() => onSelect(building)}
                />
              </div>
            );
          })}
        </div>
      </div>
    </div>
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
  contactSet?: Set<number>;
  onSelectBuilding: (building: Building | null) => void;
  onMapBuildings: (buildings: Building[]) => void;
}

export default function Sidebar({
  bounds,
  selectedBuilding,
  contactSet,
  onSelectBuilding,
  onMapBuildings,
}: SidebarProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [activeFilter, setActiveFilter] = useState("all");
  const [contactFilter, setContactFilter] = useState(false);
  const [sortKey, setSortKey] = useState<SortKey>("name");
  const searchDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [debouncedSearch, setDebouncedSearch] = useState("");

  const handleSearchChange = useCallback((val: string) => {
    setSearchQuery(val);
    if (searchDebounceRef.current) clearTimeout(searchDebounceRef.current);
    searchDebounceRef.current = setTimeout(() => setDebouncedSearch(val), 300);
  }, []);

  const isSearching = debouncedSearch.length >= 2;

  // contactSet is now passed from parent (App.tsx) to share with BuildingMap

  const { data: mapBuildings, isLoading: mapLoading } = useMapBuildings(
    isSearching ? null : bounds,
    isSearching ? undefined : activeFilter
  );

  const { data: searchResults, isLoading: searchLoading } = useSearchBuildings(
    isSearching ? debouncedSearch : ""
  );

  // Apply contact filter + sorting
  const rawBuildings = isSearching ? (searchResults ?? []) : (mapBuildings ?? []);
  const filteredBuildings = useMemo(() => {
    if (!contactFilter || !contactSet) return rawBuildings;
    return rawBuildings.filter((b) => contactSet.has(b.id));
  }, [rawBuildings, contactFilter, contactSet]);
  const buildings = useMemo(() => sortBuildings(filteredBuildings, sortKey, contactSet), [filteredBuildings, sortKey, contactSet]);
  const isLoading = isSearching ? searchLoading : mapLoading;

  // Notify parent of building list changes for map markers
  // Use a stable ref comparison to avoid unnecessary re-renders
  const prevRawRef = useRef<Building[]>([]);
  if (rawBuildings !== prevRawRef.current) {
    prevRawRef.current = rawBuildings;
    Promise.resolve().then(() => onMapBuildings(rawBuildings));
  }

  return (
    <div className="flex flex-col h-full bg-sidebar border-r border-sidebar-border">

      {/* ── Header ── */}
      <div className="flex items-center justify-between gap-2 px-3 py-3 border-b border-sidebar-border shrink-0">
        <div className="flex items-center gap-2">
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
        <DataSettings />
      </div>

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
                placeholder="Search name, address, city, owner, contact..."
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
                <button
                  onClick={() => setContactFilter(!contactFilter)}
                  className={`text-[11px] px-2 py-0.5 rounded-full border transition-colors inline-flex items-center gap-1 ${
                    contactFilter
                      ? "bg-[#4ADE80]/20 text-[#4ADE80] border-[#4ADE80]/50"
                      : "bg-transparent text-muted-foreground border-border hover:border-[#4ADE80]/50"
                  }`}
                  data-testid="chip-filter-has-contact"
                >
                  <Phone className="w-2.5 h-2.5" />
                  Has Contact
                </button>
              </div>
            </div>
          )}

          {/* ── Results label + sort ── */}
          <div className="px-3 pb-1 shrink-0 flex items-center justify-between">
            <span className="text-[11px] text-muted-foreground">
              {isSearching
                ? `Results for "${searchQuery}"`
                : `In current view`}
            </span>
            <div className="flex items-center gap-2">
              <SortDropdown sortKey={sortKey} onSortChange={setSortKey} />
              {isLoading ? (
                <Loader2 className="w-3 h-3 animate-spin text-muted-foreground" />
              ) : (
                <span className="text-[11px] text-muted-foreground tabular-nums" data-testid="text-result-count">
                  {buildings.length.toLocaleString()}
                </span>
              )}
            </div>
          </div>

          {/* ── Building list (virtualized) ── */}
          {isLoading ? (
            <div className="flex-1 px-2">
              {[...Array(5)].map((_, i) => (
                <div key={i} className="px-3 py-2.5">
                  <Skeleton className="h-4 w-4/5 mb-1.5 rounded" />
                  <Skeleton className="h-3 w-3/5 rounded" />
                </div>
              ))}
            </div>
          ) : buildings.length === 0 ? (
            <div className="flex-1 flex flex-col items-center justify-center py-10 text-center px-4">
              <Building2 className="w-8 h-8 text-muted-foreground/40 mb-2" />
              <p className="text-sm text-muted-foreground">No buildings found</p>
              <p className="text-xs text-muted-foreground/70 mt-1">
                {isSearching
                  ? "Try a different search term"
                  : "Zoom in or pan the map to see buildings in this area"}
              </p>
            </div>
          ) : (
            <VirtualBuildingList
              buildings={buildings}
              selectedId={selectedBuilding?.id ?? null}
              contactSet={contactSet}
              onSelect={onSelectBuilding}
            />
          )}

          {/* ── Stats bar ── */}
          <StatsBar />
        </>
      )}
    </div>
  );
}
