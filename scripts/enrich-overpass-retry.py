#!/usr/bin/env python3
"""
Retry the 22 failed Overpass tiles from the initial enrichment run.
Uses longer delays (5s) to avoid rate limits.
"""
import json, re, time, math
from urllib.request import urlopen, Request
from urllib.error import HTTPError
from urllib.parse import quote

SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
CON_KEYS = ["building_id", "name", "role", "phone", "email", "notes", "source"]

# Failed tile indices (1-based from log)
FAILED = [3, 4, 6, 9, 10, 14, 17, 18, 21, 24, 28, 30, 31, 33, 35, 36, 40, 41, 48, 49, 51, 52]

def clean_phone(phone):
    if not phone: return None
    digits = re.sub(r'\D', '', str(phone))
    if len(digits) == 10: return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1': return f"{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    return None

def norm(row):
    return {k: row.get(k) for k in CON_KEYS}

def supabase_post(rows):
    if not rows: return 0
    total = 0
    for i in range(0, len(rows), 200):
        batch = rows[i:i+200]
        data = json.dumps(batch).encode()
        req = Request(f"{SUPABASE_URL}/rest/v1/ba_contacts", data=data, headers={
            "apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json", "Prefer": "resolution=ignore-duplicates,return=minimal"
        }, method="POST")
        try:
            urlopen(req, timeout=30)
            total += len(batch)
        except HTTPError as e:
            err = e.read().decode()[:200]
            if "Empty or invalid" not in err:
                print(f"  Insert err: {err}")
        time.sleep(0.2)
    return total

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

# Load no-phone metro buildings
with open('no_phone_metro.json') as f:
    buildings = json.load(f)
print(f"Loaded {len(buildings)} metro buildings without phone contacts")

# Build spatial grid
grid = {}
for b in buildings:
    lat, lng = b.get("lat"), b.get("lng")
    if not lat or not lng: continue
    key = (round(lat, 2), round(lng, 2))
    if key not in grid:
        grid[key] = []
    grid[key].append(b)
print(f"Built grid with {len(grid)} cells")

# Reconstruct tiles
all_tiles = []
for lat_start in [x/10 for x in range(447, 453, 1)]:
    for lng_start in [x/10 for x in range(-937, -928, 1)]:
        all_tiles.append((lat_start, lng_start, lat_start + 0.1, lng_start + 0.1))

# Get only the failed tiles (convert 1-based to 0-based)
retry_tiles = [(i, all_tiles[i-1]) for i in FAILED if i-1 < len(all_tiles)]
print(f"Retrying {len(retry_tiles)} failed tiles with 5s delays")

total_osm = 0
total_matched = 0
total_inserted = 0
all_contacts = []

for attempt, (ti, (s_lat, s_lng, e_lat, e_lng)) in enumerate(retry_tiles):
    query = f"""
[out:json][timeout:30];
(
  nwr["phone"](
    {s_lat},{s_lng},{e_lat},{e_lng}
  );
  nwr["contact:phone"](
    {s_lat},{s_lng},{e_lat},{e_lng}
  );
);
out center tags;
"""
    max_retries = 3
    for retry in range(max_retries):
        try:
            req = Request(OVERPASS_URL, data=f"data={quote(query)}".encode(),
                         headers={"Content-Type": "application/x-www-form-urlencoded",
                                  "User-Agent": "BuildingAccess/1.0"})
            resp = urlopen(req, timeout=45)
            result = json.loads(resp.read())
            break
        except Exception as e:
            err_str = str(e)[:60]
            if retry < max_retries - 1:
                wait = 10 * (retry + 1)
                print(f"  Tile {ti}: retry {retry+1} after {wait}s ({err_str})")
                time.sleep(wait)
            else:
                print(f"  Tile {ti}: FAILED all retries ({err_str})")
                result = {"elements": []}

    elements = result.get("elements", [])
    tile_matched = 0

    for el in elements:
        tags = el.get("tags", {})
        phone = tags.get("phone") or tags.get("contact:phone")
        name = tags.get("name", "")
        email = tags.get("email") or tags.get("contact:email")

        if not phone or not name:
            continue

        cleaned = clean_phone(phone)
        if not cleaned:
            continue

        total_osm += 1

        el_lat = el.get("lat") or (el.get("center", {}).get("lat"))
        el_lng = el.get("lon") or (el.get("center", {}).get("lon"))
        if not el_lat or not el_lng:
            continue

        best_bid = None
        best_dist = 50

        for dlat in [-0.01, 0, 0.01]:
            for dlng in [-0.01, 0, 0.01]:
                key = (round(el_lat + dlat, 2), round(el_lng + dlng, 2))
                for b in grid.get(key, []):
                    dist = haversine(el_lat, el_lng, b["lat"], b["lng"])
                    if dist < best_dist:
                        best_dist = dist
                        best_bid = b["id"]

        if best_bid:
            tile_matched += 1
            amenity = tags.get("amenity", tags.get("shop", tags.get("office", "")))
            con = norm({
                "building_id": best_bid,
                "name": name,
                "role": amenity.replace("_", " ").title() if amenity else "Business",
                "phone": cleaned,
                "email": email if email else None,
                "source": "osm-overpass-retry",
                "notes": None
            })
            all_contacts.append(con)

    total_matched += tile_matched
    print(f"  Tile {ti}: {len(elements)} elements, {tile_matched} matched (total: {total_osm}/{total_matched})")

    # Insert every 5 tiles
    if (attempt + 1) % 5 == 0 or attempt == len(retry_tiles) - 1:
        if all_contacts:
            n = supabase_post(all_contacts)
            total_inserted += n
            all_contacts = []

    time.sleep(5)  # Longer delay for retry

# Final insert
if all_contacts:
    n = supabase_post(all_contacts)
    total_inserted += n

results = {
    "tiles_retried": len(retry_tiles),
    "osm_elements_with_phone": total_osm,
    "matched_to_buildings": total_matched,
    "contacts_inserted": total_inserted
}
with open('overpass_retry_results.json', 'w') as f:
    json.dump(results, f, indent=2)

print(f"\n{'='*60}")
print(f"OVERPASS RETRY COMPLETE")
print(f"  Tiles retried:           {len(retry_tiles)}")
print(f"  OSM elements w/ phone:   {total_osm:,}")
print(f"  Matched to buildings:    {total_matched:,}")
print(f"  Contacts inserted:       {total_inserted:,}")
