#!/usr/bin/env python3
"""
OSM Overpass Enrichment: Search for businesses with phone numbers
near buildings that lack contacts. Uses geographic tiles.
"""
import json, re, time, math
from urllib.request import urlopen, Request
from urllib.error import HTTPError
from urllib.parse import quote

SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
CON_KEYS = ["building_id", "name", "role", "phone", "email", "notes", "source"]

def clean_phone(phone):
    if not phone: return None
    # Handle various formats: +1-xxx, (xxx) xxx-xxxx, xxx.xxx.xxxx
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
    """Distance in meters between two lat/lon points"""
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

# Build spatial index: grid of 0.01 degree (~1km) cells
grid = {}
for b in buildings:
    lat, lng = b.get("lat"), b.get("lng")
    if not lat or not lng: continue
    key = (round(lat, 2), round(lng, 2))
    if key not in grid:
        grid[key] = []
    grid[key].append(b)

print(f"Built grid with {len(grid)} cells")

# Divide metro area into tiles for Overpass queries
# Metro bounding box: 44.7-45.3 lat, -93.7 to -92.8 lng
# Use 0.1 degree tiles (~11km x 8km)
tiles = []
for lat_start in [x/10 for x in range(447, 453, 1)]:
    for lng_start in [x/10 for x in range(-937, -928, 1)]:
        lat_end = lat_start + 0.1
        lng_end = lng_start + 0.1
        tiles.append((lat_start, lng_start, lat_end, lng_end))

print(f"Created {len(tiles)} geographic tiles")

total_osm = 0
total_matched = 0
total_inserted = 0
all_contacts = []

for ti, (s_lat, s_lng, e_lat, e_lng) in enumerate(tiles):
    # Query Overpass for nodes/ways with phone tag in this tile
    query = f"""
[out:json][timeout:25];
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
    try:
        data = quote(query).encode()
        req = Request(OVERPASS_URL, data=f"data={quote(query)}".encode(),
                     headers={"Content-Type": "application/x-www-form-urlencoded",
                              "User-Agent": "BuildingAccess/1.0"})
        resp = urlopen(req, timeout=30)
        result = json.loads(resp.read())
    except Exception as e:
        print(f"  Tile {ti+1}: error - {str(e)[:60]}")
        time.sleep(5)
        continue
    
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
        
        # Get element location
        el_lat = el.get("lat") or (el.get("center", {}).get("lat"))
        el_lng = el.get("lon") or (el.get("center", {}).get("lon"))
        if not el_lat or not el_lng:
            continue
        
        # Find nearest building within 50m
        best_bid = None
        best_dist = 50  # max 50 meters
        
        # Check nearby grid cells
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
            
            # Build contact
            amenity = tags.get("amenity", tags.get("shop", tags.get("office", "")))
            con = norm({
                "building_id": best_bid,
                "name": name,
                "role": amenity.replace("_", " ").title() if amenity else "Business",
                "phone": cleaned,
                "email": email if email else None,
                "source": "osm-overpass-enrich",
                "notes": None
            })
            all_contacts.append(con)
    
    total_matched += tile_matched
    
    # Insert every 10 tiles
    if (ti + 1) % 10 == 0 or ti == len(tiles) - 1:
        if all_contacts:
            n = supabase_post(all_contacts)
            total_inserted += n
            all_contacts = []
        print(f"  [{ti+1}/{len(tiles)}] OSM elements: {total_osm}, matched: {total_matched}, inserted: {total_inserted}")
    
    time.sleep(2)  # Be polite to Overpass

# Final insert
if all_contacts:
    n = supabase_post(all_contacts)
    total_inserted += n

results = {
    "tiles_queried": len(tiles),
    "osm_elements_with_phone": total_osm,
    "matched_to_buildings": total_matched,
    "contacts_inserted": total_inserted
}
with open('overpass_enrichment_results.json', 'w') as f:
    json.dump(results, f, indent=2)

print(f"\n{'='*60}")
print(f"OVERPASS ENRICHMENT COMPLETE")
print(f"  Tiles queried:          {len(tiles)}")
print(f"  OSM elements w/ phone:  {total_osm:,}")
print(f"  Matched to buildings:   {total_matched:,}")
print(f"  Contacts inserted:      {total_inserted:,}")
