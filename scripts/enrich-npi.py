#!/usr/bin/env python3
"""
NPI Registry Enrichment: Query NPI API by city, match organizations
to existing buildings by address, add phone contacts.
"""
import json, re, time, sys
from urllib.request import urlopen, Request
from urllib.error import HTTPError

SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg"

CON_KEYS = ["building_id", "name", "role", "phone", "email", "notes", "source"]

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

def normalize_addr(addr):
    """Normalize address for fuzzy matching"""
    a = addr.upper().strip()
    # Remove common variations
    a = a.replace(".", "").replace(",", "")
    a = re.sub(r'\bSTREET\b', 'ST', a)
    a = re.sub(r'\bAVENUE\b', 'AVE', a)
    a = re.sub(r'\bBOULEVARD\b', 'BLVD', a)
    a = re.sub(r'\bDRIVE\b', 'DR', a)
    a = re.sub(r'\bLANE\b', 'LN', a)
    a = re.sub(r'\bROAD\b', 'RD', a)
    a = re.sub(r'\bNORTH\b', 'N', a)
    a = re.sub(r'\bSOUTH\b', 'S', a)
    a = re.sub(r'\bEAST\b', 'E', a)
    a = re.sub(r'\bWEST\b', 'W', a)
    a = re.sub(r'\bSUITE\s*#?\s*', 'STE ', a)
    a = re.sub(r'\s+', ' ', a).strip()
    return a

# Load address -> building_id mapping
with open('addr_to_bid_nophone.json') as f:
    raw_map = json.load(f)

# Build normalized lookup
addr_map = {}
for addr, bid in raw_map.items():
    addr_map[normalize_addr(addr)] = bid
    # Also index just the street part (before first comma)
    street = addr.split(",")[0].strip().upper()
    addr_map[normalize_addr(street)] = bid

print(f"Loaded {len(addr_map)} address variants for matching")

# Load city list
with open('npi_cities.txt') as f:
    all_cities = [line.strip() for line in f if line.strip()]

# Focus on top cities by building count (most productive)
# NPI API caps at 1200 results per city query
top_cities = all_cities[:100]  # Top 100 cities
print(f"Querying NPI for {len(top_cities)} cities")

total_found = 0
total_matched = 0
total_inserted = 0
contacts_batch = []

for ci, city in enumerate(top_cities):
    skip = 0
    city_found = 0
    city_matched = 0
    
    while skip <= 1000:
        url = f"https://npiregistry.cms.hhs.gov/api/?version=2.1&state=MN&city={city.replace(' ', '+')}&enumeration_type=NPI-2&limit=200&skip={skip}"
        try:
            req = Request(url, headers={"User-Agent": "BuildingAccess/1.0"})
            resp = urlopen(req, timeout=20)
            d = json.loads(resp.read())
        except Exception as e:
            break
        
        results = d.get("results", [])
        if not results:
            break
        
        for r in results:
            basic = r.get("basic", {})
            org_name = basic.get("organization_name", "")
            if not org_name:
                continue
            
            for a in r.get("addresses", []):
                if a.get("address_purpose") == "LOCATION":
                    addr1 = a.get("address_1", "").strip()
                    acity = a.get("city", "").strip()
                    phone = a.get("telephone_number", "")
                    fax = a.get("fax_number", "")
                    
                    if not addr1 or not phone:
                        continue
                    
                    city_found += 1
                    
                    # Try to match
                    full_addr = f"{addr1}, {acity}, MN"
                    norm_full = normalize_addr(full_addr)
                    bid = addr_map.get(norm_full)
                    
                    if not bid:
                        # Try street-only match with city
                        norm_street = normalize_addr(addr1)
                        for city_var in [acity.upper(), city.upper()]:
                            test = f"{norm_street}, {city_var}, MN"
                            bid = addr_map.get(normalize_addr(test))
                            if bid: break
                    
                    if bid:
                        city_matched += 1
                        taxos = r.get("taxonomies", [])
                        tax_desc = taxos[0].get("desc", "") if taxos else ""
                        
                        con = norm({
                            "building_id": bid,
                            "name": org_name.title(),
                            "role": "Healthcare Organization",
                            "phone": clean_phone(phone),
                            "email": None,
                            "source": "npi-registry",
                            "notes": f"Specialty: {tax_desc}" if tax_desc else None
                        })
                        contacts_batch.append(con)
                    break
        
        if len(results) < 200:
            break
        skip += 200
        time.sleep(0.3)
    
    total_found += city_found
    total_matched += city_matched
    
    # Insert batch every 10 cities
    if (ci + 1) % 10 == 0 or ci == len(top_cities) - 1:
        if contacts_batch:
            n = supabase_post(contacts_batch)
            total_inserted += n
            contacts_batch = []
        print(f"  [{ci+1}/{len(top_cities)}] {city}: {city_found} found, {city_matched} matched | Running total: {total_inserted} inserted")
    
    time.sleep(0.2)

# Final insert
if contacts_batch:
    n = supabase_post(contacts_batch)
    total_inserted += n

# Save results
results = {
    "total_npi_orgs": total_found,
    "matched_to_buildings": total_matched,
    "contacts_inserted": total_inserted,
    "cities_queried": len(top_cities)
}
with open('npi_enrichment_results.json', 'w') as f:
    json.dump(results, f, indent=2)

print(f"\n{'='*60}")
print(f"NPI ENRICHMENT COMPLETE")
print(f"  Cities queried:      {len(top_cities)}")
print(f"  NPI orgs found:      {total_found:,}")
print(f"  Matched to buildings: {total_matched:,}")
print(f"  Contacts inserted:   {total_inserted:,}")
