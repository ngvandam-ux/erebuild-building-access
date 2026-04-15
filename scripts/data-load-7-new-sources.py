#!/usr/bin/env python3
"""
Data Load 7: New Sources
- MN Hospitals (GeoPackage) - 134 facilities with phone, fax
- MN Nursing/Boarding Homes (GeoPackage) - 364 facilities with phone, fax, administrator
- MN DOH Healthcare Directory via ArcGIS - 325 facilities with phone, administrator
- MN Schools (CSV) - 3,008 unique locations with web URLs
- Minneapolis Liquor Licenses (ArcGIS) - on-sale + off-sale with business names
"""

import json, csv, sqlite3, struct, time, math
from urllib.request import urlopen, Request
from urllib.parse import quote
from pyproj import Transformer

SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg"

# UTM Zone 15N to WGS84 converter
transformer = Transformer.from_crs("EPSG:26915", "EPSG:4326", always_xy=True)

def extract_point(blob):
    """Extract x,y from GeoPackage standard binary geometry"""
    if not blob or len(blob) < 21:
        return None, None
    try:
        if blob[0:2] == b'GP':
            flags = blob[3]
            envelope_indicator = (flags >> 1) & 0x07
            env_sizes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}
            env_size = env_sizes.get(envelope_indicator, 0)
            wkb_offset = 8 + env_size
            byte_order = blob[wkb_offset]
            fmt = '<' if byte_order == 1 else '>'
            wkb_type = struct.unpack(fmt + 'I', blob[wkb_offset+1:wkb_offset+5])[0]
            if wkb_type == 1:
                x, y = struct.unpack(fmt + 'dd', blob[wkb_offset+5:wkb_offset+21])
                return x, y
        return None, None
    except:
        return None, None

def utm_to_latlon(easting, northing):
    """Convert UTM Zone 15N to lat/lon"""
    lon, lat = transformer.transform(easting, northing)
    return lat, lon

def supabase_post(table, rows):
    """Insert rows into Supabase, skip conflicts"""
    if not rows:
        return 0
    total = 0
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        data = json.dumps(batch).encode()
        req = Request(
            f"{SUPABASE_URL}/rest/v1/{table}",
            data=data,
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=ignore-duplicates,return=minimal"
            },
            method="POST"
        )
        try:
            resp = urlopen(req)
            total += len(batch)
        except Exception as e:
            body = e.read().decode() if hasattr(e, 'read') else str(e)
            print(f"  Error inserting batch: {body[:200]}")
            # Try one by one
            for row in batch:
                try:
                    d2 = json.dumps([row]).encode()
                    req2 = Request(
                        f"{SUPABASE_URL}/rest/v1/{table}",
                        data=d2,
                        headers={
                            "apikey": SUPABASE_KEY,
                            "Authorization": f"Bearer {SUPABASE_KEY}",
                            "Content-Type": "application/json",
                            "Prefer": "resolution=ignore-duplicates,return=minimal"
                        },
                        method="POST"
                    )
                    urlopen(req2)
                    total += 1
                except:
                    pass
        time.sleep(0.3)
    return total

def fetch_arcgis_all(base_url, where="1=1", max_records=2000):
    """Fetch all records from ArcGIS FeatureServer with pagination"""
    all_features = []
    offset = 0
    while True:
        url = f"{base_url}/query?where={quote(where)}&outFields=*&f=json&resultRecordCount={max_records}&resultOffset={offset}"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urlopen(req)
        data = json.loads(resp.read())
        features = data.get("features", [])
        all_features.extend(features)
        if len(features) < max_records:
            break
        offset += max_records
        time.sleep(0.5)
    return all_features

def normalize_address(addr):
    """Normalize address for matching"""
    if not addr:
        return ""
    return addr.upper().strip().replace(".", "").replace(",", "").replace("  ", " ")

def clean_phone(phone):
    """Clean phone number"""
    if not phone:
        return None
    import re
    digits = re.sub(r'\D', '', str(phone))
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    return phone.strip() if phone.strip() else None


# ============================================================
# 1. HOSPITALS (GeoPackage)
# ============================================================
print("=" * 60)
print("1. MN HOSPITALS (GeoPackage)")
print("=" * 60)

conn = sqlite3.connect('hospitals_dir/health_facility_hospitals.gpkg')
rows = conn.execute("""
    SELECT HFID, NAME, ADDRESS, CITY, STATE, ZIP, TELEPHONE, FAX, Shape
    FROM hospitals
""").fetchall()
conn.close()

hospital_buildings = []
hospital_contacts = []

for r in rows:
    hfid, name, address, city, state, zipcode, phone, fax, shape = r
    easting, northing = extract_point(shape)
    lat, lon = None, None
    if easting and northing:
        lat, lon = utm_to_latlon(easting, northing)
    
    if not address or not city:
        continue
    
    full_addr = f"{address}, {city}, MN"
    zipstr = str(int(zipcode)) if zipcode else ""
    
    bld = {
        "address": full_addr,
        "city": city.title(),
        "state": "MN",
        "zip": zipstr,
        "building_type": "hospital",
        "building_name": name.title() if name else None,
        "source": "mn-hospitals",
        "units": None,
    }
    if lat and lon:
        bld["latitude"] = round(lat, 6)
        bld["longitude"] = round(lon, 6)
    hospital_buildings.append(bld)
    
    # Contact for the hospital
    contact = {
        "building_address": full_addr,
        "contact_name": name.title() if name else None,
        "contact_role": "Hospital Admin",
        "phone": clean_phone(phone),
        "email": None,
        "source": "mn-hospitals",
    }
    if fax:
        contact["notes"] = f"Fax: {fax}"
    hospital_contacts.append(contact)

print(f"  Parsed {len(hospital_buildings)} hospitals, {len(hospital_contacts)} contacts")

# ============================================================
# 2. NURSING & BOARDING HOMES (GeoPackage)
# ============================================================
print("\n" + "=" * 60)
print("2. MN NURSING & BOARDING HOMES (GeoPackage)")
print("=" * 60)

conn = sqlite3.connect('nursing_dir/health_facility_nursing_boarding.gpkg')
rows = conn.execute("""
    SELECT HFID, NAME, ADDRESS, CITY, STATE, ZIP, TELEPHONE, FAX, ADMINISTRATOR, LIC_TYPE, PROV_TYPE, Shape
    FROM nursing_boarding_homes
""").fetchall()
conn.close()

nursing_buildings = []
nursing_contacts = []

for r in rows:
    hfid, name, address, city, state, zipcode, phone, fax, admin, lic_type, prov_type, shape = r
    easting, northing = extract_point(shape)
    lat, lon = None, None
    if easting and northing:
        lat, lon = utm_to_latlon(easting, northing)
    
    if not address or not city:
        continue
    
    full_addr = f"{address}, {city}, MN"
    zipstr = str(int(zipcode)) if zipcode else ""
    
    btype = "nursing_home"
    if prov_type and "BOARD" in prov_type.upper():
        btype = "boarding_care"
    
    bld = {
        "address": full_addr,
        "city": city.title(),
        "state": "MN",
        "zip": zipstr,
        "building_type": btype,
        "building_name": name.title() if name else None,
        "source": "mn-nursing-homes",
        "units": None,
    }
    if lat and lon:
        bld["latitude"] = round(lat, 6)
        bld["longitude"] = round(lon, 6)
    nursing_buildings.append(bld)
    
    # Admin contact
    contact = {
        "building_address": full_addr,
        "contact_name": admin.title() if admin else (name.title() if name else None),
        "contact_role": "Administrator" if admin else "Facility",
        "phone": clean_phone(phone),
        "email": None,
        "source": "mn-nursing-homes",
    }
    notes_parts = []
    if fax:
        notes_parts.append(f"Fax: {fax}")
    if prov_type:
        notes_parts.append(f"Type: {prov_type}")
    if notes_parts:
        contact["notes"] = " | ".join(notes_parts)
    nursing_contacts.append(contact)

print(f"  Parsed {len(nursing_buildings)} nursing homes, {len(nursing_contacts)} contacts")

# ============================================================
# 3. MN DOH HEALTHCARE DIRECTORY (ArcGIS)
# ============================================================
print("\n" + "=" * 60)
print("3. MN DOH HEALTHCARE DIRECTORY (ArcGIS)")
print("=" * 60)

doh_url = "https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/Health_Care_Directory_10_17_25/FeatureServer/0"
features = fetch_arcgis_all(doh_url)

doh_buildings = []
doh_contacts = []

for feat in features:
    a = feat.get("attributes", {})
    g = feat.get("geometry", {})
    
    name = a.get("USER_NAME", "")
    address = a.get("USER_ADDRESS", "")
    city = a.get("USER_CITY", "")
    state = a.get("USER_STATE", "MN")
    zipcode = str(a.get("USER_ZIP", "")) if a.get("USER_ZIP") else ""
    phone = a.get("USER_TELEPHONE", "")
    admin = a.get("USER_ADMINISTRATOR_AUTHORIZED_A", "")
    prov_type = a.get("USER_ALL_PROV", "")
    
    if not address or not city:
        continue
    
    full_addr = f"{address}, {city}, MN"
    
    bld = {
        "address": full_addr,
        "city": city.title(),
        "state": "MN",
        "zip": zipcode,
        "building_type": "healthcare",
        "building_name": name.title() if name else None,
        "source": "mn-doh-healthcare",
    }
    if g.get("x") and g.get("y"):
        bld["longitude"] = round(g["x"], 6)
        bld["latitude"] = round(g["y"], 6)
    doh_buildings.append(bld)
    
    contact = {
        "building_address": full_addr,
        "contact_name": admin.title() if admin else (name.title() if name else None),
        "contact_role": "Administrator" if admin else "Facility",
        "phone": clean_phone(phone),
        "email": None,
        "source": "mn-doh-healthcare",
    }
    if prov_type:
        contact["notes"] = f"Provider: {prov_type}"
    doh_contacts.append(contact)

print(f"  Parsed {len(doh_buildings)} DOH facilities, {len(doh_contacts)} contacts")

# ============================================================
# 4. MN SCHOOLS (CSV)
# ============================================================
print("\n" + "=" * 60)
print("4. MN SCHOOLS (CSV)")
print("=" * 60)

school_buildings = []
school_contacts = []
seen_addrs = set()

with open('mn_schools_dir/school_program_locations.csv', encoding='latin-1') as f:
    reader = csv.DictReader(f)
    for r in reader:
        addr = r.get('gisaddr', '').strip()
        lat = r.get('latitude', '').strip()
        lng = r.get('longitude', '').strip()
        name = r.get('gisname', '').strip()
        city_match = ""
        
        if not addr or not lat or not lng:
            continue
        
        # Extract city from address (format: "123 Main St, City, MN 55555")
        parts = addr.split(',')
        if len(parts) >= 2:
            city_match = parts[-2].strip() if len(parts) >= 3 else parts[-1].strip()
            # Remove state/zip
            city_match = city_match.replace(" MN ", "").replace(" MN", "").strip()
            for d in "0123456789":
                city_match = city_match.replace(d, "")
            city_match = city_match.strip()
        
        norm = normalize_address(addr)
        if norm in seen_addrs:
            continue
        seen_addrs.add(norm)
        
        county = r.get('countyname', '').strip()
        web = r.get('web_url', '').strip()
        pubpriv = r.get('pubpriv', '').strip()
        
        bld = {
            "address": addr,
            "city": city_match.title() if city_match else "",
            "state": "MN",
            "zip": "",
            "building_type": "school",
            "building_name": name,
            "source": "mn-schools",
            "latitude": round(float(lat), 6),
            "longitude": round(float(lng), 6),
        }
        school_buildings.append(bld)
        
        # Contact with web URL
        contact = {
            "building_address": addr,
            "contact_name": name,
            "contact_role": f"{pubpriv} School" if pubpriv else "School",
            "phone": None,
            "email": None,
            "source": "mn-schools",
        }
        notes = []
        if web and web != "NULL":
            notes.append(f"Web: {web}")
        if county:
            notes.append(f"County: {county}")
        if notes:
            contact["notes"] = " | ".join(notes)
        school_contacts.append(contact)

print(f"  Parsed {len(school_buildings)} schools, {len(school_contacts)} contacts")

# ============================================================
# 5. MINNEAPOLIS LIQUOR LICENSES (ArcGIS)
# ============================================================
print("\n" + "=" * 60)
print("5. MINNEAPOLIS LIQUOR LICENSES (ArcGIS)")
print("=" * 60)

liquor_buildings = []
liquor_contacts = []

for ltype in ["Off_Sale_Liquor", "On_Sale_Liquor"]:
    url = f"https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/{ltype}/FeatureServer/0"
    features = fetch_arcgis_all(url)
    
    for feat in features:
        a = feat.get("attributes", {})
        name = a.get("licenseName", "")
        address = a.get("address", "")
        lat = a.get("lat")
        lon = a.get("long")
        lic_type = a.get("licenseType", "")
        status = a.get("licenseStatus", "")
        
        if not address or not name:
            continue
        
        full_addr = f"{address}, MINNEAPOLIS, MN"
        
        bld = {
            "address": full_addr,
            "city": "Minneapolis",
            "state": "MN",
            "zip": "",
            "building_type": "commercial",
            "building_name": name.title() if name else None,
            "source": "mpls-liquor-license",
        }
        if lat and lon:
            bld["latitude"] = round(float(lat), 6)
            bld["longitude"] = round(float(lon), 6)
        liquor_buildings.append(bld)
        
        contact = {
            "building_address": full_addr,
            "contact_name": name.title() if name else None,
            "contact_role": "Business",
            "phone": None,
            "email": None,
            "source": "mpls-liquor-license",
            "notes": f"License: {lic_type} ({a.get('liquorType','')}) | Status: {status}",
        }
        liquor_contacts.append(contact)

print(f"  Parsed {len(liquor_buildings)} liquor-licensed businesses, {len(liquor_contacts)} contacts")

# ============================================================
# LOAD INTO SUPABASE
# ============================================================
print("\n" + "=" * 60)
print("LOADING INTO SUPABASE")
print("=" * 60)

all_buildings = hospital_buildings + nursing_buildings + doh_buildings + school_buildings + liquor_buildings
all_contacts = hospital_contacts + nursing_contacts + doh_contacts + school_contacts + liquor_contacts

print(f"\nTotal buildings to load: {len(all_buildings)}")
print(f"Total contacts to load: {len(all_contacts)}")

# Load buildings
print("\nLoading buildings...")
b_count = supabase_post("ba_buildings", all_buildings)
print(f"  Inserted {b_count} buildings")

# Load contacts
print("\nLoading contacts...")
c_count = supabase_post("ba_contacts", all_contacts)
print(f"  Inserted {c_count} contacts")

# ============================================================
# FINAL STATS
# ============================================================
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  Hospitals:         {len(hospital_buildings):>5} buildings, {len(hospital_contacts):>5} contacts")
print(f"  Nursing Homes:     {len(nursing_buildings):>5} buildings, {len(nursing_contacts):>5} contacts")
print(f"  DOH Healthcare:    {len(doh_buildings):>5} buildings, {len(doh_contacts):>5} contacts")
print(f"  Schools:           {len(school_buildings):>5} buildings, {len(school_contacts):>5} contacts")
print(f"  Liquor Licenses:   {len(liquor_buildings):>5} buildings, {len(liquor_contacts):>5} contacts")
print(f"  ─────────────────────────────────────────────")
print(f"  TOTAL:             {len(all_buildings):>5} buildings, {len(all_contacts):>5} contacts")
print(f"  Inserted:          {b_count:>5} buildings, {c_count:>5} contacts")
