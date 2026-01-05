import os
import pandas as pd
import requests
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter, Retry
import random
from src.config import GOOGLE_MAPS_API_KEY

MAX_WORKERS = 5  # Increase if API quota allows

# ---------------------------
# HELPERS
# ---------------------------
def clean_text(text):
    if not isinstance(text, str):
        return ""
    return re.sub(r"\s+", " ", text.strip())

def normalize_key(text):
    """Normalize text to create a robust lookup key."""
    if not isinstance(text, str):
        return str(text).lower().strip()
    # Remove all whitespace, underscores, lower case
    return re.sub(r"[\s_]+", "", text.lower())

import json
from src.config import DATA_DIR

# ---------------------------
# CATEGORY MAP MANAGEMENT
# ---------------------------
CATEGORY_MAP_FILE = f"{DATA_DIR}/category_map.json"

def load_category_map():
    if os.path.exists(CATEGORY_MAP_FILE):
        with open(CATEGORY_MAP_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_category_mapping(name, uuid):
    current_map = load_category_map()
    current_map[name] = uuid
    with open(CATEGORY_MAP_FILE, 'w', encoding='utf-8') as f:
        json.dump(current_map, f, indent=4)
    refresh_category_map()

CATEGORY_MAP = load_category_map()

# Pre-compute normalized map for case-insensitive/fuzzy matching
NORMALIZED_CATEGORY_MAP = {
    k.lower().replace("_", "").replace(" ", "").strip(): v 
    for k, v in CATEGORY_MAP.items()
}

def refresh_category_map():
    global CATEGORY_MAP, NORMALIZED_CATEGORY_MAP
    CATEGORY_MAP = load_category_map()
    CATEGORY_MAP = load_category_map()
    NORMALIZED_CATEGORY_MAP = {}
    for k, v in CATEGORY_MAP.items():
        NORMALIZED_CATEGORY_MAP[normalize_key(k)] = v

def get_category_id(format_type):
    if not isinstance(format_type, str):
        return None
    # Normalize input: lowercase, remove underscores/spaces
    # Normalize input using robust regex
    key = normalize_key(format_type)
    result = NORMALIZED_CATEGORY_MAP.get(key, None)
    return result

def map_lighting_type(lighting):
    if not isinstance(lighting, str):
        return "NL"
    lighting = lighting.strip().lower()
    if "digital" in lighting:
        return "Digital"
    if "back" in lighting or lighting == "bl":
        return "BL"
    if "front" in lighting or lighting == "fl":
        return "FL"
    return "NL"

# ---------------------------
# ADDRESS CACHE + FETCH
# ---------------------------
_address_cache = {}

session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

def fetch_address(lat, lng):
    key = f"{lat},{lng}"
    if key in _address_cache:
        return _address_cache[key]
    
    if not GOOGLE_MAPS_API_KEY:
        print("⚠️ Google Maps API Key missing.")
        return (None, None, None, None)

    try:
        time.sleep(random.uniform(0, 0.3))
        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lng}&key={GOOGLE_MAPS_API_KEY}"
        res = session.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        if data.get("status") != "OK" or not data.get("results"):
            result = (None, None, None, None)
        else:
            result = parse_address(data["results"][0])
    except Exception as e:
        print(f"⚠️ Error fetching {key}: {e}")
        result = (None, None, None, None)

    _address_cache[key] = result
    return result

def parse_address(result):
    formatted = result.get("formatted_address", None)
    comps = result.get("address_components", [])
    city = area = street = None

    for comp in comps:
        types = comp["types"]
        if "locality" in types:
            city = comp["long_name"]
        elif "sublocality" in types or "neighborhood" in types:
            area = comp["long_name"]
        elif "route" in types:
            street = comp["long_name"]
    return formatted, city, area, street

# ---------------------------
# TITLE/DESCRIPTION
# ---------------------------
def enhance_title_and_description(row, city, lighting_type):
    format_type = clean_text(row.get("format_type", ""))
    location = clean_text(row.get("location", ""))
    area = clean_text(row.get("area", ""))
    city = clean_text(city or row.get("city", ""))
    
    width = row.get("width_ft", 0)
    height = row.get("height_ft", 0)
    size_info = f'{width}x{height} ft'
    
    rate = row.get("base_rate_per_unit", 0)
    try:
        rate_val = int(float(rate)) if rate else 0
        rate_str = f"₹{rate_val:,}"
    except:
        rate_str = "Price on Request"

    lighting_label = lighting_type.capitalize()

    title = f"{format_type.replace('_', ' ')} in {location}, {city} ({size_info})"
    description = (
        f"{lighting_label} {format_type.replace('_', ' ')} located at {location}, {city}. "
        f"This unit measures {size_info} and is available at {rate_str} per month."
    )

    return title, description

# ---------------------------
# MAIN TRANSFORMATION
# ---------------------------
def transform_dataframe(df):
    # Ensure we have the latest mapping
    refresh_category_map()
    
    # Parallel address fetching
    # Normalize columns
    df.columns = [c.lower().strip() for c in df.columns]
    
    # Map common variations to standard columns
    col_map = {
        'latitude': 'lat',
        'longitude': 'lon',
        'lng': 'lon',
        'base_rate_per_month': 'base_rate_per_unit',
        'card_rate_per_month': 'card_rate_per_unit'
    }
    df = df.rename(columns=col_map)

    # ---------------------------------------------------------
    # NEW: Single Column Parsing (Dimensions & Coordinates)
    # ---------------------------------------------------------
    
    # 1. Parse Dimensions (e.g. "10x20", "10 x 20", "10*20") -> width_ft, height_ft
    if 'dimensions' in df.columns:
        def parse_dims(val):
            if pd.isna(val): return None, None
            s = str(val).lower().strip()
            # Match pattern like number separator number
            # Separators can be x, *, or whitespace
            match = re.search(r'(\d+(?:\.\d+)?)\s*[xX\*]\s*(\d+(?:\.\d+)?)', s)
            if match:
                return float(match.group(1)), float(match.group(2))
            return None, None

        # Apply parsing
        parsed = df['dimensions'].apply(parse_dims)
        # Only overwrite if width/height don't exist or are empty
        if 'width_ft' not in df.columns:
            df['width_ft'] = parsed.apply(lambda x: x[0])
        if 'height_ft' not in df.columns:
            df['height_ft'] = parsed.apply(lambda x: x[1])
            
    # 2. Parse Coordinates (e.g. "12.34, 56.78", "12.34 56.78") -> lat, lon
    if 'coordinates' in df.columns:
        def parse_coords(val):
            if pd.isna(val): return None, None
            s = str(val).strip()
            # Match two numbers separated by comma or space
            # \d+(\.\d+)? matches a float
            match = re.search(r'(-?\d+(?:\.\d+)?)[,\s]+(-?\d+(?:\.\d+)?)', s)
            if match:
                try:
                    lat_val = float(match.group(1))
                    lon_val = float(match.group(2))
                    return lat_val, lon_val
                except:
                    pass
            return None, None

        parsed_coords = df['coordinates'].apply(parse_coords)
        
        # We need to map to internal 'lat'/'lon' names used by this script
        if 'lat' not in df.columns:
            df['lat'] = parsed_coords.apply(lambda x: x[0])
        if 'lon' not in df.columns:
            df['lon'] = parsed_coords.apply(lambda x: x[1])

    # ---------------------------------------------------------

    # Clean format_type column to ensure consistent matching
    if 'format_type' in df.columns:
        df['format_type'] = df['format_type'].astype(str).replace('nan', '').apply(lambda x: re.sub(r'\s+', ' ', x).strip())

    # Ensure lat/lon are clean floats
    if 'lat' not in df.columns or 'lon' not in df.columns:
        raise ValueError(f"Missing coordinate columns. Found: {df.columns.tolist()}")

    df["lat"] = pd.to_numeric(df.get("lat"), errors='coerce')
    df["lon"] = pd.to_numeric(df.get("lon"), errors='coerce')
    
    coords = [(row["lat"], row["lon"]) for _, row in df.iterrows() if pd.notnull(row["lat"]) and pd.notnull(row["lon"])]
    address_results = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_coord = {executor.submit(fetch_address, lat, lon): (lat, lon) for lat, lon in coords}
        for future in as_completed(future_to_coord):
            lat, lon = future_to_coord[future]
            address_results[(lat, lon)] = future.result()

    output = []
    missing_categories = set()
    
    for _, row in df.iterrows():
        lat, lng = row["lat"], row["lon"]
        current_lighting = row.get("lighting_type", "")
        lighting_type = map_lighting_type(current_lighting)
        
        formatted_address, city, area, street = address_results.get((lat, lng), (None, None, None, None))

        title, description = enhance_title_and_description(row, city, lighting_type)
        thumbnail_url = row.get("image_urls", "")
        
        format_type = row.get("format_type")
        category_id = get_category_id(format_type)
        if category_id is None and format_type:
            missing_categories.add(format_type)

        record = {
            "organization_id": "57c3a02b-47bd-4184-a7c7-f6119eb8af59",
            "owner_id": "58ef0e87-ffe4-478e-8d92-c3d922fd015b",
            "source_iid": row.get("billboard_id", ""),
            "category_id": category_id,
            "lighting_type": lighting_type,
            "quantity": row.get("quantity", 1),
            "title": title,
            "description": description,
            "latitude": lat,
            "longitude": lng,
            "google_location": formatted_address,
            "address": row.get("location", ""),
            "city": city or row.get("city", ""),
            "street": street or row.get("area", ""),
            "area": area or row.get("area", ""),
            "landmark": row.get("district", ""),
            "height": row.get("height_ft", ""),
            "width": row.get("width_ft", ""),
            "unit": "ft",
            "resolution": None,
            "area_sqm": None,
            "card_rate_per_unit": row.get("card_rate_per_unit", 0),
            "base_rate_per_unit": row.get("base_rate_per_unit", 0),
            "listing_status": "active",
            "verification_status": "approved",
            "thumbnail_url": thumbnail_url,
            "verified_by": None, 
            "verified_at": None, 
            "admin_notes": None, 
            "supporting_documents": None,
            "is_archived": False, 
            "updated_by": None, 
            "representative_id": None, 
            "representative_name": None, 
            "representative_email": None,
        }

        output.append(record)

    return pd.DataFrame(output), list(missing_categories)
