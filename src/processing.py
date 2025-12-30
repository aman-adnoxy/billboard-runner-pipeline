import pandas as pd
import numpy as np
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- HELPER FUNCTIONS ---

def clean_numeric(val):
    """Extracts valid numbers from messy strings (e.g., 'Rs. 50,000' -> 50000.0)."""
    if pd.isna(val): return np.nan
    s = str(val).replace(',', '').strip()
    match = re.search(r'(\d+(\.\d+)?)', s)
    return float(match.group(1)) if match else np.nan

def parse_coord_string(val):
    """Splits '77.60, 12.95' into (12.95, 77.60)."""
    if pd.isna(val) or str(val).strip() in ['', '0,0', '0', 'nan']: 
        return None, None
    try:
        parts = str(val).split(',')
        if len(parts) >= 2:
            return float(parts[1].strip()), float(parts[0].strip())
    except: 
        pass
    return None, None

def extract_dim_str(s, marker):
    """Extracts number before 'W' or 'H'."""
    if pd.isna(s): return None
    pattern = r'(\d+(\.\d+)?)\s*' + marker
    match = re.search(pattern, str(s), re.IGNORECASE)
    return float(match.group(1)) if match else None

# --- CORE TRANSFORMATION ---

def standard_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes IDs and basic string cleanups."""
    initial_rows = len(df)
    
    if 'id' in df.columns: df = df.drop(columns=['id'])
    
    if 'billboard_id' not in df.columns:
        print(f"INFO >>> Step 1 | input_rows: {initial_rows} | output_rows: 0")
        return df.iloc[0:0]

    df = df.dropna(subset=['billboard_id'])
    df['billboard_id'] = df['billboard_id'].astype(str).str.strip()
    df = df.drop_duplicates(subset=['billboard_id'])
    
    final_rows = len(df)
    print(f"INFO >>> Step 1 | input_rows: {initial_rows} | output_rows: {final_rows}")

    # Format Mapping
    format_map = {
        "Bus Shelter": "Bus_Shelter", "Skywalk": "Gantry", 
        "Digital OOH": "Digital_OOH", "Road Median": "Road_Median", 
        "Pole Kiosk": "Pole_Kiosk", "Hoarding": "Hoarding"
    }
    if 'format_type' in df.columns:
        df['format_type'] = df['format_type'].map(lambda x: format_map.get(x, x))

    # Lighting Mapping
    lighting_map = {
        "NON LIT": "Unlit", "UNLIT": "Unlit", "BACK LIT": "Backlit", 
        "FRONT LIT": "Frontlit", "LED": "Digital", "DIGITAL": "Digital"
    }
    if 'lighting_type' in df.columns:
        df['lighting_type'] = df['lighting_type'].astype(str).str.upper().str.strip()
        df['lighting_type'] = df['lighting_type'].map(lambda x: lighting_map.get(x, x.title()))

    return df

def extract_geography(df: pd.DataFrame) -> pd.DataFrame:
    """Parses Coordinates and fills location hierarchy."""
    total_rows = len(df)
    
    # Initialize columns if missing
    if 'latitude' not in df.columns: df['latitude'] = np.nan
    if 'longitude' not in df.columns: df['longitude'] = np.nan

    # Smart Parse
    if 'coordinates' in df.columns:
        mask_missing = df['latitude'].isna() | df['longitude'].isna()
        if mask_missing.any():
            parsed = df.loc[mask_missing, 'coordinates'].apply(parse_coord_string)
            df.loc[mask_missing, 'latitude'] = parsed.apply(lambda x: x[0] if x else None)
            df.loc[mask_missing, 'longitude'] = parsed.apply(lambda x: x[1] if x else None)

    # Force Numeric
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

    # Hierarchy
    if 'city' in df.columns:
        df['city'] = df['city'].astype(str).str.title()
        if 'district' not in df.columns: df['district'] = df['city']
        else: df['district'] = df['district'].fillna(df['city'])

    if 'locality' in df.columns and 'area' not in df.columns:
        df['area'] = df['locality'].astype(str).apply(lambda x: x.split(',')[0].strip() if ',' in x else x)

    # Fill Location from Address if exists
    if 'address' in df.columns and 'location' not in df.columns:
        df['location'] = df['address']
    
    # --- REVERSE GEOCODING LOGIC ---
    if 'location' not in df.columns:
        df['location'] = np.nan
        
    mask_loc_missing = df['location'].isna() | (df['location'].astype(str).str.strip() == '')
    mask_has_coords = df['latitude'].notna() & df['longitude'].notna()
    
    rows_to_geocode = mask_loc_missing & mask_has_coords
    
    empty_loc_count = rows_to_geocode.sum()
    success_count = 0
    failed_count = 0

    if rows_to_geocode.any():
        print(f"   Geocoding {empty_loc_count} rows marked for missing location...")
        geolocator = Nominatim(user_agent="billboard_pipeline_v1")
        geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)
        
        def get_address(lat, lon):
            try:
                location = geocode(f"{lat}, {lon}")
                return location.address if location else None
            except Exception as e:
                print(f"   Geo Error ({lat},{lon}): {e}")
                return None

        # Apply and track success
        indices = df[rows_to_geocode].index
        total_geo = len(indices)
        processed_count = 0
        
        print(f"INFO >>> Step 2 Progress | processed: 0 | remaining: {total_geo}")
        
        for idx in indices:
            row = df.loc[idx]
            res = get_address(row['latitude'], row['longitude'])
            df.at[idx, 'location'] = res
            
            processed_count += 1
            if processed_count % 10 == 0 or processed_count == total_geo:
                print(f"INFO >>> Step 2 Progress | processed: {processed_count} | remaining: {total_geo - processed_count}")
        
        # Recalculate counts based on what we just filled
        # We need to check only the rows we just touched to be accurate to the logic
        newly_filled = df.loc[indices, 'location']
        success_count = newly_filled.notna().sum()
        failed_count = total_geo - success_count

    print(f"INFO >>> Step 2 | total_rows: {total_rows} | empty_location: {empty_loc_count} | geocode_success: {success_count} | geocode_failed: {failed_count}")
    return df

def fill_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    """Handles Width, Height, and Format Type defaults."""
    input_rows = len(df)
    
    # Ensure columns exist and are numeric
    if 'width_ft' not in df.columns: df['width_ft'] = np.nan
    if 'height_ft' not in df.columns: df['height_ft'] = np.nan

    df['width_ft'] = pd.to_numeric(df['width_ft'], errors='coerce')
    df['height_ft'] = pd.to_numeric(df['height_ft'], errors='coerce')

    # Extract single string 'Dimensions' if needed
    if 'dimensions' in df.columns:
        mask_w_miss = df['width_ft'].isna()
        if mask_w_miss.any():
            df.loc[mask_w_miss, 'width_ft'] = df.loc[mask_w_miss, 'dimensions'].apply(lambda x: extract_dim_str(x, 'W'))
        
        mask_h_miss = df['height_ft'].isna()
        if mask_h_miss.any():
            df.loc[mask_h_miss, 'height_ft'] = df.loc[mask_h_miss, 'dimensions'].apply(lambda x: extract_dim_str(x, 'H'))

    # Fill missing based on Format Type averages
    if 'format_type' in df.columns:
        # Groupby transform mean requires numeric types, which we enforced above
        means = df.groupby('format_type')[['width_ft', 'height_ft']].transform('mean')
        
        mask_bs = df['format_type'] == 'Bus_Shelter'
        df.loc[mask_bs & df['width_ft'].isna(), 'width_ft'] = 25.0
        df.loc[mask_bs & df['height_ft'].isna(), 'height_ft'] = 5.0
        
        df['width_ft'] = df['width_ft'].fillna(means['width_ft']).fillna(20.0)
        df['height_ft'] = df['height_ft'].fillna(means['height_ft']).fillna(10.0)

    # Inventory / Digital defaults
    if 'frequency_per_minute' not in df.columns: df['frequency_per_minute'] = np.nan
    is_digital = pd.Series(False, index=df.index)
    if 'format_type' in df.columns: is_digital |= (df['format_type'] == 'Digital_OOH')
    if 'lighting_type' in df.columns: is_digital |= (df['lighting_type'] == 'Digital')
    
    defaults = np.where(is_digital, 10, 0)
    df['frequency_per_minute'] = df['frequency_per_minute'].fillna(pd.Series(defaults, index=df.index))

    if 'quantity' not in df.columns: df['quantity'] = np.nan
    df['quantity'] = df['quantity'].fillna(1).apply(lambda x: 1 if x == 0 else x)

    # We are not removing distinct rows here, so removed=0
    print(f"INFO >>> Step 3 | input_rows: {input_rows} | rows_removed: 0 | output_rows: {input_rows}")
    return df

def calculate_financials(df: pd.DataFrame) -> pd.DataFrame:
    """Calcs Base Rate and Card Rate."""
    input_rows = len(df)

    price_cols = ['minimal_price', 'base_rate_per_month', 'card_rate_per_month']
    for col in price_cols:
        if col in df.columns: df[col] = df[col].apply(clean_numeric)

    if 'base_rate_per_month' not in df.columns: df['base_rate_per_month'] = np.nan
    
    mask_needs_calc = df['base_rate_per_month'].isna()
    if mask_needs_calc.any() and 'minimal_price' in df.columns:
        median_price = df['minimal_price'].median()
        if pd.isna(median_price) or median_price == 0: median_price = 15000.0
        prices = df.loc[mask_needs_calc, 'minimal_price'].fillna(median_price)
        df.loc[mask_needs_calc, 'base_rate_per_month'] = prices * 4.285

    if 'card_rate_per_month' not in df.columns: df['card_rate_per_month'] = np.nan
    mask_card_calc = df['card_rate_per_month'].isna()
    if mask_card_calc.any():
        df.loc[mask_card_calc, 'card_rate_per_month'] = df.loc[mask_card_calc, 'base_rate_per_month'] * 1.10

    df['base_rate_per_unit'] = df['base_rate_per_month']
    df['card_rate_per_unit'] = df['card_rate_per_month']

    # Assuming 'nourished' means we added value to them
    print(f"INFO >>> Step 4 | enriched_rows: {input_rows} | output_rows: {input_rows}")
    return df
