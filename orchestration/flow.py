import os
import sys
from dotenv import load_dotenv

# Load .env explicitly BEFORE importing prefect to ensure config is picked up
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(BASE_DIR, '.env')
load_dotenv(env_path)

# Debug: Check if config is loaded
print(f"DEBUG: PREFECT_API_URL loaded: {os.environ.get('PREFECT_API_URL', 'Not Found')}")

from prefect import flow, task
import pandas as pd
import numpy as np
import json
import re

OUTPUT_DIR = os.path.join(BASE_DIR, "data", "outputs")

# --- HELPER FUNCTIONS ---
def parse_coords(coord_str):
    if pd.isna(coord_str) or str(coord_str).strip() in ['', '0,0']:
        return None, None
    try:
        parts = [float(x.strip()) for x in str(coord_str).split(',')]
        if len(parts) == 2:
            return parts[1], parts[0] # Returns: Lat, Lon
    except:
        pass
    return None, None

def extract_dimensions(dim_str):
    if pd.isna(dim_str) or str(dim_str).strip() == '':
        return None, None
    # Regex to find numbers before 'W' and 'H'
    w_match = re.search(r'(\d+)\s*W', str(dim_str), re.IGNORECASE)
    h_match = re.search(r'(\d+)\s*H', str(dim_str), re.IGNORECASE)
    w = float(w_match.group(1)) if w_match else None
    h = float(h_match.group(1)) if h_match else None
    return w, h

def remove_illegal_chars(val):
    if isinstance(val, str):
        return re.sub(r'[\000-\010]|[\013-\014]|[\016-\037]', '', val)
    return val

# --- TASKS ---

@task
def load_config(config_path):
    print(f"Loading config from {config_path}...")
    with open(config_path, "r") as f:
        return json.load(f)

@task
def load_data(file_path):
    print(f"Loading data from {file_path}...")
    return pd.read_csv(file_path)

@task
def apply_initial_mapping(df: pd.DataFrame, rename_mapping: dict, static_mapping: dict):
    print("Applying initial column mapping and static values...")
    
    # 1. Rename columns
    # rename_mapping is {source_col: target_col}
    df = df.rename(columns=rename_mapping)
    
    # 2. Add static values (new columns)
    # static_mapping is {target_col: value}
    for col, val in static_mapping.items():
        df[col] = val
        
    return df

@task
def apply_business_logic(df: pd.DataFrame):
    print("Applying Business Logic Transformations...")
    
    # 1. ID Processing
    if 'billboard_id' in df.columns:
        df = df.dropna(subset=['billboard_id'])
        df['billboard_id'] = df['billboard_id'].astype(str).str.strip()
        df = df.drop_duplicates(subset=['billboard_id'])
    
    # 2. Categories & Lighting
    if 'lighting_type' in df.columns:
        lighting_map = {
            "NON LIT": "Unlit", "BACK LIT": "Backlit", "FRONT LIT": "Frontlit",
            "LED": "Digital", "DIGITAL": "Digital", "UNLIT": "Unlit"
        }
        df['lighting_type'] = df['lighting_type'].astype(str).str.strip().str.upper().map(lighting_map).fillna(df['lighting_type'])
    
    if 'media_type' in df.columns and 'format_type' not in df.columns:
        format_map = {
            "Bus Shelter": "Bus_Shelter", "Skywalk": "Gantry", 
            "Digital OOH": "Digital_OOH", "Road Median": "Road_Median", 
            "Pole Kiosk": "Pole_Kiosk", "Hoarding": "Hoarding"
        }
        df['format_type'] = df['media_type'].map(format_map)

    # 3. Coordinates
    if 'coordinates' in df.columns:
        coords = df['coordinates'].apply(parse_coords)
        df['latitude'] = coords.apply(lambda x: x[0])
        df['longitude'] = coords.apply(lambda x: x[1])
        df = df.dropna(subset=['latitude', 'longitude'])
    
    # 4. Dimensions
    if 'dimensions' in df.columns:
        dims = df['dimensions'].apply(extract_dimensions)
        df['width_ft'] = dims.apply(lambda x: x[0])
        df['height_ft'] = dims.apply(lambda x: x[1])
        
        # Fallbacks (simplified from notebook)
        if 'format_type' in df.columns:
            # Bus Shelter defaults
            mask_bs = (df['format_type'] == 'Bus_Shelter')
            df.loc[mask_bs & df['width_ft'].isna(), 'width_ft'] = 25.0
            df.loc[mask_bs & df['height_ft'].isna(), 'height_ft'] = 5.0
            
            # Simple fallback for others
            df['width_ft'] = df['width_ft'].fillna(20.0)
            df['height_ft'] = df['height_ft'].fillna(10.0)

    # 5. Frequency
    mask_digital = pd.Series(False, index=df.index)
    if 'format_type' in df.columns:
        mask_digital |= (df['format_type'] == 'Digital_OOH')
    if 'lighting_type' in df.columns:
        mask_digital |= (df['lighting_type'] == 'Digital')
    
    df['frequency_per_minute'] = np.where(mask_digital, 10, 0)
    
    # 6. Quantity
    if 'quantity' in df.columns:
        df['quantity'] = df['quantity'].fillna(1)
        df['quantity'] = df['quantity'].apply(lambda x: 1 if float(x) == 0 else int(float(x)))
    
    # 7. Location (City, Area)
    if 'locality' in df.columns:
        df['city'] = df['locality'].astype(str).apply(lambda x: x.split(',')[-1].strip() if ',' in x else 'Mumbai')
        df['district'] = df['city']
        df['area'] = df['locality'].astype(str).apply(lambda x: x.split(',')[0].strip())
    
    if 'location' not in df.columns:
        if 'address' in df.columns:
            df['location'] = df['address'].fillna(df.get('landmark', ''))
        elif 'landmark' in df.columns:
            df['location'] = df['landmark']
            
    # 8. Rates
    if 'minimal_price' in df.columns:
        median_price = 15000.0
        df['minimal_price'] = pd.to_numeric(df['minimal_price'], errors='coerce')
        clean_price = df['minimal_price'].fillna(median_price)
        
        df['base_rate_per_month'] = (clean_price / 7) * 30
        df['base_rate_per_unit'] = df['base_rate_per_month']
        df['card_rate_per_month'] = df['base_rate_per_month'] * 1.10
        df['card_rate_per_unit'] = df['card_rate_per_month']
    
    # 9. Images
    if 'image_url' in df.columns and 'image_urls' not in df.columns:
         df['image_urls'] = df['image_url']
         
    # 10. Final Cleanup
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(remove_illegal_chars)
        
    return df

@task
def filter_final_columns(df: pd.DataFrame, keep_columns: list):
    print("Filtering final columns...")
    # Add logic to include calculated columns that might not be in the initial 'keep_columns' list from config
    # but strictly speaking, the user config defines what they want.
    # However, our business logic ADDS internal columns (like base_rate_per_month).
    # We should probably keep all relevant columns or define a strict output schema.
    
    # For now, we will merge the config 'keep_columns' with our known generated columns
    generated_cols = [
        'billboard_id', 'latitude', 'longitude', 'width_ft', 'height_ft',
        'frequency_per_minute', 'quantity', 'lighting_type', 'format_type',
        'city', 'area', 'district', 'location',
        'base_rate_per_month', 'base_rate_per_unit',
        'card_rate_per_month', 'card_rate_per_unit', 'image_urls'
    ]
    
    # Intersection of what exists and what is important
    final_cols = list(set(keep_columns + generated_cols))
    
    # Ensure they exist
    existing_cols = [c for c in final_cols if c in df.columns]
    
    return df[existing_cols]

@task
def save_output(df: pd.DataFrame, original_filename: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_filename = f"processed_{original_filename}"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    
    print(f"Saving output to {output_path}...")
    df.to_csv(output_path, index=False)
    return output_path

@flow(name="CSV Mapping Pipeline")
def mapping_pipeline(config_path: str):
    import prefect.runtime

    config = load_config(config_path)
    
    source_file = config["source_file"]
    rename_mapping = config.get("rename_mapping", config.get("mapping", {}))
    static_mapping = config.get("static_mapping", {})
    keep_columns = config.get("keep_columns", [])
    original_filename = config["original_filename"]
    
    # Pipeline Execution
    df = load_data(source_file)
    df = apply_initial_mapping(df, rename_mapping, static_mapping)
    df = apply_business_logic(df)
    df = filter_final_columns(df, keep_columns)
    output_path = save_output(df, original_filename)
    
    # --- DASHBOARD LINK GENERATION ---
    try:
        flow_run_id = prefect.runtime.flow_run.id
        api_url = os.environ.get("PREFECT_API_URL", "")
        
        dashboard_url = ""
        if "api.prefect.cloud" in api_url:
            # Cloud URL Logic
            # Extract account/workspace from API URL
            # Format: https://api.prefect.cloud/api/accounts/{acc}/workspaces/{ws}
            # Target: https://app.prefect.cloud/account/{acc}/workspace/{ws}/flow-runs/flow-run/{id}
            
            parts = api_url.split("/workspaces/")
            if len(parts) == 2:
                ws_id = parts[1]
                acc_part = parts[0].split("/accounts/")
                if len(acc_part) == 2:
                    acc_id = acc_part[1]
                    dashboard_url = f"https://app.prefect.cloud/account/{acc_id}/workspace/{ws_id}/flow-runs/flow-run/{flow_run_id}"
        
        elif api_url:
             # Server/Self-Hosted Logic (often just /flow-runs/...)
             # Assuming standard simple server
             dashboard_url = f"{api_url.replace('/api', '')}/flow-runs/flow-run/{flow_run_id}"
        else:
             # Local ephemeral fallback
             dashboard_url = f"http://127.0.0.1:4200/flow-runs/flow-run/{flow_run_id}"

        if dashboard_url:
            print(f"\nPrefect Flow Run URL: {dashboard_url}\n")
            
    except Exception as e:
        print(f"Could not generate dashboard link: {e}")

    print(f"Pipeline completed. Output saved at: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python flow.py <path_to_config.json>")
        sys.exit(1)
        
    config_path = sys.argv[1]
    mapping_pipeline(config_path)
