import os
import sys
import io
import json
import pandas as pd
from prefect import flow, task

# --- Import from SRC ---
# Add parent directory to path to allow importing src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import get_supabase_client
from src.config import BUCKET_INPUT, BUCKET_MAPPING, BUCKET_OUTPUT
from src.processing import (
    standard_cleanup, 
    extract_geography, 
    fill_dimensions, 
    calculate_financials as proc_calculate_financials
)

# Initialize Supabase
supabase = get_supabase_client()

# --- TASKS ---

@task
def load_and_init(config_filename):
    print(f"Loading Config: {config_filename}...")
    res_conf = supabase.storage.from_(BUCKET_MAPPING).download(config_filename)
    config = json.loads(res_conf)
    
    source_file = config["source_file"]
    print(f"Loading Data: {source_file}...")
    res_data = supabase.storage.from_(BUCKET_INPUT).download(source_file)
    
    if source_file.lower().endswith(('.xlsx', '.xls')):
        df = pd.read_excel(io.BytesIO(res_data))
    else:
        try:
            df = pd.read_csv(io.BytesIO(res_data), encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(res_data), encoding='cp1252')

    # Cleanup Headers & Rename
    df.columns = [str(c).strip() for c in df.columns]
    df = df.rename(columns=config.get("rename_mapping", {}))
    
    for col, val in config.get("static_mapping", {}).items():
        df[col] = val
        
    return df, config

@task
def standardize_schema(df: pd.DataFrame):
    """Task 1: ID & Category Standardization"""
    print(">>> Step 1: Standardizing Schema...")
    return standard_cleanup(df)

@task
def extract_geo_features_task(df: pd.DataFrame):
    """Task 2: Smart Geo Parsing"""
    print(">>> Step 2: Extracting Geography...")
    return extract_geography(df)

@task
def process_dimensions_inventory(df: pd.DataFrame):
    """Task 3: Dimensions & Defaults"""
    print(">>> Step 3: Processing Dimensions & Inventory...")
    return fill_dimensions(df)

@task
def calculate_financials_task(df: pd.DataFrame):
    """Task 4: Financial Calculation"""
    print(">>> Step 4: Calculating Financials...")
    return proc_calculate_financials(df)

@task
def validate_and_clean(df: pd.DataFrame, config: dict):
    """
    Task 5: ROW-LEVEL VALIDATION
    Only removes specific rows that are missing mandatory data.
    """
    print(">>> Step 5: Final Row-Level Validation...")
    start_count = len(df)
    dropped_images = 0
    dropped_coords = 0

    # 1. IMAGE CHECK (Row by Row)
    img_col = 'image_urls' if 'image_urls' in df.columns else 'image_url'
    
    print(f"DEBUG: Checking for image column: '{img_col}' in {df.columns.tolist()}")
    if img_col in df.columns:
        print(f"DEBUG: Rows before image drop: {len(df)}")
        df = df.dropna(subset=[img_col])
        mask_invalid_img = df[img_col].astype(str).str.strip().str.lower().isin(['', 'nan', 'null', 'none', '[]'])
        df = df[~mask_invalid_img]
        print(f"DEBUG: Rows after image drop: {len(df)}")
        dropped_images = start_count - len(df)
        print(f"   Removed {dropped_images} rows due to missing images.")
    else:
        print("   WARNING: Image column missing entirely. All rows dropped.")
        return df.iloc[0:0]

    current_count = len(df)

    # 2. COORDINATE CHECK (Row by Row)
    # Note: 'latitude' and 'longitude' should have been populated by extract_geo_features_task
    # if valid coordinates were found in 'coordinates' column.
    
    print(f"DEBUG: Checking for coordinate columns in {df.columns.tolist()}")
    if 'lat' in df.columns and 'lon' in df.columns:
        print(f"DEBUG: Rows before coord drop: {len(df)}")
        # Check for NaN first
        df = df.dropna(subset=['lat', 'lon'])
        # Check for 0,0
        mask_zero = (df['lat'] == 0) & (df['lon'] == 0)
        df = df[~mask_zero]
        print(f"DEBUG: Rows after coord drop: {len(df)}")
        dropped_coords = current_count - len(df)
        print(f"   Removed {dropped_coords} rows due to missing coordinates.")
    else:
        print("   WARNING: Coordinate columns missing entirely. All rows dropped.")
        return df.iloc[0:0]

    # 3. SELECT FINAL COLUMNS
    keep_cols = config.get("keep_columns", [])
    core_cols = [
        'billboard_id', 'lat', 'lon', 'width_ft', 'height_ft',
        'frequency_per_minute', 'quantity', 'base_rate_per_month', 
        'card_rate_per_month', 'base_rate_per_unit', 'card_rate_per_unit', 
        'city', 'area', 'district', 'location', 
        'format_type', 'lighting_type', img_col
    ]
    
    # Clean up single columns if we successfully split them
    cols_to_exclude = set()
    if 'width_ft' in df.columns and 'height_ft' in df.columns:
        cols_to_exclude.add('dimensions')
    if 'lat' in df.columns and 'lon' in df.columns:
        cols_to_exclude.add('coordinates')

    final_cols = list(set(keep_cols + core_cols) - cols_to_exclude)
    
    # Sort to user preference
    desired_order = [
         "billboard_id", "location", "area", "locality", "city", "district", 
         "format_type", "lighting_type", "width_ft", "height_ft", 
         "base_rate_per_month", "base_rate_per_unit", "card_rate_per_unit", "card_rate_per_month", 
         "minimal_price", "lat", "lon", "quantity", "frequency_per_minute", "image_urls", "image_url"
     ]
    ordered_list = [c for c in desired_order if c in final_cols]
    others_list = [c for c in final_cols if c not in ordered_list]
    final_cols_sorted = ordered_list + others_list
    
    existing_cols = [c for c in final_cols_sorted if c in df.columns]
    
    validated_rows = len(df)
    
    # Determine Status based on rows dropped
    status_str = "SUCCESS"
    if validated_rows < start_count:
        status_str = "SUCCESS_WITH_DROPS"
        
    print(f"   Final Valid Row Count: {validated_rows}")
    print(f"INFO >>> Step 5 | validated_rows: {validated_rows} | dropped_images: {dropped_images} | dropped_coords: {dropped_coords} | status: {status_str}")
    
    return df[existing_cols]

@task
def save_output(df: pd.DataFrame, original_filename: str):
    base_name = os.path.splitext(original_filename)[0]
    output_filename = f"processed_{base_name}.csv"
    print(f"Saving Output: {output_filename}...")
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    supabase.storage.from_(BUCKET_OUTPUT).upload(
        path=output_filename,
        file=csv_buffer.getvalue().encode('utf-8'),
        file_options={"content-type": "text/csv", "upsert": "true"}
    )
    return output_filename

@flow(name="Row-Level Strict Pipeline", log_prints=True)
def mapping_pipeline(config_filename: str):
    df, config = load_and_init(config_filename)
    df = standardize_schema(df)
    df = extract_geo_features_task(df)
    df = process_dimensions_inventory(df)
    df = calculate_financials_task(df)
    df = validate_and_clean(df, config)
    save_output(df, config["original_filename"])

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python flow.py <config_filename>")
        sys.exit(1)
    mapping_pipeline(sys.argv[1])