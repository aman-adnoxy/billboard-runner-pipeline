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

    # 1. IMAGE CHECK (Row by Row)
    img_col = 'image_urls' if 'image_urls' in df.columns else 'image_url'
    
    if img_col in df.columns:
        df = df.dropna(subset=[img_col])
        mask_invalid_img = df[img_col].astype(str).str.strip().str.lower().isin(['', 'nan', 'null', 'none', '[]'])
        df = df[~mask_invalid_img]
        print(f"   Removed {start_count - len(df)} rows due to missing images.")
    else:
        print("   WARNING: Image column missing entirely. All rows dropped.")
        return df.iloc[0:0]

    current_count = len(df)

    # 2. COORDINATE CHECK (Row by Row)
    if 'latitude' in df.columns and 'longitude' in df.columns:
        df = df.dropna(subset=['latitude', 'longitude'])
        mask_zero = (df['latitude'] == 0) & (df['longitude'] == 0)
        df = df[~mask_zero]
        print(f"   Removed {current_count - len(df)} rows due to missing coordinates.")
    else:
        print("   WARNING: Coordinate columns missing entirely. All rows dropped.")
        return df.iloc[0:0]

    # 3. SELECT FINAL COLUMNS
    keep_cols = config.get("keep_columns", [])
    core_cols = [
        'billboard_id', 'latitude', 'longitude', 'width_ft', 'height_ft',
        'frequency_per_minute', 'quantity', 'base_rate_per_month', 
        'card_rate_per_month', 'city', 'area', 'district', 'location', 
        'format_type', 'lighting_type', img_col
    ]
    final_cols = list(set(keep_cols + core_cols))
    existing_cols = [c for c in final_cols if c in df.columns]
    
    print(f"   Final Valid Row Count: {len(df)}")
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