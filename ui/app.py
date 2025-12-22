import streamlit as st
import pandas as pd
import os
import json
import uuid
import sys

# Add parent directory to path to allow importing orchestration if needed directly (optional validation)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
CONFIG_DIR = os.path.join(DATA_DIR, "configs")
OUTPUT_DIR = os.path.join(DATA_DIR, "outputs")

# Ensure directories exist
for d in [DATA_DIR, UPLOAD_DIR, CONFIG_DIR, OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

# 1. New Standard Fields (User Requested)
REQUIRED_FIELDS = {
    'billboard_id': {'label': 'Billboard ID *', 'aliases': ['id', 'assetid', 'asset_id', 'code', 's.no', 'sno', 'serial_no']},
    'latitude': {'label': 'Latitude', 'aliases': ['lat', 'latitude']},
    'longitude': {'label': 'Longitude', 'aliases': ['long', 'lng', 'longitude']},
    'width_ft': {'label': 'Width (ft)', 'aliases': ['width', 'w']},
    'height_ft': {'label': 'Height (ft)', 'aliases': ['height', 'h']},
    'quantity': {'label': 'Quantity', 'aliases': ['qty', 'quantity', 'units', 'no_of_units']},
    'lighting_type': {'label': 'Lighting', 'aliases': ['lighting', 'light', 'illumination', 'type', 'illumination_type']},
    'format_type': {'label': 'Format Type *', 'aliases': ['media_type', 'format', 'media', 'category', 'type_of_media']},
    'location': {'label': 'Location', 'aliases': ['address', 'landmark', 'location_name', 'loc']},
    'image_urls': {'label': 'Image URL', 'aliases': ['image', 'photo', 'url', 'img', 'media_image']}
}

def save_uploaded_file(uploaded_file):
    file_id = str(uuid.uuid4())[:8]
    file_name = f"{file_id}_{uploaded_file.name}"
    file_path = os.path.join(UPLOAD_DIR, file_name)
    
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    return file_path, file_name

def main():
    st.set_page_config(page_title="Data Import Pipeline", layout="wide")
    st.title("CSV Import & Mapping Pipeline")

    with st.expander("System Status", expanded=True):
        st.write(f"📂 Upload Directory: `{UPLOAD_DIR}`")
        st.write(f"⚙️ Config Directory: `{CONFIG_DIR}`")

    # Initialize session state for custom columns
    if "custom_columns" not in st.session_state:
        st.session_state.custom_columns = []

    # 1. CSV Upload
    st.header("1. Upload CSV")
    uploaded_file = st.file_uploader("Upload your data CSV", type=["csv"])

    if uploaded_file is not None:
        try:
            # We need to seek to 0 if re-reading
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file)
            st.success(f"Loaded {len(df)} rows.")
            
            # Clean headers
            df.columns = [c.strip() for c in df.columns]
            
            with st.expander("Preview Data"):
                st.dataframe(df.head())
            
            source_columns = df.columns.tolist()
            # Options: (Skip), (Manual Input), [All source columns...]
            col_options = ["(Select Column)"] + sorted(source_columns)
            
            # 2. Column Mapping
            st.header("2. Map Columns")
            st.info("Map your CSV columns to the System Standard Columns.")

            # --- STANDARD FIELDS ---
            st.subheader("Standard Columns")
            
            rename_mapping = {}   # {source_col: target_col}
            static_mapping = {}   # {target_col: static_value}
            
            # Use columns to lay out nicely
            cols = st.columns(2)
            
            # Iterate through standard required fields
            for i, (field_key, config) in enumerate(REQUIRED_FIELDS.items()):
                col = cols[i % 2]
                
                # Auto-detection logic
                default_index = 0
                
                # Check for exact match first
                if field_key in source_columns:
                     default_index = col_options.index(field_key)
                else:
                    # Check aliases
                    found = False
                    for alias in config['aliases']:
                        for raw_col in source_columns:
                            clean_col = str(raw_col).lower().replace(' ', '_').replace('.', '')
                            if alias in clean_col:
                                default_index = col_options.index(raw_col)
                                found = True
                                break
                        if found: break
                
                with col:
                    selection = st.selectbox(
                        f"{config['label']} ({field_key})",
                        options=col_options,
                        index=default_index,
                        key=f"std_{field_key}"
                    )
                    
                    if selection != "(Select Column)":
                        rename_mapping[selection] = field_key

            # --- CUSTOM COLUMNS ---
            st.write("---")
            st.subheader("Custom Columns")
            
            # Button to add new custom column
            if st.button("+ Add Custom Column"):
                st.session_state.custom_columns.append({"id": str(uuid.uuid4())})
            
            # Render input rows for custom columns
            # We need to collect these values
            custom_column_data = [] # List of (target_name, source_selection, manual_value)
            
            # We iterate over session state to render persistent widgets
            for idx, c_item in enumerate(st.session_state.custom_columns):
                c_col1, c_col2, c_col3, c_col4 = st.columns([2, 2, 2, 1])
                
                with c_col1:
                    target_name = st.text_input(f"New Column Name #{idx+1}", key=f"custom_name_{c_item['id']}")
                with c_col2:
                    current_options = ["(Select Column)", "(Manual Input)"] + sorted(source_columns)
                    source_sel = st.selectbox(f"Map From #{idx+1}", options=current_options, key=f"custom_src_{c_item['id']}")
                with c_col3:
                    # Show manual input only if (Manual Input) selected or for clarity always enable but ignore if mapping selected?
                    # The prompt implied conditional visibility, but simple text input is easier.
                    manual_val = ""
                    # if source_sel == "(Manual Input)":
                    manual_val = st.text_input(f"Static Value #{idx+1}", key=f"custom_val_{c_item['id']}")
                    # else:
                    #     st.empty() # Placeholder
                with c_col4:
                    if st.button("x", key=f"del_{c_item['id']}"):
                        st.session_state.custom_columns.pop(idx)
                        st.rerun()
                
                if target_name:
                    custom_column_data.append((target_name, source_sel, manual_val))

            # --- PREVIEW MAPPED DATA ---
            st.write("---")
            st.subheader("Preview Mapped Data")
            
            # Calculate Preview on the fly
            try:
                # 1. Prepare mappings (Same logic as save)
                preview_rename = rename_mapping.copy()
                preview_static = static_mapping.copy()
                preview_keep = list(rename_mapping.values())

                for t_name, s_sel, m_val in custom_column_data:
                    clean_target = t_name.strip().lower().replace(" ", "_")
                    if not clean_target: continue
                    
                    if s_sel == "(Manual Input)":
                        preview_static[clean_target] = m_val
                        preview_keep.append(clean_target)
                    elif s_sel != "(Select Column)":
                        preview_rename[s_sel] = clean_target
                        preview_keep.append(clean_target)
                
                # 2. Apply to head
                df_preview = df.head().copy()
                df_preview = df_preview.rename(columns=preview_rename)
                
                for col, val in preview_static.items():
                    df_preview[col] = val
                
                # Filter to only kept columns (handle missing if any logic requires it, but for preview just show what we have)
                # We need to be careful: the 'keep' list has target names. 
                # df_preview columns are now a mix of Original (unmapped) and Target (mapped)
                
                # Let's filter to show ONLY the target structure if possible, 
                # or just show the whole thing with mapped columns highlighted?
                # User usually wants to see the RESULT.
                
                available_preview_cols = [c for c in list(set(preview_keep)) if c in df_preview.columns]
                if available_preview_cols:
                    st.dataframe(df_preview[available_preview_cols])
                else:
                    st.warning("No columns mapped yet.")
                    
            except Exception as e:
                st.error(f"Could not preview: {e}")

            # 3. Execution
            st.header("3. Execution")
            
            if st.button("Save Configuration & Run Pipeline", type="primary"):
                # Prepare final mappings
                final_rename_map = rename_mapping.copy()
                final_static_map = static_mapping.copy()
                keep_columns = list(rename_mapping.values()) # Standard columns actively mapped
                
                # Process Custom Cols
                for t_name, s_sel, m_val in custom_column_data:
                    clean_target = t_name.strip().lower().replace(" ", "_")
                    if not clean_target: continue
                    
                    if s_sel == "(Manual Input)":
                        final_static_map[clean_target] = m_val
                        keep_columns.append(clean_target)
                    elif s_sel != "(Select Column)":
                        final_rename_map[s_sel] = clean_target
                        keep_columns.append(clean_target)

                # Save CSV
                saved_path, saved_filename = save_uploaded_file(uploaded_file)
                st.toast(f"File saved to {saved_filename}")

                # Save Mapping
                mapping_filename = saved_filename.replace(".csv", "_config.json")
                config_path = os.path.join(CONFIG_DIR, mapping_filename)
                
                config_data = {
                    "source_file": saved_path,
                    "rename_mapping": final_rename_map,   # {src: tgt}
                    "static_mapping": final_static_map,   # {tgt: val}
                    "keep_columns": list(set(keep_columns)), # [tgt1, tgt2...]
                    "original_filename": uploaded_file.name
                }
                
                with open(config_path, "w") as f:
                    json.dump(config_data, f, indent=4)
                st.toast("Mapping configuration saved.")

                # Trigger Prefect
                st.write("---")
                st.subheader("🚀 Triggering Orchestration")
                
                import subprocess
                cmd = [sys.executable, os.path.join(os.path.dirname(os.path.dirname(__file__)), "orchestration", "flow.py"), config_path]
                
                with st.spinner("Running Prefect Flow..."):
                    result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    st.success("Pipeline executed successfully!")
                    
                    # Extract Dashboard URL
                    import re
                    dashboard_match = re.search(r"Prefect Flow Run URL: (https?://[^\s]+)", result.stdout)
                    
                    if dashboard_match:
                        dashboard_url = dashboard_match.group(1)
                        st.link_button("👉 View Run in Prefect Cloud", url=dashboard_url, type="primary")
                    
                    with st.expander("Execution Logs"):
                        st.code(result.stdout)
                else:
                    st.error("Pipeline failed!")
                    st.code(result.stderr)
                    
        except Exception as e:
            st.error(f"Error reading CSV: {e}")

if __name__ == "__main__":
    main()
