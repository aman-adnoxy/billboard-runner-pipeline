import streamlit as st
import pandas as pd
import os
import json
import uuid
import sys
import subprocess
from dotenv import load_dotenv

# --- Import from SRC ---
# Add parent directory to path to allow importing src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import REQUIRED_FIELDS, BUCKET_INPUT, BUCKET_MAPPING, BUCKET_OUTPUT, load_required_fields
from src.database import get_supabase_client

# Initialize Supabase
supabase = get_supabase_client()

# Re-load fields dynamically if needed or use imported constant
REQUIRED_FIELDS = load_required_fields()

def save_uploaded_file_to_supabase(uploaded_file):
    file_id = str(uuid.uuid4())[:8]
    file_name = f"{file_id}_{uploaded_file.name}"
    
    try:
        file_bytes = uploaded_file.getvalue()
        res = supabase.storage.from_(BUCKET_INPUT).upload(
            path=file_name,
            file=file_bytes,
            file_options={"content-type": uploaded_file.type}
        )
        return file_name
    except Exception as e:
        st.error(f"Supabase Upload Error: {e}")
        return None

def main():
    st.set_page_config(page_title="Data Import Pipeline", layout="wide")
    st.title("CSV Import & Mapping Pipeline")

    with st.expander("System Status", expanded=True):
        st.write(f"☁️ Storage: Supabase")
        st.write(f"📦 Buckets: `{BUCKET_INPUT}`, `{BUCKET_MAPPING}`, `{BUCKET_OUTPUT}`")

    # Initialize session state for custom columns
    if "custom_columns" not in st.session_state:
        st.session_state.custom_columns = []

    # 1. CSV Upload
    st.header("1. Upload Data")
    uploaded_file = st.file_uploader("Upload your data (CSV or Excel)", type=["csv", "xlsx", "xls"])

    if uploaded_file is not None:
        try:
            uploaded_file.seek(0)
            if uploaded_file.name.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(uploaded_file)
            else:
                try:
                    df = pd.read_csv(uploaded_file)
                except UnicodeDecodeError:
                    uploaded_file.seek(0)
                    df = pd.read_csv(uploaded_file, encoding='cp1252')
            
            st.success(f"Loaded {len(df)} rows.")
            
            # Clean headers
            df.columns = [c.strip() for c in df.columns]
            
            with st.expander("Preview Data"):
                st.dataframe(df.head())
            
            source_columns = df.columns.tolist()
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
            
            # Helper for auto-mapping
            def get_default_index(key, conf):
                if key in source_columns:
                    return col_options.index(key)
                if 'aliases' in conf:
                    for alias in conf['aliases']:
                        for raw_col in source_columns:
                            clean_col = str(raw_col).lower().replace(' ', '_').replace('.', '')
                            if alias in clean_col:
                                return col_options.index(raw_col)
                return 0

            # Layout tracking
            layout_idx = 0
            coord_keys = ['latitude', 'longitude', 'coordinates']
            dim_keys = ['width_ft', 'height_ft', 'dimensions']
            
            location_block_rendered = False
            dimension_block_rendered = False
            
            # Iterate through standard required fields
            for field_key, config in REQUIRED_FIELDS.items():
                
                # Special handling for Coordinate/Location fields
                if field_key in coord_keys:
                    if location_block_rendered:
                        continue
                    
                    # Render Location Block
                    col = cols[layout_idx % 2]
                    with col:
                        with st.container(border=True):
                            st.markdown("**Location Mapping**")
                            loc_mode = st.radio(
                                "Coordinate Format",
                                ["Separate Columns (Lat/Long)", "Single Column (Coordinates)"],
                                horizontal=True,
                                key="loc_mode_radio",
                                label_visibility="collapsed"
                            )
                            
                            if loc_mode == "Single Column (Coordinates)":
                                c_conf = REQUIRED_FIELDS.get('coordinates', {})
                                c_idx = get_default_index('coordinates', c_conf)
                                sel = st.selectbox(
                                    f"{c_conf.get('label', 'Coordinates')} (coordinates)", 
                                    col_options, 
                                    index=c_idx, 
                                    key="std_coordinates"
                                )
                                if sel != "(Select Column)":
                                    rename_mapping[sel] = "coordinates"
                            else:
                                sub_c1, sub_c2 = st.columns(2)
                                with sub_c1:
                                    # Latitude
                                    l_conf = REQUIRED_FIELDS.get('latitude', {})
                                    l_idx = get_default_index('latitude', l_conf)
                                    sel_lat = st.selectbox(
                                        f"{l_conf.get('label', 'Latitude')}", 
                                        col_options, 
                                        index=l_idx, 
                                        key="std_latitude"
                                    )
                                    if sel_lat != "(Select Column)":
                                        rename_mapping[sel_lat] = "latitude"
                                with sub_c2:
                                    # Longitude
                                    g_conf = REQUIRED_FIELDS.get('longitude', {})
                                    g_idx = get_default_index('longitude', g_conf)
                                    sel_long = st.selectbox(
                                        f"{g_conf.get('label', 'Longitude')}", 
                                        col_options, 
                                        index=g_idx, 
                                        key="std_longitude"
                                    )
                                    if sel_long != "(Select Column)":
                                        rename_mapping[sel_long] = "longitude"
                    
                    location_block_rendered = True
                    layout_idx += 1
                    continue

                # Special handling for Dimension fields
                if field_key in dim_keys:
                    if dimension_block_rendered:
                        continue
                    
                    # Render Dimension Block
                    col = cols[layout_idx % 2]
                    with col:
                        with st.container(border=True):
                            st.markdown("**Dimension Mapping**")
                            dim_mode = st.radio(
                                "Dimension Format",
                                ["Separate Columns (WxH)", "Single Column (Dimensions)"],
                                horizontal=True,
                                key="dim_mode_radio",
                                label_visibility="collapsed"
                            )
                            
                            if dim_mode == "Single Column (Dimensions)":
                                d_conf = REQUIRED_FIELDS.get('dimensions', {})
                                d_idx = get_default_index('dimensions', d_conf)
                                sel_dim = st.selectbox(
                                    f"{d_conf.get('label', 'Dimensions')} (dimensions)", 
                                    col_options, 
                                    index=d_idx, 
                                    key="std_dimensions"
                                )
                                if sel_dim != "(Select Column)":
                                    rename_mapping[sel_dim] = "dimensions"
                            else:
                                sub_d1, sub_d2 = st.columns(2)
                                with sub_d1:
                                    # Width
                                    w_conf = REQUIRED_FIELDS.get('width_ft', {})
                                    w_idx = get_default_index('width_ft', w_conf)
                                    sel_width = st.selectbox(
                                        f"{w_conf.get('label', 'Width')}", 
                                        col_options, 
                                        index=w_idx, 
                                        key="std_width"
                                    )
                                    if sel_width != "(Select Column)":
                                        rename_mapping[sel_width] = "width_ft"
                                with sub_d2:    
                                    # Height
                                    h_conf = REQUIRED_FIELDS.get('height_ft', {})
                                    h_idx = get_default_index('height_ft', h_conf)
                                    sel_height = st.selectbox(
                                        f"{h_conf.get('label', 'Height')}", 
                                        col_options, 
                                        index=h_idx, 
                                        key="std_height"
                                    )
                                    if sel_height != "(Select Column)":
                                        rename_mapping[sel_height] = "height_ft"
                    
                    dimension_block_rendered = True
                    layout_idx += 1
                    continue

                # Standard Fields
                col = cols[layout_idx % 2]
                d_idx = get_default_index(field_key, config)
                
                with col:
                    selection = st.selectbox(
                        f"{config['label']} ({field_key})", 
                        options=col_options, 
                        index=d_idx, 
                        key=f"std_{field_key}"
                    )
                    
                    if selection != "(Select Column)":
                        rename_mapping[selection] = field_key
                
                layout_idx += 1

            # --- CUSTOM COLUMNS ---
            st.write("---")
            st.subheader("Custom Columns")
            
            if st.button("+ Add Custom Column"):
                st.session_state.custom_columns.append({"id": str(uuid.uuid4())})
            
            custom_column_data = [] # List of (target_name, source_selection, manual_value)
            
            for idx, c_item in enumerate(st.session_state.custom_columns):
                c_col1, c_col2, c_col3, c_col4 = st.columns([2, 2, 2, 1])
                
                with c_col1:
                    target_name = st.text_input(f"New Column Name #{idx+1}", key=f"custom_name_{c_item['id']}")
                with c_col2:
                    current_options = ["(Select Column)", "(Manual Input)"] + sorted(source_columns)
                    source_sel = st.selectbox(f"Map From #{idx+1}", options=current_options, key=f"custom_src_{c_item['id']}")
                with c_col3:
                    manual_val = st.text_input(f"Static Value #{idx+1}", key=f"custom_val_{c_item['id']}")
                with c_col4:
                    if st.button("x", key=f"del_{c_item['id']}"):
                        st.session_state.custom_columns.pop(idx)
                        st.rerun()
                
                if target_name:
                    custom_column_data.append((target_name, source_sel, manual_val))

            # --- PREVIEW MAPPED DATA ---
            st.write("---")
            st.subheader("Preview Mapped Data")
            
            try:
                preview_rename = rename_mapping.copy()
                preview_static = static_mapping.copy()
                preview_keep = list(rename_mapping.values())

                for t_name, s_sel, m_val in custom_column_data:
                    clean_target = t_name.strip()
                    if not clean_target: continue
                    if s_sel == "(Manual Input)":
                        preview_static[clean_target] = m_val
                        preview_keep.append(clean_target)
                    elif s_sel != "(Select Column)":
                        preview_rename[s_sel] = clean_target
                        preview_keep.append(clean_target)
                
                df_preview = df.head().copy()
                df_preview = df_preview.rename(columns=preview_rename)
                for col, val in preview_static.items():
                    df_preview[col] = val
                
                available_preview_cols = [c for c in list(set(preview_keep)) if c in df_preview.columns]
                if available_preview_cols:
                    st.dataframe(df_preview[available_preview_cols])
                else:
                    st.warning("No columns mapped yet.")
                    
            except Exception as e:
                st.error(f"Could not preview: {e}")

# --- DOWNLOAD BUTTONS ---
            st.write("---")
            st.subheader(" Download Mapped Data")
            
            try:
                # Same logic as preview to get the full mapped data
                df_final = df.copy()
                df_final = df_final.rename(columns=preview_rename)
                for col, val in preview_static.items():
                    df_final[col] = val
                
                # Sirf mapped columns hi select karna
                final_cols = [c for c in list(set(preview_keep)) if c in df_final.columns]
                if final_cols:
                    df_download = df_final[final_cols]
                    
                    d_col1, d_col2 = st.columns(2)
                    
                    # 1. CSV Download
                    csv = df_download.to_csv(index=False).encode('utf-8')
                    d_col1.download_button(
                        label="Download as CSV",
                        data=csv,
                        file_name="mapped_data.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                    
                    # 2. Excel Download
                    import io
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                        df_download.to_excel(writer, index=False, sheet_name='Sheet1')
                    
                    d_col2.download_button(
                        label="Download as Excel",
                        data=buffer.getvalue(),
                        file_name="mapped_data.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                else:
                    st.warning("Pehle columns map karein taaki download enable ho sake.")
            except Exception as e:
                st.error(f"Download error: {e}")
            # 3. Execution
            st.header("3. Execution")
            
            if st.button("Save Configuration & Run Pipeline", type="primary"):
                final_rename_map = rename_mapping.copy()
                final_static_map = static_mapping.copy()
                keep_columns = list(rename_mapping.values())
                
                for t_name, s_sel, m_val in custom_column_data:
                    clean_target = t_name.strip()
                    if not clean_target: continue
                    if s_sel == "(Manual Input)":
                        final_static_map[clean_target] = m_val
                        keep_columns.append(clean_target)
                    elif s_sel != "(Select Column)":
                        final_rename_map[s_sel] = clean_target
                        keep_columns.append(clean_target)

                # Save CSV
                saved_filename = save_uploaded_file_to_supabase(uploaded_file)
                
                if saved_filename:
                    st.toast(f"File uploaded to Supabase: {saved_filename}")

                    # Save Config
                    mapping_filename = saved_filename.replace(".csv", "_config.json")
                    config_data = {
                        "source_file": saved_filename,
                        "rename_mapping": final_rename_map,
                        "static_mapping": final_static_map,
                        "keep_columns": list(set(keep_columns)),
                        "original_filename": uploaded_file.name
                    }
                    
                    try:
                        config_json = json.dumps(config_data, indent=4)
                        res = supabase.storage.from_(BUCKET_MAPPING).upload(
                            path=mapping_filename,
                            file=config_json.encode('utf-8'),
                            file_options={"content-type": "application/json"}
                        )
                        st.toast("Mapping configuration saved.")
                    except Exception as e:
                        st.error(f"Failed to save config: {e}")
                        st.stop()

                    # Trigger Prefect
                    st.write("---")
                    st.subheader("🚀 Triggering Orchestration")
                    
                    cmd = [sys.executable, os.path.join(os.path.dirname(os.path.dirname(__file__)), "orchestration", "flow.py"), mapping_filename]
                    with st.spinner("Running Prefect Flow..."):
                        result = subprocess.run(cmd, capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        st.success("Pipeline executed successfully!")
                        import re
                        # Prefect logs often go to stderr, and standard format is "View at http..."
                        # Check both just in case
                        combined_output = (result.stdout or "") + "\n" + (result.stderr or "")
                        
                        dashboard_match = re.search(r"View at (https?://[^\s]+)", combined_output)
                        
                        if dashboard_match:
                            dashboard_url = dashboard_match.group(1)
                            # Clean up trailing characters if any (like ANSI colors hidden or punctuation)
                            dashboard_url = dashboard_url.rstrip('.')
                            st.link_button("👉 View Run in Prefect Cloud", url=dashboard_url, type="primary")
                        
                        with st.expander("Execution Logs"):
                            st.code(combined_output)
                    else:
                        st.error("Pipeline failed!")
                        st.code(result.stderr)
                else:
                    st.error("Upload failed, cannot proceed.")
                    
        except Exception as e:
            st.error(f"Error reading CSV: {e}")

if __name__ == "__main__":
    main()
