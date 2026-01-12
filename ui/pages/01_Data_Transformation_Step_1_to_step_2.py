import streamlit as st
import pandas as pd
import os
import json
import uuid
import sys
import subprocess
import time
from datetime import datetime
import altair as alt
from dotenv import load_dotenv

# --- Import from SRC ---
# Add parent directory (ROOT) to path to allow importing src
# Current file: .../ui/pages/Data_Transformation.py
# Dirname 1: .../ui/pages
# Dirname 2: .../ui
# Dirname 3: .../ (ROOT)
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import REQUIRED_FIELDS, BUCKET_INPUT, BUCKET_MAPPING, BUCKET_OUTPUT, load_required_fields
from src.database import get_supabase_client

# Initialize Supabase
supabase = get_supabase_client()

# Re-load fields dynamically if needed or use imported constant
REQUIRED_FIELDS = load_required_fields()

# --- NEW: Helper to Load Validation Schema ---
def load_validation_schema():
    """Loads the output validation schema from config folder."""
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_path = os.path.join(base_dir, "config", "output_validation.json")
    
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)
    return None

def save_uploaded_file_to_supabase(uploaded_file):
    # 1. single source of identity: epoch milliseconds
    upload_uid = int(time.time() * 1000)

    # 2. derive filenames
    original_filename = uploaded_file.name
    file_ext = original_filename.split('.')[-1].lower()
    stored_filename = f"{upload_uid}_{original_filename}"

    # 3. read bytes
    file_bytes = uploaded_file.getvalue()

    # 4. upload to Supabase Storage
    supabase.storage.from_(BUCKET_INPUT).upload(
        path=stored_filename,
        file=file_bytes,
        file_options={"content-type": uploaded_file.type}
    )

    # 5. public URL
    public_url = supabase.storage.from_(BUCKET_INPUT).get_public_url(
        stored_filename
    )

    # 6. persist ingestion metadata (atomic truth)
    supabase.table("uploaded_files").insert({
        "upload_timestamp": upload_uid,
        "original_filename": original_filename,
        "stored_filename": stored_filename,
        "file_format": file_ext,
        "uploaded_at": time.strftime('%Y-%m-%dT%H:%M:%S%z'),
        "storage_bucket": BUCKET_INPUT,
        "storage_path": stored_filename,
        "public_url": public_url,
        "status": "uploaded"
    }).execute()

    return stored_filename

# --- NEW: Import LogParser ---
try:
    from ui.log_utils import LogParser
except ImportError:
    # If running from pages, ui module should be importable if root is in path
    try:
        from ui.log_utils import LogParser
    except:
        # Fallback manual path add
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from log_utils import LogParser

def main():
    st.set_page_config(page_title="Data Transformation Step 1--> step 2", layout="wide")
    st.title("Step 1 --> Step 2: Data Transformation")

    with st.expander("System Status", expanded=True):
        st.write(f" Storage: Supabase")
        st.write(f" Buckets: `{BUCKET_INPUT}`, `{BUCKET_MAPPING}`, `{BUCKET_OUTPUT}`")

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
            # Column Selection Options
            # Define two sets of options:
            col_options_base = ["(Select Column)"] + sorted(source_columns)
            col_options_auto = ["(Select Column)", "(Auto Calculate)"] + sorted(source_columns)
            
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
            def get_default_index(key, conf, options_list):
                if key in source_columns:
                    try:
                        return options_list.index(key)
                    except ValueError:
                        return 0
                if 'aliases' in conf:
                    for alias in conf['aliases']:
                        for raw_col in source_columns:
                            clean_col = str(raw_col).lower().replace(' ', '_').replace('.', '')
                            if alias in clean_col:
                                try:
                                    return options_list.index(raw_col)
                                except ValueError:
                                    continue
                return 0

            # Layout tracking
            layout_idx = 0
            coord_keys = ['lat', 'lon', 'coordinates']
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
                                c_idx = get_default_index('coordinates', c_conf, col_options_base)
                                sel = st.selectbox(
                                    f"{c_conf.get('label', 'Coordinates')} (coordinates)", 
                                    col_options_base, 
                                    index=c_idx, 
                                    key="std_coordinates"
                                )
                                if sel != "(Select Column)":
                                    rename_mapping[sel] = "coordinates"
                            else:
                                sub_c1, sub_c2 = st.columns(2)
                                with sub_c1:
                                    # Latitude
                                    l_conf = REQUIRED_FIELDS.get('lat', {})
                                    l_idx = get_default_index('lat', l_conf, col_options_base)
                                    sel_lat = st.selectbox(
                                        f"{l_conf.get('label', 'Latitude')}", 
                                        col_options_base, 
                                        index=l_idx, 
                                        key="std_latitude"
                                    )
                                    if sel_lat != "(Select Column)":
                                        rename_mapping[sel_lat] = "lat"
                                with sub_c2:
                                    # Longitude
                                    g_conf = REQUIRED_FIELDS.get('lon', {})
                                    g_idx = get_default_index('lon', g_conf, col_options_base)
                                    sel_long = st.selectbox(
                                        f"{g_conf.get('label', 'Longitude')}", 
                                        col_options_base, 
                                        index=g_idx, 
                                        key="std_longitude"
                                    )
                                    if sel_long != "(Select Column)":
                                        rename_mapping[sel_long] = "lon"
                    
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
                                d_idx = get_default_index('dimensions', d_conf, col_options_base)
                                sel_dim = st.selectbox(
                                    f"{d_conf.get('label', 'Dimensions')} (dimensions)", 
                                    col_options_base, 
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
                                    w_idx = get_default_index('width_ft', w_conf, col_options_base)
                                    sel_width = st.selectbox(
                                        f"{w_conf.get('label', 'Width')}", 
                                        col_options_base, 
                                        index=w_idx, 
                                        key="std_width"
                                    )
                                    if sel_width != "(Select Column)":
                                        rename_mapping[sel_width] = "width_ft"
                                with sub_d2:    
                                    # Height
                                    h_conf = REQUIRED_FIELDS.get('height_ft', {})
                                    h_idx = get_default_index('height_ft', h_conf, col_options_base)
                                    sel_height = st.selectbox(
                                        f"{h_conf.get('label', 'Height')}", 
                                        col_options_base, 
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
                
                # Determine which options list to use
                # Auto Calculate only for frequency_per_minute and location
                if field_key in ['frequency_per_minute', 'location']:
                    current_opts = col_options_auto
                else:
                    current_opts = col_options_base
                    
                d_idx = get_default_index(field_key, config, current_opts)
                
                with col:
                    selection = st.selectbox(
                        f"{config['label']} ({field_key})", 
                        options=current_opts, 
                        index=d_idx, 
                        key=f"std_{field_key}"
                    )
                    
                    if selection == "(Auto Calculate)":
                        # Record intent to auto-calculate (skip validation for this field)
                        # We don't add it to rename_mapping, effectively making it missing in df_final
                        # which allows backend logic (e.g. fillna) to run.
                        pass
                    elif selection != "(Select Column)":
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

            # --- PREVIEW MAPPED DATA & VALIDATION ---
            st.write("---")
            st.subheader("Preview & Validation")
            
            validation_passed = False # Default state
            available_cols = []

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
                    if s_sel == "(Auto Calculate)":
                         # User explicitly wants backend to handle it
                         continue
                    elif s_sel != "(Select Column)":
                        preview_rename[s_sel] = clean_target
                        preview_keep.append(clean_target)
                
                df_final = df.copy()
                df_final = df_final.rename(columns=preview_rename)
                for col, val in preview_static.items():
                    df_final[col] = val

                # --- Reorder Columns (User Request) ---
                desired_order = [
                    "billboard_id", "location", "area", "locality", "city", "district", 
                    "format_type", "lighting_type", "width_ft", "height_ft", 
                    "base_rate_per_month", "base_rate_per_unit", "card_rate_per_unit", "card_rate_per_month", 
                    "minimal_price", "lat", "lon", "quantity", "frequency_per_minute", "image_urls"
                ]
                
                ordered_cols = [c for c in desired_order if c in df_final.columns]
                remaining_cols = [c for c in df_final.columns if c not in ordered_cols]
                df_final = df_final[ordered_cols + remaining_cols]

                # Custom Columns Numeric Cleaning (width_ft, height_ft)
                # If these came from custom columns (static or mapped), ensure they are numeric
                numeric_fields = ['width_ft', 'height_ft', 'base_rate_per_month', 'base_rate_per_unit', 'card_rate_per_month', 'card_rate_per_unit']
                for nf in numeric_fields:
                    if nf in df_final.columns:
                        # Force to numeric, coerce errors to NaN
                        df_final[nf] = pd.to_numeric(df_final[nf], errors='coerce')
                
                # Filter to show only mapped columns
                available_cols = [c for c in list(set(preview_keep)) if c in df_final.columns]
                
                if available_cols:
                    st.dataframe(df_final[available_cols].head())
                    
                    # --- NEW: VALIDATION CHECK LOGIC ---
                    schema = load_validation_schema()
                    
                    if schema:
                        missing_required = []
                        current_columns = df_final[available_cols].columns.tolist()
                        
                        for field, rules in schema.items():
                            # Check if field is required and missing from our mapped columns
                            if rules.get("required") and field not in current_columns:
                                
                                # --- NEW: Single Column Exception Logic ---
                                # 1. Dimensions Check
                                if field in ['width_ft', 'height_ft']:
                                    # If 'dimensions' exists, we consider width/height satisfied
                                    if 'dimensions' in current_columns:
                                        continue
                                
                                # 2. Coordinates Check
                                if field in ['lat', 'lon']:
                                    # If 'coordinates' exists, we consider lat/long satisfied
                                    if 'coordinates' in current_columns:
                                        continue
                                
                                # Check if user selected "Auto Calculate" for this field?
                                # Unfortunately we didn't track it cleanly above. Let's inspect st.session_state keys.
                                widget_key = f"std_{field}"
                                user_selection = st.session_state.get(widget_key)
                                
                                if user_selection == "(Auto Calculate)":
                                    continue # Skip validation error
                                
                                missing_required.append(field)
                        
                        if missing_required:
                            st.error(f" **Validation Failed!** The following required columns are missing: {missing_required}")
                            st.warning("Please map these columns above (using Standard or Custom Columns) to proceed.")
                            validation_passed = False
                        else:
                            st.success("**Validation Passed:** All required output columns are present.")
                            validation_passed = True
                    else:
                        st.warning(" Validation schema file (output_validation.json) not found. Skipping validation.")
                        # If schema is missing, you can decide whether to allow run or not. 
                        # Here I'm allowing it but with a warning.
                        validation_passed = True 

                else:
                    st.warning("No columns mapped yet.")
                    validation_passed = False
                    
            except Exception as e:
                st.error(f"Could not preview or validate: {e}")
                validation_passed = False

            # --- DOWNLOAD BUTTONS ---
            if available_cols:
                df_download = df_final[available_cols]
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

            # --- 3. Execution ---
            st.header("3. Execution")
            
            # Button is DISABLED if validation_passed is False
            if st.button("Save Configuration & Run Pipeline", type="primary", disabled=not validation_passed):
                
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
                    st.subheader(" Triggering Orchestration")
                    
                    cmd = [sys.executable, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "orchestration", "flow.py"), mapping_filename]
                    
                    # --- REAL-TIME MONITORING UI ---

                    # 4. Pipeline Execution & Visualization

                    # --- REAL-TIME MONITORING UI ---
                    st.write("---")
                    st.subheader("🚀 Pipeline Execution Status")
                    
                    # 1. Timeline / Gantt Chart Container
                    timeline_container = st.empty()
                    
                    # 2. Progress & Status
                    progress_container = st.empty()
                    progress_bar = progress_container.progress(0, text="Initializing Pipeline...")
                    
                    # Status container for step-by-step updates
                    status_container = st.status("Starting Pipeline...", expanded=True)
                    
                    # 3. Live Issues Container (New)
                    st.write("---")
                    issues_expander = st.expander("⚠️ Live Issues & Warnings", expanded=True)
                    issues_placeholder = issues_expander.empty()
                    issues_list = []
                    
                    # Log container to show output
                    log_expander = st.expander("Show Execution Logs", expanded=False) # Collapsed by default now
                    log_placeholder = log_expander.empty()
                    
                    # Placeholder for the Dashboard Button (Early Detection)
                    dashboard_button_placeholder = st.empty()
                    
                    full_logs = []
                    
                    # Data structures for Gantt Chart
                    task_events = [] # List of dicts: {Task, Start, End, Status}
                    current_task = None
                    start_time = datetime.now()
                    
                    # Capture variable
                    final_output_filename = None
                    step_placeholder = None

                    # Helper to render logs
                    def render_logs(raw_lines, limit=50):
                        parsed = LogParser.parse_logs(raw_lines)
                        # Show last N entries if limit is set
                        if limit:
                            recent = parsed[-limit:]
                        else:
                            recent = parsed
                        
                        html_content = "<div style='font-family:monospace; font-size:12px; line-height:1.4;'>"
                        for entry in recent:
                            color = LogParser.get_color_for_level(entry['level'])
                            ts = entry['timestamp']
                            lvl = entry['level']
                            msg = entry['message']
                            details = entry['details']
                            
                            # Escape HTML sensitive chars
                            msg = msg.replace("<", "&lt;").replace(">", "&gt;")
                            
                            html_content += f"<div style='border-bottom: 1px solid #333; padding: 2px 0;'>"
                            html_content += f"<span style='color:#666; margin-right:8px;'>{ts}</span>"
                            html_content += f"<span style='color:{color}; font-weight:bold; margin-right:8px; min-width:60px; display:inline-block;'>{lvl}</span>"
                            html_content += f"<span style='color:#ddd;'>{msg}</span>"
                            
                            if details:
                                safe_details = details.replace("<", "&lt;").replace(">", "&gt;")
                                html_content += f"<div style='background:#1e1e1e; color:#e74c3c; padding:4px; margin-top:2px; white-space:pre-wrap;'>{safe_details}</div>"
                            
                            html_content += "</div>"
                        html_content += "</div>"
                        return html_content
                    
                    # Use Popen for real-time output reading
                    try:
                        process = subprocess.Popen(
                            cmd, 
                            stdout=subprocess.PIPE, 
                            stderr=subprocess.STDOUT, 
                            text=True, 
                            bufsize=1, 
                            universal_newlines=True,
                            encoding='utf-8', 
                            errors='replace'
                        )
                        
                        # Pipeline Steps for Tracking Progress
                        steps_map = {
                            "Step 1": 15,
                            "Step 2": 30,
                            "Step 3": 50,
                            "Step 4": 70,
                            "Step 5": 85,
                            "Saving Output": 95
                        }
                        
                        dashboard_url_found = False

                        while True:
                            # Read line by line
                            line = process.stdout.readline()
                            
                            if not line and process.poll() is not None:
                                break
                                
                            if line:
                                clean_line = line.strip()
                                
                                # Only append to visual logs if it's NOT a high-frequency progress update
                                if "Step 2 Progress" not in clean_line:
                                    full_logs.append(clean_line)
                                    
                                # Render structured logs
                                log_placeholder.markdown(render_logs(full_logs), unsafe_allow_html=True)
                                
                                # --- PARSING LOGIC ---
                                now = datetime.now()
                                
                                # 1. Issue Detection (Warnings/Errors)
                                if "WARNING" in clean_line or "ERROR" in clean_line or "Retrying" in clean_line or "Exception" in clean_line:
                                    # Aggregate Retries
                                    if "Retrying" in clean_line and "urllib3" in clean_line:
                                        # Parse count if possible or just increment global retry count
                                        retry_count = sum(1 for i in issues_list if "Retrying" in i) + 1
                                        issues_list.append(clean_line)
                                        
                                        # Clear and re-render the issues container with a summary
                                        with issues_placeholder.container():
                                            st.warning(f"⚠️ Connection Instability: {retry_count} Retries detected (OpenStreetMap/Geocoding)", icon="📡")
                                            # Show only unique *other* errors below
                                            other_issues = [i for i in issues_list if "Retrying" not in i]
                                            for issue in other_issues[-5:]:
                                                st.error(issue, icon="🚨")
                                    else:
                                        # Standard error display
                                        issues_list.append(clean_line)
                                        with issues_placeholder.container():
                                            # If we have retries, show that summary first
                                            retry_count = sum(1 for i in issues_list if "Retrying" in i)
                                            if retry_count > 0:
                                                st.warning(f"⚠️ Connection Instability: {retry_count} Retries detected (OpenStreetMap/Geocoding)", icon="📡")
                                            
                                            # Then show other errors
                                            other_issues = [i for i in issues_list if "Retrying" not in i]
                                            for issue in other_issues[-10:]:
                                                if "ERROR" in issue or "Exception" in issue:
                                                    st.error(issue, icon="🚨")
                                                else:
                                                    st.warning(issue, icon="⚠️")
                                
                                # 2. Check for Prefect Cloud URL
                                if not dashboard_url_found and "View at https" in clean_line:
                                    import re
                                    match = re.search(r"View at (https?://[^\s]+)", clean_line)
                                    if match:
                                        url = match.group(1).rstrip('.')
                                        dashboard_button_placeholder.link_button("👉 Monitor Real-Time in Prefect Cloud", url=url, type="primary")
                                        dashboard_url_found = True

                                # 3. Check for Steps & Metrics
                                if ">>> Step" in clean_line:
                                    try:
                                        content_part = clean_line.split(">>> ", 1)[1]
                                        
                                        # --- CASE A: METRICS or PROGRESS (Updates existing step) ---
                                        if "|" in content_part:
                                            # Ensure we have a placeholder to update
                                            if step_placeholder is None:
                                                step_placeholder = status_container.empty()
                                                
                                            if "Progress" in content_part:
                                                # PROGRESS UPDATE
                                                metric_parts = content_part.split("|")
                                                prog_dict = {}
                                                for part in metric_parts[1:]:
                                                    if ":" in part:
                                                        k, v = part.split(":", 1)
                                                        prog_dict[k.strip()] = v.strip()
                                                
                                                processed = prog_dict.get('processed', '0')
                                                remaining = prog_dict.get('remaining', '?')
                                                
                                                # Clean, Single-Line Live Status
                                                step_placeholder.info(f"⏳ **Geocoding in Progress** — Processed: `{processed}` | Remaining: `{remaining}`")
                                                
                                            else:
                                                # FINAL SUMMARY TABLE
                                                metric_parts = content_part.split("|")
                                                step_name = metric_parts[0].strip()
                                                
                                                # Build a clean Markdown Table
                                                metrics_md = f"**{step_name} Stats**\n\n"
                                                metrics_md += "| Metric | Value |\n|---|---|\n"
                                                
                                                for part in metric_parts[1:]:
                                                    if ":" in part:
                                                        k, v = part.split(":", 1)
                                                        k = k.strip().replace("_", " ").title()
                                                        v = v.strip()
                                                        metrics_md += f"| {k} | `{v}` |\n"
                                                
                                                # Overwrite the placeholder with the final table
                                                step_placeholder.markdown(metrics_md)
                                            
                                        # --- CASE B: NEW STEP START ---
                                        else:
                                            # Standard Step Start text
                                            step_info = content_part.strip()
                                            
                                            if ":" in step_info:
                                                step_prefix, step_desc = step_info.split(":", 1)
                                                step_prefix = step_prefix.strip()
                                                step_desc = step_desc.strip()
                                            else:
                                                step_prefix = step_info
                                                step_desc = ""

                                            # Close previous task logic (Gantt)
                                            if current_task:
                                                current_task['End'] = now
                                                current_task['Status'] = 'Completed'
                                            
                                            # Start new task logic
                                            new_task = {
                                                "Task": f"{step_prefix}: {step_desc}",
                                                "Start": now,
                                                "End": now,
                                                "Status": "Running"
                                            }
                                            task_events.append(new_task)
                                            current_task = new_task
                                            
                                            # 1. Append Header to Container
                                            status_container.markdown(f"### ✅ {step_prefix}: {step_desc}")
                                            
                                            # 2. Create NEW Placeholder for this step's future metrics
                                            step_placeholder = status_container.empty()
                                            
                                            # Update Progress Bar
                                            prog_val = 0
                                            for k, v in steps_map.items():
                                                if k in step_prefix:
                                                    prog_val = v
                                                    break
                                            if prog_val > 0:
                                                progress_bar.progress(prog_val, text=f"Running: {step_desc}")

                                    except Exception as e:
                                        pass

                                # Capture Output Filename
                                if "Saving Output" in clean_line:
                                    try:
                                        # Reset current task if needed
                                        if current_task:
                                            current_task['End'] = now
                                            current_task['Status'] = 'Completed'
                                            
                                        # Start saving task
                                        new_task = {
                                            "Task": "Saving Output",
                                            "Start": now,
                                            "End": now,
                                            "Status": "Running"
                                        }
                                        task_events.append(new_task)
                                        current_task = new_task
                                        
                                        status_container.markdown("### 💾 Saving Result to Supabase...")
                                        progress_bar.progress(95, text="Finalizing...")
                                        
                                        # Regex to find filename if needed
                                        import re
                                        match_fname = re.search(r"Saving Output:\s*(.*?)(?:\.\.\.|$)", clean_line)
                                        if match_fname:
                                            final_output_filename = match_fname.group(1).strip()
                                    except Exception as rx:
                                        pass

                                # --- UPDATE GANTT CHART ---
                                if task_events:
                                    # Update current task end time to now for visualization effect
                                    if current_task and current_task["Status"] == "Running":
                                        current_task["End"] = now
                                    
                                    # Create DataFrame
                                    df_gantt = pd.DataFrame(task_events)
                                    
                                    # Render Chart
                                    chart = alt.Chart(df_gantt).mark_bar().encode(
                                        x=alt.X('Start', title='Time', axis=alt.Axis(format='%H:%M:%S')),
                                        x2='End',
                                        y=alt.Y('Task', sort=None, title=None), # Keep insertion order
                                        color=alt.Color('Status', scale=alt.Scale(domain=['Running', 'Completed', 'Failed'], range=['#3498db', '#2ecc71', '#e74c3c'])),
                                        tooltip=['Task', 'Start', 'End', 'Status']
                                    ).properties(
                                        title="Live Pipeline Timeline",
                                        width="container",
                                        height=200
                                    )
                                    
                                    timeline_container.altair_chart(chart, theme="streamlit")


                        # Wait for process to finish
                        return_code = process.wait()
                        
                        # Close any remaining running task
                        if current_task:
                            current_task['End'] = datetime.now()
                            current_task['Status'] = 'Completed' if return_code == 0 else 'Failed'
                            
                            # Final Chart Update
                            df_gantt = pd.DataFrame(task_events)
                            chart = alt.Chart(df_gantt).mark_bar().encode(
                                x=alt.X('Start', title='Time', axis=alt.Axis(format='%H:%M:%S')),
                                x2='End',
                                y=alt.Y('Task', sort=None),
                                color=alt.Color('Status', scale=alt.Scale(domain=['Running', 'Completed', 'Failed'], range=['#3498db', '#2ecc71', '#e74c3c'])),
                            ).properties(title="Final Execution Timeline", width="container", height=200)
                            timeline_container.altair_chart(chart, theme="streamlit")

                        # Final log dump (full)
                        log_placeholder.markdown(render_logs(full_logs, limit=None), unsafe_allow_html=True)

                        if return_code == 0:
                            progress_bar.progress(100, text="Pipeline Completed Successfully!")
                            status_container.update(label="Pipeline Finished ✅", state="complete", expanded=False)
                            st.success("Pipeline executed successfully!")
                            
                            # If we hadn't found the URL yet (rare), check full logs again
                            if not dashboard_url_found:
                                combined_output = "\n".join(full_logs)
                                import re
                                dashboard_match = re.search(r"View at (https?://[^\s]+)", combined_output)
                                if dashboard_match:
                                    dashboard_url = dashboard_match.group(1).rstrip('.')
                                    dashboard_button_placeholder.link_button("👉 View Run in Prefect Cloud", url=dashboard_url, type="primary")
                            
                            # --- DOWNLOAD BUTTON FOR OUTPUT ---
                            # Fallback: Calculate expected filename deterministically
                            if not final_output_filename:
                                try:
                                    base_name = os.path.splitext(uploaded_file.name)[0]
                                    final_output_filename = f"processed_{base_name}.csv"
                                except:
                                    pass

                            if final_output_filename:
                                try:
                                    st.write("---")
                                    st.subheader("📥 Download Results")
                                    # Check if file exists (optimized check not really possible via simple storage API, just try download)
                                    with st.spinner(f"Fetching processed file: {final_output_filename}..."):
                                        data = supabase.storage.from_(BUCKET_OUTPUT).download(final_output_filename)
                                        
                                        col_dl1, col_dl2 = st.columns([1, 1])
                                        with col_dl1:
                                            st.download_button(
                                                label="📥 Download Processed File (CSV)",
                                                data=data,
                                                file_name=final_output_filename,
                                                mime="text/csv",
                                                type="primary"
                                            )
                                        st.success("File ready for download!")
                                except Exception as e:
                                    st.error(f"Error fetching output file `{final_output_filename}`: {str(e)}")
                                    st.warning("Please check your Supabase 'output' bucket to verify if the file was saved.")
                            else:
                                st.warning("Could not determine output filename (Logic Error).")
                                
                        else:
                            progress_bar.progress(0, text="Pipeline Failed ❌")
                            status_container.update(label="Pipeline Failed ❌", state="error", expanded=True)
                            # Dont show generic error if issues list has details
                            if issues_list:
                                st.error("Pipeline failed with issues found above.")
                            else:
                                st.error("Pipeline failed! Check the logs above.")                            
                    except Exception as e:
                        st.error(f"Execution Error: {e}")
                else:
                    st.error("Upload failed, cannot proceed.")
                    
        except Exception as e:
            st.error(f"Error reading CSV: {e}")

if __name__ == "__main__":
    main()
