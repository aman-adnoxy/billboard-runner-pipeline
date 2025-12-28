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
# Add parent directory to path to allow importing src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import REQUIRED_FIELDS, BUCKET_INPUT, BUCKET_MAPPING, BUCKET_OUTPUT, load_required_fields
from src.database import get_supabase_client

# Initialize Supabase
supabase = get_supabase_client()

# Re-load fields dynamically if needed or use imported constant
REQUIRED_FIELDS = load_required_fields()

# --- NEW: Helper to Load Validation Schema ---
def load_validation_schema():
    """Loads the output validation schema from config folder."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "output_validation.json")
    
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)
    return None

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
                                    l_conf = REQUIRED_FIELDS.get('latitude', {})
                                    l_idx = get_default_index('latitude', l_conf, col_options_base)
                                    sel_lat = st.selectbox(
                                        f"{l_conf.get('label', 'Latitude')}", 
                                        col_options_base, 
                                        index=l_idx, 
                                        key="std_latitude"
                                    )
                                    if sel_lat != "(Select Column)":
                                        rename_mapping[sel_lat] = "latitude"
                                with sub_c2:
                                    # Longitude
                                    g_conf = REQUIRED_FIELDS.get('longitude', {})
                                    g_idx = get_default_index('longitude', g_conf, col_options_base)
                                    sel_long = st.selectbox(
                                        f"{g_conf.get('label', 'Longitude')}", 
                                        col_options_base, 
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
                    
                    cmd = [sys.executable, os.path.join(os.path.dirname(os.path.dirname(__file__)), "orchestration", "flow.py"), mapping_filename]
                    
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
                                full_logs.append(clean_line)
                                # Show last 20 lines to keep UI snappy
                                log_placeholder.code("\n".join(full_logs[-20:]))
                                
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

                                # 3. Check for Steps (Gantt Chart Logic)
                                if ">>> Step" in clean_line:
                                    # Example: ">>> Step 1: Standardizing Schema..."
                                    parts = clean_line.split(":")
                                    if len(parts) > 1:
                                        step_prefix = parts[0].replace(">>> ", "").strip()
                                        step_desc = parts[1].strip()
                                        
                                        # Close previous task
                                        if current_task:
                                            current_task['End'] = now
                                            current_task['Status'] = 'Completed'
                                        
                                        # Start new task
                                        new_task = {
                                            "Task": f"{step_prefix}: {step_desc}",
                                            "Start": now,
                                            "End": now, # Will update dynamically
                                            "Status": "Running"
                                        }
                                        task_events.append(new_task)
                                        current_task = new_task
                                        
                                        # Write to status box
                                        status_container.write(f"✅ Executing: **{step_prefix}** - {step_desc}")
                                        
                                        # Update Progress logic
                                        prog_val = 0
                                        for k, v in steps_map.items():
                                            if k in step_prefix:
                                                prog_val = v
                                                break
                                        
                                        if prog_val > 0:
                                            progress_bar.progress(prog_val, text=f"Running: {step_desc}")
                                            
                                    # Capture Output Filename (Regex for safety)
                                    if "Saving Output" in clean_line:
                                        try:
                                            # Regex to find: match "Saving Output: " followed by filename until "..." or end
                                            import re
                                            # Look for "Saving Output: <filename>..." or just <filename>
                                            match_fname = re.search(r"Saving Output:\s*(.*?)(?:\.\.\.|$)", clean_line)
                                            if match_fname:
                                                final_output_filename = match_fname.group(1).strip()
                                                # Debug: verify capture (can remove later)
                                                # log_placeholder.write(f"DEBUG: Captured filename: {final_output_filename}") 
                                        except Exception as rx:
                                            pass

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
                                    
                                    status_container.write("💾 Saving Result to Supabase...")
                                    progress_bar.progress(95, text="Finalizing...")

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
                        log_placeholder.code("\n".join(full_logs))

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