import streamlit as st
import pandas as pd
import time
import io
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.prefect_utils import (
    check_and_get_running_flow,
    save_flow_run_state,
    clear_flow_run_state,
    get_flow_run_status,
    get_flow_run_progress,
    pause_flow_run,
    resume_flow_run,
    cancel_flow_run
)

# --- Page Configuration ---
st.set_page_config(
    page_title="Billboard API Ingestion",
    page_icon="🏢",
    layout="wide"
)

# --- Header Section ---
st.title("🏢 Billboard API Data Ingestion")
st.markdown("""
This step takes raw billboard data and processes it through the **Billboard Profile API** to:
- **Generate AI-Enhanced Profiles**: Creates SEO-optimized titles and descriptions.
- **Enrich with Context**: Adds nearby POIs, traffic data, and visibility scores.
- **Batch Processing**: Efficiently handles large datasets in configurable batches.
- **Database Persistence**: Upserts results directly to Supabase.
""")

st.divider()

# --- System Status ---
with st.expander("📊 System Status & Configuration", expanded=False):
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Batch Size", "25 records", help="Records processed per API call")
    with col2:
        st.metric("Retry Policy", "3 attempts", help="Automatic retries with 60s delay")
    with col3:
        st.metric("Timeout", "900s", help="Maximum time per API request")
    
    st.info("💡 **Tip**: Ensure `BILLBOARD_API_URL` and `HF_TOKEN` environment variables are set.")

# --- File Upload Section ---
st.header("1️⃣ Upload Billboard Data")

uploaded_file = st.file_uploader(
    "Upload Raw Billboard CSV",
    type=["csv"],
    help="Upload a CSV file containing billboard data with required columns"
)

# Required columns definition with descriptions
REQUIRED_COLUMNS = {
    "billboard_id": "Unique identifier for each billboard",
    "lat": "Latitude coordinate",
    "lon": "Longitude coordinate",
    "width_ft": "Billboard width in feet",
    "height_ft": "Billboard height in feet",
    "lighting_type": "Type of lighting (e.g., frontlit, backlit, digital)",
    "format_type": "Billboard format (e.g., static, digital, LED)",
    "quantity": "Number of billboard faces",
    "frequency_per_minute": "Ad rotation frequency (for digital)",
    "locality": "Location/area name",
    "image_urls": "URLs of billboard images",
    "area": "Billboard area in square feet",
    "base_rate_per_month": "Base rental rate per month",
    "base_rate_per_unit": "Base rental rate per unit",
    "card_rate_per_month": "Card rate per month",
    "card_rate_per_unit": "Card rate per unit",
    "city": "City where billboard is located",
    "district": "District where billboard is located",
    "location": "Full location description",
}

# Show required columns reference
with st.expander("📋 Required Columns Reference", expanded=False):
    col_df = pd.DataFrame([
        {"Column": col, "Description": desc}
        for col, desc in REQUIRED_COLUMNS.items()
    ])
    st.dataframe(col_df, use_container_width=True, hide_index=True)

# --- Session State Initialization ---
if 'pipeline_running' not in st.session_state:
    st.session_state.pipeline_running = False
if 'pipeline_results' not in st.session_state:
    st.session_state.pipeline_results = None
if 'last_run_time' not in st.session_state:
    st.session_state.last_run_time = None
if 'flow_run_id' not in st.session_state:
    st.session_state.flow_run_id = None
if 'skip_existing_docs' not in st.session_state:
    st.session_state.skip_existing_docs = True  # Default to True to avoid reprocessing

# --- Navigation Confirmation When Pipeline is Running ---
# Inject JavaScript to warn user before navigating away during active pipeline
def inject_navigation_guard(is_running: bool):
    """Inject JavaScript to prevent accidental navigation when pipeline is running."""
    if is_running:
        st.components.v1.html(
            """
            <script>
                // Set up beforeunload event to warn user
                window.addEventListener('beforeunload', function(e) {
                    e.preventDefault();
                    e.returnValue = 'A pipeline is currently running. Are you sure you want to leave? Your progress tracking may be lost.';
                    return e.returnValue;
                });
                
                // Also intercept clicks on sidebar navigation links
                const observer = new MutationObserver(function(mutations) {
                    const sidebarLinks = parent.document.querySelectorAll('[data-testid="stSidebarNav"] a');
                    sidebarLinks.forEach(function(link) {
                        if (!link.hasAttribute('data-nav-guard')) {
                            link.setAttribute('data-nav-guard', 'true');
                            link.addEventListener('click', function(e) {
                                if (!confirm('⚠️ A pipeline is currently running!\\n\\nAre you sure you want to navigate away?\\n\\nNote: The pipeline will continue running in Prefect, but you may lose real-time progress tracking.')) {
                                    e.preventDefault();
                                    e.stopPropagation();
                                }
                            });
                        }
                    });
                });
                
                // Observe sidebar for changes
                const sidebar = parent.document.querySelector('[data-testid="stSidebar"]');
                if (sidebar) {
                    observer.observe(sidebar, { childList: true, subtree: true });
                }
                
                // Initial setup for existing links
                setTimeout(function() {
                    const sidebarLinks = parent.document.querySelectorAll('[data-testid="stSidebarNav"] a');
                    sidebarLinks.forEach(function(link) {
                        if (!link.hasAttribute('data-nav-guard')) {
                            link.setAttribute('data-nav-guard', 'true');
                            link.addEventListener('click', function(e) {
                                if (!confirm('⚠️ A pipeline is currently running!\\n\\nAre you sure you want to navigate away?\\n\\nNote: The pipeline will continue running in Prefect, but you may lose real-time progress tracking.')) {
                                    e.preventDefault();
                                    e.stopPropagation();
                                }
                            });
                        }
                    });
                }, 500);
            </script>
            """,
            height=0,
            width=0,
        )

# Check if we need to show the navigation guard
# (either session state says running, or we detect a running flow)
running_flow = check_and_get_running_flow()
should_guard_navigation = st.session_state.pipeline_running or (running_flow and running_flow.get("is_active"))

if should_guard_navigation:
    inject_navigation_guard(True)

# --- Check for Running Flow on Page Load ---
# Note: running_flow was already fetched above for navigation guard check

if running_flow and running_flow.get("is_active"):
    st.session_state.pipeline_running = True
    st.session_state.flow_run_id = running_flow.get("flow_run_id")
    
    # Determine the status message based on flow state
    flow_status = running_flow.get("status", {})
    if flow_status.get("is_paused"):
        st.warning("⏸️ **Pipeline Paused!**")
        st.info("""
        The Billboard API pipeline is currently paused. You can resume it below or stop it completely.
        The flow will remain paused until you take action.
        """)
    else:
        st.warning("🔄 **Active Pipeline Detected!**")
        st.info("""
        A Billboard API pipeline is currently running. This page was refreshed, but your 
        flow continues to run in Prefect. You can monitor its progress below or dismiss it 
        to start a new run.
        """)
    
    # Display running flow information
    with st.expander("📊 Running Flow Details", expanded=True):
        flow_status = running_flow.get("status", {})
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Flow Run ID", running_flow.get("flow_run_id", "N/A")[:8] + "...")
        with col2:
            # Show status with color coding
            status_name = flow_status.get("state_name", "Unknown")
            if flow_status.get("is_paused"):
                st.metric("Status", f"⏸️ {status_name}")
            elif flow_status.get("is_cancelled"):
                st.metric("Status", f"⏹️ {status_name}")
            else:
                st.metric("Status", status_name)
        with col3:
            started = running_flow.get("started_at", "Unknown")
            if started != "Unknown":
                try:
                    from datetime import datetime as dt
                    start_dt = dt.fromisoformat(started.replace('Z', '+00:00'))
                    elapsed = (dt.utcnow() - start_dt.replace(tzinfo=None)).total_seconds()
                    st.metric("Running For", f"{int(elapsed // 60)}m {int(elapsed % 60)}s")
                except:
                    st.metric("Started At", started[:19])
            else:
                st.metric("Started At", started)
        
        st.markdown(f"**Total Records:** {running_flow.get('total_records', 'N/A')}")
        st.markdown(f"**Output Path:** `{running_flow.get('output_json_path', 'N/A')}`")
        
        # Link to Prefect Dashboard
        flow_run_id = running_flow.get("flow_run_id", "")
        st.markdown(f"🔗 [View in Prefect Cloud](https://app.prefect.cloud/flow-runs/{flow_run_id})")

    
    # Actions for running flow
    st.subheader("Actions")
    
    # Get current flow status to determine which buttons to show
    flow_status = running_flow.get("status", {})
    is_paused = flow_status.get("is_paused", False)
    is_running_active = flow_status.get("is_running", False)
    
    action_col1, action_col2, action_col3, action_col4 = st.columns(4)
    
    with action_col1:
        if st.button("🔄 Refresh Status", use_container_width=True):
            st.rerun()
    
    with action_col2:
        # Show Pause button only if flow is actively running (not paused)
        if is_running_active and not is_paused:
            if st.button("⏸️ Pause", type="secondary", use_container_width=True):
                with st.spinner("Pausing flow..."):
                    success = pause_flow_run(running_flow.get("flow_run_id"))
                    if success:
                        st.success("✅ Flow paused successfully!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Failed to pause flow. Check Prefect Cloud for details.")
        # Show Resume button if flow is paused
        elif is_paused:
            if st.button("▶️ Resume", type="primary", use_container_width=True):
                with st.spinner("Resuming flow..."):
                    success = resume_flow_run(running_flow.get("flow_run_id"))
                    if success:
                        st.success("✅ Flow resumed successfully!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Failed to resume flow. Check Prefect Cloud for details.")
        else:
            st.button("⏸️ Pause", disabled=True, use_container_width=True, help="Flow is not in a pausable state")
    
    with action_col3:
        # Stop button - available for running or paused flows
        if is_running_active or is_paused:
            if st.button("⏹️ Stop", type="secondary", use_container_width=True, help="Cancel the running flow"):
                # Confirmation dialog using session state
                if 'confirm_stop' not in st.session_state:
                    st.session_state.confirm_stop = True
                    st.rerun()
        else:
            st.button("⏹️ Stop", disabled=True, use_container_width=True, help="No active flow to stop")
    
    with action_col4:
        if st.button("❌ Dismiss & Start New", type="secondary", use_container_width=True):
            clear_flow_run_state()
            st.session_state.pipeline_running = False
            st.session_state.flow_run_id = None
            st.success("Flow tracking cleared. You can now start a new run.")
            st.rerun()
    
    # Handle stop confirmation
    if st.session_state.get('confirm_stop', False):
        st.warning("⚠️ **Confirm Stop Action**")
        st.markdown("Are you sure you want to stop this flow? This action cannot be undone.")
        
        confirm_col1, confirm_col2, confirm_col3 = st.columns([1, 1, 2])
        with confirm_col1:
            if st.button("✅ Yes, Stop Flow", type="primary"):
                with st.spinner("Stopping flow..."):
                    success = cancel_flow_run(running_flow.get("flow_run_id"))
                    if success:
                        st.success("✅ Flow stopped successfully!")
                        clear_flow_run_state()
                        st.session_state.pipeline_running = False
                        st.session_state.flow_run_id = None
                        st.session_state.confirm_stop = False
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Failed to stop flow. Check Prefect Cloud for details.")
                        st.session_state.confirm_stop = False
        with confirm_col2:
            if st.button("❌ Cancel"):
                st.session_state.confirm_stop = False
                st.rerun()
    
    # Auto-refresh toggle (moved below confirmation dialog)
    auto_refresh = st.checkbox("Auto-refresh (every 5s)", value=False)
    if auto_refresh:
        time.sleep(5)
        st.rerun()
    
    # Show progress if we can get it
    st.subheader("📈 Live Progress")
    progress = get_flow_run_progress(running_flow.get("flow_run_id"))
    
    if progress and not progress.get("error"):
        task_runs = progress.get("task_runs", [])
        
        # Count completed batch tasks
        batch_tasks = [t for t in task_runs if "process_batch" in t.get("name", "")]
        completed_batches = sum(1 for t in batch_tasks if t.get("is_completed"))
        
        if batch_tasks:
            progress_pct = int((completed_batches / len(batch_tasks)) * 100)
            st.progress(progress_pct / 100, text=f"Batch {completed_batches}/{len(batch_tasks)}")
        
        # Show recent task activity
        with st.expander("🔍 Task Activity", expanded=False):
            if task_runs:
                for task in sorted(task_runs, key=lambda x: x.get("start_time") or "", reverse=True)[:10]:
                    status_icon = "✅" if task.get("is_completed") else "🔄"
                    st.markdown(f"{status_icon} **{task.get('name', 'Unknown')}** - {task.get('state_name', 'Unknown')}")
            else:
                st.info("No task activity yet...")
    else:
        st.info("Loading progress information...")
    
    # Check if flow completed
    if running_flow.get("status", {}).get("is_completed"):
        st.success("✅ **Pipeline Completed!**")
        
        output_path = running_flow.get("output_json_path")
        if output_path and os.path.exists(output_path):
            with open(output_path, 'r', encoding='utf-8') as f:
                results_json = f.read()
            
            st.download_button(
                label="📥 Download Results JSON",
                data=results_json,
                file_name=f"billboard_api_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                type="primary"
            )
        
        if st.button("✓ Clear Completed Run"):
            clear_flow_run_state()
            st.session_state.pipeline_running = False
            st.rerun()
    
    elif running_flow.get("status", {}).get("is_failed"):
        st.error("❌ **Pipeline Failed!**")
        st.markdown(f"State: `{running_flow.get('status', {}).get('state_name', 'Unknown')}`")
        
        if st.button("Clear Failed Run"):
            clear_flow_run_state()
            st.session_state.pipeline_running = False
            st.rerun()
    
    st.divider()
    st.info("⬇️ To start a new run, dismiss the running flow above first, then upload a new file below.")
    
    # IMPORTANT: Stop here to prevent starting a new pipeline when one is already running
    # The rest of the page handles file upload and new pipeline execution
    st.stop()

# --- Main Processing Logic ---
if uploaded_file:
    # --- Data Loading ---
    try:
        df = pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"❌ Failed to read CSV: {e}")
        st.stop()

    # --- Column Validation ---
    st.header("2️⃣ Data Validation")
    
    required_set = set(REQUIRED_COLUMNS.keys())
    available_columns = set(df.columns)
    missing = required_set - available_columns
    present = required_set & available_columns
    extra = available_columns - required_set

    # Validation Status Display
    val_col1, val_col2, val_col3 = st.columns(3)
    
    with val_col1:
        if len(present) == len(required_set):
            st.success(f"✅ **{len(present)}/{len(required_set)}** required columns found")
        else:
            st.error(f"⚠️ **{len(present)}/{len(required_set)}** required columns found")
    
    with val_col2:
        st.info(f"📄 **{len(df):,}** records loaded")
    
    with val_col3:
        if extra:
            st.warning(f"ℹ️ **{len(extra)}** extra columns (will be ignored)")

    # Show validation details
    with st.expander("🔍 Validation Details", expanded=missing != set()):
        if missing:
            st.error("**Missing Required Columns:**")
            for col in sorted(missing):
                st.markdown(f"- ❌ `{col}`: {REQUIRED_COLUMNS[col]}")
        
        if present:
            st.success("**Present Columns:**")
            present_text = ", ".join([f"`{c}`" for c in sorted(present)])
            st.markdown(present_text)
        
        if extra:
            st.info("**Extra Columns (ignored):**")
            extra_text = ", ".join([f"`{c}`" for c in sorted(extra)])
            st.markdown(extra_text)

    if missing:
        st.error(f"❌ Cannot proceed. Missing required columns: **{', '.join(sorted(missing))}**")
        st.stop()

    # --- Data Preview ---
    st.header("3️⃣ Data Preview")
    
    preview_tabs = st.tabs(["📊 Data Table", "📈 Statistics", "🗺️ Location Preview"])
    
    with preview_tabs[0]:
        st.dataframe(df.head(20), use_container_width=True)
        st.caption(f"Showing first 20 of {len(df):,} records")
    
    with preview_tabs[1]:
        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
        with stat_col1:
            st.metric("Total Records", f"{len(df):,}")
        with stat_col2:
            st.metric("Unique Localities", df['locality'].nunique() if 'locality' in df.columns else "N/A")
        with stat_col3:
            st.metric("Avg Width (ft)", f"{df['width_ft'].mean():.1f}" if 'width_ft' in df.columns else "N/A")
        with stat_col4:
            st.metric("Avg Height (ft)", f"{df['height_ft'].mean():.1f}" if 'height_ft' in df.columns else "N/A")
        
        # Format type distribution
        if 'format_type' in df.columns:
            st.subheader("Format Type Distribution")
            format_counts = df['format_type'].value_counts()
            st.bar_chart(format_counts)
        
        # Lighting type distribution
        if 'lighting_type' in df.columns:
            st.subheader("Lighting Type Distribution")
            lighting_counts = df['lighting_type'].value_counts()
            st.bar_chart(lighting_counts)
    
    with preview_tabs[2]:
        if 'lat' in df.columns and 'lon' in df.columns:
            # Create map data
            map_df = df[['lat', 'lon']].dropna()
            map_df = map_df.rename(columns={'lat': 'latitude', 'lon': 'longitude'})
            if not map_df.empty:
                st.map(map_df, zoom=10)
                st.caption(f"Showing {len(map_df):,} billboard locations")
            else:
                st.warning("No valid coordinates to display on map")
        else:
            st.warning("Latitude/Longitude columns not available for map preview")

    # --- Pipeline Execution ---
    st.divider()
    st.header("4️⃣ Execute Pipeline")
    
    # Configuration options
    with st.expander("⚙️ Execution Options", expanded=True):
        batch_info_col1, batch_info_col2 = st.columns(2)
        with batch_info_col1:
            total_batches = -(-len(df) // 25)  # Ceiling division
            st.info(f"📦 **Total Batches**: {total_batches} (25 records each)")
        with batch_info_col2:
            est_time = total_batches * 10  # Rough estimate: 10 seconds per batch
            st.info(f"⏱️ **Estimated Time**: ~{est_time // 60} min {est_time % 60} sec")
        
        st.divider()
        
        # Skip existing documents toggle
        skip_existing = st.checkbox(
            "⏭️ Skip Already Processed Documents",
            value=st.session_state.skip_existing_docs,
            help="Check MongoDB for existing billboard IDs and skip reprocessing them. Uncheck to reprocess all records."
        )
        st.session_state.skip_existing_docs = skip_existing
        
        if skip_existing:
            st.success("✅ Will check MongoDB and skip existing billboard IDs")
        else:
            st.warning("⚠️ Will reprocess all records, even if they already exist in MongoDB")

    # Run button - only enable if no flow is actively running
    # Note: If a flow is running (detected from Prefect), we would have hit st.stop() earlier
    run_col1, run_col2 = st.columns([1, 3])
    with run_col1:
        run_button = st.button(
            "🚀 Run Billboard API Pipeline",
            type="primary",
            use_container_width=True
        )

    # Only start pipeline when button is clicked - NOT based on session state from prior runs
    # This prevents accidental re-execution on page refresh
    if run_button:
        st.session_state.pipeline_running = True
        
        total_records = len(df)
        batch_size = 25
        total_batches = -(-total_records // batch_size)
        
        # Progress tracking UI
        st.subheader("📊 Pipeline Progress")
        
        # Prefect Cloud Dashboard Link
        prefect_dashboard_placeholder = st.empty()
        prefect_dashboard_placeholder.info("🔗 **Prefect Dashboard**: Run will be visible in Prefect Cloud once started.")
        
        progress_bar = st.progress(0)
        status_container = st.status("🔄 Initializing Pipeline...", expanded=True)
        
        # Metrics placeholders
        metric_cols = st.columns(5)
        with metric_cols[0]:
            processed_metric = st.empty()
            processed_metric.metric("Processed", "0")
        with metric_cols[1]:
            remaining_metric = st.empty()
            remaining_metric.metric("Remaining", str(total_records))
        with metric_cols[2]:
            batch_metric = st.empty()
            batch_metric.metric("Current Batch", f"0/{total_batches}")
        with metric_cols[3]:
            success_metric = st.empty()
            success_metric.metric("Success", "0")
        with metric_cols[4]:
            error_metric = st.empty()
            error_metric.metric("Errors", "0")
        
        # Log container
        log_expander = st.expander("📜 Execution Logs (Real-Time)", expanded=True)
        log_placeholder = log_expander.empty()
        
        # Download placeholder (will appear after completion)
        download_placeholder = st.empty()
        
        # Environment check section
        env_expander = st.expander("🔧 Environment Status", expanded=False)
        with env_expander:
            import os
            api_url = os.getenv("BILLBOARD_API_URL", "")
            hf_token = os.getenv("HF_TOKEN", "")
            
            env_col1, env_col2 = st.columns(2)
            with env_col1:
                if api_url:
                    st.success(f"✅ BILLBOARD_API_URL: `{api_url[:30]}...`")
                else:
                    st.error("❌ BILLBOARD_API_URL: Not Set")
            with env_col2:
                if hf_token:
                    st.success(f"✅ HF_TOKEN: `{hf_token[:10]}***`")
                else:
                    st.warning("⚠️ HF_TOKEN: Not Set (may be optional)")
        
        import subprocess
        import sys
        import os
        import tempfile
        import re
        import json
        
        start_time = time.time()
        logs = []
        output_json_path = None
        total_success = 0
        total_errors = 0
        current_processed = 0
        current_batch = 0
        
        try:
            with status_container:
                st.write("🔄 Preparing pipeline execution...")
                
                # Save uploaded CSV to temp file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as tmp_csv:
                    df.to_csv(tmp_csv, index=False)
                    input_csv_path = tmp_csv.name
                
                # Create output JSON path
                output_json_path = tempfile.mktemp(suffix='_billboard_results.json')
                
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 📂 Input CSV saved to temp file")
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 📦 Output will be saved to: {os.path.basename(output_json_path)}")
                log_placeholder.code("\n".join(logs[-50:]))  # Show last 50 lines
                
                # Build command
                runner_script = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                    "orchestration",
                    "billboard_api_runner.py"
                )
                
                
                cmd = [sys.executable, runner_script, input_csv_path, output_json_path]
                
                # Add skip-existing flag if enabled
                if st.session_state.skip_existing_docs:
                    cmd.append("--skip-existing")
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ⏭️ Skip existing documents: ENABLED")
                else:
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 Skip existing documents: DISABLED (will reprocess all)")
                
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 Starting subprocess...")
                log_placeholder.code("\n".join(logs[-50:]))

                
                st.write("🔄 Running Billboard API Pipeline...")
                
                # Update dashboard link
                prefect_dashboard_placeholder.success("🔗 **Pipeline Running** - Check [Prefect Cloud](https://app.prefect.cloud) for real-time monitoring")
                
                # Run subprocess with real-time output
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
                
                # Parse output in real-time
                while True:
                    line = process.stdout.readline()
                    
                    if not line and process.poll() is not None:
                        break
                    
                    if line:
                        clean_line = line.strip()
                        if clean_line:
                            logs.append(clean_line)
                            log_placeholder.code("\n".join(logs[-50:]))
                            
                            # Parse FLOW_RUN_ID for reconnection support
                            if "FLOW_RUN_ID >>>" in clean_line:
                                try:
                                    flow_run_id = clean_line.split(">>>")[1].strip()
                                    st.session_state.flow_run_id = flow_run_id
                                    # Save flow state for reconnection if page refreshes
                                    save_flow_run_state(
                                        flow_run_id=flow_run_id,
                                        input_csv_path=input_csv_path,
                                        output_json_path=output_json_path,
                                        total_records=total_records
                                    )
                                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 📋 Flow Run ID captured for reconnection")
                                    prefect_dashboard_placeholder.success(
                                        f"🔗 **Pipeline Running** - [View in Prefect Cloud](https://app.prefect.cloud/flow-runs/{flow_run_id})"
                                    )
                                except Exception as e:
                                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ Could not capture flow ID: {e}")
                            
                            # Parse PROGRESS lines
                            elif "PROGRESS >>>" in clean_line:
                                try:
                                    # Extract batch info
                                    batch_match = re.search(r'Batch (\d+)/(\d+)', clean_line)
                                    processed_match = re.search(r'processed: (\d+)', clean_line)
                                    remaining_match = re.search(r'remaining: (\d+)', clean_line)
                                    
                                    if batch_match:
                                        current_batch = int(batch_match.group(1))
                                        batch_metric.metric("Current Batch", f"{current_batch}/{total_batches}")
                                        progress_pct = int((current_batch / total_batches) * 100)
                                        progress_bar.progress(min(progress_pct, 99))
                                    
                                    if processed_match:
                                        current_processed = int(processed_match.group(1))
                                        processed_metric.metric("Processed", str(current_processed))
                                    
                                    if remaining_match:
                                        remaining = int(remaining_match.group(1))
                                        remaining_metric.metric("Remaining", str(remaining))
                                        
                                except Exception:
                                    pass
                            
                            # Parse BATCH result lines
                            elif "BATCH >>>" in clean_line:
                                try:
                                    success_match = re.search(r'success: (\d+)', clean_line)
                                    error_match = re.search(r'errors: (\d+)', clean_line)
                                    
                                    if success_match:
                                        batch_success = int(success_match.group(1))
                                        total_success += batch_success
                                        success_metric.metric("Success", str(total_success))
                                    
                                    if error_match:
                                        batch_errors = int(error_match.group(1))
                                        total_errors += batch_errors
                                        error_metric.metric("Errors", str(total_errors))
                                        
                                except Exception:
                                    pass
                            
                            # Parse ERROR lines
                            elif "ERROR >>>" in clean_line:
                                st.warning(clean_line)
                            
                            # Parse completion
                            elif "PIPELINE_COMPLETE >>>" in clean_line:
                                progress_bar.progress(100)
                                processed_metric.metric("Processed", str(total_records))
                                remaining_metric.metric("Remaining", "0")
                                batch_metric.metric("Current Batch", f"{total_batches}/{total_batches}")
                
                # Wait for process to complete
                return_code = process.wait()
                
                if return_code != 0:
                    raise RuntimeError(f"Pipeline process exited with code {return_code}")
            
            elapsed_time = time.time() - start_time
            st.session_state.last_run_time = elapsed_time
            st.session_state.pipeline_running = False
            
            # Clear the saved flow state since pipeline completed
            clear_flow_run_state()
            
            # Update final status
            prefect_dashboard_placeholder.success("✅ **Pipeline Complete** - View run details in [Prefect Cloud](https://app.prefect.cloud)")
            
            # Success message
            st.success(f"✅ Pipeline execution completed in {elapsed_time:.1f} seconds!")
            
            # Results summary
            st.subheader("📋 Execution Summary")
            summary_cols = st.columns(5)
            with summary_cols[0]:
                st.metric("Total Processed", f"{total_records:,}")
            with summary_cols[1]:
                st.metric("Successful", f"{total_success:,}")
            with summary_cols[2]:
                st.metric("Errors", f"{total_errors:,}")
            with summary_cols[3]:
                success_rate = (total_success / total_records * 100) if total_records > 0 else 0
                st.metric("Success Rate", f"{success_rate:.1f}%")
            with summary_cols[4]:
                st.metric("Execution Time", f"{elapsed_time:.1f}s")
            
            # Download JSON results
            st.subheader("📥 Download Results")
            
            if output_json_path and os.path.exists(output_json_path):
                with open(output_json_path, 'r', encoding='utf-8') as f:
                    results_json = f.read()
                
                # Store in session state for persistence
                st.session_state.pipeline_results = results_json
                
                # Parse for preview
                try:
                    results_data = json.loads(results_json)
                    
                    # Show results preview
                    with st.expander("👀 Results Preview", expanded=False):
                        st.json(results_data.get("summary", {}))
                        
                        # Show first 5 results
                        results_list = results_data.get("results", [])
                        if results_list:
                            st.write(f"**First 5 of {len(results_list)} results:**")
                            for i, result in enumerate(results_list[:5]):
                                with st.container():
                                    st.markdown(f"**{i+1}. Billboard ID:** `{result.get('billboard_id', 'N/A')}`")
                                    st.markdown(f"   - Status: `{result.get('status', 'N/A')}`")
                                    if result.get('status') == 'success' and result.get('profile'):
                                        profile = result.get('profile', {})
                                        st.markdown(f"   - Title: {profile.get('title', 'N/A')[:100]}...")
                except Exception as e:
                    st.warning(f"Could not parse results for preview: {e}")
                
                # Download button
                download_col1, download_col2 = st.columns([1, 3])
                with download_col1:
                    st.download_button(
                        label="📥 Download Results JSON",
                        data=results_json,
                        file_name=f"billboard_api_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        type="primary",
                        use_container_width=True
                    )
                
                # Cleanup temp files
                try:
                    os.unlink(input_csv_path)
                except Exception:
                    pass
                    
            else:
                st.warning("⚠️ Results file not found. Check logs for errors.")
            
            st.info("💡 **Next Steps**: Check the Prefect Dashboard for detailed flow run information, or download the JSON results above.")
            
        except Exception as e:
            st.session_state.pipeline_running = False
            elapsed_time = time.time() - start_time
            
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ ERROR: {str(e)}")
            log_placeholder.code("\n".join(logs[-50:]))
            
            st.error(f"❌ Pipeline execution failed: {e}")
            
            # Note: Don't clear flow state on error - the flow might still be running in Prefect
            # even if our subprocess connection was lost
            if st.session_state.flow_run_id:
                st.warning(f"""
                ⚠️ **Note**: The flow may still be running in Prefect even though an error occurred here.
                Check [Prefect Cloud](https://app.prefect.cloud/flow-runs/{st.session_state.flow_run_id}) 
                for the actual status. Refresh this page to see if the flow is still active.
                """)
            
            import traceback
            with st.expander("🔍 Error Details", expanded=True):
                st.code(traceback.format_exc())

else:
    # No file uploaded yet - show instructions
    st.info("👆 Upload a CSV file to get started with billboard data ingestion.")
    
    # Sample data format
    with st.expander("📝 Sample Data Format", expanded=False):
        sample_data = pd.DataFrame({
            "billboard_id": ["BB001", "BB002", "BB003"],
            "lat": [19.0760, 19.0896, 19.0544],
            "lon": [72.8777, 72.8656, 72.8403],
            "width_ft": [20, 30, 40],
            "height_ft": [10, 15, 20],
            "lighting_type": ["frontlit", "backlit", "digital"],
            "format_type": ["static", "static", "digital"],
            "quantity": [1, 1, 2],
            "frequency_per_minute": [0, 0, 6],
            "locality": ["Bandra", "Andheri", "Juhu"],
            "image_urls": ["https://example.com/bb001.jpg", "https://example.com/bb002.jpg", "https://example.com/bb003.jpg"],
            "area": [200, 450, 800],
            "base_rate_per_month": [50000, 75000, 120000],
            "base_rate_per_unit": [2500, 2500, 3000],
            "card_rate_per_month": [60000, 90000, 150000],
            "card_rate_per_unit": [3000, 3000, 3750],
            "city": ["Mumbai", "Mumbai", "Mumbai"],
            "district": ["Mumbai Suburban", "Mumbai Suburban", "Mumbai Suburban"],
            "location": ["Bandra West Main Road", "Andheri East Highway", "Juhu Beach Road"],
        })
        st.dataframe(sample_data, use_container_width=True)
        
        # Download sample
        csv_sample = sample_data.to_csv(index=False)
        st.download_button(
            label="📥 Download Sample CSV",
            data=csv_sample,
            file_name="sample_billboard_data.csv",
            mime="text/csv"
        )

