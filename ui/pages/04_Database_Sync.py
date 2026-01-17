import streamlit as st
import pandas as pd
import time
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import load_environment

# Ensure environment is loaded
load_environment()

# --- Page Configuration ---
st.set_page_config(
    page_title="Database Sync",
    page_icon="🔗",
    layout="wide"
)

# --- Header Section ---
st.title("🔗 Database Sync")
st.markdown("""
This step syncs **MongoDB** billboard data with **Supabase** listings to add:
- **Organization ID**: Links each billboard to its organization
- **Market ID**: The marketplace listing ID

The sync automatically matches MongoDB documents with Supabase listings using `source_iid` and `_id` fields.
""")

st.divider()

# --- Database Connection Status ---
st.header("1️⃣ Database Connections")

def check_mongo_connection():
    """Check if MongoDB is accessible."""
    try:
        from src.database import collection, _client
        # Ping to verify connection
        _client.admin.command('ping')
        count = collection.count_documents({})
        return True, count
    except Exception as e:
        return False, str(e)

def check_supabase_connection():
    """Check if Supabase is accessible."""
    try:
        from src.database import get_supabase_client
        client = get_supabase_client()
        # Try a simple query
        result = client.table("listings").select("id").limit(1).execute()
        return True, "Connected"
    except Exception as e:
        return False, str(e)

# Connection status display
with st.container():
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🍃 MongoDB")
        mongo_ok, mongo_info = check_mongo_connection()
        if mongo_ok:
            st.success(f"✅ Connected - **{mongo_info:,}** documents in collection")
        else:
            st.error(f"❌ Connection failed: {mongo_info}")
            st.stop()
    
    with col2:
        st.subheader("⚡ Supabase")
        supa_ok, supa_info = check_supabase_connection()
        if supa_ok:
            st.success(f"✅ {supa_info}")
        else:
            st.error(f"❌ Connection failed: {supa_info}")
            st.stop()

st.divider()

# --- Preview Section ---
st.header("2️⃣ Data Preview")

@st.cache_data(ttl=60)  # Cache for 1 minute
def fetch_mongo_documents(limit=100):
    """Fetch sample MongoDB documents."""
    try:
        from src.database import collection
        docs = list(collection.find({}, {"_id": 1, "billboard_id": 1, "profile": 1, "organization_id": 1, "market_id": 1}).limit(limit))
        return docs
    except Exception as e:
        st.error(f"Failed to fetch MongoDB documents: {e}")
        return []

@st.cache_data(ttl=60)  # Cache for 1 minute
def fetch_listings_count():
    """Get count of listings from Supabase."""
    try:
        from src.database import get_supabase_client
        client = get_supabase_client()
        # Get count by fetching with head=True or counting rows
        result = client.table("listings").select("id", count="exact").limit(1).execute()
        return result.count if result.count else 0
    except Exception as e:
        return 0

mongo_docs = fetch_mongo_documents()
listings_count = fetch_listings_count()

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("MongoDB Documents", len(mongo_docs) if mongo_docs else 0)
with col2:
    st.metric("Supabase Listings", listings_count)
with col3:
    # Count documents already synced
    synced_count = sum(1 for doc in mongo_docs if doc.get('organization_id') and doc.get('market_id'))
    st.metric("Already Synced", synced_count)

# Preview MongoDB data
with st.expander("👀 MongoDB Documents Preview", expanded=False):
    if mongo_docs:
        preview_data = []
        for doc in mongo_docs[:20]:
            preview_data.append({
                "_id": str(doc.get("_id", "")),
                "has_profile": "profile" in doc,
                "organization_id": doc.get("organization_id", "Not Set"),
                "market_id": doc.get("market_id", "Not Set"),
            })
        st.dataframe(pd.DataFrame(preview_data), use_container_width=True)
    else:
        st.info("No documents to preview")

st.divider()

# --- Sync Execution ---
st.header("3️⃣ Execute Sync")

st.info("""
**Sync Process:**
1. Fetches all listings from Supabase (id, source_iid, organization_id)
2. Matches each listing's source_iid with MongoDB document _id
3. Updates MongoDB with organization_id and market_id (from listings.id)
""")

# Session state for sync tracking
if 'sync_running' not in st.session_state:
    st.session_state.sync_running = False
if 'sync_results' not in st.session_state:
    st.session_state.sync_results = None

def run_database_sync():
    """
    Execute the database sync process.
    Fetches organization_id and market_id from Supabase listings and syncs to MongoDB.
    
    Returns:
        dict: Summary of sync results
    """
    from src.database import collection, get_supabase_client
    
    results = {
        "total_listings": 0,
        "matched": 0,
        "updated": 0,
        "not_found_in_mongo": 0,
        "errors": [],
        "not_found_ids": []
    }
    
    try:
        # Get Supabase client
        supabase = get_supabase_client()
        
        # Fetch all listings from Supabase with required fields
        # We need: id (for market_id), source_iid (to match with MongoDB _id), organization_id
        all_listings = []
        page_size = 1000
        offset = 0
        
        while True:
            response = supabase.table("listings").select("id, source_iid, organization_id").range(offset, offset + page_size - 1).execute()
            
            if not response.data:
                break
            
            all_listings.extend(response.data)
            
            if len(response.data) < page_size:
                break
            
            offset += page_size
        
        results["total_listings"] = len(all_listings)
        
        if not all_listings:
            return results
        
        # Update MongoDB documents
        for listing in all_listings:
            source_iid = listing.get("source_iid")
            market_id = listing.get("id")
            organization_id = listing.get("organization_id")
            
            # Skip if source_iid is missing
            if not source_iid:
                continue
            
            # Check if MongoDB document exists
            mongo_doc = collection.find_one({"_id": source_iid})
            
            if mongo_doc:
                results["matched"] += 1
                try:
                    # Update with both organization_id and market_id
                    update_fields = {
                        "market_id": market_id,
                        "synced_at": datetime.utcnow()
                    }
                    
                    # Only add organization_id if it exists
                    if organization_id:
                        update_fields["organization_id"] = organization_id
                    
                    collection.update_one(
                        {"_id": source_iid},
                        {"$set": update_fields}
                    )
                    results["updated"] += 1
                except Exception as e:
                    results["errors"].append(f"Update {source_iid}: {str(e)}")
            else:
                results["not_found_in_mongo"] += 1
                results["not_found_ids"].append(source_iid)
        
        return results
        
    except Exception as e:
        results["errors"].append(f"Critical error: {str(e)}")
        return results

# Sync options
with st.expander("⚙️ Sync Information", expanded=False):
    st.markdown("""
    - Syncs **all listings** from Supabase to MongoDB
    - Matches using `source_iid` (Supabase) ↔ `_id` (MongoDB)
    - Updates both `organization_id` and `market_id` fields
    - Adds/updates `synced_at` timestamp
    """)

# Run button
col1, col2 = st.columns([1, 3])
with col1:
    run_button = st.button(
        "🔄 Run Database Sync",
        type="primary",
        use_container_width=True,
        disabled=st.session_state.sync_running
    )

if run_button:
    st.session_state.sync_running = True
    
    # Progress UI
    progress_bar = st.progress(0)
    status_container = st.status("🔄 Starting sync process...", expanded=True)
    
    with status_container:
        start_time = time.time()
        
        # Phase 1: Fetching data
        st.write("📥 Fetching listings from Supabase...")
        progress_bar.progress(20)
        
        # Phase 2: Matching
        st.write("🔍 Matching with MongoDB documents...")
        progress_bar.progress(40)
        
        # Phase 3: Updating
        st.write("📝 Updating MongoDB documents...")
        progress_bar.progress(60)
        
        # Run the sync
        results = run_database_sync()
        
        progress_bar.progress(100)
        elapsed_time = time.time() - start_time
        
        st.write(f"✅ Sync completed in **{elapsed_time:.1f}s**")
    
    st.session_state.sync_running = False
    st.session_state.sync_results = results
    
    # Display results
    st.subheader("📊 Sync Results")
    
    result_cols = st.columns(4)
    with result_cols[0]:
        st.metric("Total Listings", results["total_listings"])
    with result_cols[1]:
        st.metric("Matched", results["matched"])
    with result_cols[2]:
        st.metric("Updated", results["updated"])
    with result_cols[3]:
        st.metric("Not in MongoDB", results["not_found_in_mongo"])
    
    # Success/Warning messages
    if results["updated"] > 0:
        st.success(f"✅ Successfully updated **{results['updated']}** documents with organization_id and market_id")
    
    if results["not_found_in_mongo"] > 0:
        st.warning(f"⚠️ **{results['not_found_in_mongo']}** listings have source_iid values not found in MongoDB")
        
        with st.expander("📋 Unmatched Source IDs", expanded=False):
            if results["not_found_ids"]:
                # Show first 50
                st.write("First 50 unmatched IDs:")
                st.code("\n".join(results["not_found_ids"][:50]))
                if len(results["not_found_ids"]) > 50:
                    st.caption(f"...and {len(results['not_found_ids']) - 50} more")
    
    if results["errors"]:
        st.error(f"❌ **{len(results['errors'])}** errors occurred during sync")
        with st.expander("🔍 Error Details", expanded=True):
            for error in results["errors"]:
                st.markdown(f"- {error}")
    
    # Clear cache to reflect updates
    fetch_mongo_documents.clear()

# Display previous results if available
elif st.session_state.sync_results:
    results = st.session_state.sync_results
    st.subheader("📊 Previous Sync Results")
    
    result_cols = st.columns(4)
    with result_cols[0]:
        st.metric("Total Listings", results["total_listings"])
    with result_cols[1]:
        st.metric("Matched", results["matched"])
    with result_cols[2]:
        st.metric("Updated", results["updated"])
    with result_cols[3]:
        st.metric("Not in MongoDB", results["not_found_in_mongo"])
    
    if st.button("🗑️ Clear Results"):
        st.session_state.sync_results = None
        st.rerun()

st.divider()

# --- Verification Section ---
st.header("4️⃣ Verification")

if st.button("🔍 Verify Synced Data"):
    from src.database import collection
    
    # Fetch sample of synced documents
    synced_docs = list(collection.find(
        {"organization_id": {"$exists": True}, "market_id": {"$exists": True}},
        {"_id": 1, "organization_id": 1, "market_id": 1, "synced_at": 1}
    ).limit(10))
    
    if synced_docs:
        st.success(f"✅ Found **{len(synced_docs)}** synced documents (showing first 10)")
        
        verify_data = []
        for doc in synced_docs:
            verify_data.append({
                "Billboard ID": str(doc.get("_id", "")),
                "Organization ID": doc.get("organization_id", ""),
                "Market ID": doc.get("market_id", ""),
                "Synced At": doc.get("synced_at", "N/A"),
            })
        st.dataframe(pd.DataFrame(verify_data), use_container_width=True)
    else:
        st.info("No synced documents found. Run the sync process first.")

# --- Footer ---
st.divider()
st.markdown("""
<div style="text-align: center; color: #888; font-size: 0.9em;">
    💡 <strong>Tip:</strong> After syncing, refresh the preview section to see updated data.
</div>
""", unsafe_allow_html=True)
