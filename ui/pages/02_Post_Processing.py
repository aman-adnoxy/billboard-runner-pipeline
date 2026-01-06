import streamlit as st
import pandas as pd
from src.post_processing import transform_dataframe
import io
import time

st.set_page_config(page_title="Post Processing", page_icon="📍", layout="wide")

st.title("📍 Data Transformation & Enhancement")
st.markdown("""
This step takes the cleaned output from the pipeline and enriches it with:
- **Google Maps Data**: Precise address components (Street, Area, City).
- **SEO Optimization**: Enhanced Titles and Descriptions.
- **Formatting**: Standardizes fields for database import.
""")

# File Uploader
uploaded_file = st.file_uploader("Upload Pipeline Output (Excel or CSV)", type=["xlsx", "csv"])


if uploaded_file:
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        st.info(f"Loaded {len(df)} records.")
        
        with st.expander("Preview Raw Data"):
            st.dataframe(df.head())

        # Initialize session state for process tracking
        if 'processing_active' not in st.session_state:
            st.session_state.processing_active = False

        # Button triggers processing OR we are already processing (from a reload)
        if st.button("Run Transformation 🚀", type="primary") or st.session_state.processing_active:
            st.session_state.processing_active = True # Ensure it stays true
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.text("Initializing transformation...")
            start_time = time.time()
            
            # Run transformation
            with st.spinner("Fetching addresses from Google Maps and enhancing data..."):
                result_df, missing_cats = transform_dataframe(df)

            if missing_cats:
                st.warning("⚠️ Some Category formats are missing from our database.")
                st.write("Please provide the Category ID (UUID) for the following:")
                
                from src.post_processing import save_category_mapping
                
                with st.form("missing_cats_form"):
                    new_mappings = {}
                    for cat in missing_cats:
                        new_mappings[cat] = st.text_input(f"UUID for '{cat}'", key=f"input_{cat}")
                    
                    submitted = st.form_submit_button("Save & Retry")
                    if submitted:
                        for cat, uuid in new_mappings.items():
                            if uuid and uuid.strip():
                                save_category_mapping(cat, uuid.strip())
                        st.success("Configuration updated! Retrying...")
                        # We stay in active processing state and reload
                        time.sleep(1) 
                        st.rerun()
                
                # Stop further processing until resolved
                st.stop()
            
            # If we get here, everything is successful
            st.session_state.processing_active = False # Reset for next time
            
            duration = time.time() - start_time
            progress_bar.progress(100)
            status_text.success(f"✅ Transformation complete in {duration:.2f}s!")

            st.subheader("Transformed Data Preview")
            st.dataframe(result_df)

            # Download Button
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                result_df.to_excel(writer, index=False)
            
            col1, col2 = st.columns([1, 4])
            with col1:
                st.download_button(
                    label="Download Final Excel",
                    data=buffer,
                    file_name="final_enriched_output.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
    except Exception as e:
        st.error(f"Error loading file: {e}")
        # Show traceback for debugging
        import traceback
        st.code(traceback.format_exc())
