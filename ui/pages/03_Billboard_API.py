import streamlit as st
from orchestration.billboard_api_flow import run_billboard_api_pipeline
import json

st.set_page_config(page_title="Billboard API Runner")

st.title("Billboard API Processing")

uploaded_file = st.file_uploader(
    "Upload Raw Billboard JSON",
    type=["json"]
)

if uploaded_file:
    raw_data = json.load(uploaded_file)

    st.write(f"Records detected: {len(raw_data)}")

    if st.button("Run Billboard API Pipeline"):
        with st.spinner("Running Prefect flow..."):
            run_billboard_api_pipeline(raw_data)

        st.success("Pipeline execution triggered. Check Prefect logs.")
