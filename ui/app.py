import streamlit as st
import os
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

st.set_page_config(
    page_title="Billboard Pipeline Home",
    page_icon="🚀",
    layout="wide"
)

st.title("🚀 Billboard Data Pipeline")

st.markdown("""
### Welcome to the Billboard Runner Pipeline

This application allows you to transform, process, and upload billboard inventory data.

#### **Workflow Steps:**

1.  **Data Transformation**  
    Go to the **Data Transformation** page to upload your raw CSV/Excel files, map columns, and run the initial cleaning pipeline.

2.  **Post Processing**  
    Go to the **Post Processing** page to review the transformed data, apply final business logic adjustments, and prepare it for the API.

3.  **Billboard API**  
    Go to the **Billboard API** page to push the final verified data to the production database via the Billboard API.

---
**👈 Select a step from the sidebar to get started.**
""")

st.sidebar.success("Select a page above.")