# Adnoxy Data Pipeline 🚀

A robust, full-stack data ingestion pipeline designed to standardize, validate, and process billboard inventory data. The system combines a user-friendly **Streamlit** interface with powerful **Prefect** orchestration, using **Supabase** for secure cloud storage.

---

## ✨ Key Features

- **Smart Column Mapping**: Auto-detects column names using fuzzy matching and aliases.
- **Dynamic Schema**: Fully configurable via JSON.
- **Advanced Validation**: Checks for missing coordinates, images, and invalid dimensions.
- **Financial Logic**: Automatically calculates Base Rate and Card Rate logic.
- **Modular Architecture**: Clean separation of concerns (UI, Business Logic, Orchestration).

---

## 📂 Project Structure

```bash
check-git-main/
├── config/                  # Configuration files
│   └── standardized_fields.json  # Target schema definition
├── orchestration/           # Prefect Workflow definitions
│   └── flow.py              # Main execution pipeline
├── src/                     # Core Business Logic
│   ├── config.py            # Environment & variable handling
│   ├── database.py          # Supabase client wrapper
│   └── processing.py        # DataFrame transformation & cleaning
├── ui/                      # Frontend Application
│   └── app.py               # Streamlit Dashboard
├── .env                     # Secrets (Not committed)
└── requirements.txt         # Python Dependencies
```

---

## 🚀 Getting Started

### Prerequisites

- **Python 3.10+** installed.
- **Supabase** project (URL and Key).

### Installation

1. **Install Dependencies**
   You can use the provided npm script which auto-installs python requirements:
   ```bash
   npm install
   ```
   *Alternatively, using pure pip:*
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Setup**
   Create a `.env` file in the root directory:
   ```ini
   SUPABASE_URL="your_supabase_url"
   SUPABASE_KEY="your_supabase_anon_key"
   ```

---

## ▶️ Usage

Start the application using the consolidated development command:

```bash
npm run dev
```

This will launch the **Streamlit UI** at `http://localhost:8502`.

### Workflow
1. **Upload**: Drag & drop your raw CSV/Excel file.
2. **Map**: Use the UI to map your columns to the standard schema.
   - *Location*: Choose between separate Lat/Long columns or a single Coordinate column.
   - *Dimensions*: Choose between separate W/H columns or a single Dimension string.
3. **Execute**: Click "Save & Run". This uploads the file to Supabase and triggers the Prefect flow.
4. **Monitor**: Click the "View Run in Prefect Cloud" button to track progress.

---

## 🛠️ Configuration

The target schema is defined in **`config/standardized_fields.json`**. You can modify this file to change the mapping behavior without touching the code.

**Example Field:**
```json
"billboard_id": {
    "label": "Billboard ID *",
    "aliases": ["id", "code", "serial_no", "asset_id"] 
}
```
- **label**: What the user sees in the UI.
- **aliases**: List of column names to auto-detect and select by default.

---

## 🏗️ Architecture Details

- **UI Layer (`ui/`)**: Handles file upload, visual column mapping, and configuration generation. It assumes no heavy processing, merely dispatching instructions.
- **Core Layer (`src/`)**: 
    - `processing.py`: Pure functions for data cleaning (regex, type coercion, pandas logic).
    - `database.py`: Singleton pattern for database connections.
- **Orchestration Layer (`orchestration/`)**: Defines valid Prefect Tasks and Flows. It imports logic from `src/` to keep the flow file clean and readable.

---

## 📝 License

Proprietary/Internal Use.
