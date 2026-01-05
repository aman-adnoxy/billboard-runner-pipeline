import os
import json
from dotenv import load_dotenv

# Path Definitions
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "config")
ENV_PATH = os.path.join(BASE_DIR, '.env')

# Load Environment
def load_environment():
    """Explicitly load .env file."""
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH)
    else:
        print(f"Warning: .env file not found at {ENV_PATH}")

# Explicitly load on import so constants below are populated
load_environment()

# Load Schema
def load_required_fields():
    """Load the standardized fields definition from JSON."""
    fields_path = os.path.join(DATA_DIR, "standardized_fields.json")
    if os.path.exists(fields_path):
        with open(fields_path, "r") as f:
            return json.load(f)
    else:
        # Fallback or Error
        print(f"Configuration file not found: {fields_path}")
        return {}

# Config Constants
BUCKET_INPUT = "input"
BUCKET_MAPPING = "mapping"
BUCKET_OUTPUT = "output"

# Exported Constants
REQUIRED_FIELDS = load_required_fields()
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
