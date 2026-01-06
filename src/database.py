import os
import sys
from supabase import create_client, Client, ClientOptions
from src.config import load_environment
from datetime import datetime
import os
from pymongo import MongoClient
from datetime import datetime

# ============================
# ENV
# ============================

# Ensure env is loaded when this module is imported if not already
load_environment()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI not set")

if not MONGO_DB:
    raise RuntimeError("MONGO_DB not set")

if not MONGO_COLLECTION:
    raise RuntimeError("MONGO_COLLECTION not set")

# ============================
# CLIENT (initialized once)
# ============================

_client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000,
    connectTimeoutMS=5000,
)

_db = _client[MONGO_DB]
collection = _db[MONGO_COLLECTION]

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def get_supabase_client() -> Client:
    """
    Returns an initialized Supabase client.
    Exits if credentials are missing.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("CRITICAL ERROR: Supabase credentials not found in environment.")
        sys.exit(1)
    
    opts = ClientOptions(postgrest_client_timeout=600, storage_client_timeout=600)
    client: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options=opts)
    return client

def upsert_billboard_profiles(results: list):

    for r in results:
        profile = r["profile"]
        billboard_id = r["billboard_id"]

        collection.update_one(
            {"_id": billboard_id},
            {
                "$set": {
                    "profile": profile,
                    "computed_at": datetime.utcnow(),
                }
            },
            upsert=True
        )
