import os
import sys
from supabase import create_client, Client, ClientOptions
from src.config import load_environment

# Ensure env is loaded when this module is imported if not already
load_environment()

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
