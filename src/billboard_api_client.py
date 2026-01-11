import os
import requests
from typing import List, Dict
from datetime import datetime


def call_billboard_api(billboards: List[Dict]) -> List[Dict]:
    """
    Call the Billboard Profile API with a batch of billboards.
    
    Args:
        billboards: List of billboard dictionaries with required fields:
            - billboard_id, lat, lon, width_ft, height_ft,
            - lighting_type, format_type, quantity, frequency_per_minute, locality
    
    Returns:
        List of result dictionaries from the API
    """
    # Get environment variables at runtime (lazy loading)
    billboard_api_url = os.getenv("BILLBOARD_API_URL")
    hf_token = os.getenv("HF_TOKEN")
    
    if not billboard_api_url:
        raise RuntimeError("BILLBOARD_API_URL not set in environment")
    
    # Headers - only Authorization, Content-Type is auto-set by requests when using json=
    headers = {
        "Authorization": f"Bearer {hf_token}"
    }
    
    # Preprocess billboards to ensure image_urls is always a list
    processed_billboards = []
    for billboard in billboards:
        processed = dict(billboard)  # Create a copy to avoid mutating original
        
        # Convert image_urls to list if it's a string
        if "image_urls" in processed:
            image_urls = processed["image_urls"]
            if isinstance(image_urls, str):
                # Handle comma-separated URLs or single URL
                if image_urls.strip():
                    # Split by comma and strip whitespace, filter empty strings
                    processed["image_urls"] = [url.strip() for url in image_urls.split(",") if url.strip()]
                else:
                    processed["image_urls"] = []
            elif image_urls is None:
                processed["image_urls"] = []
            # If already a list, keep as is
        
        processed_billboards.append(processed)
    
    # Build payload
    payload = {
        "batch_id": f"prefect-{datetime.utcnow().isoformat()}",
        "billboards": processed_billboards
    }
    
    # Build URL
    url = billboard_api_url.rstrip("/") + "/v1/billboards/profile/batch"
    
    # Make request
    resp = requests.post(
        url,
        json=payload,
        headers=headers,
        timeout=900
    )
    
    # Check for errors
    if resp.status_code != 200:
        raise RuntimeError(f"API returned {resp.status_code}: {resp.text}")
    
    # Parse response
    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"Response is not valid JSON: {resp.text[:500]}")
    
    if "results" not in data:
        raise RuntimeError(f"Missing 'results' in API response: {data}")
    
    return data["results"]

