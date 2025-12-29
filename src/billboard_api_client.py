import os
import requests
from typing import List, Dict
from datetime import datetime

BILLBOARD_API_URL = os.getenv("BILLBOARD_API_URL")
HF_TOKEN = os.getenv("HF_TOKEN")

if not BILLBOARD_API_URL:
    raise RuntimeError("BILLBOARD_API_URL not set")

HEADERS = {
    "Authorization": f"Bearer {HF_TOKEN}",
    "Content-Type": "application/json"
}


def call_billboard_api(billboards: List[Dict]) -> List[Dict]:

    payload = {
        "batch_id": f"prefect-{datetime.utcnow().isoformat()}",
        "billboards": billboards
    }

    resp = requests.post(
        f"{BILLBOARD_API_URL}/v1/billboards/profile/batch",
        json=payload,
        headers=HEADERS,
        timeout=900
    )

    resp.raise_for_status()

    data = resp.json()
    return data["results"]
