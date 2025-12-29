from prefect import flow, task
from typing import List, Dict
import math

from src.billboard_api_client import call_billboard_api
from src.database import upsert_billboard_profiles

BATCH_SIZE = 25


@task(retries=3, retry_delay_seconds=60)
def process_batch(batch: List[Dict]) -> List[Dict]:
    return call_billboard_api(batch)


@task
def persist_results(results: List[Dict]):
    successes = [r for r in results if r["status"] == "success"]
    if successes:
        upsert_billboard_profiles(successes)


@flow(name="billboard-api-pipeline")
def run_billboard_api_pipeline(raw_records: List[Dict]):

    total = len(raw_records)
    batches = math.ceil(total / BATCH_SIZE)

    for i in range(batches):
        start = i * BATCH_SIZE
        end = start + BATCH_SIZE
        batch = raw_records[start:end]

        results = process_batch(batch)
        persist_results(results)
