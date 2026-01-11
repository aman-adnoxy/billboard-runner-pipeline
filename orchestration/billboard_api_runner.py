"""
Billboard API Pipeline Runner

This script is designed to be run as a subprocess from the Streamlit UI,
enabling real-time log streaming and progress updates.

Usage:
    python billboard_api_runner.py <input_csv_path> <output_json_path>
"""

import os
import sys
import json
import math
import time
from datetime import datetime
from typing import List, Dict

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_environment

# Load environment variables
load_environment()

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
import pandas as pd

# Configuration
BATCH_SIZE = 10
RETRY_COUNT = 3
RETRY_DELAY = 60


def log_step(message: str, level: str = "INFO"):
    """Emit structured log for UI parsing."""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {level} >>> {message}", flush=True)


def log_progress(batch_num: int, total_batches: int, processed: int, total: int, status: str = "processing"):
    """Emit progress update for UI parsing."""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] PROGRESS >>> Batch {batch_num}/{total_batches} | processed: {processed} | remaining: {total - processed} | status: {status}", flush=True)


def log_batch_result(batch_num: int, success_count: int, error_count: int, errors: List[str] = None):
    """Emit batch result for UI parsing."""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] BATCH >>> Batch {batch_num} complete | success: {success_count} | errors: {error_count}", flush=True)
    if errors:
        for err in errors[:3]:  # Show first 3 errors
            print(f"[{timestamp}] ERROR >>> {err}", flush=True)


@task(retries=RETRY_COUNT, retry_delay_seconds=RETRY_DELAY)
def process_batch_task(batch: List[Dict], batch_num: int) -> List[Dict]:
    """Process a single batch through the Billboard API."""
    from src.billboard_api_client import call_billboard_api
    
    log_step(f"Processing batch {batch_num} with {len(batch)} records...")
    
    try:
        results = call_billboard_api(batch)
        
        success_count = sum(1 for r in results if r.get("status") == "success")
        error_count = len(results) - success_count
        errors = [r.get("error", "Unknown error") for r in results if r.get("status") != "success"]
        
        log_batch_result(batch_num, success_count, error_count, errors)
        
        return results
    except Exception as e:
        log_step(f"Batch {batch_num} failed: {str(e)}", level="ERROR")
        raise


@task
def persist_batch_results(results: List[Dict], batch_num: int) -> int:
    """Persist successful results to MongoDB."""
    from src.database import upsert_billboard_profiles
    
    successes = [r for r in results if r.get("status") == "success"]
    
    if successes:
        upsert_billboard_profiles(successes)
        log_step(f"Batch {batch_num}: Persisted {len(successes)} profiles to MongoDB")
    
    return len(successes)


@flow(name="billboard-api-pipeline-v2", log_prints=True)
def run_billboard_api_pipeline_v2(input_csv_path: str, output_json_path: str):
    """
    Main pipeline flow that processes billboard data through the API.
    
    Args:
        input_csv_path: Path to the input CSV file
        output_json_path: Path where the results JSON will be saved
    """
    log_step("=" * 60)
    log_step("🚀 Billboard API Pipeline Starting")
    log_step("=" * 60)
    
    # Emit flow run ID for UI reconnection
    try:
        ctx = get_run_context()
        flow_run_id = str(ctx.flow_run.id)
        log_step(f"📋 Flow Run ID: {flow_run_id}")
        # Special format for UI parsing
        print(f"FLOW_RUN_ID >>> {flow_run_id}", flush=True)
    except Exception as e:
        log_step(f"⚠️ Could not get flow run ID: {e}")
    
    # Load input data
    log_step(f"📂 Loading input CSV: {input_csv_path}")
    try:
        df = pd.read_csv(input_csv_path)
        log_step(f"✅ Loaded {len(df)} records from CSV")
    except Exception as e:
        log_step(f"❌ Failed to load CSV: {e}", level="ERROR")
        raise
    
    # Convert to records
    records = df.to_dict(orient="records")
    total_records = len(records)
    
    # Calculate batches
    total_batches = math.ceil(total_records / BATCH_SIZE)
    log_step(f"📦 Total batches: {total_batches} (batch size: {BATCH_SIZE})")
    
    # Initialize results collection
    all_results = []
    total_processed = 0
    total_success = 0
    total_errors = 0
    
    start_time = time.time()
    
    # Process batches
    for i in range(total_batches):
        batch_num = i + 1
        start_idx = i * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, total_records)
        batch = records[start_idx:end_idx]
        
        log_progress(batch_num, total_batches, total_processed, total_records, "processing")
        
        try:
            # Process batch
            results = process_batch_task(batch, batch_num)
            
            # Persist to database
            persisted = persist_batch_results(results, batch_num)
            
            # Collect results
            all_results.extend(results)
            
            # Update counters
            batch_success = sum(1 for r in results if r.get("status") == "success")
            batch_errors = len(results) - batch_success
            total_success += batch_success
            total_errors += batch_errors
            total_processed += len(batch)
            
            log_progress(batch_num, total_batches, total_processed, total_records, "completed")
            
        except Exception as e:
            log_step(f"❌ Batch {batch_num} failed after retries: {e}", level="ERROR")
            # Mark all records in batch as failed
            for record in batch:
                all_results.append({
                    "billboard_id": record.get("billboard_id", "unknown"),
                    "status": "error",
                    "error": str(e),
                    "input": record
                })
            total_errors += len(batch)
            total_processed += len(batch)
    
    elapsed_time = time.time() - start_time
    
    # Build final output
    output_data = {
        "pipeline_run": {
            "timestamp": datetime.utcnow().isoformat(),
            "input_file": input_csv_path,
            "total_records": total_records,
            "total_batches": total_batches,
            "batch_size": BATCH_SIZE,
            "execution_time_seconds": round(elapsed_time, 2),
        },
        "summary": {
            "total_processed": total_processed,
            "total_success": total_success,
            "total_errors": total_errors,
            "success_rate": round((total_success / total_records) * 100, 2) if total_records > 0 else 0
        },
        "results": all_results
    }
    
    # Save JSON output
    log_step(f"💾 Saving results to: {output_json_path}")
    try:
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
        log_step(f"✅ Results saved successfully")
    except Exception as e:
        log_step(f"❌ Failed to save results: {e}", level="ERROR")
        raise
    
    # Final summary
    log_step("=" * 60)
    log_step("📋 PIPELINE COMPLETE")
    log_step(f"   Total Records: {total_records}")
    log_step(f"   Successful: {total_success}")
    log_step(f"   Errors: {total_errors}")
    log_step(f"   Success Rate: {output_data['summary']['success_rate']}%")
    log_step(f"   Execution Time: {elapsed_time:.2f}s")
    log_step(f"   Output File: {output_json_path}")
    log_step("=" * 60)
    
    # Emit final status for UI parsing
    print(f"PIPELINE_COMPLETE >>> output_file: {output_json_path}", flush=True)
    
    return output_data


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python billboard_api_runner.py <input_csv_path> <output_json_path>")
        sys.exit(1)
    
    input_csv = sys.argv[1]
    output_json = sys.argv[2]
    
    if not os.path.exists(input_csv):
        print(f"ERROR: Input file not found: {input_csv}")
        sys.exit(1)
    
    run_billboard_api_pipeline_v2(input_csv, output_json)
