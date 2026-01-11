"""
Prefect Utilities

Provides functions to interact with Prefect Cloud for checking flow run status,
enabling reconnection to running flows after Streamlit page refresh.
"""

import os
import json
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path

# State file path - persists across page refreshes
BASE_DIR = Path(__file__).parent.parent
FLOW_STATE_FILE = BASE_DIR / "config" / "active_flow_run.json"


def save_flow_run_state(flow_run_id: str, input_csv_path: str, output_json_path: str, 
                         total_records: int, started_at: str = None) -> None:
    """
    Save the current flow run state to a persistent file.
    
    Args:
        flow_run_id: The Prefect flow run ID
        input_csv_path: Path to the input CSV file
        output_json_path: Path where results will be saved
        total_records: Total number of records being processed
        started_at: Timestamp when the flow started
    """
    state = {
        "flow_run_id": flow_run_id,
        "input_csv_path": input_csv_path,
        "output_json_path": output_json_path,
        "total_records": total_records,
        "started_at": started_at or datetime.utcnow().isoformat(),
        "last_checked": datetime.utcnow().isoformat()
    }
    
    # Ensure config directory exists
    FLOW_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    with open(FLOW_STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)


def load_flow_run_state() -> Optional[Dict[str, Any]]:
    """
    Load the saved flow run state if it exists.
    
    Returns:
        Dictionary with flow run state or None if no state exists
    """
    if not FLOW_STATE_FILE.exists():
        return None
    
    try:
        with open(FLOW_STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def clear_flow_run_state() -> None:
    """Remove the saved flow run state file."""
    if FLOW_STATE_FILE.exists():
        FLOW_STATE_FILE.unlink()


async def get_flow_run_status_async(flow_run_id: str) -> Optional[Dict[str, Any]]:
    """
    Get the status of a flow run from Prefect Cloud (async version).
    
    Args:
        flow_run_id: The Prefect flow run ID
        
    Returns:
        Dictionary with flow run info or None if not found
    """
    try:
        from prefect.client.orchestration import get_client
        
        async with get_client() as client:
            flow_run = await client.read_flow_run(flow_run_id)
            
            return {
                "flow_run_id": str(flow_run.id),
                "name": flow_run.name,
                "state_name": flow_run.state_name,
                "state_type": flow_run.state_type.value if flow_run.state_type else None,
                "is_running": flow_run.state_name in ["Running", "Pending", "Scheduled"],
                "is_completed": flow_run.state_name == "Completed",
                "is_failed": flow_run.state_name in ["Failed", "Crashed", "Cancelled"],
                "start_time": flow_run.start_time.isoformat() if flow_run.start_time else None,
                "end_time": flow_run.end_time.isoformat() if flow_run.end_time else None,
                "total_task_run_count": flow_run.total_task_run_count,
            }
    except Exception as e:
        print(f"Error fetching flow run status: {e}")
        return None


def get_flow_run_status(flow_run_id: str) -> Optional[Dict[str, Any]]:
    """
    Get the status of a flow run from Prefect Cloud (sync wrapper).
    
    Args:
        flow_run_id: The Prefect flow run ID
        
    Returns:
        Dictionary with flow run info or None if not found
    """
    try:
        # Try to get the existing event loop
        try:
            loop = asyncio.get_running_loop()
            # If we're in an async context, we need to use a different approach
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, get_flow_run_status_async(flow_run_id))
                return future.result(timeout=30)
        except RuntimeError:
            # No running event loop, safe to use asyncio.run
            return asyncio.run(get_flow_run_status_async(flow_run_id))
    except Exception as e:
        print(f"Error in sync wrapper: {e}")
        return None


async def get_task_runs_for_flow_async(flow_run_id: str) -> list:
    """
    Get all task runs for a flow run (async version).
    
    Args:
        flow_run_id: The Prefect flow run ID
        
    Returns:
        List of task run information
    """
    try:
        from prefect.client.orchestration import get_client
        from uuid import UUID
        
        async with get_client() as client:
            task_runs = await client.read_task_runs(
                flow_run_filter={"id": {"any_": [flow_run_id]}}
            )
            
            return [
                {
                    "task_run_id": str(tr.id),
                    "name": tr.name,
                    "state_name": tr.state_name,
                    "is_completed": tr.state_name == "Completed",
                    "start_time": tr.start_time.isoformat() if tr.start_time else None,
                    "end_time": tr.end_time.isoformat() if tr.end_time else None,
                }
                for tr in task_runs
            ]
    except Exception as e:
        print(f"Error fetching task runs: {e}")
        return []


def get_task_runs_for_flow(flow_run_id: str) -> list:
    """
    Get all task runs for a flow run (sync wrapper).
    
    Args:
        flow_run_id: The Prefect flow run ID
        
    Returns:
        List of task run information
    """
    try:
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, get_task_runs_for_flow_async(flow_run_id))
                return future.result(timeout=30)
        except RuntimeError:
            return asyncio.run(get_task_runs_for_flow_async(flow_run_id))
    except Exception as e:
        print(f"Error in sync wrapper: {e}")
        return []


def check_and_get_running_flow() -> Optional[Dict[str, Any]]:
    """
    Check if there's a running flow and return its status.
    
    This is the main function to call from Streamlit on page load.
    
    Returns:
        Dictionary with flow state and status, or None if no running flow
    """
    saved_state = load_flow_run_state()
    
    if not saved_state:
        return None
    
    flow_run_id = saved_state.get("flow_run_id")
    if not flow_run_id:
        clear_flow_run_state()
        return None
    
    # Get current status from Prefect
    status = get_flow_run_status(flow_run_id)
    
    if not status:
        # Couldn't reach Prefect - keep the state but indicate unknown status
        return {
            **saved_state,
            "status": "unknown",
            "message": "Could not connect to Prefect to check status"
        }
    
    # If the flow is no longer running, we might want to clean up
    if status.get("is_completed") or status.get("is_failed"):
        # Flow finished - return final status but don't auto-clear
        # Let the UI handle cleanup after displaying final status
        return {
            **saved_state,
            "status": status,
            "is_active": False
        }
    
    # Flow is still running
    if status.get("is_running"):
        # Update last checked time
        saved_state["last_checked"] = datetime.utcnow().isoformat()
        save_flow_run_state(**{k: v for k, v in saved_state.items() if k != "last_checked"})
        
        return {
            **saved_state,
            "status": status,
            "is_active": True
        }
    
    return {
        **saved_state,
        "status": status,
        "is_active": False
    }


def get_flow_run_progress(flow_run_id: str) -> Dict[str, Any]:
    """
    Get detailed progress information for a running flow.
    
    Args:
        flow_run_id: The Prefect flow run ID
        
    Returns:
        Dictionary with progress information
    """
    status = get_flow_run_status(flow_run_id)
    task_runs = get_task_runs_for_flow(flow_run_id)
    
    if not status:
        return {"error": "Could not fetch flow status"}
    
    completed_batches = sum(
        1 for tr in task_runs 
        if tr.get("name", "").startswith("process_batch_task") and tr.get("is_completed")
    )
    
    total_batches = sum(
        1 for tr in task_runs 
        if tr.get("name", "").startswith("process_batch_task")
    )
    
    return {
        "flow_status": status,
        "task_runs": task_runs,
        "completed_batches": completed_batches,
        "total_batches_started": total_batches,
        "is_running": status.get("is_running", False),
        "is_completed": status.get("is_completed", False),
        "is_failed": status.get("is_failed", False)
    }
