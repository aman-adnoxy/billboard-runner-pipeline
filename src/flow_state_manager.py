"""
Flow State Manager

Manages checkpoints and state persistence for resumable flows.
Allows flows to save their progress and resume from the last checkpoint.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

# State directory
BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "config" / "flow_states"


def ensure_state_dir():
    """Ensure the state directory exists."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def save_checkpoint(flow_run_id: str, checkpoint_data: Dict[str, Any]) -> None:
    """
    Save a checkpoint for a flow run.
    
    Args:
        flow_run_id: The Prefect flow run ID
        checkpoint_data: Dictionary containing checkpoint information
    """
    ensure_state_dir()
    
    checkpoint_file = STATE_DIR / f"{flow_run_id}_checkpoint.json"
    
    checkpoint = {
        "flow_run_id": flow_run_id,
        "timestamp": datetime.utcnow().isoformat(),
        "data": checkpoint_data
    }
    
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f, indent=2)


def load_checkpoint(flow_run_id: str) -> Optional[Dict[str, Any]]:
    """
    Load a checkpoint for a flow run.
    
    Args:
        flow_run_id: The Prefect flow run ID
        
    Returns:
        Dictionary with checkpoint data or None if no checkpoint exists
    """
    checkpoint_file = STATE_DIR / f"{flow_run_id}_checkpoint.json"
    
    if not checkpoint_file.exists():
        return None
    
    try:
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def clear_checkpoint(flow_run_id: str) -> None:
    """
    Clear a checkpoint for a flow run.
    
    Args:
        flow_run_id: The Prefect flow run ID
    """
    checkpoint_file = STATE_DIR / f"{flow_run_id}_checkpoint.json"
    
    if checkpoint_file.exists():
        checkpoint_file.unlink()


def save_batch_results(flow_run_id: str, batch_num: int, results: list) -> None:
    """
    Save results for a specific batch.
    
    Args:
        flow_run_id: The Prefect flow run ID
        batch_num: The batch number
        results: List of results for this batch
    """
    ensure_state_dir()
    
    batch_file = STATE_DIR / f"{flow_run_id}_batch_{batch_num}.json"
    
    with open(batch_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, default=str)


def load_all_batch_results(flow_run_id: str) -> Dict[int, list]:
    """
    Load all saved batch results for a flow run.
    
    Args:
        flow_run_id: The Prefect flow run ID
        
    Returns:
        Dictionary mapping batch numbers to their results
    """
    ensure_state_dir()
    
    batch_results = {}
    
    # Find all batch files for this flow run
    for batch_file in STATE_DIR.glob(f"{flow_run_id}_batch_*.json"):
        try:
            # Extract batch number from filename
            batch_num = int(batch_file.stem.split('_')[-1])
            
            with open(batch_file, 'r', encoding='utf-8') as f:
                batch_results[batch_num] = json.load(f)
        except (ValueError, json.JSONDecodeError, IOError):
            continue
    
    return batch_results


def clear_all_batch_results(flow_run_id: str) -> None:
    """
    Clear all batch results for a flow run.
    
    Args:
        flow_run_id: The Prefect flow run ID
    """
    ensure_state_dir()
    
    for batch_file in STATE_DIR.glob(f"{flow_run_id}_batch_*.json"):
        batch_file.unlink()


def cleanup_flow_state(flow_run_id: str) -> None:
    """
    Clean up all state files for a flow run.
    
    Args:
        flow_run_id: The Prefect flow run ID
    """
    clear_checkpoint(flow_run_id)
    clear_all_batch_results(flow_run_id)
