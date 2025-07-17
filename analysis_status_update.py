import uuid
import logging
import requests
import Config
from datetime import datetime
from typing import Dict, Any, Optional, Union
from data_extraction.Enums.analysis_status_enums import StepStatus
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def update_step_status(
    task_id: uuid.UUID,
    step_name: str,
    status: str,
    error: Optional[Union[str, list]] = None,
    count: Optional[int] = None,
    total: Optional[int] = None,
    timezone_str: str = 'Asia/Kolkata',
    info: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Updates the status of a specific analysis step for a given task via the API.

    Args:
        task_id: The UUID of the task.
        step_name: The name of the step to update (e.g., "files_embed").
        status: The new status for the step.
        error: Optional error log for the step.
        count: Optional current count for the step.
        total: Optional total count for the step.

    Returns:
        The updated task status data if successful, None otherwise.
    """
    endpoint = f"{Config.APPMOD_DOMAIN}/api/analysis-status/v1/tasks/{task_id}/steps/{step_name}"
    
    step_description_mapping = {
        StepStatus.FILES_SUMMARIZED: "Count of files summarized.",
        StepStatus.FILES_CHUNKED: "Count of files processed for chunking."
    }
    if error:
        if isinstance(error, list):
            error = '\n'.join([f"[{datetime.now(ZoneInfo(timezone_str)).isoformat()}] - {err}" for err in error])
        else:
            error = f"[{datetime.now(ZoneInfo(timezone_str)).isoformat()}] - {error}"
        
    if info:
        if isinstance(info, list):
            info = '\n'.join([f"[{datetime.now(ZoneInfo(timezone_str)).isoformat()}] - {err}" for err in info])
        else:
            info = f"[{datetime.now(ZoneInfo(timezone_str)).isoformat()}] - {info}"

    payload = {
        "status": status,
        "error": error,
        "count": count,
        "total": total,
        "description": step_description_mapping.get(step_name, ""),
        "info": info
    }
    # Remove None values from payload to avoid sending them if not provided
    payload = {k: v for k, v in payload.items() if v is not None}

    logger.info(f"Attempting to update step '{step_name}' for task {task_id}...")
    try:
        # REMOVED: async with httpx.AsyncClient() as client:
        # Changed to requests.patch
        response = requests.patch(endpoint, json=payload, timeout=30.0)
        response.raise_for_status()
        logger.info(f"Step '{step_name}' for task {task_id} updated successfully. Status: {response.status_code}")
        return response.json()
    except requests.exceptions.HTTPError as e: # FIX: Changed to HTTPError
        logger.error(f"HTTP error updating step '{step_name}' for task {task_id}: {e.response.status_code} - {e.response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error updating step '{step_name}' for task {task_id}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    return None

def update_task_fields(
    task_id: uuid.UUID,
    total_files: Optional[int] = None,
    overall_status: Optional[str] = None,
    overall_error_log: Optional[str] = None,
    overall_coverage: Optional[int] = None,
    total_files_processed: Optional[int] = None,
    total_files_skipped: Optional[int] = None,
    total_files_chunked: Optional[int] = None,
    total_files_summarized: Optional[int] = None,
    total_chunks: Optional[int] = None,
    total_supported_files: Optional[int] = None,
    total_unsupported_files: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """
    Updates specific top-level fields of a project analysis task via the API.
    Only provided fields will be updated.

    Args:
        task_id: The UUID of the task.
        total_files: New total number of files.
        overall_status: New overall status.
        overall_error_log: New overall error log.

    Returns:
        The updated task status data if successful, None otherwise.
    """
    endpoint = f"{Config.APPMOD_DOMAIN}/api/analysis-status/v1/tasks/{task_id}" # Note: same path as GET
    payload = {
        "total_files": total_files,
        "overall_status": overall_status,
        "overall_error_log": overall_error_log,
        "overall_coverage": overall_coverage,
        "total_files_processed": total_files_processed,
        "total_files_skipped": total_files_skipped,
        "total_files_chunked": total_files_chunked,
        "total_files_summarized": total_files_summarized,
        "total_chunks": total_chunks,
        "total_supported_files": total_supported_files,
        "total_unsupported_files": total_unsupported_files,
    }
    # Remove None values from payload as the API expects only provided fields
    payload = {k: v for k, v in payload.items() if v is not None}

    if not payload:
        logger.warning(f"No fields provided for update for task {task_id}.")
        return None

    logger.info(f"Attempting to update fields for task {task_id} with {payload}...")
    try:
        response = requests.patch(endpoint, json=payload, timeout=30.0)
        response.raise_for_status()
        logger.info(f"Task {task_id} fields updated successfully. Status: {response.status_code}")
        return response.json()
    except requests.exceptions.HTTPError as e: # FIX: Changed to HTTPError
        logger.error(f"HTTP error updating fields for task {task_id}: {e.response.status_code} - {e.response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error updating fields for task {task_id}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    return None