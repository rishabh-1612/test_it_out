from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from datetime import timezone
import json
import os
import threading
import traceback
from pymongo import MongoClient
from google.cloud import storage
from flask import jsonify
# from FHPostProcessing import add_misc_files_to_ft_hierarchy, post_processing
from access_control_pipeline.db_operations import increment_analyze_count
from analyser_repo_pipeline.api_helpers import _handle_error, _initialize_project, _start_analysis_thread
from analyser_repo_pipeline.crypto_helpers import decrypting_token
from analyser_repo_pipeline.pipeline_helpers import clone_repositories, generate_services, get_complete_v2_architecture, \
    get_executive_summary, get_feature_summary, get_file_paths, get_repository_languages, initialize_project, \
    perform_vulnerability_checks, process_feature_hierarchy, upload_dependencies
from analyser_repo_pipeline.rlef_helpers import remove_keys_recursively
from clone_repo import get_current_commit_hash
import config

from email_utils import send_completion_mail, send_pipeline_error_email

from logger import info_logger, error_logger, warning_logger


from analysis_status_update import update_step_status, update_task_fields
from utils.analysis_status_enums import StepStatus, PossibleStatuses

from absolute_path.language_support import read_json_from_bucket

from db_update import update_folder_summaries_in_db, save_to_ecg_db



def _start_resync_thread(task_id, user_id, initial_data, sync_type, git_pat_token, active_user_id):
    """Start the analysis thread."""
    thread = threading.Thread(
        target=local_sync_pipeline,
        args=(task_id, user_id, initial_data, sync_type, git_pat_token, active_user_id),
    )
    thread.start()


def local_sync_pipeline(task_id, user_id, initial_data, sync_type, git_pat_token, active_user_id):
    """
    Start resync thread
    """
    collection_ids = []
    sync_thread_results = None
    intial_dir = os.getcwd()

    try:
        info_logger.info(f"Starting resync thread for task_id: {task_id}")

        update_task_fields(
            task_id=task_id,
            overall_status=PossibleStatuses.IN_PROGRESS.value
        )

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.CLONING.value,
            status=PossibleStatuses.IN_PROGRESS.value
        )

        github_urls = initial_data["repo_urls"]
        branch_names = initial_data["branch_names"]
        egpt_token = None
        batch_size = initial_data["batch_size"]
        project_name = initial_data["project_name"]
        chatbot_name = initial_data["assistant_id"]

        info_logger.info(
            f"GITHUB URLS: {github_urls} \n BRANCH NAMES: {branch_names} \n EGPT TOKEN: {egpt_token} \n BATCH SIZE: {batch_size} \n PROJECT NAME: {project_name} \n CHATBOT NAME: {chatbot_name}")


        # Todo: call data extraction pipeline from EGPT_AI capture file summaries with pub/sub - done
        # Todo: Embed the summaries and push to alloydb - done
        # Todo: Update Dependency Graph with updated file summaries
        # Todo: feature hierarchy generation with updated files




        update_task_fields(
            task_id=task_id,
            overall_status=PossibleStatuses.COMPLETED.value
        )

    except Exception as e:
        error_logger.error(f"Error in _start_resync_thread: {e}")
        version = config.ENV
        error_logger.error(
            f"Error occurred in Project Analyzer process: {str(e)} \n\n Traceback: {traceback.format_exc()}")
        error = f"Error occurred in Sync Project Analyzer process: {str(e)} \n\n Traceback: {traceback.format_exc()}"
        tracking_error_in_pipeline(task_id, error)

        update_task_fields(
            task_id=task_id,
            overall_status=PossibleStatuses.FAILED.value,
            overall_error_log=error
        )

        payload_recieved = {
            "task_id": task_id, "user_id": user_id, "sync_type": sync_type}
        send_pipeline_error_email(task_id, error, version, analysis_type="sync", payload_recieved=payload_recieved)
        raise e
    finally:
        info_logger.info(f"Completed resync thread for task_id: {task_id}")


def start_sync_project_summary(resync_task_id, request_data):
    """
    Start sync project summary
    """
    try:
        user_id = request_data.get("owner_user_id")
        task_id = request_data.get("task_id")
        sync_type = request_data.get("sync_type")
        active_user_id = request_data.get("active_user_id")
        git_token = decrypting_token(request_data.get("github_token"))
        info_logger.info(f"Starting sync project summary for task_id: {task_id}")

        info_logger.info(f"Request Data: {request_data}")
        data = get_intial_analysis_data(task_id, user_id)

        data['version'] = data.get('version', 0) + 1

        resync_task_id = _initialise_resync_project(resync_task_id, user_id, task_id, sync_type, data)

        _start_resync_thread(resync_task_id, user_id, data, sync_type, git_token, active_user_id)

        return {"status": "Resync Started", "task_id": resync_task_id}, 200

    except Exception as e:
        print(f"Error in start_sync_project_summary: {e}")
        error_logger.info(
            f"Error occurred in Sync request in Background after approval function: {str(e)} Traceback: {traceback.format_exc()}")
        error_message = f"Error occurred in Sync request Background after approval function : {str(e)} Traceback: {traceback.format_exc()}"
        _handle_error(user_id, task_id, error_message, "sync", request_data)

        return jsonify({"status": "Error", "message": str(e)})


def read_json_from_gcs(source_blob_name, bucket_name=None):
    """
    Reads a JSON file directly from a GCS bucket and returns its contents as a dictionary.

    Args:
        source_blob_name (str): The path to the blob in the bucket.
        bucket_name (str): The name of the GCS bucket.

    Returns:
        dict: The JSON content as a dictionary, or empty dict if an error occurs.
    """

    try:
        # Initialize the client
        storage_client = storage.Client()

        # Get bucket reference
        bucket = storage_client.bucket(bucket_name)

        # Get blob reference
        blob = bucket.blob(source_blob_name)

        # Download the content as string
        json_content = blob.download_as_text()

        # Parse JSON content
        data = json.loads(json_content)

        print(f"Successfully read JSON from {source_blob_name} in bucket {bucket_name}.")
        return data

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return {}
    except Exception as e:
        print(f"An error occurred while reading the file: {str(e)}")
        return {}


from enum import Enum


class GCSFilePath(Enum):
    """
    Enum for GCS file paths.
    """
    RLEF_DATA = "rlef_data"
    FOLDER_SUMMARIES = "folder_summaries"
    FEATURE_HIERARCHY = "feature_hierarchy"


def get_folder_summary_gcs_path(task_id: str, gcs_file_path: str) -> str:
    """
    Get the folder summary GCS path for a given task ID.
    Args:
        task_id (str): The task ID for which to retrieve the folder summary GCS path.
    Returns:
        str: The GCS path of the folder summary.
    """
    try:
        # Connect to MongoDB
        client = MongoClient(config.MONGODB_CLIENT)
        filter = {
            'task_id': task_id
        }

        folder_json_path = list(client['project_summary']['projects'].find(
            filter=filter
        ))[0].get('dashboard', {}).get(gcs_file_path)
        return folder_json_path
    except Exception as e:
        error_logger.error(f"Error in get_folder_summary_gcs_path: {e}")
        raise e
    finally:
        client.close()

# if __name__ == "__main__":
#     json_folder_summary_path = get_folder_summary_gcs_path("785f88fd-bab4-aaf4-76f8-bc3eb791f677")
#     folder_summary = read_json_from_gcs(json_folder_summary_path, config.GCP_BUCKET_BASE_PATH)