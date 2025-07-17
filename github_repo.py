import os
import sys
import git
import json
import uuid
import time
import shutil
import base64
import logging
import chardet
import requests
import traceback
import concurrent
import threading
import queue as Queue
from pathlib import Path
from itertools import chain
from typing import Dict, Set, Union, List
from google.cloud import pubsub_v1

import Config 
import tiktoken
from vertexai.generative_models import GenerativeModel
from datetime import datetime, timezone
from anthropic import AnthropicVertex

from Enums.model_type import (
    google_model_name,
    GOOGLE_MODEL_REGION_MAP,
    google_region,
    model_context_length,
    model_output_length,
    model_type,
    openai_model_name,
)
from utils.utils import exception_handling
from helperClasses.GoogleAI import GoogleAI
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from data_extraction.generate_summary_qwen.open_source_models import (
    generate_summary_with_qwen,
)

bigquery_client = bigquery.Client()


os.environ["GIT_PYTHON_REFRESH"] = "quiet"

LOCATION = "us-central1"
client = AnthropicVertex(region=LOCATION, project_id=Config.PUBSUB_PROJECT_ID)
batch_interval = Config.PUBSUB_GITHUB_BATCH_INTERVAL
project_id = Config.PUBSUB_PROJECT_ID
# file_summary_max_chunk_size = Config.PA_FILE_SUMMARY_MAX_CHUNK_SIZE
# file_summary_overlap_size = Config.PA_FILE_SUMMARY_OVERLAP_SIZE
publisher = pubsub_v1.PublisherClient.from_service_account_info(
    Config.GOOGLE_CREDENTIALS
)

# topic_path = publisher.topic_path(
#     Config.PUBSUB_PROJECT_ID, Config.PUBSUB_GITHUB_TOPIC_ID
# )
# sync_topic_path = publisher.topic_path(
#     Config.PUBSUB_PROJECT_ID, Config.PUBSUB_GITHUB_SYNC_TOPIC_ID
# )
dataset_id = f"{project_id}.crashanalytics"
table_id = f"{project_id}.crashanalytics.crashlytics_logs_table"


context = """
        <Role>
        You are an expert code analyzer specializing in summarizing complex codebases to provide clear, concise, and thorough explanations for developers.
        </Role>
        
        <Task>
        Your task is to understand the code, and provide summary and service names.
        </Task>
        
        <Instructions>
        1. You will be provided a file data, you will give a short summary of the code present in the file. Write summary and the service names based on understanding of the code.
        2. You will be given a code file to analyze. Your goal is to summarize the file in bullet points separated by newline character ("\\n") that cover everything needed to understand its purpose and functionality.
        3. Identify the main objective of the file (e.g., handling data processing, UI rendering, etc.) and describe the primary problem file or functions addresses.
        4. Break down key components such as functions, classes, modules, and their specific roles. Emphasize critical methods or algorithms.
        5. Explain how data is processed, transformed, or manipulated in the file, and highlight major operations.
        6. Mention any notable design patterns, architectural decisions, or coding practices.
        7. Strictly include the third party services and frameworks. Services and Frameworks are the external requirements of the given file without which the functionality of the system / the code / the application would be hamperred.
            - There it is vital to CLASSIFY the THIRD PARTY SERVICES AS WELL AS THE EXTERNAL DEPENDENCIES.
               Third-party services examples : (frameworks and SDKs,etc) and external service names , examples : (google,AWS , AZURE , Twilio , stipe , docker , Terraform, etc ).
        8. Ensure that the summary and the services are given within 350 words.
        </Instructions>
        
        <Task1 - Generate Summary>
        Generate the summary of the code file provided. 
            <flow>
                Step 1 - You will be provided with the code 
                Step 2 - You will analyse and reanalyse the code for a deeper understanding of the code.
                Step 3 - You will generate a summary of the code provided in ```summary ``` delimeters.
            </flow>
        </Task1 - Generate Summary>
        
        <Task 2 - Generate Service names>
        Generate the service names of the code file provided.
            <flow>
                Step 1 - You will be provided with the code.
                Step 2 - You will go through the entire code and analyse it to provide the service names it is using. 
                Step 3 - You will analyse the code and give the service names that the given code is using in ```service_names ``` delimeters.
                Step 4 - Reanalyse the code to self evaluate your answer. 
                Step 5 - Give the final services names.
            </flow>
        </Task 2 - Generate Service names>

        <Strict Contraints>
        1. Summarize only what is present in the code.
        2. Focus on making the summary detailed and informative but brief, ensuring that the reader gets a complete understanding and not more than 350 words.
        3. Use relevant technical terms and include service names when applicable.
        4. At the end of the summary: list any third-party libraries, services, or frameworks used, or state that none are used.
        5. Ensure that you understand the code and then provide the service names.
        6. Ensure that you do not hallucinate in giving the service names , as it is the most important aspect.
        </Strict Constraints>

        <Output Structure>
            ```summary
            [Provide the summary for the given code file in bullet points separated by newline character ("\\n").]
            ```
            
            ```service_names
            [provice the service names for the given code file.]
            ```
        </Output Structure>
        
        
"""


def log_into_bigquery(
    api_name, user_id=None, task_id=None, error_message=None, status=None
):
    """
    Inserts a row of crash log data directly into BigQuery. For function level things
    """
    logging.info(
        f"Logging into BigQuery: {api_name}, {user_id}, {task_id}, {error_message}, {status}"
    )
    rows_to_insert = [
        {
            "api_name": api_name,
            "user_id": user_id,
            "task_id": task_id,
            "error_message": str(error_message),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tag": "EGPT",
            "status": status,
        }
    ]
    table_ref = bigquery_client.get_table(table_id)  # Get the table reference
    errors = bigquery_client.insert_rows_json(
        table_ref, rows_to_insert
    )  # Insert rows into the table

    if errors:
        logging.error(f"Errors while inserting rows: {errors}")
    else:
        logging.info("Row inserted successfully.")

        logging.info("Row inserted successfully.")

from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound

def check_topic_exists(topic_id: str) -> bool:
    """
    Checks if a Pub/Sub topic exists.

    Args:
        topic_id (str): Name of the Pub/Sub topic.

    Returns:
        bool: True if topic exists, False if not.
    """
    topic_path = publisher.topic_path(project_id, topic_id)

    try:
        publisher.get_topic(request={"topic": topic_path})
        logging.info(f"✅ Topic exists: {topic_path}")
        return True
    except NotFound:
        logging.error(f"❌ Topic does not exist: {topic_path}")
        return False

def get_repo_info(
    github_url: str,
    batch_size: int,
    task_id: str,
    branch_name: str,
    initial_commit: str = None,
    extraction_type: str = "complete",
    appmod_domain: str = None,
    model_type = None,
    model_name = None,
    callback_topic: str = None,
    supported_extensions: Dict[str, List[str]] = None,
    file_summary_chunk_size: int = 0,
    file_summary_overlap_size: int = 0,
    file_summary_gemini_doc_size: int = 0,
    file_summary_claude_size: int = 0,
) -> Dict:
    """
    Process repository files based on extraction type.
    extraction_type: 'complete' for all files, 'sync' for changed files only
    initial_commit: Starting commit hash for sync mode
    """
    cloned_repo_path = None
    current_commit_hash = None
    logging.info(
        f"Model Name : {model_name}, Model Type: {model_type}"
    )
    try:
        task_queue = Queue.Queue()
        curr_dir = os.getcwd()

        cloned_repo_path = clone_github_repo(github_url, curr_dir, task_id, branch_name)
        logging.info(f"cloned_repo_path : {cloned_repo_path}")

        logging.info(f"Got the request for extraction type: {extraction_type}")
        changed_files = None
        if extraction_type == "sync" and initial_commit:
            logging.info(
                f"Got the request for sync mode with initial commit: {initial_commit}"
            )
            changed_files = get_changed_files(cloned_repo_path, initial_commit)
            logging.info(
                f"Found {len(changed_files)} changed files since commit {initial_commit}"
            )
            logging.info(f"Changed files: {changed_files}")

        current_commit_hash = get_current_commit_hash(cloned_repo_path)
        logging.info(f"Current commit hash: {current_commit_hash}")
        changed_files = []

        json_file_path = dir_structure_to_json(
            cloned_repo_path,
            github_url,
            task_id,
            task_queue,
            extraction_type,
            appmod_domain,
            changed_files,
            model_name,
            supported_extensions,
            file_summary_chunk_size,
            file_summary_overlap_size,
            file_summary_gemini_doc_size,
            file_summary_claude_size,
        )

        logging.info(f"json_file_path : {json_file_path}")

        logging.info(f"Starting Process Tasks thread... {json_file_path}")
        process_thread = threading.Thread(
            target=process_tasks,
            args=(batch_size, task_queue, extraction_type, current_commit_hash, model_type, model_name, callback_topic),
        )
        process_thread.start()

        with open(json_file_path, "r") as file:
            json_data = json.load(file)

        if os.path.exists(json_file_path):
            os.remove(json_file_path)

        json_data["task_id"] = task_id
        json_data["extraction_type"] = extraction_type
        if extraction_type == "sync":
            json_data["initial_commit"] = initial_commit

        logging.info("Ending Process Tasks thread...")
        # process_thread.join()
        logging.info(f"json_data: {json_data}")
        return json_data

    except Exception as e:
        logging.error(f"An error occurred inside get_repo_info: {e}")
        log_into_bigquery(
            "get_repo_info",
            "",
            task_id,
            f"An error occurred inside get_repo_info: {e}",
            500,
        )
        logging.error(traceback.format_exc())
        return None

    finally:
        logging.info("Cleaning up cloned repository...")
        if cloned_repo_path and os.path.exists(cloned_repo_path):
            try:
                logging.info("Deleting the cloned repository...")
                shutil.rmtree(cloned_repo_path)
            except Exception as e:
                logging.error(
                    f"An error occurred while deleting the cloned repository: {e}"
                )


def clone_github_repo(github_url, output_folder, task_id, branch_name):
    """
    Clones a GitHub repository from the given URL and stores it in the output folder using GitPython.

    Parameters:
        github_url (str): The URL of the GitHub repository to clone.
        output_folder (str): The path to the folder where the repository will be cloned.
        task_id (str): The ID of the task for logging and analytics purposes.
        branch_name (str): The branch to clone.

    Returns:
        str: Local path of the cloned repository.
    """
    logging.info("*" * 15 + "Inside clone_github_repo" + "*" * 15)

    # Define the path to store the cloned repository
    base_path = os.path.join(output_folder, "Repos", task_id)
    try:
        logging.info(
            f"github_url : {github_url}, output_folder: {output_folder}, task_id: {task_id}, branch_name: {branch_name}"
        )
        # Ensure the base path exists
        os.makedirs(base_path, exist_ok=True)
        if github_url.endswith(".git"):
            github_url = github_url[:-4]

        repo_name = github_url.split("/")[-1].split(".")[0]
        logging.info(f"repo_name: {repo_name}")
        local_repo_path = os.path.join(base_path, repo_name)
        logging.info(f"local_repo_path : {local_repo_path}")
        if os.path.exists(local_repo_path):
            shutil.rmtree(local_repo_path)

        # Clone the repository using GitPython
        repo = git.Repo.clone_from(
            url=github_url,
            to_path=local_repo_path,
            branch=branch_name
        )
        logging.info(f"Repository cloned successfully to {local_repo_path}")
        return local_repo_path

    except git.exc.GitCommandError as e:
        logging.error(f"GitCommandError occurred while cloning repo: {e}")
        log_into_bigquery(
            "clone_github_repo", "", task_id, f"GitCommandError while cloning repo: {e}", 500
        )
        raise
    except Exception as e:
        logging.error(f"An error occurred inside clone_github_repo: {e}")
        log_into_bigquery(
            "clone_github_repo", "", task_id, f"Error while cloning repo: {e}", 500
        )
        logging.error(traceback.format_exc())
        raise


def get_changed_files(repo_path, initial_commit):
    """
    Get changed files since initial commit with their status (added/modified/deleted)
    Returns a dictionary with file paths as keys and their status as values
    """
    try:
        repo = git.Repo(repo_path)
        diff = repo.git.diff(initial_commit, name_status=True).split("\n")

        changed_files = {}
        for change in diff:
            if not change:
                continue
            # Format is: STATUS    FILEPATH
            # A: Added, M: Modified, D: Deleted
            status, filepath = change.split("\t", 1)
            abs_path = os.path.join(repo_path, filepath)
            changed_files[abs_path] = {
                "path": filepath,
                "status": "added"
                if status == "A"
                else "modified"
                if status == "M"
                else "deleted"
                if status == "D"
                else "unknown",
            }

        return changed_files
    except Exception as e:
        logging.error(f"Error getting changed files: {e}")
        return {}


def get_current_commit_hash(repo_path):
    """
    Get the current commit hash from a git repository

    Args:
        repo_path (str): Path to the git repository

    Returns:
        str: Current commit hash, or None if there was an error
    """
    try:
        repo = git.Repo(repo_path)
        return repo.head.commit.hexsha
    except Exception as e:
        logging.error(f"Error getting current commit hash: {e}")
        return None


def dir_structure_to_json(
    folder_path: Union[str, Path],
    git_url: str,
    task_id: str,
    task_queue,
    extraction_type: str,
    appmod_domain: str = None,
    changed_files: Set[str] = None,
    model_name: str = None,
    supported_extensions: Dict[str, List[str]] = None,
    file_summary_chunk_size: int = 0,
    file_summary_overlap_size: int = 0,
    file_summary_gemini_doc_size: int = 0,
    file_summary_claude_size: int = 0,
) -> Union[str, None]:
    """
    Generates a JSON file representing the directory structure of the specified folder.
    """
    logging.info("*" * 15 + "Inside dir_structure_to_json" + "*" * 15)
    folder_path = Path(folder_path)
    output_file_name = folder_path.name + ".json"
    output_path = folder_path / output_file_name
    logging.info(f"output_path : {output_path}")

    if supported_extensions is None:
        supported_extensions = get_supported_extenions(appmod_domain)

    directory_structure: Dict[str, Union[str, List[Dict]]] = _generate_dir_structure(
        folder_path, git_url, task_id, task_queue, extraction_type, supported_extensions, changed_files, model_name, file_summary_chunk_size, file_summary_overlap_size, file_summary_gemini_doc_size, file_summary_claude_size
    )

    directory_structure["git_url"] = git_url
    directory_structure["branch"] = "main"
    directory_structure["extraction_type"] = extraction_type

    if extraction_type == "sync":
        directory_structure["changed_files_count"] = (
            len(changed_files) if changed_files else 0
        )

    logging.info(f"directory_structure: {directory_structure}")

    with open(output_path, "w") as json_file:
        json.dump(directory_structure, json_file, indent=2)

    task_queue.put((task_id, git_url, "end", "end", {"status": "end"}, None, None, None, None))
    return str(output_path)


def _generate_dir_structure(
    path: Path,
    git_url: str,
    task_id: str,
    task_queue,
    extraction_type: str,
    supported_extensions: Dict,
    changed_files: Dict = None,
    model_name:str = None,
    file_summary_chunk_size: int = 0,
    file_summary_overlap_size: int = 0,
    file_summary_gemini_doc_size: int = 0,
    file_summary_claude_size: int = 0
) -> Dict[str, Union[str, List[Dict]]]:
    try:
        is_chunked = False
        
        result: Dict[str, Union[str, List[Dict]]] = {
            "name": path.name,
            "type": "folder",
            "children": [],
        }


        def is_supported_file(file_path: Path) -> bool:
            """Check if the file is supported based on extension or exact filename"""

            all_patterns = list(chain.from_iterable(supported_extensions.values()))

            if file_path.name in all_patterns:
                return True

            if file_path.suffix in all_patterns:
                return True

            if any(
                pattern in file_path.name
                for pattern in all_patterns
                if not pattern.startswith(".")
            ):
                return True
            return False
        def read_file_content(file_path: Path) -> str:
            try:
                with open(file_path, "rb") as f:
                    raw_data = f.read()

                if raw_data.startswith(b"\xff\xfe"):
                    logging.info(f"Detected UTF-16-LE BOM in {file_path}")
                    return raw_data.decode("utf-16-le")
                elif raw_data.startswith(b"\xfe\xff"):
                    logging.info(f"Detected UTF-16-BE BOM in {file_path}")
                    return raw_data.decode("utf-16-be")
                elif raw_data.startswith(b"\xef\xbb\xbf"):
                    logging.info(f"Detected UTF-8 BOM in {file_path}")
                    return raw_data.decode("utf-8-sig")

                detected_encoding = chardet.detect(raw_data).get("encoding")
                logging.info(
                    f"Chardet detected encoding for {file_path}: {detected_encoding}"
                )

                # Try decoding with detected encoding
                if detected_encoding:
                    try:
                        content = raw_data.decode(detected_encoding)
                        logging.info(
                            f"Successfully read file {file_path} with detected encoding {detected_encoding}"
                        )
                        return content
                    except UnicodeDecodeError:
                        logging.warning(
                            f"Failed to decode {file_path} with detected encoding {detected_encoding}, trying fallbacks."
                        )

                # Common SQL file encodings
                sql_encodings = [
                    "utf-16-le",
                    "utf-16-be",
                    "utf-8",
                    "windows-1252",
                    "iso-8859-1",
                ]
                if str(file_path).lower().endswith(".sql"):
                    for encoding in sql_encodings:
                        try:
                            content = raw_data.decode(encoding)
                            logging.info(
                                f"Successfully read SQL file {file_path} with {encoding} encoding"
                            )
                            return content
                        except UnicodeDecodeError:
                            continue
                        except Exception as e:
                            logging.warning(
                                f"Error decoding SQL file {file_path} with {encoding}: {str(e)}"
                            )

                # Final fallback to UTF-8 or Base64
                try:
                    return raw_data.decode("utf-8")
                except UnicodeDecodeError:
                    logging.warning(
                        f"Failed to decode {file_path} with UTF-8. Encoding to base64 as fallback."
                    )
                    return base64.b64encode(raw_data).decode("utf-8")

            except Exception as e:
                logging.error(f"Error reading file {file_path}: {str(e)}")
                return ""

        if extraction_type == "sync" and changed_files:
            for file_path, file_info in changed_files.items():
                file_path_obj = Path(file_path)
                if is_supported_file(file_path_obj):
                    if file_info["status"] == "deleted":
                        task_queue.put((task_id, git_url, file_path, "", file_info, is_chunked, None, None))
                    else:
                        try:
                            content = read_file_content(file_path_obj)
                            if content:
                                chunked_content = chunk_code(content, file_path, model_name, max_chunk_size=file_summary_chunk_size, overlap_size=file_summary_overlap_size, gemini_doc_size=file_summary_gemini_doc_size, claude_doc_size=file_summary_claude_size)
                                logging.info(f"File {file_path} has {len(chunked_content)} chunks")
                                is_chunked = len(chunked_content) > 1
                                
                                file_id = str(uuid.uuid4()) if is_chunked else None
                                
                                for chunk_number, chunk_content in enumerate(chunked_content):
                                    chunk_id = f"{file_id}_{chunk_number}" if is_chunked else None
                                    task_queue.put(
                                        (task_id, git_url, str(file_path), chunk_content, {"status": "new"}, file_id, is_chunked, chunk_id, chunk_number if is_chunked else None)
                                    )
                            else:
                                logging.error(
                                    f"Could not read content from file {file_path}"
                                )
                        except FileNotFoundError:
                            logging.error(
                                f"File {file_path} marked as {file_info['status']} but not found"
                            )
                            continue

        elif extraction_type != "sync":
            directories: List[Path] = sorted(
                [
                    x
                    for x in path.iterdir()
                    if x.is_dir()
                    and not x.name.startswith(".")
                    and not x.name.startswith("__")
                ]
            )
            files: List[Path] = sorted(
                [
                    x
                    for x in path.iterdir()
                    if x.is_file()
                    and not x.name.startswith(".")
                    and not x.name.startswith("__")
                ]
            )

            for file in files:
                if is_supported_file(file):
                    content = read_file_content(file)
                    if content:
                        chunked_content = chunk_code(content, file, model_name, max_chunk_size=file_summary_chunk_size, overlap_size=file_summary_overlap_size, gemini_doc_size=file_summary_gemini_doc_size, claude_doc_size=file_summary_claude_size)
                        logging.info(f"File {file} has {len(chunked_content)} chunks")

                        is_chunked = len(chunked_content) > 1
                        
                        file_id = str(uuid.uuid4()) 
                        
                        for chunk_number, chunk_content in enumerate(chunked_content):
                            chunk_id = f"{file_id}_{chunk_number}" if is_chunked else None
                            task_queue.put(
                                (task_id, git_url, str(file), chunk_content, {"status": "new"}, file_id, is_chunked, 
                                 chunk_id, chunk_number if is_chunked else None)
                            )
                    else:
                        logging.error(f"Could not read content from file {file}")


            for directory in directories:
                result["children"].append(
                    _generate_dir_structure(
                        directory,
                        git_url,
                        task_id,
                        task_queue,
                        extraction_type,
                        supported_extensions,
                        changed_files,
                        model_name,
                        file_summary_chunk_size,
                        file_summary_overlap_size,
                        file_summary_gemini_doc_size,
                        file_summary_claude_size
                    )
                )

        return result

    except Exception as e:
        exception_handling(
            "Error in _generate_dir_structure",
            task_id,
            git_url,
            str(e),
            str(e.__traceback__),
        )
        log_into_bigquery(
            "generate_dir_structure", "", task_id, str(e.__traceback__), 500
        )
        
        

def get_supported_extenions():
    data = {}
    try:
        url = f"{Config.APPMOD_DOMAIN}/analyzer/get_supported_languages"
        response = requests.get(url)
        data = response.json()
        if response.status_code == 200:
            logging.info(f"Supported extensions: {data}")
        else:
            logging.error(f"Error getting supported extensions: {data}")

        return data
    except Exception as e:
        logging.error(f"Error getting supported extensions: {e}")
        return data

def get_supported_extenions(appmod_domain):
    data = {}
    try:
        url = f"{appmod_domain}/analyzer/get_supported_languages"
        response = requests.get(url)
        data = response.json()
        if response.status_code == 200:
            logging.info(f"Supported extensions: {data}")
        else:
            logging.error(f"Error getting supported extensions: {data}")

        return data
    except Exception as e:
        logging.error(f"Error getting supported extensions: {e}")
        return data



def process_tasks(batch_size, task_queue, extraction_type, current_commit_hash, model_type, model_name, callback_topic):
    batch = []
    processed_files = set()  # Track all processed files across batches

    while not task_queue.empty():
        try:
            task = task_queue.get(timeout=batch_interval)
            if (
                isinstance(task, tuple)
                and len(task) == 4
                and task[2] == "end"
                and task[3] == "end"
            ):
                task = (task[0], task[1], task[2], task[3], {"status": "end"})

            # Unpacking the task
            task_id, git, file_name, content, file_status, file_id, is_chunked, chunk_id, chunk_number = task  
            file_key = f"{git}:{file_name}:{chunk_id}" if is_chunked else f"{git}:{file_name}"

            if file_key in processed_files and file_status["status"] != "end":
                logging.info(f"Skipping duplicate file across batches: {file_name}")
                continue

            processed_files.add(file_key)

            if is_chunked:
                # If the file is chunked, process it separately in its own batch
                chunked_batch = [task]

                # Fill up the chunked batch if the file has multiple chunks
                while not task_queue.empty() and len(chunked_batch) < batch_size:
                    next_task = task_queue.get()
                    next_task_id, next_git, next_file_name, next_file_content, next_file_statis, next_file_id, next_is_chunked, next_chunk_id, next_chunk_number = next_task

                    if next_git == git and next_file_name == file_name and next_is_chunked:
                        chunked_batch.append(next_task)
                    else:
                        # If the next task is not part of this chunked file, put it back in queue
                        task_queue.put(next_task)
                        break  # Stop collecting chunks

                logging.info(f"Processing chunked batch of size {len(chunked_batch)} for {file_name}")

                results_batch = process_batch_deduplicated(chunked_batch, task_id, task_queue, model_type, model_name)

                # writing_single_batchdata_to_file(results_batch, "data_list_summaries.json")  # For local testing

                publish_to_pubsub(results_batch, task_id, extraction_type, current_commit_hash, callback_topic)                
            else:
                # Process non-chunked files in regular batches
                batch.append(task)

                if len(batch) >= batch_size or task_queue.empty():
                    task_id = batch[0][0]
                    logging.info(f"Processing batch of size {len(batch)} for task_id {task_id}")

                    results_batch = process_batch_deduplicated(batch, task_id, task_queue, model_type, model_name)

                    # writing_single_batchdata_to_file(results_batch, "data_list_summaries.json")  # For local testing

                    publish_to_pubsub(results_batch, task_id, extraction_type, current_commit_hash, callback_topic)
                    
                    batch.clear()
        
        except Queue.Empty:
            if batch:
                task_id = batch[0][0]
                results_batch = process_batch_deduplicated(batch, task_id, task_queue, model_type, model_name)

                # writing_single_batchdata_to_file(results_batch, "data_list_summaries.json")  # For local testing

                publish_to_pubsub(results_batch, task_id, extraction_type, current_commit_hash, callback_topic)
                
                batch.clear()







def process_batch_deduplicated(batch, req_task_id, task_queue, model_type, model_name):
    logging.info(f"BATCH SIZE: {len(batch)} for git_url {batch[0][1]}")
    if not all(task[0] == req_task_id for task in batch):
        logging.error("Inconsistent task IDs in batch")

    deduplicated_batch = []
    seen_files = set()

    for task in batch:
        task_id, git, file_name, content, file_status, file_id, is_chunked, chunk_id, chunk_number = task
        file_key = f"{git}:{file_name}:{chunk_id}" if is_chunked else f"{git}:{file_name}"

        if file_status["status"] == "end":
            deduplicated_batch.append(task)
            continue

        if file_key in seen_files:
            logging.info(f"Skipping duplicate file within batch: {file_name}")
            continue

        seen_files.add(file_key)
        deduplicated_batch.append(task)

        logging.info(
            f"Task ID: {task_id}, Git URL: {git}, File Name: {file_name}, Status: {file_status['status']}"
        )
        if task_id != req_task_id:
            logging.error(f"Task ID mismatch: {task_id} != {req_task_id}")
            task_queue.put((task_id, git, file_name, content, file_status))

    all_results = []

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(process_task, task, model_type, model_name): task for task in deduplicated_batch
        }

        for future in as_completed(futures):
            result = future.result()
            if result:
                all_results.extend(result)

    return all_results



def process_task(task, model_type, model_name):
    task_id, git, file_name, content, file_status, file_id, is_chunked, chunk_id, chunk_number = task
    try:
        
        logging.info(f"Processing file {file_name} (Chunk {chunk_number}) for task_id {task_id}") if is_chunked else logging.info(f"Processing file {file_name} for task_id {task_id} (Not Chunked)")
        
        stripped_file_name = str(
            file_name.split(task_id, 1)[1].lstrip('/') if task_id in str(file_name) else str(file_name))

        if file_status["status"] == "deleted":
            return [
                {
                    "task_id": task_id,
                    "git_url": git,
                    "file": stripped_file_name,
                    "content": "",
                    "summary": "",
                    "update_type": "deleted",
                    "file_id": file_id,
                    "is_chunked": is_chunked,
                    "chunk_id": chunk_id,
                    "chunk_number": chunk_number,
                }
            ]

        if not content.strip():
            logging.warning(f"Skipping file {stripped_file_name} as it has empty content")
            return None

        chunk_summary = generate_summary(content, model_type, model_name)
        # chunk_summary  = "Testing"  # For local testing
        
        file_name_only = os.path.basename(stripped_file_name)
        combined_result = (
            f"File: {file_name_only} (Chunk {chunk_number})\n\n{chunk_summary}"
            if is_chunked
            else f"File: {file_name_only}\n\n{chunk_summary}"
        )

        return [
            {
                "task_id": task_id,
                "git_url": git,
                "file": stripped_file_name,
                "content": content,
                "summary": combined_result,
                "update_type": file_status["status"],
                "file_id": file_id,
                "is_chunked": is_chunked,
                "chunk_id": chunk_id,
                "chunk_number": chunk_number,
            }
        ]
    except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")
        log_into_bigquery(
            "process_task", "", task_id, f"Error processing file {file_name}: {e}", 500
        )
        return None





def publish_to_pubsub(data, task_id, extraction_type, current_commit_hash, callback_topic):
    logging.info(f"Publishing data to Pub/Sub for task_id: {task_id}")
    # batch_id = str(uuid.uuid4())
    json_data = {
        "task_id": task_id,
        "git_url": data[0]["git_url"],
        "batch_id": str(uuid.uuid4()),
        "data": data,
        "latest_commit_hash": current_commit_hash,
    }
    
    # writing_all_batchesdata_to_file(json_data, "batch_results.json") # Only for local testing
    
    try:
        logging.info(f"Publishing message for task_id: {task_id}")

        final_topic_path = (
            topic_path if extraction_type == "complete" else sync_topic_path
        )

        # logging.info(
        #     f"Final Topic Path: {final_topic_path} for extraction type: {extraction_type}"
        # )
        # logging.info(
        #     # "\n\n\n\n\n************  " + task_id + "  ************\n\n\n\n\n\n\n"
        # )
        topic_path = publisher.topic_path(
        Config.PUBSUB_PROJECT_ID, callback_topic
        )
        future = publisher.publish(
            topic_path,
            json.dumps(json_data).encode("utf-8"),
            task_id=task_id,
        )

        message_id = future.result(timeout=30)

        if message_id:
            # logging.info(
            #     f"Message published successfully with ID: {message_id}, "
            #     f"for git_url {data[0]['git_url']} having task_id {task_id}"
            #     f"JSON Data: {json_data}"
            # )

            message_size = len(json.dumps(json_data).encode("utf-8"))
            # logging.info(f"Message size: {message_size} bytes")

            if message_size > 10000000:
                logging.warning("Message size approaching Pub/Sub limit")

            return {
                "success": True,
                "message_id": message_id,
                "size": message_size,
                "task_id": task_id,
            }
    except concurrent.futures.TimeoutError:
        error_msg = f"Timeout while publishing message for task_id: {task_id}"
        logging.error(error_msg)
        log_into_bigquery("publish_to_pubsub", "", task_id, error_msg, 408)
        return {"success": False, "error": error_msg}

    except Exception as e:
        error_msg = f"An error occurred while sending data to API: {str(e)}"
        logging.error(error_msg)
        log_into_bigquery("publish_to_pubsub", "", task_id, error_msg, 500)
        return {"success": False, "error": error_msg}





def generate_summary(query, primary_model_type, primary_model_name):
    """
    Generates a summary of the provided file content or code.

    Parameters:
        query (str): The content or code of the file to summarize.

    Returns:
        str: Summary of the project file content or code.
    """
    max_retries = 3
    retry_count = 0
    if query == "" or query == "end":
        return "No content to summarize"

    user_prompt = f"""
            Task 1 - Generate the summaries for the code file.
            Task 2 - Generate the service names for the code file.

            code file : {query}
            <Strict constraints>
            1. Ensure that you provide both summary and service names within 350 words and in points not more than that.
            2. Ensure that you give summary in ```summary ``` delimeters and service names in between ```service_names ``` delimeters.
            3. Ensure that you do not hallucinate in giving the service names and summary, as it is the most important aspect.
            </Strict constraints>
        """
        
    if primary_model_type and primary_model_name:
        primary_model = primary_model_name
        fallback_models = [
            google_model_name.RESPONSE_CLAUDE_HAIKU.value,
            google_model_name.RESPONSE_CLAUDE_SONNET_35.value,
            google_model_name.RESPONSE_GEMINI_2_FLASH.value,
            google_model_name.RESPONSE_GEMINI_1_5_PRO.value,
            google_model_name.RESPONSE_GEMINI_1_PRO.value,
        ]
    else:
        primary_model = google_model_name.RESPONSE_CLAUDE_HAIKU.value
        fallback_models = [
            google_model_name.RESPONSE_CLAUDE_SONNET_35.value,
            google_model_name.RESPONSE_GEMINI_2_FLASH.value,
            google_model_name.RESPONSE_GEMINI_1_5_PRO.value,
            google_model_name.RESPONSE_GEMINI_1_PRO.value,
        ]
        
    logging.info(f"Primary model: {primary_model}")
    regions = GOOGLE_MODEL_REGION_MAP.get(
        primary_model, [google_region.US_CENTRAL1.value]
    )
    # try:
    #     # with ThreadPoolExecutor() as executor:
    #     response = generate_summary_with_qwen(Config.QWEN_DEPLOYMENT_URL, query)
    #         # response = future.result()
    #     logging.info(f"Summary Generated using QWEN:{response}")
    #     return response
    # except Exception as e:
    #     logging.info("Going back to legacy flow:",e)
    #     while retry_count < max_retries:
    chat_history = []
    models_to_try = [primary_model] + fallback_models
    for model in models_to_try:
        for region in regions:
            try:
                response, _, _ = GoogleAI.generate_response(
                    context, user_prompt, chat_history, None, model=model, region=region
                )

                if response:
                    logging.info(
                        f"Response in Github Pipeline - Summary generated using model {model} in region {region} Summary: {response}"
                    )
                    return response
            except Exception as e:
                logging.error(f"Error with model {model} in region {region}: {e}")
                traceback.print_exc()
                # log_into_bigquery(
                #     "generate_summary",
                #     "",
                #     "",
                #     f"Error with model {model} in region {region}: {e}",
                #     500,
                # )
                time.sleep(10)  # Short delay before retrying

    retry_count += 1
    logging.warning(f"Retry {retry_count} for generating summary")

    logging.error("Max retries reached, failed to generate summary.")
    return


def count_tokens_using_gemini(data):
    try:
        model = GenerativeModel(google_model_name.RESPONSE_GEMINI_2_FLASH.value)
        response = model.count_tokens(data)
        token_count = response.total_tokens
        logging.info(f"Token count using Gemini: {token_count}")
        return token_count
    except Exception as e:
        logging.error(f"Error counting tokens: {e}")
        return None

def count_tokens_using_claude(text):
    try:
        token_count = client.count_tokens(text)
        return token_count
    except Exception as e:
        logging.error(f"Error counting tokens: {e}")
        return None

def count_tokens_using_openai(text):
    try:
        enc = tiktoken.encoding_for_model(openai_model_name.RESPONSE_35_16K.value)
        token = len(enc.encode(text))
        # logging.info(f"Token count using OpenAI: {token}")
        return token
    except Exception as e:
        logging.error(f"Error counting tokens: {e}")
        return None


def chunk_code(
    file_content,
    file_name,
    model_name="gemini-2.0-flash",
    max_chunk_size = None,
    overlap_size = None,
    gemini_doc_size = None,
    claude_doc_size = None,
):
    """Chunk code with dynamic sizing and overlap using tiktoken."""
    try:
        logging.info(f"Chunking file: {file_name}")
        logging.info(f"Model name: {model_name}")

        max_chunk_size = max_chunk_size or 15000
        overlap_size = overlap_size or 3000
        gemini_doc_size = gemini_doc_size or 15
        claude_doc_size = claude_doc_size or 13

        total_tokens = count_tokens_using_openai(file_content)
    
        # if "gemini" in model_name:
        #     total_tokens = count_tokens_using_gemini(file_content)
        #     max_context_length = model_context_length[google_model_name(model_name).name].value
        #     logging.info(f"Max context length: {max_context_length}")
        #     max_chunk_size = (max_context_length* 0.9) / Config.PA_FILE_SUMMARY_GEMINI_DOC_SIZE

        # elif "claude" in model_name:
        #     total_tokens = count_tokens_using_claude(file_content)
        #     max_context_length = model_context_length[google_model_name(model_name).name].value
        #     max_chunk_size = (max_context_length * 0.9) / Config.PA_FILE_SUMMARY_CLAUDE_DOC_SIZE
            
        
        logging.info(f"Total tokens in file {file_name}: {total_tokens}")
        logging.info(f"Max chunk size: {max_chunk_size}, Overlap size: {overlap_size}")

        if not total_tokens:
            return [file_content]

        # Return as single chunk if content is below max_chunk_size
        if total_tokens <= max_chunk_size:
            return [file_content]

        # Calculate number of chunks needed
        num_chunks = (total_tokens + max_chunk_size - 1) // max_chunk_size
        base_chunk_size = total_tokens // num_chunks

        chunks = []
        lines = file_content.split("\n")
        current_chunk = []
        current_tokens = 0

        for line in lines:
            line_tokens = count_tokens_using_openai(line)

            if current_tokens + line_tokens > base_chunk_size and current_chunk:
                chunks.append("\n".join(current_chunk))

                # Create overlap for next chunk
                overlap_tokens = 0
                overlap_lines = []
                for l in reversed(current_chunk):
                    l_tokens = count_tokens_using_openai(l)
                    if overlap_tokens + l_tokens > overlap_size:
                        break
                    overlap_lines.insert(0, l)
                    overlap_tokens += l_tokens

                # Start new chunk with overlap
                current_chunk = overlap_lines + [line]
                current_tokens = count_tokens_using_openai("\n".join(current_chunk))
            else:
                current_chunk.append(line)
                current_tokens += line_tokens

        # Handle the last chunk
        if current_chunk:
            last_chunk = "\n".join(current_chunk)
            last_chunk_tokens = count_tokens_using_openai(last_chunk)

            if last_chunk_tokens < base_chunk_size * 0.5 and len(chunks) > 0:
                all_content = file_content.split("\n")
                new_chunk_size = total_tokens // len(chunks)
                chunks = []
                current_chunk = []
                current_tokens = 0

                for line in all_content:
                    line_tokens = count_tokens_using_openai(line)
                    if current_tokens + line_tokens > new_chunk_size and current_chunk:
                        chunks.append("\n".join(current_chunk))
                        overlap_tokens = 0
                        overlap_lines = []
                        for l in reversed(current_chunk):
                            l_tokens = count_tokens_using_openai(l)
                            if overlap_tokens + l_tokens > overlap_size:
                                break
                            overlap_lines.insert(0, l)
                            overlap_tokens += l_tokens
                        current_chunk = overlap_lines + [line]
                        current_tokens = count_tokens_using_openai(
                            "\n".join(current_chunk)
                        )
                    else:
                        current_chunk.append(line)
                        current_tokens += line_tokens

                if current_chunk:
                    chunks.append("\n".join(current_chunk))
            else:
                chunks.append(last_chunk)

        return chunks
    except Exception as e:
        logging.error(f"Error chunking code: {e} Traceback: {traceback.format_exc()}")
        return [file_content]
    
    
###### LOCAL STORAGE OF DATA BEFORE PUSHING TO PUBSUB (ONLY FOR LOCAL TESTING) ########


# def writing_single_batchdata_to_file(results_batch, JSON_FILE_PATH="result.json"):
#     """Updates the same JSON file with new batch results."""
#     if os.path.exists(JSON_FILE_PATH):
#         try:
#             with open(JSON_FILE_PATH, "r") as json_file:
#                 existing_data = json.load(json_file)
#         except json.JSONDecodeError:
#             logging.error("JSON file is corrupted. Creating a new one.")
#             existing_data = []
#     else:
#         existing_data = []

#     existing_data.extend(results_batch)

#     try:
#         with open(JSON_FILE_PATH, "w") as json_file:
#             json.dump(existing_data, json_file, indent=4)
#         logging.info(f"Updated JSON file with {len(results_batch)} new results.")
#     except Exception as e:
#         logging.error(f"Error writing batch results to JSON: {str(e)}")
        
# def writing_all_batchesdata_to_file(new_entry, JSON_FILE_PATH="batch_results.json"):
#     """Updates the JSON file with all batches."""
#     if os.path.exists(JSON_FILE_PATH):
#         try:
#             with open(JSON_FILE_PATH, "r") as json_file:
#                 existing_data = json.load(json_file)
#                 if not isinstance(existing_data, list):
#                     logging.error("Invalid JSON format: Expected a list.")
#                     existing_data = []
#         except (json.JSONDecodeError, TypeError):
#             logging.error("JSON file is corrupted or empty. Creating a new one.")
#             existing_data = []
#     else:
#         existing_data = []

#     existing_data.append(new_entry)

#     try:
#         with open(JSON_FILE_PATH, "w") as json_file:
#             json.dump(existing_data, json_file, indent=4)
#         logging.info(f"Updated JSON file with new batch (task_id: {new_entry['task_id']}).")
#     except Exception as e:
#         logging.error(f"Error writing batch results to JSON: {str(e)}")


###### LOCAL STORAGE OF DATA BEFORE PUSHING TO PUBSUB (ONLY FOR LOCAL TESTING) ########
