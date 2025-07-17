import os
import json
import shutil
import time
from pathlib import Path
from typing import Dict, Union, List
import traceback
import requests
from git import Repo, GitCommandError
import logging
import uuid
import queue as Queue
import threading
import traceback
from google.cloud import pubsub_v1
from vertexai.generative_models import GenerativeModel

import Config
from anthropic import AnthropicVertex

from Enums.model_type import google_model_name, model_output_length, GOOGLE_MODEL_REGION_MAP, google_region
from utils.utils import exception_handling

from helperClasses.GoogleAI import GoogleAI

from concurrent.futures import ThreadPoolExecutor, as_completed

os.environ["GIT_PYTHON_REFRESH"] = "quiet"

LOCATION = "us-central1"
client = AnthropicVertex(region=LOCATION, project_id=Config.PUBSUB_PROJECT_ID)

task_queue = Queue.Queue()
batch_interval = Config.PUBSUB_GITHUB_BATCH_INTERVAL
project_id = Config.PUBSUB_PROJECT_ID

publisher = pubsub_v1.PublisherClient.from_service_account_info(Config.GOOGLE_CREDENTIALS)
topic_path = publisher.topic_path(Config.PUBSUB_PROJECT_ID, Config.PUBSUB_GITHUB_TOPIC_ID)
logging.debug(f"Publisher created with topic path: {topic_path}")

context = """
        <Role>
        you are an AI assistant that helps write a summary for the code
        </Role>

        <Task>
        you will be provided a file data, you will give a short summary or metadata of the code present in the file. Write summary based on function name and docstring of the function present in the file. Include functions name in the summary.Give within 200 words.
        </Task>
        """


def generate_summary(query):
    """
    Generates a summary of the provided file content or code.

    Parameters:
        query (str): The content or code of the file to summarize.

    Returns:
        str: Summary of the project file content or code.
    """
    max_retries = 3
    retry_count = 0

    primary_model = google_model_name.RESPONSE_CLAUDE_SONNET_35.value
    fallback_models = [
        google_model_name.RESPONSE_CLAUDE_HAIKU.value,
        google_model_name.RESPONSE_GEMINI_1_5_PRO.value,
        google_model_name.RESPONSE_GEMINI_1_PRO.value,
    ]
    regions = GOOGLE_MODEL_REGION_MAP.get(primary_model, [google_region.US_CENTRAL1.value])

    while retry_count < max_retries:
        chat_history = []
        models_to_try = [primary_model] + fallback_models
        for model in models_to_try:
            for region in regions:
                try:
                    response = GoogleAI.generate_response(context, query, chat_history, None, model=model,
                                                          region=region)
                    if response and response.text:
                        return response.text
                except Exception as e:
                    logging.error(f"Error with model {model} in region {region}: {e}")
                    time.sleep(10)  # Short delay before retrying

        retry_count += 1
        logging.warning(f"Retry {retry_count} for generating summary")

    logging.error("Max retries reached, failed to generate summary.")
    return


def clone_github_repo(github_url, output_folder, task_id):
    """
    Clones a GitHub repository from the given URL and stores it in the output folder.

    Parameters:
        github_url (str): The URL of the GitHub repository to clone.
        output_folder (str): The path to the folder where the repository will be cloned.

    Returns:
        str: Local path of the cloned repository.
    """

    # Define the path to store the cloned repository
    base_path = os.path.join(output_folder, 'Repos', task_id)
    try:
        # Ensure the base path exists
        os.makedirs(base_path, exist_ok=True)
        if github_url.endswith(".git"):
            github_url = github_url[:-4]

        repo_name = github_url.split('/')[-1].split('.')[0]
        local_repo_path = os.path.join(base_path, repo_name)
        if os.path.exists(local_repo_path):
            shutil.rmtree(local_repo_path)

        req_url = github_url.split("github.com/")
        api_call = "https://api.github.com/repos/" + req_url[1]
        tok = req_url[0].split("://")
        req_token = '' if tok[1] == '' else f"Bearer {tok[1][0:-1]}"
        headers = {
            'Accept': 'application/vnd.github+json',
            'Authorization': req_token,
            'X-GitHub-Api-Version': '2022-11-28'
        }
        response = requests.get(api_call, headers=headers)

        if response.status_code == 200:
            Repo.clone_from(github_url, local_repo_path)
            return local_repo_path
        elif response.status_code == 401:
            logging.error(f"Error : Invalid Token")
            return f"Error : Invalid Token {tok[1][0:-1]}"
        elif response.status_code == 404:
            logging.error(f"Error : Repo Not Found or Access Denied")
            return f"Error : Repo Not Found or Access Denied"
    except Exception as e:
        logging.error(f"An error occurred inside clone_github_repo: {e}")
        print(traceback.format_exc())
        raise


results = []


def _generate_dir_structure(path: Path, git_url: str, task_id: str, egpt_rlef_data) -> Dict[str, Union[str, List[Dict]]]:
    """
        Recursively generates the directory structure of a folder.

        Parameters:
            path (Path): The path to the folder.

        Returns:
            Dict[str, Union[str, List[Dict]]]: The directory structure represented as a dictionary.
        """
    try:
        result: Dict[str, Union[str, List[Dict]]] = {
            "name": path.name,
            "type": "folder",
            "children": []
        }

        directories: List[Path] = sorted(
            [x for x in path.iterdir() if x.is_dir() and not x.name.startswith('.') and not x.name.startswith('__')])
        files: List[Path] = sorted(
            [x for x in path.iterdir() if x.is_file() and not x.name.startswith('.') and not x.name.startswith('__')])

        for directory in directories:
            result["children"].append(_generate_dir_structure(directory, git_url, task_id,egpt_rlef_data))

        for file in files:
            if file.suffix in ['.py', '.cpp', '.c', '.js', '.md', '.java', '.ts', '.jsx', '.tsx', '.php', '.html',
                               '.cs']:
                try:
                    with open(file, "r", encoding="utf-8") as f:
                        content = f.read()
                    f.close()

                except UnicodeDecodeError:
                    with open(file, "r", encoding="ISO-8859-1 ") as f:
                        content = f.read()
                    f.close()
                task_queue.put((task_id, git_url, str(file), content,egpt_rlef_data))
        return result
    except Exception as e:
        exception_handling("Error in _generate_dir_structure", task_id, git_url, str(e), str(e.__traceback__))


def process_task(task):
    task_id, git, file_name, content, egpt_rlef_data = task
    try:
        result = generate_summary(content)
        stripped_file_name = str(file_name.split(task_id, 1)[1].lstrip('/') if task_id in str(file_name) else str(file_name))
        return {
            "task_id": task_id,
            "git_url": git,
            "file": stripped_file_name,
            "content": content,
            "summary": result,
            "egpt_rlef_data": egpt_rlef_data
        }
    except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")
        return None

def process_batch(batch,req_task_id):
    logging.info(f"Processing batch of size {len(batch)}")
    logging.info(f"BATCH SIZE: {Config.PUBSUB_GITHUB_BATCH_SIZE}")
    for task in batch:
        task_id, git, file_name, content, egpt_rlef_data = task
        if task_id != req_task_id:
            logging.error(f"Task ID mismatch: {task_id} != {req_task_id}")
            task_queue.put((task_id, git, file_name, content,egpt_rlef_data))
    results_batch = []

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_task, task): task for task in batch}
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                results_batch.append(result)
    return results_batch


def publish_to_pubsub(data, task_id):
    logging.info(f"Publishing data to Pub/Sub")
    egpt_rlef_data = data[0]["egpt_rlef_data"]

    json_data = {
        "task_id": task_id,
        "git_url": data[0]["git_url"],
        "batch_id": str(uuid.uuid4()),
        "assistant_name": egpt_rlef_data["assistant_name"],
        "organization": egpt_rlef_data["organization"],
        "model_id": egpt_rlef_data["model_id"],
        "project_id": egpt_rlef_data["project_id"],
        "default_colletion_id": egpt_rlef_data["default_colletion_id"],
        "chat_details": egpt_rlef_data["chat_details"],
        "data": data,
    }
    print("JSON DATA: ", json_data)
    try:
        future = publisher.publish(topic_path, json.dumps(json_data).encode('utf-8'))
        logging.info(f"Data successfully sent to API: {future.result()}")
    except Exception as e:
        logging.error(f"An error occurred while sending data to API: {e}")


def process_tasks():
    batch_size = Config.PUBSUB_GITHUB_BATCH_SIZE
    batch = []
    while True:
        try:
            task = task_queue.get(timeout=batch_interval)
            batch.append(task)
            if len(batch) >= batch_size:
                task_id = batch[0][0]
                results_batch = process_batch(batch,task_id)
                publish_to_pubsub(results_batch, task_id)
                batch.clear()
        except Queue.Empty:
            if batch:
                task_id = batch[0][0]
                results_batch = process_batch(batch,task_id)
                publish_to_pubsub(results_batch, task_id)
                batch.clear()


threading.Thread(target=process_tasks, daemon=True).start()


def dir_structure_to_json(folder_path: Union[str, Path], git_url: str, task_id: str, egpt_rlef_data) -> Union[str, None]:
    """
    Generates a JSON file representing the directory structure of the specified folder.

    Parameters:
        folder_path (Union[str, Path]): The path to the folder whose directory structure needs to be converted to JSON.

    Returns:
        Union[str, None]: The path where the JSON file is stored, or None if an error occurs.
    """
    folder_path = Path(folder_path)
    output_file_name = folder_path.name + ".json"
    output_path = folder_path / output_file_name
    directory_structure: Dict[str, Union[str, List[Dict]]] = _generate_dir_structure(folder_path, git_url, task_id,egpt_rlef_data)
    directory_structure['git_url'] = git_url
    directory_structure['branch'] = 'main'

    # Write the directory structure to the JSON file
    with open(output_path, "w") as json_file:
        json.dump(directory_structure, json_file, indent=2)
    json_file.close()
    task_queue.put((task_id, git_url, "end", "end", egpt_rlef_data))
    return str(output_path)


def get_repo_info_v2(github_url, batch_size, task_id, egpt_rlef_data):
    Config.PUBSUB_GITHUB_BATCH_SIZE = batch_size
    cloned_repo_path = None
    try:
        curr_dir = os.getcwd()
        cloned_repo_path = clone_github_repo(github_url, curr_dir, task_id)
        json_file_path = dir_structure_to_json(cloned_repo_path, github_url, task_id,egpt_rlef_data)

        with open(json_file_path, 'r') as file:
            json_data = json.load(file)
        if os.path.exists(json_file_path):
            os.remove(json_file_path)
        json_data['task_id'] = task_id
        return json_data
    except Exception as e:
        logging.error(f"An error occurred inside get_repo_info: {e}")
        print(traceback.format_exc())
        return None
    finally:
        if cloned_repo_path and os.path.exists(cloned_repo_path):
            try:
                logging.info("Deleting the cloned repository...")
                shutil.rmtree(cloned_repo_path)
            except Exception as e:
                logging.error(f"An error occurred while deleting the cloned repository: {e}")
