import os
import ast
import git
import json
import time
import uuid
import shutil
import base64
import Config
import chardet
import logging
import requests
import traceback
import threading
import queue as Queue

from pathlib import Path
from utils.utils import exception_handling
from typing import Dict, Union, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from Enums.model_type import (
    google_model_name,
    google_region,
)
from data_extraction.utils.github_repo_utils import (
    is_supported_file,
    get_supported_extenions,
    # writing_single_batchdata_to_file,
    extract_accurate_summary
)
from data_extraction.analysis_status_update import update_step_status, update_task_fields
from data_extraction.Enums.analysis_status_enums import StepStatus, PossibleStatuses
from data_extraction.codeChunker import CodeChunker
from data_extraction.dataExtractionBase import ExtractionBase
from data_extraction.codeFileSummaryGeneartion import CodeSummarizer
from data_extraction.utils.github_repo_pubsub import publish_to_pubsub
from data_extraction.utils.github_repo_bigquery_logger import log_into_bigquery
from data_extraction.Enums.github_repo_enums import ExtractionTypes, FileStatus, TaskObject, ChunkOutput
from data_extraction.analysis_status_update import update_step_status, update_task_fields

from data_extraction.dbFileSummaryGeneration import DBSummarizer
from data_extraction.dbChunker import DBChunker

class DatabaseExtraction(ExtractionBase):
    def __init__(
        self, 
        task_id: str, 
        github_url: str, 
        output_folder: str = os.getcwd(), 
        branch_name: str = "main",
        model_name: str = google_model_name.RESPONSE_GEMINI_2_FLASH.value,
        batch_size: int = 25,
        initial_commit_hash: str = "",
        extraction_type: str = ExtractionTypes.complete.value,
        region: str = google_region.US_CENTRAL1.value
    ):
        self.github_url = github_url
        self.output_folder = output_folder
        self.task_id = task_id
        self.branch_name = branch_name
        self.model_name = model_name if model_name else google_model_name.RESPONSE_GEMINI_2_FLASH.value
        self.batch_size = batch_size
        self.initial_commit_hash = initial_commit_hash
        self.extraction_type = extraction_type
        self.region = region
        self.task_queue = Queue.Queue()
        self.summarizer = DBSummarizer(
            model_name=self.model_name,
            region=self.region,
            task_id=task_id
        )
        self.file_summarized_count = 0
        self.files_chunked_count = 0
        self.files_got_chunked_count = 0
        self.files_skipped_count = 0
        self.error_messages = []

    def connectToDataSource(self, *args, **kwargs):
        pass

    def clone_github_repo(self):
        """
        Clones a GitHub repository from the given URL and stores it in the output folder.

        Parameters:
            github_url (str): The URL of the GitHub repository to clone.
            output_folder (str): The path to the folder where the repository will be cloned.
            task_id (str): The ID of the task for logging and analytics purposes.
            brach_name (str): the branch name which needs to be cloned

        Returns:
            str: Local path of the cloned repository.
        """

        logging.debug("*" * 15 + "Inside clone_github_repo" + "*" * 15)

        # Define the path to store the cloned repository
        base_path = os.path.join(self.output_folder, "Repos", self.task_id)
        try:
            logging.info(
                f"github_url : {self.github_url}, output_folder: {self.output_folder}, task_id: {self.task_id}, branch_name: {self.branch_name}"
            )
            
            # Ensure the base path exists
            os.makedirs(base_path, exist_ok=True)
            if self.github_url.endswith(".git"):
                github_url = self.github_url[:-4]

            repo_name = github_url.split("/")[-1].split(".")[0]
            
            logging.info(f"repo_name: {repo_name}")
            
            local_repo_path = os.path.join(base_path, repo_name)
            logging.info(f"local_repo_path : {local_repo_path}")
            
            if os.path.exists(local_repo_path):
                shutil.rmtree(local_repo_path)

            req_url = github_url.split("github.com/")
            api_call = "https://api.github.com/repos/" + req_url[1]
            tok = req_url[0].split("://")
            req_token = "" if tok[1] == "" else f"Bearer {tok[1][0:-1]}"
            headers = {
                "Accept": "application/vnd.github+json",
                "Authorization": req_token,
                "X-GitHub-Api-Version": "2022-11-28",
            }
            response = requests.get(api_call, headers=headers)
            logging.info(f"response from github repos :{response}")

            if response.status_code == 200:
                # clone specified branch
                os.system(
                    f"git clone -b {self.branch_name} {github_url} {local_repo_path}"
                )  # comment for running on windows
                # use below for running on windows
                # subprocess.run(["git", "clone", "-b", branch_name, github_url, local_repo_path], check=True)
                return local_repo_path
            
            elif response.status_code == 401:
                logging.error("Error : Invalid Token")
                return f"Error : Invalid Token {tok[1][0:-1]}"
            
            elif response.status_code == 404:
                logging.error("Error : Repo Not Found or Access Denied")
                return "Error : Repo Not Found or Access Denied"
        except Exception as e:
            logging.error(f"An error occurred inside clone_github_repo: {traceback.format_exc()}")
            log_into_bigquery(
                "clone_github_repo", "", self.task_id, f"Error while cloning repo: {e}", 500
            )
            raise e
    
    @staticmethod
    def get_changed_files(repo_path: str, initial_commit: str) -> dict:
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
            logging.error(f"Error getting changed files: {traceback.format_exc()}")
            return {}
    
    @staticmethod
    def get_current_commit_hash(repo_path: str) -> str:
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
        self,
        directory_structure: Dict,
        folder_path: Union[str, Path]
    ) -> Union[str, None]:
        """
        Generates a JSON file representing the directory structure of the specified folder.
        """
        logging.debug("*" * 15 + "Inside dir_structure_to_json" + "*" * 15)
        folder_path = Path(folder_path)
        output_file_name = folder_path.name + ".json"
        output_path = folder_path / output_file_name
        logging.info(f"output_path : {output_path}")

        with open(output_path, "w") as json_file:
            json.dump(directory_structure, json_file, indent=2)

        logging.info(f"Adding {FileStatus.end.value} in the task queue.")
        self.task_queue.put(
            TaskObject(
                task_id=self.task_id,
                github_url=self.github_url,
                file_path=FileStatus.end.value,
                file_status=FileStatus.end.value,
                file_content=FileStatus.end.value,
                chunk_number=100000
            )
        )
        return str(output_path)
    
    @staticmethod
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

    def extract_sync_files(
        self,
        changed_files: dict,
        chunker: DBChunker
    ):
        update_step_status(
            task_id=self.task_id,
            step_name=StepStatus.FILES_CHUNKED.value,
            status=PossibleStatuses.IN_PROGRESS.value,
            count=self.files_chunked_count,
        )
        for file_path, file_info in changed_files.items():
            file_path_obj = Path(file_path)

            # if not is_supported_file(file_path_obj):
            #     continue
            
            if file_info["status"] == "deleted":
                self.task_queue.put(
                    TaskObject(
                        task_id=self.task_id,
                        github_url=self.github_url,
                        file_path=file_path,
                        file_status=FileStatus.deleted.value,
                        chunk_number=0,
                    )
                )
            else:
                try:
                    content = self.read_file_content(file_path_obj)
                    chunked_content, cost, llm_call_count = chunker.run(
                        codeFileString=content,
                        codeFileName=file_path,
                    )
                    self.files_chunked_count += 1

                    update_step_status(
                        task_id=self.task_id,
                        step_name=StepStatus.FILES_CHUNKED.value,
                        status=PossibleStatuses.IN_PROGRESS.value,
                        count=self.files_chunked_count,
                    )
                    logging.info(f"File {file_path} has {len(chunked_content)} chunks")
                    is_chunked = len(chunked_content) > 1
                    if is_chunked:
                        self.files_got_chunked_count += 1 
                        update_task_fields(
                            task_id=self.task_id,
                            total_files_chunked=self.files_got_chunked_count,
                        )
                    
                    file_id = str(uuid.uuid4()) if is_chunked else ''
                    
                    for chunk_number, chunk_content in enumerate(chunked_content):
                        chunk_id = f"{file_id}_{chunk_number}" if is_chunked else ''
                        self.task_queue.put(
                            TaskObject(
                                task_id=self.task_id,
                                github_url=self.github_url,
                                file_path=str(file_path),
                                file_content=f"{chunk_content}", 
                                file_status=FileStatus.modified.value,
                                file_id=file_id,
                                is_chunked=is_chunked,
                                chunk_id=chunk_id,
                                chunk_number=chunk_number
                            )
                        )
                except FileNotFoundError:
                    logging.error(
                        f"File {file_path} marked as {file_info['status']} but not found"
                    )
                    update_step_status(
                        task_id=self.task_id,
                        step_name=StepStatus.FILES_CHUNKED.value,
                        status=PossibleStatuses.IN_PROGRESS.value,
                        count=self.files_chunked_count,
                        error=f"File {file_path} marked as {file_info['status']} but not found"
                    )
                    continue
            
        update_step_status(
            task_id=self.task_id,
            step_name=StepStatus.FILES_CHUNKED.value,
            status=PossibleStatuses.COMPLETED.value,
            count=self.files_chunked_count,
        )

    def extract_complete_files(
        self,
        files: List[Path],
        chunker: DBChunker,
        supported_extensions: list
    ):
        logging.info(f"Working on files: {files}")
        update_step_status(
            task_id=self.task_id,
            step_name=StepStatus.FILES_CHUNKED.value,
            status=PossibleStatuses.IN_PROGRESS.value,
            count=self.files_chunked_count,
        )
        for file in files:
            logging.info(f"Working on file: {file}")
            if not is_supported_file(file, supported_extensions=supported_extensions):
                logging.info(f"Skipping unsupported file: {file}")
                self.files_skipped_count += 1
                update_step_status(
                    task_id=self.task_id,
                    step_name=StepStatus.FILES_CHUNKED.value,
                    status=PossibleStatuses.IN_PROGRESS.value,
                    count=self.files_chunked_count,
                    error=f"Skipping unsupported file format: {file}, count: {self.files_chunked_count}"
                )
                update_task_fields(
                    task_id=self.task_id,
                    total_files_skipped=self.files_skipped_count,
                )
                continue
            try:
                content = self.read_file_content(file)

                # self.sql_metadata = {k:v for k,v in json.loads(str(content).strip()).items() if k != "ddl"}
                # self.table_info = json.loads(str(content).strip()).get("ddl", "")
                
                if not content:
                    logging.error(f"Could not read content from file {file}")
                    self.files_skipped_count += 1
                    update_step_status(
                        task_id=self.task_id,
                        step_name=StepStatus.FILES_CHUNKED.value,
                        status=PossibleStatuses.IN_PROGRESS.value,
                        count=self.files_chunked_count,
                        error=f"Skipping file due to file encoding issue or empty file: {file}, count: {self.files_chunked_count}"
                    )
                    update_task_fields(
                        task_id=self.task_id,
                        total_files_skipped=self.files_skipped_count,
                    )
                    continue

                chunked_content, cost, llm_call_count = chunker.run(
                    DbFileString=content,
                    DbFileName=str(file),
                )
                logging.info(f"File {str(file)} has {len(chunked_content)} chunks")
                is_chunked = len(chunked_content) > 1

                self.files_chunked_count += 1
                update_step_status(
                    task_id=self.task_id,
                    step_name=StepStatus.FILES_CHUNKED.value,
                    status=PossibleStatuses.IN_PROGRESS.value,
                    count=self.files_chunked_count,
                )
                update_task_fields(
                    task_id=self.task_id,
                    total_files_processed=self.files_chunked_count
                )
                if is_chunked:
                    self.files_got_chunked_count += 1 
                    update_task_fields(
                        task_id=self.task_id,
                        total_files_chunked=self.files_got_chunked_count,
                    )
                
                
                file_id = str(uuid.uuid4()) 
                
                for chunk_number, chunk_content in enumerate(chunked_content):
                    chunk_id = f"{file_id}_{chunk_number}" if is_chunked else ''
                    logging.info(f"Putting Chunk {chunk_number} in the task queue for file {str(file)}.")
                    self.task_queue.put(
                        TaskObject(
                            task_id=self.task_id,
                            github_url=self.github_url,
                            file_path=str(file),
                            file_content=f"{chunk_content}", 
                            file_status=FileStatus.new.value,
                            file_id=file_id,
                            is_chunked=is_chunked,
                            chunk_id=chunk_id,
                            chunk_number=chunk_number,
                            max_chunk_count=len(chunked_content)
                        )
                    )
            except FileNotFoundError:
                logging.error(f"File {file} not found, skipping.")
                update_step_status(
                    task_id=self.task_id,
                    step_name=StepStatus.FILES_CHUNKED.value,
                    status=PossibleStatuses.IN_PROGRESS.value,
                    count=self.files_chunked_count,
                    error=f"File {file} not found, skipping."
                )
                continue
            except Exception as e:
                logging.error(f"Error processing file {file}: {traceback.format_exc()}")
                update_step_status(
                    task_id=self.task_id,
                    step_name=StepStatus.FILES_CHUNKED.value,
                    status=PossibleStatuses.IN_PROGRESS.value,
                    count=self.files_chunked_count,
                    error=f"Error processing file {file}: {traceback.format_exc()}")
                continue
                
        update_step_status(
            task_id=self.task_id,
            step_name=StepStatus.FILES_CHUNKED.value,
            status=PossibleStatuses.COMPLETED.value,
            count=self.files_chunked_count,
        )
        update_task_fields(
            task_id=self.task_id,
            total_files_processed=self.files_chunked_count
        )

    def _generate_dir_structure(
        self,
        path: Path,
        supported_extensions: Dict,
        changed_files: Dict = None,
    ) -> Dict[str, Union[str, List[Dict]]]:
        try:
            result: Dict[str, Union[str, List[Dict]]] = {
                "name": path.name,
                "type": "folder",
                "children": [],
            }
            # chunker = CodeChunker(modelName=self.model_name)
            chunker = DBChunker()
            update_step_status(
                task_id=self.task_id,
                step_name=StepStatus.FILES_CHUNKED.value,
                status=PossibleStatuses.IN_PROGRESS.value,
                count=0
            )

            if self.extraction_type == ExtractionTypes.sync.value and changed_files:
                self.extract_sync_files(
                    changed_files=changed_files,
                    chunker=chunker
                )

            elif self.extraction_type == ExtractionTypes.complete.value:
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

                self.extract_complete_files(
                    files=files,
                    chunker=chunker,
                    supported_extensions=supported_extensions
                )

                for directory in directories:
                    result["children"].append(
                        self._generate_dir_structure(
                            directory,
                            supported_extensions,
                            changed_files
                        )
                    )

            return result

        except Exception as e:
            exception_handling(
                "Error in _generate_dir_structure",
                self.task_id,
                self.github_url,
                str(e),
                traceback.format_exc(),
            )
            log_into_bigquery(
                "generate_dir_structure", "", self.task_id, str(e.__traceback__), 500
            )

    def process_tasks(self, current_commit_hash):
        batch = []
        processed_files = set()  # Track all processed files across batches
        results_batch = []
        while not self.task_queue.empty():
            try:
                task: TaskObject = self.task_queue.get(timeout=Config.PUBSUB_GITHUB_BATCH_INTERVAL)
                # if (
                #     isinstance(task, TaskObject)
                #     and task.file_path == FileStatus.end.value
                #     and task.file_content == FileStatus.end.value
                #     and task.file_status == FileStatus.end.value
                # ):
                #     task = TaskObject(
                #         task_id=task.task_id,
                #         github_url=task.github_url,
                #         file_path=FileStatus.end.value,
                #         file_content=FileStatus.end.value,
                #         file_status=FileStatus.end.value
                #     )

                file_key = f"{task.github_url}:{task.file_path}:{task.chunk_id}" if task.is_chunked else f"{task.github_url}:{task.file_path}"
                logging.info(f"File Key: {file_key}")

                if file_key in processed_files and task.file_status != FileStatus.end.value:
                    logging.info(f"Skipping duplicate file across batches: {task.file_path}")
                    continue

                processed_files.add(file_key)

                if task.is_chunked:
                    # If the file is chunked, process it separately in its own batch
                    chunked_batch = [task] 

                    temp_stack = []
                    # Use a loop to collect all chunks for the current file
                    while len(chunked_batch) < task.max_chunk_count:
                        try:
                            next_task: TaskObject = self.task_queue.get(timeout=Config.PUBSUB_GITHUB_BATCH_INTERVAL)
                            logging.info(f"Github Url: {next_task.github_url}, File Path: {next_task.file_path}, Chunked or not: {next_task.is_chunked}")
                            if next_task.github_url == task.github_url and next_task.file_path == task.file_path and next_task.is_chunked:
                                chunked_batch.append(next_task)
                            else:
                                # If the next task is not part of this chunked file, put it back in queue
                                temp_stack.append(next_task)
                        except Queue.Empty:
                            logging.warning(f"Queue got exmpty breaking the queu loop for task_id: {task_id}, file: {task.file_path}, chunks count in the batch: {len(chunked_batch)}")
                            break  # Exit loop if the queue is empty

                    # Reinsert any tasks that were temporarily removed back into the queue
                    for temp_task in reversed(temp_stack):
                        self.task_queue.put(temp_task)

                    logging.info(f"Processing chunked batch of size {len(chunked_batch)} for {task.file_path}")
                    
                    try:
                        results_batch = self.process_batch_deduplicated(chunked_batch)
                    except Exception as e:
                        error_message = f"Error occured while summarizing chunks for task_id: {self.task_id}, file: {task.file_path}, github_url: {task.github_url} with traceback:\n{traceback.format_exc()}" 
                        logging.error(error_message)
                        update_step_status(
                            task_id=self.task_id,
                            step_name=StepStatus.FILES_SUMMARIZED.value,
                            status=PossibleStatuses.IN_PROGRESS.value,
                            error=error_message,
                            count=self.file_summarized_count
                        )
                        continue
                    
                    # writing_single_batchdata_to_file(results_batch, "data_list_summaries.json")  # For local testing
                    logging.info(f"\n\nPUBLISHING TO PUBSUB : \n\n{results_batch}\n\n")

                    publish_to_pubsub(results_batch, self.task_id, self.extraction_type, current_commit_hash)
                    
                else:
                    # Process non-chunked files in regular batches
                    batch.append(task)

                    if (len(batch) >= self.batch_size) or self.task_queue.empty():
                        batch : List[TaskObject]
                        
                        logging.info(f"Processing batch of size {len(batch)} for task_id {task.task_id}")
                        
                        try:
                            results_batch = self.process_batch_deduplicated(batch)
                        except Exception as e:
                            error_message = f"Error occured while summarizing file for task_id: {self.task_id}, file: {task.file_path}, github_url: {task.github_url} with traceback:\n{traceback.format_exc()}"
                            logging.error(error_message)
                            update_step_status(
                                task_id=self.task_id,
                                step_name=StepStatus.FILES_SUMMARIZED.value,
                                status=PossibleStatuses.IN_PROGRESS.value,
                                error=error_message,
                                count=self.file_summarized_count
                            )
                            continue

                        # writing_single_batchdata_to_file(results_batch, "data_list_summaries.json")  # For local testing
                        logging.info(f"\n\nPUBLISHING TO PUBSUB : \n\n{results_batch}\n\n")
                        publish_to_pubsub(results_batch, task.task_id, self.extraction_type, current_commit_hash)
                        
                        batch.clear()
            
            except Queue.Empty:
                if batch:
                    logging.info(f"Processing batch of size {len(batch)} for task_id {task.file_path}")
                    task_id = batch[0].task_id
                    try:
                        results_batch = self.process_batch_deduplicated(batch)
                    except Exception as e:
                        error_message = f"Error occured while summarizing file for task_id: {self.task_id}, file: {task.file_path}, github_url: {task.github_url} with traceback:\n{traceback.format_exc()}"
                        logging.error(error_message)
                        update_step_status(
                            task_id=self.task_id,
                            step_name=StepStatus.FILES_SUMMARIZED.value,
                            status=PossibleStatuses.IN_PROGRESS.value,
                            error=error_message,
                            count=self.file_summarized_count
                        )
                        continue

                    # writing_single_batchdata_to_file(results_batch, "data_list_summaries.json")  # For local testing
                    logging.info(f"\n\nPUBLISHING TO PUBSUB : \n\n{results_batch}\n\n")
                    publish_to_pubsub(results_batch, task_id, self.extraction_type, current_commit_hash)
                    
                    batch.clear()
            except Exception as e:
                error = f"Something failed in the Summarization process: {traceback.format_exc()}, ending summarization."
                tracking_error_in_pipeline(self.task_id, error)
                update_step_status(
                    task_id=self.task_id,
                    step_name=StepStatus.FILES_SUMMARIZED.value,
                    status=PossibleStatuses.FAILED.value,
                    count=self.file_summarized_count,
                    error=error
                )
                log_into_bigquery(
                    "get_repo_info",
                    "",
                    self.task_id,
                    error,
                    500,
                )
                # End result batch
                results_batch.append(
                    ChunkOutput(
                        task_id=self.task_id,
                        git_url="",
                        file=FileStatus.end.value,
                        content=FileStatus.end.value,
                        summary=FileStatus.end.value,
                        file_id=FileStatus.end.value,
                        chunk_id=FileStatus.end.value,
                        update_type=FileStatus.end.value,
                        is_chunked=False,
                        chunk_number=0,
                        cost=0.0,
                        time_taken=0.0,
                        chunk_details={},
                        file_stats={
                            "functions_count": 0,
                            "classes_count": 0,
                            "markuptags_count": 0,
                            "code_lines_count": 0,
                            "total_chunks_count": 0,
                            "global_variables_count": 0,
                            "tables_count": 0,
                            "markup_tags_count": 0
                        }
                    )
                )
                publish_to_pubsub(results_batch, self.task_id, self.extraction_type, current_commit_hash)
                raise e
            
            # self.file_summarized_count += len(results_batch)
            logging.info(f"Total files summarized so far: {self.file_summarized_count}")
            update_step_status(
                task_id=self.task_id,
                step_name=StepStatus.FILES_SUMMARIZED.value,
                status=PossibleStatuses.IN_PROGRESS.value,
                count=self.file_summarized_count,
            )
            update_task_fields(
                task_id=self.task_id,
                total_files_summarized=self.file_summarized_count
            )
        
        update_step_status(
            task_id=self.task_id,
            step_name=StepStatus.FILES_SUMMARIZED.value,
            status=PossibleStatuses.COMPLETED.value,
            count=self.file_summarized_count,
        )
        update_task_fields(
                task_id=self.task_id,
                total_files_summarized=self.file_summarized_count
            )


    def process_chunked_bacth_deduplicated(self, chunked_batch: List[TaskObject]):
        logging.info(f"BATCH SIZE: {len(chunked_batch)} for git_url {chunked_batch[0].github_url}")
        
        if not all(task.task_id == self.task_id for task in chunked_batch):
            logging.error("Inconsistent task IDs in batch")

        deduplicated_batch = []
        seen_files = set()

        for task in chunked_batch:
            task: TaskObject
            file_key = f"{task.github_url}:{task.file_path}:{task.chunk_id}" if task.is_chunked else f"{task.github_url}:{task.file_path}"

            if task.file_status == FileStatus.end.value:
                deduplicated_batch.append(task)
                continue

            if file_key in seen_files:
                logging.info(f"Skipping duplicate file within batch: {task.file_path}")
                continue

            seen_files.add(file_key)
            deduplicated_batch.append(task)

            logging.info(
                f"Task ID: {task.task_id}, Git URL: {task.github_url}, File Name: {task.file_path}, Status: {task.file_status}"
            )
            if task.task_id != self.task_id:
                logging.error(f"Task ID mismatch: {task.task_id} != {self.task_id}")
                self.task_queue.put(
                    TaskObject(
                        task_id=task.task_id,
                        github_url=task.github_url,
                        file_path=task.file_path,
                        file_content=task.file_content,
                        file_status=task.file_status
                    )
                )

        # sort basis on chunk number in case not sorted already
        deduplicated_batch: List[TaskObject] = sorted(deduplicated_batch, key=lambda x: x.chunk_number)
    
        #### Getting metadata for chunked files
        query_details = self.__class__.read_file_content(task.file_path)
        logging.info(f"QUERY DETAILS(Chunked file): {query_details}")
        if not query_details:
            logging.info("Query details is not found")
            sql_metadata = {}
        else:
            sql_metadata = {k: v for k, v in json.loads(query_details.strip()).items() if k != "ddl"}
            logging.info(f"Metadata for chunked file: {sql_metadata}")

        all_results = self.summarizer.run(deduplicated_batch,sql_metadata=sql_metadata)
        self.file_summarized_count += 1
        logging.info(f"Final chunked batch Results: {all_results}")
        return all_results
        
    def process_batch_deduplicated(self, batch: List[TaskObject]):
        logging.info(f"BATCH SIZE: {len(batch)} for git_url {batch[0].github_url}")
        
        if not all(task.task_id == self.task_id for task in batch):
            logging.error("Inconsistent task IDs in batch")

        deduplicated_batch = []
        seen_files = set()

        for task in batch:
            task: TaskObject
            file_key = f"{task.github_url}:{task.file_path}:{task.chunk_id}" if task.is_chunked else f"{task.github_url}:{task.file_path}"

            if task.file_status == FileStatus.end.value:
                deduplicated_batch.append(task)
                continue

            if file_key in seen_files:
                logging.info(f"Skipping duplicate file within batch: {task.file_path}")
                continue

            seen_files.add(file_key)
            deduplicated_batch.append(task)

            logging.info(
                f"Task ID: {task.task_id}, Git URL: {task.github_url}, File Name: {task.file_path}, Status: {task.file_status}"
            )
            if task.task_id != self.task_id:
                logging.error(f"Task ID mismatch: {task.task_id} != {self.task_id}")
                self.task_queue.put(
                    TaskObject(
                        task_id=task.task_id,
                        github_url=task.github_url,
                        file_path=task.file_path,
                        file_content=f"{task.file_content}",
                        file_status=task.file_status
                    )                    
                )

        all_results = []

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self.process_task, task): task for task in deduplicated_batch
            }

            for future in as_completed(futures):
                result: ChunkOutput = future.result()
                if result:
                    all_results.append(result.model_dump())
                  
        logging.info(f"Final Results: {all_results}")
        return all_results

    def process_task(self, task: TaskObject):
        try:
            start = time.time()
            logging.info(f"Processing file {task.file_path} (Chunk {task.chunk_number}) for task_id {self.task_id}") if task.is_chunked else logging.info(f"Processing file {task.file_path} for task_id {self.task_id} (Not Chunked)")
            logging.info(f"Task Object {task} inside process_task")

            query_details = self.__class__.read_file_content(task.file_path)
            logging.info(f"QUERY DETAILS: {query_details}")
            if not query_details:
                logging.info("Query details is not found")
                sql_metadata = {}
            else:
                sql_metadata = {k: v for k, v in json.loads(query_details.strip()).items() if k != "ddl"}
                logging.info(f"Metadata: {sql_metadata}")
            stripped_file_name = str(
                task.file_path.split(self.task_id, 1)[1].lstrip('/') if self.task_id in str(task.file_path) else str(task.file_path))

            if task.file_status == FileStatus.deleted.value:
                return ChunkOutput(
                        task_id=task.task_id,
                        git_url=task.github_url,
                        file=stripped_file_name,
                        file_id=task.file_id,
                        chunk_id=task.chunk_id,
                        update_type=FileStatus.deleted.value,
                        is_chunked=task.is_chunked,
                        chunk_number=task.chunk_number,
                        cost=0,
                        time_taken=time.time() - start
                    )

            if not task.file_content.strip():
                logging.warning(f"Skipping file {stripped_file_name} as it has empty content")
                return None
            

            if task.file_status == FileStatus.end.value:
                logging.info(f"Received the {FileStatus.end.value} value.")
                return ChunkOutput(
                    task_id=task.task_id,
                    git_url=task.github_url,
                    file=stripped_file_name,
                    content=f"{task.file_content}",
                    summary=FileStatus.end.value,
                    file_id=task.file_id,
                    chunk_id=task.chunk_id,
                    update_type=task.file_status,
                    is_chunked=task.is_chunked,
                    chunk_number=task.chunk_number,
                    cost=0.0,
                    time_taken=0.0,
                    chunk_details={
                        "columns":{},
                            #  "column_name": {
                            #     "primary_keys": [],
                            #     "foreign_keys": [],
                            #     "composite_keys": [],
                            #     "other_columns": []
                            # },
                            "services": {
                                "external_services" : [],
                                "internal_services" : []
                            }
                    },
                    file_stats={
                        "functions_count": 0,
                        "classes_count": 0,
                        "markuptags_count": 0,
                        "code_lines_count": 0,
                        "total_chunks_count": 0,
                        "global_variables_count": 0,
                        "tables_count": 0,
                        "markup_tags_count": 0
                    }
                )


            chunk_summary, cost = self.summarizer._generate_summary(
                table_info="",
                column_chunk_info=f"{task.file_content}",
                metadata="",
                type="column"
            )
            
            try:
                # chunk_summary_json = json.loads(chunk_summary)
                # column_descriptions = []
                # logging.info(f"COLUMNS DESCRIPTIONS \n\n{column_descriptions}\n\n")
                # logging.info(f"Chunk Summry : \n\n{chunk_summary}\n\n")
                chunk_summary_dict: dict = json.loads(chunk_summary)
                # chunk_summary_list: list[dict] = chunk_summary_json.get("columns", "") 
                # chunk_cost += cost
                # chunk_cost = chunk_cost
                summary = json.dumps(chunk_summary_dict)
                
                # Add column_descriptions for Chunk Details
                column_descriptions = []
                # primary_key_descriptions = []
                # foreign_key_descriptions = []
                # composite_key_descriptions = []
                # indexed_column_descriptions = []
                # not_null_constraint_descriptions = []
                # check_constraint_descriptions = []
                # unique_key_constraint_descriptions = []
                # default_key_constarint_descriptions = []
                # other_key_descriptions = []
                for col_description in chunk_summary_dict.get("columns", []):
                    col_data = {
                        "name": str(col_description["column_name"]).strip(),
                        "end": str(col_description["end"]).strip(),
                        "description": str(col_description["column_description"]).strip(),
                        "start": str(col_description["start"]).strip(),
                        "is_primary_key": str(col_description["is_primary_key"]).strip(),
                        "is_foreign_key": str(col_description["is_foreign_key"]).strip(),
                        "is_composite_key": str(col_description["is_composite_key"]).strip(),
                        "is_indexed": str(col_description["is_indexed"]).strip(),
                        "is_not_null_key": str(col_description["is_not_null_key"]).strip(),
                        "is_check_key": str(col_description["is_check_key"]).strip(),
                        "is_unique_key": str(col_description["is_unique_key"]).strip(),
                        "is_default_key": str(col_description["is_default_key"]).strip()
                    }
                    column_descriptions.append(col_data)

                # logging.info(f"\n\n=====================\n\nColumn Descriptions : \n{column_descriptions}\n\n")
                # Add to appropriate lists based on key type
                    logging.info(f"Col descriptions {col_description}")
                    logging.info(f"Col data {col_data}")
                    # if str(col_description.get("is_primary_key", "")) == "True":
                    #         primary_key_descriptions.append(col_data)
                    # if str(col_description.get("is_foreign_key", "")) == "True":
                    #     foreign_key_descriptions.append(col_data)
                    # if str(col_description.get("is_composite_key", "")) == "True":
                    #     composite_key_descriptions.append(col_data)
                    # if str(col_description.get("is_indexed", "")) == "True":
                    #     indexed_column_descriptions.append(col_data)
                    # if str(col_description.get("is_not_null_key", "")) == "True":
                    #     not_null_constraint_descriptions.append(col_data)
                    # if str(col_description.get("is_check_key", "")) == "True":
                    #     check_constraint_descriptions.append(col_data)
                    # if str(col_description.get("is_unique_key", "")) == "True":
                    #     unique_key_constraint_descriptions.append(col_data)
                    # if str(col_description.get("is_default_key", "")) == "True":
                    #     default_key_constarint_descriptions.append(col_data)
                    # if (str(col_description.get("is_primary_key", "")) == "False"
                    #     and str(col_description.get("is_foreign_key", "")) == "False"
                    #     and str(col_description.get("is_composite_key", "")) == "False"
                    #     and str(col_description.get("is_indexed", "")) == "False"
                    #     and str(col_description.get("is_not_null_key", "")) == "False"
                    #     and str(col_description.get("is_check_key", "")) == "False"
                    #     and str(col_description.get("is_unique_key", "")) == "False"
                    #     and str(col_description.get("is_default_key", "")) == "False"
                    # ):
                    #     other_key_descriptions.append(col_data)

                # if count == len(chunks)-1:  # Last chunk
                #     logging.info(f"Last chunk of file {task.file_path} reached. Generating table summary...")
                #     # Generate Table Descriptions here for last chunk:
                #     table_summary, cost = self._generate_summary(
                #         previous_chunk_summary=f"Cumulative Summary : {cumulative_summary}\nFull Content : {full_content}",
                #         metadata=sql_metadata,
                #         type="table"
                #     )

                    # logging.info(f"Table Summary : {table_summary}")
                    # table_summary = json.loads(table_summary)
                    # table_description = table_summary.get("table_description", "")
                    # # for key in ["table_description", "table_name", "foreign_keys", "primary_keys"]:
                    # for key in table_summary.keys():
                    #     try:
                    #         chunk_summary_dict[key] = table_summary[key]
                    #     except KeyError:
                    #         logging.info(f"{key} not found. Proceeding without it.")
                    
                    # logging.info(f"Final chunk summary dict: \n{chunk_summary_dict}")

                # for col_description in chunk_summary_dict.get("columns", []):
                #     cumulative_summary += str(col_description["column_description"]).strip() + " \n"
                # logging.info(f"Cumulative Summary : {cumulative_summary}")


            except json.decoder.JSONDecodeError as e:
                try:
                    logging.warning(f"Json parsing failed for the generated summary: trying ast.literal_val")
                    chunk_summary: dict = ast.literal_eval(chunk_summary)
                except Exception as e:
                    logging.warning(f"Json parsing failed for the generated summary: {chunk_summary}")
                    self.file_summarized_count += 1
                    return ChunkOutput(
                            task_id=task.task_id,
                            git_url='',
                            file=task.file_path,
                            file_id=task.file_id,
                            chunk_id=task.chunk_id,
                            update_type=FileStatus.new.value,
                            # summary=f"""File: {stripped_file_name}\n\n```summary\n{chunk_summary}\n```""",
                            summary = summary,
                            content=f"{task.file_content}",
                            chunk_details={
                                    "columns": column_descriptions,
                                    # {

                                    #     "primary_keys": primary_key_descriptions,
                                    #     "foreign_keys": foreign_key_descriptions,
                                    #     "composite_keys": composite_key_descriptions,
                                    #     "indexed_columns": indexed_column_descriptions,
                                    #     "not_null_constraints": not_null_constraint_descriptions,
                                    #     "check_constraints": check_constraint_descriptions,
                                    #     "unique_constraints": unique_key_constraint_descriptions,
                                    #     "default_constrinats": default_key_constarint_descriptions,
                                    #     "other_columns": other_key_descriptions
                                    # },
                                    "metadata": sql_metadata,
                                    "services": {
                                        "external_services": [],
                                        "internal_services": []
                                    }
                                },
                            chunk_number=task.chunk_number,
                            cost=cost,
                            time_taken=time.time() - start,
                            file_stats={
                                "functions_count": 0,
                                "classes_count": 0,
                                "markup_tags_count": 0,
                                "global_variables_used_count": 0,
                                "global_variables_not_used_count": 0,
                                "code_lines_count": 0,
                                "total_chunks_count": 0,
                                "tables_count": 0
                            },
                            chunk_type="file",
                            is_chunked=task.is_chunked
                        )
            
            combined_result = (
                f"""File: {stripped_file_name} (Chunk {task.chunk_number})\n\n```summary\n{chunk_summary_dict.get("chunk_description")}\n```"""
                if task.is_chunked
                else f"""File: {stripped_file_name}\n\n```summary\n{chunk_summary_dict.get("chunk_description")}\n```"""
            )
            end = time.time()
            time_taken = end - start

            logging.info(f"\n\nCOLUMNS DESCRIPTIONS : \n{column_descriptions}\n\n")

            # if not isinstance(chunk_summary.get("code_artifacts", {}), dict):
            #     chunk_summary["code_artifacts"] = {}
            # if not isinstance(chunk_summary.get("services", {}), dict):
            #     chunk_summary["services"] = {}
             
            self.file_summarized_count += 1
            return ChunkOutput(
                        task_id=task.task_id,
                        git_url=task.github_url,
                        file=stripped_file_name,
                        content=f"{task.file_content}",
                        summary=combined_result,
                        file_id=task.file_id,
                        chunk_id=task.chunk_id,
                        update_type=task.file_status,
                        is_chunked=task.is_chunked,
                        chunk_number=task.chunk_number,
                        cost=cost,
                        time_taken=time_taken,
                        chunk_details={
                                "columns": column_descriptions,
                                # "column_names": {
                                #     "primary_keys": primary_key_descriptions,
                                #     "foreign_keys": foreign_key_descriptions,
                                #     "composite_keys": composite_key_descriptions,
                                #     "indexed_columns": indexed_column_descriptions,
                                #     "unique_constraints": unique_key_constraint_descriptions,
                                #     "default_constrinats": default_key_constarint_descriptions,
                                #     "other_columns": other_key_descriptions
                                # },
                                "metadata": sql_metadata,
                                "services": {}
                            },
                        file_stats={
                            "column_count": len(column_descriptions),
                        #    "primary_key_count" : len(primary_key_descriptions),
                        #     "foreign_key_count" : len(foreign_key_descriptions),
                        #     "composite_key_count" : len(composite_key_descriptions),
                        #     "indexed_column_count" : len(indexed_column_descriptions),
                        #     "unique_key_count" : len(unique_key_constraint_descriptions),
                        #     "default_key_count" : len(default_key_constarint_descriptions),
                        #     "other_key_count" : len(other_key_descriptions),
                            # "markup_tags_count": len(chunk_summary.get("code_artifacts", {}).get('markup_tags', [])),
                            # "code_lines_count": len(task.file_content.splitlines()),
                            # "total_chunks_count": 0,
                            # "global_variables_used_count": len(chunk_summary.get("code_artifacts", {}).get('global_variables', {}).get('used', [])),
                            # "global_variables_not_used_count": len(chunk_summary.get("code_artifacts", {}).get('global_variables', {}).get('not_used', [])),
                            # "tables_count": len(chunk_summary.get("code_artifacts", {}).get('tables', []))
                        }
                    )

        except Exception as e:
            logging.error(f"Error processing file {task.file_path}: {traceback.format_exc()}")
            log_into_bigquery(
                "process_task", "", task.task_id, f"Error processing file {task.file_path}: {traceback.format_exc()}", 500
            )
            return None
        
    def getData(self) -> Dict:
        cloned_repo_path = ''
        current_commit_hash = ''
        logging.info(
            f"Starting github repo extraction with Model Name : {self.model_name}"
        )
        try:
            # Clone the github repo
            cloned_repo_path = self.clone_github_repo()
            logging.info(f"cloned_repo_path : {cloned_repo_path}")
            logging.info(f"Got the request for extraction type: {self.extraction_type}")

            changed_files = {}
            # get the latest commit hash
            if self.extraction_type == "sync" and self.initial_commit_hash:
                logging.info(
                    f"Got the request for sync mode with initial commit: {self.initial_commit_hash}"
                )
                changed_files = self.get_changed_files(cloned_repo_path, self.initial_commit_hash)
                logging.info(
                    f"Found {len(changed_files)} changed files since commit {self.initial_commit_hash}"
                )
                logging.debug(f"Changed files: {changed_files}")

            current_commit_hash = self.get_current_commit_hash(cloned_repo_path)
            logging.debug(f"Current commit hash: {current_commit_hash}")

            supported_extensions = get_supported_extenions()
            directory_structure: Dict[str, Union[str, List[Dict]]] = self._generate_dir_structure(
                Path(cloned_repo_path), supported_extensions, changed_files
            )
            directory_structure["git_url"] = self.github_url
            directory_structure["branch"] = "main"
            directory_structure["extraction_type"] = self.extraction_type

            if self.extraction_type == "sync":
                directory_structure["changed_files_count"] = (
                    len(changed_files) if changed_files else 0
                )
            logging.debug(f"directory_structure: {directory_structure}")

            json_file_path = self.dir_structure_to_json(
                directory_structure=changed_files,
                folder_path=cloned_repo_path
            )
            logging.debug(f"json_file_path : {json_file_path}")

            logging.info(f"Starting Process Tasks thread... {json_file_path}")
            
            update_step_status(
                task_id=self.task_id,
                step_name=StepStatus.FILES_SUMMARIZED.value,
                status=PossibleStatuses.IN_PROGRESS.value,
                count=self.file_summarized_count
            )

            process_thread = threading.Thread(
                target=self.process_tasks,
                args=(current_commit_hash,),
            )
            process_thread.start()
            process_thread.join()

            folder_path = os.path.dirname(json_file_path)
            
            os.makedirs(folder_path, exist_ok=True)
            
            with open(json_file_path, "r") as file:
                json_data = json.load(file)

            # if os.path.exists(json_file_path):
            #     os.remove(json_file_path)

            json_data["task_id"] = self.task_id
            json_data["extraction_type"] = self.extraction_type
            if self.extraction_type == "sync":
                json_data["initial_commit"] = self.initial_commit_hash

            logging.info("Ending Process Tasks thread...")
            # process_thread.join()
            logging.info(f"json_data: {json_data}")

        except Exception as e:
            logging.error(f"An error occurred inside get_repo_info: {traceback.format_exc()}")
            update_step_status(
                task_id=self.task_id,
                step_name=StepStatus.FILES_SUMMARIZED.value,
                status=PossibleStatuses.FAILED.value,
                error=f"An error occurred inside get_repo_info: {traceback.format_exc()}",
                count=self.file_summarized_count
            )
            log_into_bigquery(
                "get_repo_info",
                "",
                self.task_id,
                f"An error occurred inside get_repo_info: {traceback.format_exc()}",
                500,
            )

        finally:
            logging.info("Cleaning up cloned repository...")
            if cloned_repo_path and os.path.exists(cloned_repo_path):
                try:
                    logging.info("Deleting the cloned repository heree...")
                    shutil.rmtree(cloned_repo_path)
                except Exception as e:
                    logging.error(
                        f"An error occurred while deleting the cloned repository: {traceback.format_exc()}"
                    )

    def json_to_markdown(self, json_data):
        if isinstance(json_data, dict):
            md = ""
            for key, value in json_data.items():
                md += f"### {key}\n\n"
                if isinstance(value, list):
                    for item in value:
                        md += self.json_to_markdown(item) + "\n"
                elif isinstance(value, dict):
                    md += self.json_to_markdown(value) + "\n"
                else:
                    md += f"{value}\n\n"
            return md
        elif isinstance(json_data, list):
            return "\n".join([self.json_to_markdown(item) for item in json_data])
        else:
            return str(json_data)
    