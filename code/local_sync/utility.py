import json
import config
import requests
import traceback
from time import sleep

from .enums import SyncEventFormat
from models import project_summary_db, ecg_db
from absolute_path.parser import absolute_parser
from helper_func_2 import get_complete_dependency_data
from sync_project_summary.local_sync.pubsub_helper import PubSubHelper
from sync_project_summary.smart_sync_ft_hierarchy import FeatureSynchronizer
from sync_project_summary.local_sync.alloydb_ingestion import AlloyDBTableManager


import re
from urllib.parse import urlparse

def get_formatted_github_url(github_url: str, PAT: str = None) -> str:
    try:
        # Handle SSH URLs like git@github.com:owner/repo.git
        ssh_pattern = r'^git@github\.com:(.+)/(.+)\.git$'
        match = re.match(ssh_pattern, github_url)
        if match:
            owner, repo = match.group(1), match.group(2)
            if PAT:
                return f"https://{PAT}@github.com/{owner}/{repo}"
            else:
                return f"https://github.com/{owner}/{repo}"

        # Handle HTTPS URLs
        if github_url.endswith('.git'):
            github_url = github_url.rsplit('.', 1)[0]

        parsed_url = urlparse(github_url)
        if 'github.com' not in parsed_url.netloc:
            raise ValueError("Not a valid GitHub URL")

        path_parts = parsed_url.path.strip('/').split('/')
        if len(path_parts) < 2:
            raise ValueError("Invalid GitHub repository URL format")

        owner, repo = path_parts[0], path_parts[1]
        if PAT:
            return f"https://{PAT}@github.com/{owner}/{repo}"
        else:
            return f"https://github.com/{owner}/{repo}"

    except Exception as e:
        return {"error": str(e)}


def get_task_id_local_sync(repo_url: str, branch_name: str) -> str:
    """
    Get the task ID for a given repository URL and branch name.
    
    Args:
        repo_url (str): The URL of the repository.
        branch_name (str): The name of the branch.
    
    Returns:
        str: The task ID if found, otherwise an empty string.
    """
    repo_url = get_formatted_github_url(repo_url)
    try:
        # Create two possible URL formats for searching
        repo_url_with_git = repo_url if repo_url.endswith('.git') else f"{repo_url}.git"
        repo_url_without_git = repo_url[:-4] if repo_url.endswith('.git') else repo_url

        filter_ = {
            'repo_urls': {
            '$in': [repo_url_with_git, repo_url_without_git]
            }, 
            'branch_names': {
            '$in': [branch_name]
            }
        }
        
        project = {
            'task_id': 1
        }
        
        sort = [('_id', -1)]
        limit = 1  # the most recent
        
        result = list(project_summary_db.projects.find(
            filter=filter_,
            projection=project,
            sort=sort,
            limit=limit
        ))

        if result and result[0].get('task_id') and isinstance(result[0]['task_id'], str):
            return result[0]['task_id']
        else:
            raise ValueError("Task ID not found in the result.")
            
    except Exception as e:
        print(f"An error occurred while fetching the task ID: {e}")
        traceback.print_exc()
        raise ValueError(f"An error occurred while fetching the task ID: {e}")


def get_language_used(user_id: str, github_url: str, branch_name: str) -> list[str]:
    try:
        url = f"{config.APPMOD_DOMAIN}/utility/dependency-mapping/get_repo_languages"

        payload = json.dumps({
                "user_id": user_id,
                "github_url": github_url,
                "branch_name": branch_name
            })
        headers = {
        'Content-Type': 'application/json'
        }

        response = requests.post(url, headers=headers, data=payload, timeout=20)
        return response.json().get('language',[])
    except Exception as e:
        print(f"An error occurred while fetching languages used in the repository: {e}")
        traceback.print_exc()
        raise Exception(f"An error occurred while fetching languages used in the repository: {e}")


def local_sync_dependency_graph(user_id:str, task_id:str, github_url:str, branch_name:str, local_sync_changes):
    
    old_dependency_graph, status = get_complete_dependency_data(task_id, data_type='all')
    if status==200:
        ## fetch the languages used for the repository (As it has github dependency we use the base branch)
        languages_used = get_language_used(user_id, github_url, branch_name)
        
        dependencies, status_code = absolute_parser(github_url, branch_name, languages_used, old_dependency_graph[0].get('data', []), local_sync_changes)
        if status_code == 200:
                return dependencies, status_code
        else:
            raise Exception(f"Failed to parse dependencies. Status code: {status_code}")
    
    else:
        raise Exception(f"Failed to fetch dependency graph from RLEF for task ID {task_id}. Status code: {status}")
    



def local_sync_feature_hierarchy(task_id:str, operations:list):
    try:
        initial_data = project_summary_db.projects.find_one(filter={'task_id': task_id},
        projection={
            'feature_view.feature_hierarchy': 1
        }) 
        updated_feature_hierarchy = FeatureSynchronizer.sync_feature_hierarchy(
                    initial_data, operations
                )

        return {"feature_hierarchy": updated_feature_hierarchy}, 200
    except Exception as e:
        traceback.print_exc()
        raise ValueError(f"An error occurred while syncing feature hierarchy: {e}") from e



def local_sync_event_sse_helper(sync_request_id: str):

    def fetch_document():
        try:
            document = ecg_db.user_local_syncs.find_one(
                {
                    "sync_request_id": sync_request_id
                },
                {
                    "_id": 0,
                    "complete_status": 1
                }
            )
            print("Fetched document:", document, "sync_request_id:", sync_request_id)
            return document
        except Exception as e:
            print("An error occurred in fetch_document:", str(e))
            traceback.print_exc()
            return None

    try:
        while True:
            document = fetch_document()

            if document is None:
                raise ValueError(f"Document not found for sync_request_id: {sync_request_id}")

            msg_to_send = SyncEventFormat(
                message="log dummy example...",
                event="sync_log",
                status_code=200
            )
            yield msg_to_send.model_dump()

            if not document or not document.get("complete_status"):
                sleep(5) # wait 10 seconds if document is None
            else:
                complete_status = document.get("complete_status")
                is_completed = complete_status.get("is_completed", False)
                is_failed = complete_status.get("is_failed", False)

                if is_completed or is_failed:
                    if is_failed:
                        error_message = complete_status.get("message", "Unknown error")
                        msg_to_send = SyncEventFormat(
                            status="failed",
                            message=error_message,
                            event="sync_failed",
                            status_code=500
                        )
                        yield msg_to_send.model_dump()
                    else: # means is_failed is False and we can safely return success event
                        msg_to_send = SyncEventFormat() # default values will be used
                        yield msg_to_send.model_dump()

                    break

                sleep(10) # wait 2 mins

    except ValueError as ve:
        print("ValueError in trigger_endpoint_ecg_sse_helper:", str(ve))
        yield SyncEventFormat(
            status="failed",
            message=str(ve),
            event="error",
            status_code=400
        ).model_dump()

    except Exception as e:
        print("An error occurred in trigger_endpoint_ecg_sse_helper:")
        traceback.print_exc()
        yield SyncEventFormat(
            status="failed",
            message=str(e),
            event="error",
            status_code=500
        ).model_dump()

def local_sync_update_folder_paths(repo_url:str, operations: list):
    try:
        project_name = repo_url.split("/")[-1].replace(".git", "")
        updated_folder_paths = {}
        for event in operations:
            if event.get('type')=="added":
                updated_folder_paths.update({
                    project_name+"/"+event.get("file_path"):{
                        "summary": "",
                        "wta": "",
                        "wnta": ""
                    } 
                })
        return updated_folder_paths, 200

    except Exception as e:
        print("An error occurred in local_sync_update_folder_paths:", str(e))
        traceback.print_exc()
        return {"error": str(e)}, 500


"""
output format of above function
{
    "status": "success",
    "message": "success",
    "event": "sync_completed" (or "sync_failed" or "sync_log"),
    "status_code": 200 (or 500),
}
"""


def generate_sse_local_sync(sync_request_id):
    try:
        for event_data in local_sync_event_sse_helper(sync_request_id):
            yield f"data: {json.dumps(event_data)}\n\n"
    except Exception as e:
        traceback.print_exc()
        error_data = SyncEventFormat(
            status="failed",
            message=str(e),
            event="error",
            status_code=500
        ).model_dump()
        yield f"data: {json.dumps(error_data)}\n\n"



def bg_thread_embedding_creation(operations, task_id, sync_id, repo_url, branch_name):

    def create_payload(operations):
        d = {}
        for old_d in operations:
            file_path = old_d.get("file_path", "")
            content = old_d.get("content", "")
            status = old_d.get("type", "")

            d[file_path] = {
                "path": file_path,
                "content": content,
                "status": status,
            }

        return {"local_changed_files": d}


    try:
        # Payload creation
        my_payload = create_payload(operations)
        my_payload["task_id"] = task_id
        my_payload["url"] = repo_url
        my_payload["batch_size"] = 10
        my_payload["branch_name"] = branch_name
        my_payload["extraction_type"] = "local_sync"

        # with open("local_sync_payload.json", "w") as f:
        #     json.dump(my_payload, f, indent=4)

        print("Payload created for local sync", task_id)
        # input("check")

        # Handle deleted Files
        # Delete embeddings in the cloned table
        deleted = []
        for key, value in my_payload.get("local_changed_files", {}).items():
            if my_payload.get("status") == "deleted":
                deleted.append(key)
        if deleted:
            table_name = f"{config.ORGANIZATION_NAME}-local-sync-{sync_id}"
            alloydbmanager = AlloyDBTableManager(table_name)
            alloydbmanager.update_deleted_file_on_alloydb(deleted)
        
        print("embeddings deleted from cloned table for files:", deleted)
        # input("check")

        # Create embeddings of new local files in separate table
        pubsub_helper = PubSubHelper(
            sync_id=sync_id, # creation based on sync_id
            task_id=task_id,
            payload=my_payload
        )

        pubsub_helper.run()

        print("ebeddings created for local sync files in cloned table")
        # input("check")

        return "success", 200

    except Exception as e:
        print(f"An error occurred in bg_thread_embedding_creation: {e}")
        traceback.print_exc()
        return "error: " + str(e), 500

