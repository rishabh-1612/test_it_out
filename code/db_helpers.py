from models import project_summary_db
from logger import info_logger, error_logger


def get_intial_analysis_data(task_id: str, user_id: str):
    """
    Get the initial analysis data for the given task_id
    """
    try:
        query = {
            "task_id": task_id, "user_id": user_id
        }

        projection = {
            "_id": 0,
        }

        data = project_summary_db.projects.find_one(query, projection)
        # info_logger.info(f"Initial analysis data for task_id {task_id}: {data}")

        return data
    except Exception as e:
        print(f"Error in get_intial_analysis_data: {e}")
        return None


def update_old_data_in_softsync(intial_data: dict, task_id: str, user_id: str):
    try:

        query = {
            "task_id": task_id, "user_id": user_id
        }
        update_fields = {
            "$set": {
                "dashboard.dependency_graph_support" : intial_data.get("dashboard", {}).get("dependency_graph_support", {}),
                "dashboard.language" : intial_data.get("dashboard", []).get("language", []),
                "dashboard.content_type" : "custome_cs",
                "dashboard.vulnerability_check" : intial_data.get("dashboard", {}).get("vulnerability_check", {}),
                "dashboard.model_used_in_repo_services" : intial_data.get("dashboard", {}).get("model_used_in_repo_services", ""),
                "dashboard.executive_summary_score" : intial_data.get("dashboard", {}).get("executive_summary_score", ""),
                "dashboard.model_used_in_executive_summary" : intial_data.get("dashboard", {}).get("model_used_in_executive_summary", ""),
                "dashboard.original_executive_summary" : intial_data.get("dashboard", {}).get("original_executive_summary", ""),
                "dashboard.updated_executive_summary": intial_data.get("dashboard", {}).get("updated_executive_summary", ""),
                "dashboard.architecture_diagram" : intial_data.get("dashboard", {}).get("architecture_diagram", ""),
                "dashboard.attempts_count" : intial_data.get("dashboard", {}).get("attempts_count", 0),
                "dashboard.default_version" : intial_data.get("dashboard", {}).get("default_version", 1),
                "dashboard.model_used_in_architecture_diagram" : intial_data.get("dashboard", {}).get("model_used_in_architecture_diagram", ""),
                "dashboard.reactflow_architecture_diagram_present" : intial_data.get("dashboard", {}).get("reactflow_architecture_diagram_present", True),
                "dashboard.system_prompt" : intial_data.get("dashboard", {}).get("system_prompt", ""),
                "dependency_ref" : intial_data.get("dependency_ref", []),
                "feature_view_loc" : intial_data.get("feature_view_loc", []),
                "repo_services" : intial_data.get("repo_services", []),
                "feature_view.feature_summary" : intial_data.get("feature_view", {}).get("feature_summary", []),
                "feature_view.feature_summary_score" : intial_data.get("feature_view", {}).get("feature_summary_score", 0),
                "feature_view.model_used_in_summary" : intial_data.get("feature_view", {}).get("model_used_in_summary", ""),
                "feature_view.system_prompt_used_in_summary" : intial_data.get("feature_view", {}).get("system_prompt_used_in_summary", ""),
                "feature_view.user_prompt_used_in_summary" : intial_data.get("feature_view", {}).get("user_prompt_used_in_summary", ""),

            }
        }

        result = project_summary_db.projects.update_one(query, update_fields, upsert=True)
        if result.modified_count:
            info_logger.info(f"Updated old data in softsync for task_id {task_id}")
        elif result.upserted_id:
            info_logger.info(f"Inserted new data in softsync for task_id {task_id}")
        else:
            error_logger.error(f"Error updating old data in softsync for task_id {task_id}")



        update_fields2 = {
            "$set": {
                "completion_status.dashboard.architecture_diagram.status": "completed",
                "completion_status.dashboard.architecture_diagram.error_message": "",
                "completion_status.dashboard.executive_summary.status": "completed",
                "completion_status.dashboard.executive_summary.error_message": "",
                "completion_status.dashboard.executive_summary_score.status": "completed",
                "completion_status.dashboard.executive_summary_score.error_message": "",
                "completion_status.dashboard.language.status": "completed",
                "completion_status.dashboard.language.error_message": "",
                "completion_status.dashboard.vulnerability_check.status": "completed",
                "completion_status.dashboard.vulnerability_check.error_message": "",
                "completion_status.dashboard.repo_services.status": "completed",
                "completion_status.dashboard.repo_services.error_message": "",
                "completion_status.feature_view.feature_summary.status": "completed",
                "completion_status.feature_view.feature_summary.error_message": "",
                "completion_status.feature_view.feature_summary_score.status": "completed",
                "completion_status.feature_view.feature_summary_score.error_message": "",
                }
            }
        result2 = project_summary_db.project_analyzer_task.update_one(query, update_fields2, upsert=True)
        if result2.modified_count:
            info_logger.info(f"Updated old data in softsync for task_id {task_id}")
        elif result2.upserted_id:
            info_logger.info(f"Inserted new data in softsync for task_id {task_id}")
        else:
            error_logger.error(f"Error updating old data in softsync for task_id {task_id}")


    except Exception as e:
        error_logger.error(f"Error in update_old_data_in_softsync: {e}")



def extract_file_paths(data):
    """
    Extracts file paths from the given list of dictionaries where the
    'update_type' is not 'end' or 'new'.

    Args:
        data: A list of dictionaries, where each dictionary has a
              'batch_data' key containing a list of dictionaries,
              and each sub-dictionary has an 'update_type' and 'file_path' key.

    Returns:
        A list of file paths. Returns an empty list if any exception occurs.
    """
    try:
        file_paths = []
        for item in data:
            if not isinstance(item, dict):
                return []  # Return empty list on TypeError
            for file_data in item.get('batch_data', []):
                if not isinstance(file_data, dict):
                    return []  # Return empty list on TypeError
                if file_data.get('update_type') not in ['end', 'new']:
                    file_paths.append(file_data.get('file_path', None))
        return file_paths
    except (KeyError, TypeError):
        return []
