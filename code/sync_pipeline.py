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
from crashanalytics import log_into_bigquery
from db_update import tracking_error_in_pipeline, update_batch_summaries_in_db, \
    update_failed_executive_summary_status_in_db, update_feature_hierarchyhierarchy_in_db, update_file_paths_in_db
from email_utils import send_completion_mail, send_pipeline_error_email
from generalised_utils import remove_repo_folder
from helper_func import generate_combined_json_strings
from ingest_project_summary import fetch_email_status
from logger import info_logger, error_logger, warning_logger
from sync_project_summary.helpers import _initialise_resync_project, check_if_resync_supported, \
    extract_rlef_collection_ids, sync_repositories_egpt, sync_rlef_file_summaries
import analyser_repo_pipeline.feature_hierarchy_generation_pipeline.FeatureHierarchy as fh
from sync_project_summary.db_helpers import extract_file_paths, get_intial_analysis_data, update_old_data_in_softsync
from sync_project_summary.smart_sync_ft_hierarchy import FeatureSynchronizer

from analysis_status_update import update_step_status, update_task_fields
from utils.analysis_status_enums import StepStatus, PossibleStatuses


from absolute_path.language_support import read_json_from_bucket
from folder_summary_generation.sync_pipeline import _update_folder_summary_json

from db_update import update_folder_summaries_in_db, save_to_ecg_db
from analyser_repo_pipeline.design_pattern_generation import generate_design_pattern

def _start_resync_thread(task_id, user_id, initial_data, sync_type, git_pat_token, active_user_id):
    """Start the analysis thread."""
    thread = threading.Thread(
        target=sync_pipeline,
        args=(task_id, user_id, initial_data, sync_type, git_pat_token, active_user_id),
    )
    thread.start()

def sync_pipeline(task_id, user_id, initial_data, sync_type, git_pat_token, active_user_id):
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

        info_logger.info(f"## STEP 1: Initialize Project for Task ID: {task_id} ##\n")
        repo_folder = initialize_project(task_id)

        info_logger.info(f"## STEP 2: Clone Repositories for Task ID: {task_id} ##\n")
        clone_threads = clone_repositories(github_urls, branch_names, git_pat_token, repo_folder, user_id, task_id)
        local_repo_paths = [thread.join() for thread in clone_threads]
        info_logger.info(f"Local Repo Paths: {local_repo_paths}")

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.CLONING.value,
            status=PossibleStatuses.COMPLETED.value
        )

        info_logger.info(f"## STEP 3: Get Current Commit Hashes for Task ID: {task_id} ##\n")
        commit_hashes = [get_current_commit_hash(repo_path) for repo_path in local_repo_paths]

        info_logger.info(f"## STEP 4: Sending Request to EGPT Node Sync API for Task ID: {task_id} ##\n")
        sync_threads = sync_repositories_egpt(task_id=task_id, github_urls=github_urls, branch_names=branch_names,
                                              git_pat_token=git_pat_token, assistant_id=chatbot_name)
        sync_thread_results = [thread.join() for thread in sync_threads]
        info_logger.info(f"Sync Thread Results: {sync_thread_results}")

        info_logger.info(f"## STEP 5: Extracting Collection IDs for Task ID: {task_id} and Sync Type: {sync_type} ##\n")
        collection_ids = extract_rlef_collection_ids(sync_results=sync_thread_results, sync_type=sync_type,
                                                     task_id=task_id, user_id=user_id, chatbot_name=chatbot_name,
                                                     github_urls=github_urls,
                                                     old_rlef_ids=initial_data.get("rlef_ids", []),
                                                     project_name=project_name)
        info_logger.info(f"Collection IDs: {collection_ids}")

        # collection_ids =['67856c4085055f72e56dbb7c']
        info_logger.info(
            f"## STEP 6: Syncing RLEF File Summaries for Task ID: {task_id} and Sync Type: {sync_type} ##\n")
        updated_batch_summary_list, updated_file_summary_list, updated_rlef_data_list = sync_rlef_file_summaries(
            user_id=user_id, task_id=task_id, collections_ids=collection_ids, initial_data=initial_data,
            sync_type=sync_type,chatbot_name= chatbot_name)

        info_logger.info(
            f"## STEP 6.1: Updating folder summaries: {task_id} and Sync Type: {sync_type} ##\n")
        folder_summary_path = initial_data.get("dashboard", {}).get("folder_summaries")
        folder_summary_json = read_json_from_gcs(folder_summary_path,config.GCP_BUCKET_BASE_PATH)

        updated_folder_summary_json,_status = _update_folder_summary_json(folder_summary_json,
                                                                          updated_file_summary_list,
                                                                          initial_data.get('git_info', []))

        info_logger.info(f"## STEP 7: Generating Component Summaries for "
                         f"Task ID: {task_id} and Sync Type: {sync_type} ##\n")

        if sync_type == "soft":
            sync_component_generation_for_soft_sync(task_id=task_id, user_id=user_id, initial_data=initial_data,
                                                    sync_type=sync_type, git_pat_token=git_pat_token,
                                                    github_urls=github_urls,
                                                    file_summary_list=updated_file_summary_list,
                                                    folder_summary_json=updated_folder_summary_json,
                                                    rlef_data_list=updated_rlef_data_list,
                                                    active_user_id=active_user_id)
        else:
            sync_component_generation_for_hard_sync(user_id=user_id, task_id=task_id, local_repo_paths=local_repo_paths,
                                                    github_urls=github_urls, git_pat_token=git_pat_token,
                                                    collection_ids=collection_ids,
                                                    batch_summary_list=updated_batch_summary_list,
                                                    file_summary_list=updated_file_summary_list,
                                                    folder_summary_json=updated_folder_summary_json,
                                                    active_user_id=active_user_id, initial_data=initial_data, current_commit_hashs=commit_hashes)

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
        os.chdir(intial_dir)
        remove_repo_folder(repo_folder)


def sync_component_generation_for_soft_sync(task_id, user_id, initial_data, sync_type, git_pat_token, github_urls,
                                            file_summary_list,folder_summary_json, rlef_data_list, active_user_id):
    try:
        info_logger.info("Starting Soft Sync Component Generation")

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FILES_SUMMARIZED.value,
            status=PossibleStatuses.IN_PROGRESS.value
        )

        update_old_data_in_softsync(initial_data, task_id, user_id)
        # changes_files_list = extract_file_paths(rlef_data_list)
        # old_feature_hierarchy = initial_data.get("feature_view", []).get("feature_hierarchy", [])
        # fh_instance = fh.FeatureHierarchy(file_summary_list, task_id=task_id)
        # updated_feature_hierarchy = fh_instance.update_feature_data(task_id=task_id,
        #                                                             old_feature_hierarchy=old_feature_hierarchy,
        #                                                             files_changed=changes_files_list)
        # updated_feature_hierarchy = FeatureSynchronizer.sync_feature_hierarchy(initial_data.get('branch_names',['main'])[0], task_id)

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FILES_SUMMARIZED.value,
            status=PossibleStatuses.COMPLETED.value
        )

        def background_tasks(folder_summaries):
            try:
                update_folder_summaries_in_db(user_id, task_id, folder_summaries)
                save_to_ecg_db(folder_summaries, task_id)
                generate_design_pattern(task_id=task_id, data=folder_summaries)
            except Exception as e:
                error_logger.error(f"Error in background tasks: {str(e)}")

        info_logger.info("## Updated folder summaries in database and created components "
                         "for ECG with design pattern and reusable components ##")

        # Start background tasks in a separate thread
        ThreadPoolExecutor().submit(background_tasks, folder_summary_json)
        # update_feature_hierarchyhierarchy_in_db(user_id, task_id, updated_feature_hierarchy, True, "error_message",
        #                                         "Claude 3.5", "system_prompt", "user_prompt")
        # Create a wrapper function to store the result
        def process_feature_hierarchy():
            nonlocal updated_feature_hierarchy
            updated_feature_hierarchy = FeatureSynchronizer.sync_feature_hierarchy(
                initial_data
            )

        # Initialize variable to store result
        updated_feature_hierarchy = {}
        
        # Create and start thread
        feature_hierarchy_thread = threading.Thread(target=process_feature_hierarchy)
        feature_hierarchy_thread.start()
        feature_hierarchy_thread.join()  # Wait for thread to complete
        
        if not isinstance(updated_feature_hierarchy, dict) and isinstance(updated_feature_hierarchy, str):
            error_message = updated_feature_hierarchy
            info_logger.error(f"Error in generating feature hierarchy: {error_message}")
            update_feature_hierarchyhierarchy_in_db(
                user_id, task_id, updated_feature_hierarchy, False, error_message,
                "claude 3.5 sonnet", "system_prompt", "user_prompt"
            )

        ## update the updated feature hierarchy in GCS and its file path to mongo 
        update_feature_hierarchyhierarchy_in_db(user_id, task_id, updated_feature_hierarchy, True, '', 'claude 3.5 sonnet', 'system_prompt', 'user_prompt')
        
        
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.EMAIL_SENDING.value,
            status=PossibleStatuses.IN_PROGRESS.value,
        )
        email_response = fetch_email_status(user_id, task_id)
        email_status = email_response.get("email_status")
        email_id = email_response.get("email_id")
        analysis_type = email_response.get("analysis_type")

        if email_status and email_id and analysis_type:
            info_logger.info(f"Email status is True and Email ID is present for user_id: {user_id}, task_id: {task_id}")
            send_completion_mail(task_id, user_id, email_id, analysis_type)
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.EMAIL_SENDING.value,
                status=PossibleStatuses.COMPLETED.value,
            )

        else:
            if not email_status:
                if email_status is None:
                    info_logger.info(f"Email status is missing for user_id: {user_id}, task_id: {task_id}")
                    update_step_status(
                            task_id=task_id,
                            step_name=StepStatus.EMAIL_SENDING.value,
                            status=PossibleStatuses.WARNING.value,
                            info=f"Email status is missing for user_id: {user_id}, task_id: {task_id}"
                        )
                else:
                    info_logger.info(f"Email status is False for user_id: {user_id}, task_id: {task_id}")
                    update_step_status(
                        task_id=task_id,
                        step_name=StepStatus.EMAIL_SENDING.value,
                        status=PossibleStatuses.WARNING.value,
                        info=f"Email status is False for user_id: {user_id}, task_id: {task_id}"
                    )

            if not email_id:
                info_logger.info(f"Email ID is missing for user_id: {user_id}, task_id: {task_id}")
                update_step_status(
                    task_id=task_id,
                    step_name=StepStatus.EMAIL_SENDING.value,
                    status=PossibleStatuses.WARNING.value,
                    info=f"Email ID is missing for user_id: {user_id}, task_id: {task_id}"
                )

        increment_analyze_count(user_id=active_user_id, field="attempted_sync_count")

    except Exception as e:
        error_logger.error(f"Error in sync_component_generation_for_soft_sync: {e}")
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FILES_SUMMARIZED.value,
            status=PossibleStatuses.FAILED.value,
            error=str(e)
        )
        raise e


def sync_component_generation_for_hard_sync(user_id, task_id, local_repo_paths, github_urls, git_pat_token,
                                            collection_ids, batch_summary_list, file_summary_list,folder_summary_json,
                                            active_user_id, initial_data, current_commit_hashs):
    
    from analyser_repo_pipeline.api_helpers import get_setting_config
    from server import mongo_reddis_config
    settings_config = get_setting_config(mongo_reddis_config, task_id)
    project_analyzer_config = settings_config['category_config']['project_analyzer']
    print(f"SETTINGS for {task_id}:\n{project_analyzer_config}" )
    intial_dir = os.getcwd()
    skip_remaining_steps = False

    try:
        info_logger.info("Starting Hard Sync Component Generation")

        info_logger.info(f"## STEP 7.1 Fetching Repository Languages for Task ID: {task_id} ##\n")

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.LANGUAGE.value,
            status=PossibleStatuses.IN_PROGRESS.value
        )

        repo_language_list = get_repository_languages(task_id, user_id, github_urls, git_pat_token)

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.LANGUAGE.value,
            status=PossibleStatuses.COMPLETED.value
        )

        info_logger.info(f"## STEP 7.2: Performing Vulnerability Checks for Task ID: {task_id} ##\n")

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.VULNERABILITY_CHECK.value,
            status=PossibleStatuses.IN_PROGRESS.value
        )

        vulnerability_threads = perform_vulnerability_checks(user_id, task_id, local_repo_paths, github_urls)

        def monitor_vulnerability_checks():
            vulnerability_checks = [thread.join() for thread in vulnerability_threads]

        monitor_thread = threading.Thread(target=monitor_vulnerability_checks)
        monitor_thread.start()

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.VULNERABILITY_CHECK.value,
            status=PossibleStatuses.COMPLETED.value
        )
        
        info_logger.info(f"## STEP 7.3: Uploading Dependencies for Task ID: {task_id} ##\n")

        depenency_ref, dependency_graph, status_code = upload_dependencies(user_id, task_id, git_pat_token,
                                                                           collection_ids)
        try:
            file_path = get_file_paths(github_urls, depenency_ref)
            dependency_graph = remove_keys_recursively(dependency_graph, ["content"])
            update_file_paths_in_db(task_id, file_path)
        except Exception as e:
            error_logger.error("Error: ", str(e))
            error_logger.error(f"Dependency response : {dependency_graph}")
            log_into_bigquery("upload_dependencies", user_id, task_id, str(e))

        info_logger.info(f"## STEP 7.4: Generating Services for Task ID: {task_id} ##\n")

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.REPOSITORY_SERVICES.value,
            status=PossibleStatuses.IN_PROGRESS.value
        )
        combined_services_json = generate_services(user_id, task_id, dependency_graph, file_summary_list, status_code, folder_summary_json, model_data=project_analyzer_config['model_config']['generate_services'])
        print(f"Combined Services Generated: {combined_services_json}")

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.REPOSITORY_SERVICES.value,
            status=PossibleStatuses.COMPLETED.value
        )

        info_logger.info(f"## STEP 7.5: Generating Executive Summary for Task ID: {task_id} ##\n")

        update_step_status(
            task_id=task_id,
            step_name=StepStatus.EXECUTIVE_SUMMARY.value,
            status=PossibleStatuses.IN_PROGRESS.value
        )

        executive_summary, executive_summary_score, executive_summary_status, executive_summary_error_messages = get_executive_summary(
            user_id, task_id, github_urls, batch_summary_list, folder_summary_list=folder_summary_json, is_folder_mapping=True, model_data=project_analyzer_config['model_config']['executive_summary'])

        if not executive_summary_status:
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.EXECUTIVE_SUMMARY.value,
                status=PossibleStatuses.FAILED.value,
                error=str(executive_summary_error_messages)
            )
        else:
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.EXECUTIVE_SUMMARY.value,
                status=PossibleStatuses.COMPLETED.value
            )

        info_logger.info("## STEP 7.6: Updating folder summaries in database and creating components for ECG...")
        # Create a ThreadPoolExecutor for background tasks
        def background_tasks(folder_summaries):
            try:
                update_folder_summaries_in_db(user_id, task_id, folder_summaries)
                save_to_ecg_db(folder_summaries, task_id)
                generate_design_pattern(task_id=task_id, data=folder_summaries)
            except Exception as e:
                error_logger.error(f"Error in background tasks: {str(e)}")

        info_logger.info("## Updated folder summaries in database and created components "
                         "for ECG with design pattern and reusable components ##")

        # Start background tasks in a separate thread
        ThreadPoolExecutor().submit(background_tasks, folder_summary_json)

        if not executive_summary_status:
            print("Error: Unable to get executive summary")
            print(f"Error Messages: {executive_summary_error_messages}")
            update_failed_executive_summary_status_in_db(task_id, executive_summary_error_messages)
            skip_remaining_steps = True

        if not skip_remaining_steps:
            combined_json_strings = generate_combined_json_strings(combined_services_json)

            info_logger.info(f"## STEP 7.7: Generating Architecture for Task ID: {task_id} ##\n")

            update_step_status(
                task_id=task_id,
                step_name=StepStatus.ARCHITECTURE_DIAGRAM.value,
                status=PossibleStatuses.IN_PROGRESS.value
            )

            final_architecture = get_complete_v2_architecture(task_id,
                                                              executive_summary + f"<services> {str(combined_json_strings)} </services> ", model_data = project_analyzer_config['model_config']["architecture_diagram"])
            info_logger.info(f"Final Architecture: {final_architecture}")

            update_step_status(
                task_id=task_id,
                step_name=StepStatus.ARCHITECTURE_DIAGRAM.value,
                status=PossibleStatuses.COMPLETED.value
            )

            info_logger.info(f"## STEP 7.8: Generating Feature Summary for Task ID: {task_id} ##\n")

            update_step_status(
                task_id=task_id,
                step_name=StepStatus.FEATURE_SUMMARY.value,
                status=PossibleStatuses.IN_PROGRESS.value
            )

            feature_summary, feature_summary_score, summary_prompt_used = get_feature_summary(user_id, task_id,
                                                                                                  file_summary_list,
                                                                                                  batch_summary_list,
                                                                                                  folder_summary_json=folder_summary_json, executive_summary=executive_summary,model_data=project_analyzer_config['model_config']['feature_summary'])

            update_step_status(
                task_id=task_id,
                step_name=StepStatus.FEATURE_SUMMARY.value,
                status=PossibleStatuses.COMPLETED.value
            )

            info_logger.info(f"## STEP 7.9: Generating Feature Hierarchy for Task ID: {task_id} ##\n")

            update_step_status(
                task_id=task_id,
                step_name=StepStatus.FEATURE_HIERARCHY.value,
                status=PossibleStatuses.IN_PROGRESS.value
            )

            # Create a wrapper function to store the result
            def process_feature_hierarchy():
                nonlocal updated_feature_hierarchy
                updated_feature_hierarchy = FeatureSynchronizer.sync_feature_hierarchy(
                    initial_data
                )

            # Initialize variable to store result
            updated_feature_hierarchy = {}
            
            # Create and start thread
            feature_hierarchy_thread = threading.Thread(target=process_feature_hierarchy)
            feature_hierarchy_thread.start()
            feature_hierarchy_thread.join()  # Wait for thread to complete
            
            if not isinstance(updated_feature_hierarchy, dict) and isinstance(updated_feature_hierarchy, str):
                error_message = updated_feature_hierarchy
                info_logger.error(f"Error in generating feature hierarchy: {error_message}")
                update_feature_hierarchyhierarchy_in_db(
                    user_id, task_id, updated_feature_hierarchy, False, error_message,
                    "claude 3.5 sonnet", "system_prompt", "user_prompt"
                )

            ## update the updated feature hierarchy in GCS and its file path to mongo 
            update_feature_hierarchyhierarchy_in_db(user_id, task_id, updated_feature_hierarchy, True, '', 'claude 3.5 sonnet', 'system_prompt', 'user_prompt')
            
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.FEATURE_HIERARCHY.value,
                status=PossibleStatuses.COMPLETED.value
            )
            
            info_logger.info(
                f"## STEP 8: Waiting for Feature Summary and Feature Hierarchy Threads to Complete for Task ID: {task_id} ##\n")
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.EMAIL_SENDING.value,
                status=PossibleStatuses.IN_PROGRESS.value,
            )
            email_response = fetch_email_status(user_id, task_id)
            email_status = email_response.get("email_status")
            email_id = email_response.get("email_id")
            analysis_type = email_response.get("analysis_type")

            if email_status and email_id and analysis_type:
                info_logger.info(
                    f"Email status is True and Email ID is present for user_id: {user_id}, task_id: {task_id}")
                send_completion_mail(task_id, user_id, email_id, analysis_type)
                update_step_status(
                    task_id=task_id,
                    step_name=StepStatus.EMAIL_SENDING.value,
                    status=PossibleStatuses.COMPLETED.value,
                )

            else:
                if not email_status:
                    if email_status is None:
                        info_logger.info(f"Email status is missing for user_id: {user_id}, task_id: {task_id}")
                        update_step_status(
                            task_id=task_id,
                            step_name=StepStatus.EMAIL_SENDING.value,
                            status=PossibleStatuses.WARNING.value,
                            info=f"Email status is missing for user_id: {user_id}, task_id: {task_id}"
                        )
                    else:
                        info_logger.info(f"Email status is False for user_id: {user_id}, task_id: {task_id}")
                        update_step_status(
                            task_id=task_id,
                            step_name=StepStatus.EMAIL_SENDING.value,
                            status=PossibleStatuses.WARNING.value,
                            info=f"Email status is False for user_id: {user_id}, task_id: {task_id}"
                        )

                if not email_id:
                    info_logger.info(f"Email ID is missing for user_id: {user_id}, task_id: {task_id}")
                    update_step_status(
                        task_id=task_id,
                        step_name=StepStatus.EMAIL_SENDING.value,
                        status=PossibleStatuses.WARNING.value,
                        info=f"Email ID is missing for user_id: {user_id}, task_id: {task_id}"
                    )
            
            update_step_status(
                task_id=task_id,
                step_name=StepStatus.EMAIL_SENDING.value,
                status=PossibleStatuses.COMPLETED.value,
            )
            
        info_logger.info(f"## STEP 9: Completed Sync Pipeline and Updating Sync count for Task ID: {task_id} ##\n")
        increment_analyze_count(user_id=active_user_id, field="attempted_sync_count")
    except Exception as e:
        error_logger.error(f"Error in sync_component_generation_for_hard_sync: {e}")
        update_step_status(
            task_id=task_id,
            step_name=StepStatus.FEATURE_HIERARCHY.value,
            status=PossibleStatuses.FAILED.value,
            error=str(e)
        )
        raise e


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


def get_folder_summary_gcs_path(task_id: str, gcs_file_path:str)-> str:
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