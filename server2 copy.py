from asyncio.log import logger
from collections import defaultdict
import logging
import os
import time
import uuid
import threading
import random
import string
import zipfile
import traceback
import json
import re
import shutil
from io import BytesIO
import redis
from flask_cors import CORS, cross_origin
from sync_project_summary.local_sync.enums import SyncEventFormat
from analyser_repo_pipeline.rlef_helpers import get_data_from_rlef
from analyser_repo_pipeline.alloydb_helpers import AlloyDBConnector
from sync_project_summary.local_sync.alloydb_ingestion import AlloyDBTableManager
from projects_share_pipeline.utils import convert_validity_to_date
from utils.file_retrieval_utils import InputPayload, check_old_file_data, OrderedOutputBasic, OrderedOutputTechnical, convert_old_file_data_to_new, process_data_from_db
from flask import Flask, Response, request, jsonify, make_response, Blueprint, send_file, stream_with_context, url_for, redirect, current_app
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from bson import ObjectId
from absolute_path.language_support import get_supported_languages_dict, load_summarization_rules, read_json_from_bucket, save_summarization_rules, upload_json_to_bucket_from_memory, upload_supported_languages_dict
from absolute_path.parser import absolute_parser
from access_control_pipeline.db_operations import get_pre_ingestion_raw_data
from access_control_pipeline.middleware import process_request
from access_control_pipeline.email_service import send_email_with_body
from access_control_pipeline.email_body import *
from access_control_pipeline.utils import get_name_of_approvers, get_project_details
from analyser_repo_pipeline.db_operations import get_request_data_for_new_analysis, import_another_user_project
from appsec_data_control.get_user_data import fetch_and_store_api_data, get_user_name_from_user_id
from architecture_builder_pipeline.db_operations import get_architecture_versions_list, update_services_in_architecture, update_services_in_architecture_v2
from architecture_builder_pipeline.modernisation_services import get_updated_services_modernisation
from cs_utils import get_vulnerability_checks , parse_list_params
from datetime import datetime, timedelta, timezone
from analyser_repo_pipeline.agent_prompts import  contextaware_tool_settings_orchestrator_prompt
from pymongo import DESCENDING, MongoClient
from sync_project_summary.local_sync.pubsub_helper import PubSubHelper
from db_update import check_email_status_get_api
from generalised_utils import fetch_user_email, get_all_projects, get_project_name, validate_payload
from ingest_project_summary import check_if_repo_supported, delete_assistants, delete_repo_from_db, delete_repo_from_db2, filter_assistant_ids , repo_already_analyzed_v2, update_email_status, update_status_project_analyzer_database, repo_already_analyzed,intilize_project_in_db, start_regeneration_component_process, fetch_analyzed_repos, fetch_user_edit_access_repos
from feedback import change_order_of_features, change_order_of_features_with_ai, executive_summary_feedback, feature_view_feedback, move_miscellaneous_to_sub_features, move_subfeatures_from_source_to_target_feature, update_edited_summary, file_summary_edit, update_edited_feature,approve_rlef_req, v2_update_edited_feature, change_grouping_of_features_with_ai
from helper_func import (
    add_dev_links_to_architecture, check_analysed_repo, check_completion_status, check_github_access, calculate_line_counts, check_repo_analysis_support, fetch_fetch_feature_hierarchy_gcp, format_version_history, get_all_repo_user_v2, get_all_summaries_v2, get_recent_versions_from_project_id, get_task_ids_from_repo_urls, get_version_list_v2, handle_fetch_different_architecture_diagram, mapping_service_name, create_service_class_json, merge_duplicate_groups,get_task_ids_from_repo_urls_v2,
    save_architecture_and_services, save_architecture_and_services_v2, get_architecture_fetch_flow, add_model_settings_to_data,
    get_architecture_generate_flow, filter_nested_json, get_all_repo_user,
    count_files_in_folder,get_readme_content, should_scale, start_regeneration_process, update_old_architecture_schema, v2_egenerate_architecture
)
from crashanalytics import log_into_bigquery, insert_log,extract_task_and_user_id
from email_utils import send_ingestion_request_email, send_report_email, send_support_email, send_email_to_users, send_language_support_email

from analyser_repo_pipeline.llm_service.default_settings import DEFAULT_SETTING_CONFIG
from analyser_repo_pipeline.api_helpers import _cleanup_temporary_files, _extract_request_data, _format_data, _handle_error, _handle_file_upload, _handle_repository_analysis, _initialize_project, _start_analysis_thread, _validate_github_access, _validate_request_v2_ingest_project_summary, _validate_required_headers, _get_uploaded_files_list, delete_pending_ingestion_details, get_files_from_gcp, get_request_status, get_userids_from_emails, prepare_and_count_files, store_approval_details, v2_handle_file_upload, _get_data_to_store, _get_pre_ingestion_data
from analyser_repo_pipeline.setting_helpers import get_setting_config, create_or_update_setting_func, mongo_reddis_config
from analyser_repo_pipeline.crypto_helpers import decrypting_token
from projects_share_pipeline.db_operations import add_shared_project_to_db, check_if_project_shared, check_share_access, get_count_of_doc, get_shared_dashboard_data, get_shared_repo_list, get_shared_repo_list_v2, update_shared_dashboard_data_pipeline

from repo_services import (
    anthropic_service_api_call, anthropic_mermaid_api_call,anthropic_service_reactflow_api_call,
    anthropic_reactflow_to_services_call, anthropic_reactflow_code_conversion, anthropic_architecture_chatbot_call,get_service_names,get_validated_service_ai_response
)
from repo_services_2 import (anthropic_architecture_chatbot_call_v3, get_formatted_architecture_data,handle_cmf,get_suggested_services)
from github_module_branch import create_module_branch
from generate_description import process_readme
from chat_ui_features import generate_uml_documentation, get_module_data_from_db,update_uml_documents_in_db, check_document_already_generated,update_uml_document,update_uml_email_status, reset_db_for_regenerate
import config
from models import bigquery_client,table_id,dataset_id,error_codes, tags_mapper, project_summery_db, user_analytics_extraction_query, egpt_agents_collection, ecg_db
from datetime import datetime
from helper_func_2 import add_to_db, delete_project, delete_project_versions, get_all_shared_repo, get_chatbot_id, get_complete_dependency_data, get_file_summaries, get_pending_repos,get_requested_repo, rearrange_actions, store_ingestion_data_for_approval, updating_reactflow_architecture_diagram,fetch_icons_under_categories, get_emb_model_config
from sync_project_summary.local_sync.utility import bg_thread_embedding_creation, get_task_id_local_sync, local_sync_dependency_graph, local_sync_feature_hierarchy, generate_sse_local_sync, local_sync_sse_combined, local_sync_update_folder_paths
import tempfile
from email_validator import validate_email, EmailNotValidError
from logger import info_logger, warning_logger, error_logger
from projects_share_pipeline.validation import validaiton_share_pipeline
from request_dashboard_pipeline.base_graph import get_base_graphs_data
from request_dashboard_pipeline.user_based_graph import get_user_based_graph_data
from request_dashboard_pipeline.validation import validate_payload_dashboard, validate_payload_dashboard_v2, validate_user_id
from request_dashboard_pipeline.helpers import get_data_for_approver, get_data_for_normal_user
from sync_project_summary.helpers import _initialise_resync_project, _store_pre_sync_data, check_if_resync_supported
from sync_project_summary.sync_pipeline import _start_resync_thread, start_sync_project_summary
from models import project_summary_db
from analyser_repo_pipeline.agent_config import setup_rag_agent_config
from analyser_repo_pipeline.pipeline_helpers import enable_folder_mapping, set_agent_settings
from analyser_repo_pipeline.agent_helper import get_llm_call_type
from pydantic import ValidationError
import psutil
import socket
from werkzeug.datastructures import Headers
from analyser_repo_pipeline.pipeline_helpers import data_analyzer_main, enable_folder_mapping, set_agent_settings

from analyser_repo_pipeline.alloydb_helpers import AlloyDBConnector
# superapprover_data_for_control = {"671682962022933f9345e454": ("riyansh.gupta@techolution.com", "Riyansh Gupta")}
# approver_data_for_control = {"668e74341260186017914baa": ("nisarg.kudgunti@techolution.com", "Nisarg Kudganti")}

app = Flask(__name__)
CORS(app, support_credentials=True)

try:
    # Instantiating AlloyDBConnector here triggers its __init__ method.
    # It will initialize the shared _connection_pool if it hasn't been already.
    # We don't need to store this specific instance, as the pool is class-level.
    AlloyDBConnector(
        database=config.EGPT_ALLOY_DB_NAME,
        username=config.EGPT_ALLOY_DB_USERNAME,
        password=config.EGPT_ALLOY_DB_PASSWORD,
        host=config.EGPT_ALLOY_DB_HOST,
        port=config.EGPT_ALLOY_DB_PORT
    )
    logging.info("AlloyDB connection pool initialized at app creation.")
except Exception as e:
    logging.critical(f"FATAL: Failed to initialize AlloyDB connection pool: {e}")
    # Re-raise the exception to prevent the Flask application from starting
    # if the database connection is absolutely essential.
    raise


app.secret_key = "appmodtest"
superapprover_data_for_control = {}
approver_data_for_control = {}
map_email_to_user_id = {}
map_user_id_to_info = {}


new_user_default_data = [
    {"project_id": "81256188-fcce-46f8-b741-ab1767571328", "access_type": "view"},
    {"project_id": "a3b71588-88cf-4e60-a069-76d0f505cc77", "access_type": "view"},
    {"project_id": "44adceef-d6ae-4cb1-9907-a36cf87d1891", "access_type": "view"},
]

blueprint_prefix = Blueprint('prefix', __name__, url_prefix='/analyzer')


@blueprint_prefix.get("/")
@cross_origin(supports_credentials=True)
def root():
    return {"server running": "true"}, 200





login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'prefix.login'
class User(UserMixin):
    def __init__(self, id, username, password):
        self.id = id
        self.username = username
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


@login_manager.user_loader
def load_user(user_id):
    user_collection = project_summery_db.users
    user_data = user_collection.find_one({'id': user_id})
    if user_data:
        return User(id=user_data['id'], username=user_data['username'], password=user_data['password_hash'])
    return None


@blueprint_prefix.route("/health", methods=["GET"])
def health_check():
    cpu_usage = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    mem_usage = mem.percent
    active_threads = len(psutil.Process().threads())
    cpu_count = psutil.cpu_count()
    load_avg_1m, load_avg_5m, load_avg_15m = map(lambda x: x/cpu_count, psutil.getloadavg())

    # timestamp = datetime.datetime.utcnow().isoformat()
    timestamp = datetime.now().isoformat()
    hostname = socket.gethostname()

    scale_recommendation = should_scale(cpu_usage, mem_usage, active_threads)

    response = {
        "timestamp": timestamp,
        "hostname": hostname,
        "cpu_count": cpu_count,
        "cpu_percent": cpu_usage,
        "load_average": {
            "1min": load_avg_1m,
            "5min": load_avg_5m,
            "15min": load_avg_15m
        },
        "memory_percent": mem_usage,
        "active_threads": active_threads,
        "scale_needed": scale_recommendation,
    }

    status_code = 200 if not scale_recommendation else 206
    return jsonify(response), status_code




@blueprint_prefix.route('/register', methods=['POST'])
def register():
    username = request.form['username']
    password = request.form['password']
    user_id = request.form['user_id'] 
    password_hash = generate_password_hash(password)

    user_collection = project_summery_db.users
    user_collection.insert_one({
        'id': user_id,
        'username': username,
        'password_hash': password_hash
    })
    return 'User registered successfully', 201


@blueprint_prefix.route('/login', methods=['POST'])
def login():
    username = request.form['username']
    password = request.form['password']

    user_collection = project_summery_db.users
    user_data = user_collection.find_one({'username': username})

    if user_data and check_password_hash(user_data['password_hash'], password):
        user = User(id=user_data['id'], username=user_data['username'], password=user_data['password_hash'])
        login_user(user)
        print(f"User {user.username} logged in successfully")  # Debugging log
        return jsonify({"status": "success", "message": "Logged in successfully"}), 200
    else:
        print(f"Invalid login attempt for username {username}")  # Debugging log
        return jsonify({"status": "error", "message": "Invalid credentials"}), 401


@blueprint_prefix.route('/logout', methods=['GET', 'POST'])
def logout():
    logout_user()
    return jsonify({"status": "success", "message": "Logged out successfully"}), 200


# POST endpoint that updates Mongo + refreshes Redis
@blueprint_prefix.route('/setting_config', methods=['POST'])
@cross_origin(supports_credentials=True)
def create_or_update_setting_config():
    try:
        data = request.json
        task_id = data.get("task_id")
        category_config = data.get("category_config")
        available_models = data.get("available_models")
        global_settings = data.get("global_settings")

        if not task_id:
            return jsonify({"error": "task_id is required"}), 400

        
        message, status_code = create_or_update_setting_func(
            mongo_reddis_config, task_id, category_config, available_models, global_settings
        )

        return jsonify({"message": message}), status_code

    except Exception as e:
        logger.error(f"Create/Update settings error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@blueprint_prefix.route('/fetch_setting_config', methods=['GET'])
def fetch_settings_api():
    try:
        task_id = request.args.get('task_id')
        if not task_id:
            return jsonify({"error": "Missing 'task_id' parameter"}), 400

        result = get_setting_config(mongo_reddis_config, task_id)
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Fetch settings error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    
@blueprint_prefix.route('/flush_setting_cache', methods=['POST'])
@cross_origin(supports_credentials=True)
def flush_setting_cache():
    try:
        # Get all keys related to setting_config
        keys = r.keys(f"{REDIS_PREFIX}*")
        if keys:
            r.delete(*keys)  # Bulk delete
            logger.info(f"✅ Flushed {len(keys)} setting_config cache entries.")
            return jsonify({"message": f"Flushed {len(keys)} cache entries."}), 200
        else:
            return jsonify({"message": "No cache entries to flush."}), 200
    except redis.RedisError as re:
        logger.error(f"❌ Redis flush error: {re}")
        return jsonify({"error": str(re)}), 500
    

@blueprint_prefix.route('/ingest-project-summary', methods=['POST'])
@cross_origin(supports_credentials=True)
def ingest_project_summary():
    if request.method != 'POST':
        return jsonify({"error": "Method not allowed"}), 405

    temp_dir = None
    zip_folder = None
    version = 1
    user_id = None
    task_id = None

    try:
        validation_error = _validate_required_headers(request)
        if validation_error:
            return validation_error

        request_data = _extract_request_data(request)
        if isinstance(request_data, tuple):  
            return request_data

        user_id = request_data['user_id']
        task_id = request_data['task_id']
        
        # Can be checked if user has access
        
        result = project_summery_db.cw_user.update_one(
            {"user_id": user_id},
            {"$set": {"onboarding_status": "True"}}
        )

        if result is None:
            print("Something went wrong while updating onboarding status!")

    
        temp_dir = tempfile.mkdtemp(prefix="project_summary_")
        logger.info(f"Created temporary directory: {temp_dir}")

        analysis_result = _handle_repository_analysis(
            request_data['regen'],
            user_id,
            request_data['github_info'],
            request_data['branch_names'],
            task_id,
            request_data['project_name']
        ) # Returns the version, task_id and old_task_id
        if isinstance(analysis_result, tuple):  
            return analysis_result
        
        version = analysis_result.get('version', 1)
        task_id = analysis_result.get('task_id', task_id)
        old_task_id = analysis_result.get('old_task_id')
        project_id = analysis_result.get('project_id')
        
        # From here I have to start the analysis part

        zip_folder = _handle_file_upload(request.files.get('text_pdf_files'), temp_dir)
        # SAVE THE FILE IN TEMP FOLDERx
        uploaded_file_count = count_files_in_folder(temp_dir) if zip_folder else 0
        
        github_access_result = _validate_github_access(
            user_id,
            task_id,
            request_data['github_token'],
            request_data['git_url']
        )
        if isinstance(github_access_result, tuple): 
            return github_access_result
        repo_supported,_,_, _ = check_if_repo_supported( request_data['git_url'],
            request_data['github_token'],
            request_data['batch_size'],
            request_data['branch_names'])
        if not repo_supported:
            return jsonify({
                "error": "Repository is not supported. Token Limit is exceeded."
            }), 400

        assistant_id, chatbot_name = _initialize_project(
            user_id,
            task_id,
            request_data,
            old_task_id,
            uploaded_file_count,
            version,
            project_id
        )

        _start_analysis_thread(
            user_id,
            task_id,
            request_data,
            assistant_id,
            temp_dir,
            chatbot_name
        )

        return jsonify({
            "user_id": user_id,
            "task_id": task_id,
            "status": False
        }), 200

    except Exception as e:
        error_message = f"Project summary ingestion error: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_message)
        _handle_error(user_id, task_id, error_message, request_data['analysis_type'], request_data)


        return jsonify({"error": "Internal server error"}), 500

    finally:
        _cleanup_temporary_files(zip_folder, temp_dir)

        
@blueprint_prefix.route('/ingest_project_request', methods=['POST'])
@cross_origin(supports_credentials=True)
def ingest_project_request():
    zip_folder = None
    try:
        email = request.form.get("email",None)
        user_name = request.form.get("user_name", None)
        if not user_name: 
            return jsonify({"error":"user_name not found"}), 422
        emailinfo = validate_email(email, check_deliverability=False)
        email = emailinfo.normalized
        
        validation_error = _validate_required_headers(request)
        if validation_error:
            return validation_error
        
        request_data = _extract_request_data(request)
        if isinstance(request_data, tuple):  
            return request_data
        
        # validation_error, _, _ = validate_payload(request_body=request_data, key_types={})
        # if validation_error:
        #     return {}


        # info_logger.info(f"Request Data: {request_data}")
        user_id = request_data['user_id']
        task_id = request_data['task_id']
        
        
        github_access_result = _validate_github_access(
            user_id,
            task_id,
            request_data['github_token'],
            request_data['git_url']
        )
        if isinstance(github_access_result, tuple): 
            return github_access_result
        
        repo_supported, number_of_files, number_of_tokens, approx_output_cost =check_if_repo_supported(
            request_data['git_url'],
            request_data['github_token'],
            request_data['batch_size'],
            request_data['branch_names']
        )
        if not repo_supported:
            return jsonify({
                "error": "Repository is not supported. Token Limit is exceeded."
            }), 400
        
        analysis_result = _handle_repository_analysis(
            request_data['regen'],
            user_id,
            request_data['github_info'],
            request_data['branch_names'],
            task_id,
            request_data['project_name'], superapprover_data_for_control, approver_data_for_control
        )
        if isinstance(analysis_result, tuple):  
            return analysis_result
        
        # info_logger.info(f"Analysis result: {analysis_result}")
        version = analysis_result.get('version', 1)
        task_id = analysis_result.get('task_id', task_id)
        old_task_id = analysis_result.get('old_task_id') # if your regen is true else None
        project_id = analysis_result.get('project_id')
        collection = project_summery_db["project_analyzer_task"]
        
        if collection.find_one({"task_id":task_id}, {"_id":1}):
            return jsonify({"error": "task_id should be unique"}), 400
        
        user_analysis_stats = project_summery_db.user_analysis_stats
        op_allowed, user_role = process_request(form_data=request_data, superapprover_data=superapprover_data_for_control, approver_data=approver_data_for_control, collection=user_analysis_stats)
                
        temp_dir = tempfile.mkdtemp(prefix="project_summary_")
        logger.info(f"Created temporary directory: {temp_dir}")
        if op_allowed:
            zip_folder = _handle_file_upload(request.files.get('text_pdf_files'), temp_dir)
            uploaded_file_count = count_files_in_folder(temp_dir) if zip_folder else 0

            # Save setting from UI on project initialization
            try:
                settings_config_str =  request.form.get("settings_config", '')
                settings_config = json.loads(settings_config_str)
                print("Type of settings_config : ", type(settings_config))
                # print("settings_config : ",settings_config)
                message, status_code = create_or_update_setting_func(mongo_reddis_config, task_id, settings_config['category_config'], settings_config['available_models'], settings_config['global_settings'])
                logger.info(f"setting func status message : {message}")
            except Exception as e:
                logger.error(f"settings not updated in db for task id {task_id}\n error found : {e}", exc_info=True)
                # logger.error(f"Setting config received from api request : ",settings_config_str)
                logger.error(f"Raw form data received from api request: {request.form}")
            assistant_id, chatbot_name = _initialize_project(user_id, task_id, request_data, old_task_id, uploaded_file_count, version, project_id)

            _start_analysis_thread(
                user_id,
                task_id,
                request_data,
                assistant_id,
                temp_dir,
                chatbot_name,
            )
            print(f"Analysis is getting started: {user_role}")
            return jsonify({
                "user_id": user_id,
                "task_id": task_id,
                "status": False,
                "approval_required": False
            }), 200
        
        else:
            '''Storing everything into db after checking that if his request is already there into db and not processed,
            but what if that is once rejected by someone, so we will keep it in db and maintain the status
            '''
            
            # Store in gcp bucket and pre processing of data
            file_urls = []
            if request.files.get("text_pdf_files"):
                with tempfile.TemporaryDirectory() as temp_dir:
                    zip_folder  = v2_handle_file_upload(request.files.get('text_pdf_files'), temp_dir, task_id)
                    file_urls = _get_uploaded_files_list(user_id, task_id,zip_folder)
            # return jsonify({"user_id": str(request_data.get("user_id"))})
            data_to_store = _get_data_to_store(request_data, task_id, old_task_id, version, file_urls, email, user_name, project_id, approx_output_cost, number_of_tokens, number_of_files)
            
            inserted, msg, status_code = store_ingestion_data_for_approval(project_data=data_to_store, collection=collection)
            
            # Check the approvers list for the user_id
            approvers_mail = [approver_data[0] for approver_data in list(approver_data_for_control.values())]
            if user_id in approver_data_for_control:
                approvers_mail = [superapprover_data[0] for superapprover_data in list(superapprover_data_for_control.values())]
           
            print(f"Analysis needs approval started: {user_role}")
            approvers_data = []

            for key in approver_data_for_control:
                mailid = approver_data_for_control[key][0]
                proj_data = get_project_details(email = mailid)
                if proj_data:
                    approvers_data+=proj_data
                
            # Get the data here to pass in 
            return jsonify({
                "user_id": request_data['active_user_id'],
                "task_id": task_id,
                "status": False,
                "approval_required": True,
                "approvers_data": approvers_data,
                "user_role": "approver" if user_id in approver_data_for_control else "normal",
                "analysis_type" : request_data['analysis_type']
            }), 200
    except EmailNotValidError as e:
        error_logger.error(f"Email is not valid: {e}")
        return jsonify({"error": "Email is not valid"}), 400
    except Exception as e:
        error_message = f"Project summary ingestion request error: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_message)
        _handle_error(user_id, task_id, error_message, request_data['analysis_type'], request_data)
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if zip_folder: _cleanup_temporary_files(zip_folder, temp_dir)

@blueprint_prefix.route('/ingest_project/assign_project', methods=['POST'])
@cross_origin(supports_credentials=True)
def ingest_project_assign_project():
    try:
        data = request.get_json()
        validation_status, validated_data, missing_keys = validate_payload(request_body=data, required_keys = ['task_id', 'user_id', 'project_name', 'description'], key_types={"task_id": str, "user_id": str, "import": bool, 'project_name': str, 'descrption':str})
        if not validation_status:
            return jsonify({"error": "Expected keys missing or invalid type", "keys": missing_keys}), 422
        
        git_pat_token = request.headers.get("git-access-token")
        if not git_pat_token:
            return jsonify({"error": "Git access token is missing"}), 400
        
        
        
        info_logger.info(f"Request to assign project: {validated_data} with import: {validated_data.get('import')}")
        new_task_id = str(uuid.uuid4())
        if validated_data.get("import"):
            task_id, user_id = validated_data["task_id"], validated_data["user_id"]
            copy_status, new_task_id = import_another_user_project(old_taskid=task_id, new_user_id=user_id, new_taskid=new_task_id, project_name=validated_data["project_name"], description=validated_data["description"],git_token=git_pat_token)
            if copy_status:
                info_logger.info(f"Project imported successfully: {new_task_id}")
                return jsonify({"status": "success", "task_id": new_task_id, "user_id": user_id}), 200
            else:
                error_logger.error(f"Error importing project: {new_task_id}")
                error_message = f"Error importing project: {task_id} for user: {user_id}"
                # _handle_error(user_id, task_id, new_task_id, "import", data)
                return jsonify({"status": "error", "message": "Error in copying the project"}), 400
            
        
        user_analysis_stats = project_summery_db.user_analysis_stats
        validated_data['regen'] = False      
        request_status, request_data = get_request_data_for_new_analysis(task_id=validated_data['task_id'], new_task_id=new_task_id, user_id=validated_data['user_id'], project_name=validated_data["project_name"], description=validated_data["description"], git_token=git_pat_token)
        if not request_status:
            error_message = "Error occurred while New Analysis data fethcing in Import Analysis request"
            _handle_error(validated_data["user_id"], validated_data['task_id'], error_message, "import", validated_data)
            return jsonify({"error": "Error occurred while New Analysis request"}), 400
        
        # Here we want the token to be always decoded and not encoded
        repo_supported, number_of_files, number_of_tokens, approx_output_cost=check_if_repo_supported(
            request_data['git_url'],
            request_data['github_token'],
            request_data['batch_size'],
            request_data['branch_names']
        )
        if not repo_supported:
            return jsonify({
                "error": "Repository is not supported. Token Limit is exceeded."
            }), 400
        
        old_task_id = None
        uploaded_file_count = 0
        version = 1
        temp_dir = ""
        collection = project_summery_db["project_analyzer_task"]
        info_logger.info(f"Request Data: {request_data}")

        op_allowed, user_role = process_request(form_data=validated_data, superapprover_data=superapprover_data_for_control, approver_data=approver_data_for_control, collection=user_analysis_stats)
        user_email, user_name = fetch_user_email(validated_data['user_id'], user_data=map_email_to_user_id)
        if op_allowed:
            # Save setting from UI on project initialization
            try:
                settings_config =  data.get("settings_config", {})
                # print("Type of settings_config : ", type(settings_config))
                message, status_code = create_or_update_setting_func(mongo_reddis_config, new_task_id, settings_config['category_config'], settings_config['available_models'], settings_config['global_settings'])
                logger.info(f"setting func status message : {message}")
            except Exception as e:
                logger.error(f"settings not updated in db for task id {new_task_id}\n error found : {e}", exc_info=True)
                # logger.error(f"Setting config received from api request : {settings_config}")
                logger.error(f"Raw form data received from api request: {request.form}")

            assistant_id, chatbot_name = _initialize_project(validated_data['user_id'], new_task_id, request_data, old_task_id, uploaded_file_count, version, project_id=request_data["project_id"])

            _start_analysis_thread(
                validated_data['user_id'],
                new_task_id,
                request_data,
                assistant_id,
                temp_dir,
                chatbot_name,
            )
            print(f"Analysis is getting started: {user_role}")
            return jsonify({
                "user_id": validated_data['user_id'],
                "task_id": new_task_id,
                "status": False,
                "approval_required": False
            }), 200 
        else:
            '''Storing everything into db after checking that if his request is already there into db and not processed,
            but what if that is once rejected by someone, so we will keep it in db and maintain the status
            '''
            info_logger.info(f"Request to assign project: {validated_data} with import: {validated_data.get('import')}")
            
            file_urls = []
            
            
            data_to_store = _get_data_to_store(request_data, new_task_id, old_task_id, version, file_urls, user_email, user_name, request_data["project_id"], approx_output_cost, number_of_tokens, number_of_files)
            
            inserted, msg, status_code = store_ingestion_data_for_approval(project_data=data_to_store, collection=collection)
            
            approvers_mail = [approver_data[0] for approver_data in list(approver_data_for_control.values())]
            if validated_data['user_id'] in approver_data_for_control:
                approvers_mail = [superapprover_data[0] for superapprover_data in list(superapprover_data_for_control.values())]
           
            print(f"Analysis needs approval started: {user_role}")
            approvers_data = []

            for key in approver_data_for_control:
                mailid = approver_data_for_control[key][0]
                proj_data = get_project_details(email = mailid)
                if proj_data:
                    approvers_data+=proj_data
                
            return jsonify({
                "user_id": validated_data['user_id'],
                "task_id": new_task_id,
                "status": False,
                "approval_required": True,
                "approvers_data": approvers_data,
                "analysis_type" : request_data['analysis_type'],
                "user_role": "approver" if validated_data["user_id"] in approver_data_for_control else "normal"
            }), 200

    except Exception as e:
        error_message = f"Error occurred in ingest_project_assign_project: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        error_logger.error(error_message)
        _handle_error(validated_data['user_id'], validated_data['task_id'], error_message, request_data['analysis_type'], request_data)
        return jsonify({"error": "Internal server error"}), 500
        
@blueprint_prefix.route('/ingest_project_summary_approval_mail', methods=['POST']) # To be hit when wants to send mail for approval
@cross_origin(supports_credentials=True)
def ingest_project_summary_approval_mail():
    user_id, task_id, data = "", "", {}
    try:
        body = request.get_json()
        batchId = body.get("batchId", None)
        keys = ["user_id", "task_id", "approver_mail", "operation_type", "assigned_project"]
        if request.get_json().get("user_id") in approver_data_for_control:
            keys.remove("approver_mail")
            keys.remove("assigned_project")
        validated_request_body, body, missing_keys = _validate_request_v2_ingest_project_summary(request_body = 
            request.get_json(),keys = keys)
        
        if not validated_request_body:
            return jsonify({"message":"Expected keys missing or invalid type", "keys": missing_keys, "status": "error"}), 422 
        validated_request_body = request.get_json()

        if request.get_json().get("user_id") not in approver_data_for_control:
            if validated_request_body["approver_mail"] not in set([approver_data[0] for approver_data in approver_data_for_control.values()]) and validated_request_body["approver_mail"] not in set([superapprover_data[0] for superapprover_data in superapprover_data_for_control.values()]):
                return jsonify({"message":"Invalid approver_mail provided", "status": "error"}), 400
        
        assigned_project = validated_request_body.get("assigned_project", "Unknown")     
        action_user_id = map_email_to_user_id.get(validated_request_body.get("approver_mail", ""), [None])[0] 
        
        task_id, user_id, operation_type = validated_request_body["task_id"], validated_request_body["user_id"], validated_request_body["operation_type"]
        data, status = get_pre_ingestion_raw_data(task_id, user_id, check_if_repo_supported=check_if_repo_supported, operation_type=operation_type)
        if status != 200:
            return jsonify({"message": data.get('message', "No data found with given task_id and user_id"), "status": "error"}), status
        if not data['user_email']:
            user_email, user_name = fetch_user_email(user_id, user_data=map_email_to_user_id)
            data['user_email'] = user_email
        if not data:
            return jsonify({"message":"No data found with given task_id and user_id", "status": "error"}), 404
        # for key in data:
        #     if not data[key] : return jsonify({"message":"No data found with given task_id and user_id", "status": "error"}), 404
        email_body, email_subject = "", ""
        
        # If user_id is in approvers then always sent the mail to superapprovers
        user_is_approver = False
        response, status_code = "", 404
        collection = project_summary_db.project_analyzer_task
        if validated_request_body["user_id"] in approver_data_for_control:
            # Sent mail to superapprover to request for approval
            email_body, email_subject = approval_request_to_superapprover_email_body(
                user_name = data.get("user_name"),
                user_email = data.get("user_email"),
                github_repos = data.get("github_repos"),  # List of tuples containing (branch, link)
                project_name = data.get("project_name"),
                number_of_files = data.get("number_of_files"),
                total_tokens = data.get("number_of_tokens"),
                task_id = task_id, user_id=user_id, operation_type=operation_type,
                batchId=batchId,
            )
            response, status_code = send_email_with_body(
                recipient_emails= [superapprover_data[0] for superapprover_data in superapprover_data_for_control.values()],
                email_body=email_body, subject=email_subject
            )  
            user_is_approver = True  
        elif validated_request_body["user_id"] not in superapprover_data_for_control:
            # This case is for normal user
            
            # Sent mail to approver to request for approval
            email_body, email_subject = approval_request_to_approver_email_body(
                user_name = data.get("user_name"),
                user_email = data.get("user_email"),
                github_repos=data.get("github_repos"),  # List of tuples containing (branch, link)
                project_name = data.get("project_name"),
                number_of_files = data.get("number_of_files"),
                total_tokens = data.get("number_of_tokens"),
                user_id=user_id, task_id=task_id, operation_type=operation_type,
                batchId=batchId,
            )
            response, status_code = send_email_with_body(
                recipient_emails= [validated_request_body["approver_mail"]],
                email_body=email_body, subject=email_subject
            )
        else:
            response = {"status": "error", "message": "Superapprover can't send any mail"}
            return jsonify(response), status_code

        if status_code == 200: 
            assigned_project = get_project_name(user_id, map_user_id_to_info).lower() if user_is_approver else assigned_project.lower()
            if action_user_id and not user_is_approver:
                # User is normal user
                collection.update_one({"task_id": task_id, "user_id": user_id}, {"$set": {
                    "pre_ingestion_data.requested_for_approval": True, 
                    "assigned_project": assigned_project,
                    "pre_ingestion_data.action_by_approver": {
                        "action": "Pending",
                        "action_user_id": action_user_id,
                        "feedback": ""
                    }
                }}, upsert=False)
            else:
                # User is approver
                collection.update_one({"task_id": task_id, "user_id": user_id}, {"$set": {
                    "pre_ingestion_data.requested_for_approval": True,
                    "assigned_project": assigned_project,
                    "pre_ingestion_data.action_by_super_approver": {
                        "action": "Pending",
                        "action_user_id": "",
                        "feedback": ""
                    }
                }}, upsert=False)
            # Send mail to user about the status
            # print("Mail sent to approver")
            approver_name = get_name_of_approvers(superapprover_data_for_control, approver_data_for_control, validated_request_body)
                     
            if validated_request_body["approver_mail"] in [superapprover_data[0] for superapprover_data in superapprover_data_for_control.values()]:
                # Sent user a mail telling that superapprover approval is pending
                
                email_body, email_subject = request_sent_to_user_about_po_approval(
                    github_repos = data.get("github_repos"),  # List of tuples containing (branch, link)
                    project_name = data.get("project_name"),
                    number_of_files = data.get("number_of_files"),
                    total_tokens = data.get("number_of_tokens"), operation_type=operation_type,
                    # po_email = validated_request_body["approver_mail"],    # This is has to be seen
                    # po_name = approver_name if approver_name else "Unknown" 
                )
                
                response, status_code = send_email_with_body(
                    recipient_emails= [data.get("user_email")],email_body=email_body, subject=email_subject)
                if status_code == 200:
                    logger.info("1. Email Sent to user about the status of his request from approver")
                return jsonify(response), status_code
            else:
                # Sent user a mail telling that approver approval is pending
                email_body, email_subject = request_sent_to_user_for_approver_mail(
                    github_repos = data.get("github_repos"),  # List of tuples containing (branch, link)
                    project_name = data.get("project_name"),
                    number_of_files = data.get("number_of_files"),
                    total_tokens = data.get("number_of_tokens"), operation_type=operation_type,
                    po_email = validated_request_body["approver_mail"],
                    po_name = approver_name if approver_name else "Unknown" # this case shouldn't be reached, if reached then there is security breach
                )
                if user_is_approver:
                    email_body, email_subject = request_sent_to_user_about_po_approval(
                        github_repos = data.get("github_repos"),  # List of tuples containing (branch, link)
                        project_name = data.get("project_name"),
                        number_of_files = data.get("number_of_files"),
                        total_tokens = data.get("number_of_tokens"), operation_type=operation_type,
                        po_email = validated_request_body["approver_mail"],
                        po_name = approver_name if approver_name else "Unknown" # this case shouldn't be reached, if reached then there is security breach
                    )
                response, status_code = send_email_with_body(recipient_emails= [data.get("user_email")], email_body=email_body, subject=email_subject)
                if status_code == 200:
                    logger.info("1. Email Sent to user about the status of his request from Approver")
                return jsonify(response), status_code
        else: 
            logger.error(f"Mailing failed with status_code: status_code {status_code}") 
        return jsonify(response), status_code
    except Exception as e:
        error_message = f"Error occurred in ingest_project_summary_approval: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_message)
        # _handle_error(user_id, task_id, error_message, data['analysis_type'], data)
        return jsonify({"status": "error", "message": "Internal server error"}), 500


        
@blueprint_prefix.route('/ingest_project_summary_action', methods=['POST'])  # Endpoint for handling project summary ingestion actions
@cross_origin(supports_credentials=True)
def ingest_project_summary_action():
    zip_folder = None
    try:
        # Parse and validate the incoming request body
        request_body = request.get_json()
        validated_request_body, body, missing_keys = _validate_request_v2_ingest_project_summary(
            request_body=request_body, keys=["task_id", "user_id", "approver_user_id", "approved", "operation_type"])
        if not validated_request_body:
            return jsonify({"message": "Expected keys missing or invalid type", "keys": missing_keys, "status":"error"}), 422
        if not request_body.get("feedback"): 
            request_body["feedback"]=""
            
        # Determine if the action is performed by an approver or superapprover
        action_by: str = "superapprover" if request_body["approver_user_id"] in superapprover_data_for_control else "approver" if request_body["approver_user_id"] in approver_data_for_control else "normal"
        operation_type = request_body.get("operation_type")
        if action_by=="normal":
            return jsonify({"message": "Unauthorized action", "status":"error"}), 403
        
        task_id, user_id = request_body.get("task_id"), request_body.get("user_id")
        collection = project_summery_db.project_analyzer_task
        data, status = get_pre_ingestion_raw_data(task_id, user_id, check_if_repo_supported=check_if_repo_supported, operation_type=operation_type)
        if status != 200:
            return jsonify({"message": data.get('message', "No data found with given task_id and user_id"), "status": "error"}), status
        if not data:
            return jsonify({"message":"No data found with given task_id and user_id", "status": "error"}), 404
            
        # Check the status of the request
        status = get_request_status(collection=collection, task_id=body["task_id"], user_id=request_body["user_id"],operation_type=operation_type)
        if not status:
            return jsonify({"message": "No data found with given task_id and user_id", "status":"error"}), 404
        if status != "Pending":
            return jsonify({"message": "Action can't be performed as already done", "status":"error"}), 406

        
        if action_by == "superapprover":
            # Verify if the user is an authorized superapprover
            if request_body["approver_user_id"] not in superapprover_data_for_control:
                return jsonify({"message": "Unauthorized approver_user_id", "status":"error"}), 403

            # Update the status of the request based on approval or rejection
            pipeline_query = {"$set": {"pre_ingestion_data.status": "Approved" if body["approved"] else "Rejected"}}
            collection.update_one(filter={"task_id": task_id}, update=pipeline_query, upsert=False)

            if request_body["approved"]:
                # Step 1: Retrieve necessary data from the database
                # data = collection.find_one({"task_id": task_id, "user_id": user_id})
                # request_data = data.get("pre_ingestion_data", {}).get("request_data", {})
                # old_task_id = request_data.get("old_task_id")
                # version = data.get("request_data", {}).get("version")
                # project_id = data.get("pre_ingestion_data", {}).get("project_id")
                
                update_status = store_approval_details(task_id=task_id, action_user_id= request_body['approver_user_id'], collection=collection, feedback = request_body.get("feedback"), action_by_super_po=True, action="Approved")
                email_body, email_subject = request_approved_by_superapprover_email_body_to_user(
                    project_name=data.get("project_name"),
                    superapprover_name=superapprover_data_for_control[request_body["approver_user_id"]][1],
                    superapprover_email=superapprover_data_for_control[request_body["approver_user_id"]][0],
                    github_repos=data.get("github_repos"), operation_type=operation_type
                )
                response, status_code = send_email_with_body(recipient_emails=[data.get("user_email")], email_body=email_body, subject=email_subject)
                logger.info(response["message"]) if status_code == 200 else logger.error(response["message"])


                # Step 1.5: Notify the user about approval via email
                
                if operation_type == "sync":
                    message, status = start_sync_project_summary(resync_task_id=task_id, request_data=data['request_data'])
                    return jsonify({"status": "success", "message": "Request Submitted"}), 200

                request_data, user_id, old_task_id, version = _get_pre_ingestion_data(task_id, collection)

                # Step 1.75 Store approvers mail in db ()
                collection.update_one({"task_id": task_id, "user_id": user_id}, {"$set":{"approvers_mail": superapprover_data_for_control[request_body["approver_user_id"]][0]}})
                
                # Step 2: Retrieve files from GCP and prepare for ingestion
                temp_dir = tempfile.mkdtemp(prefix=f"{task_id}")
                logger.info(f"Created temporary directory: {temp_dir}")
                zip_folder = get_files_from_gcp(temp_dir, task_id=task_id)

                # Step 3: Store files temporarily and count them
                temp_dir = prepare_and_count_files(temp_dir)
                uploaded_file_count = count_files_in_folder(temp_dir) if zip_folder else 0

                # Step 4: Remove unnecessary details from the database
                delete_pending_ingestion_details(task_id=task_id, collection=collection)

                # Step 5: Decrypt sensitive tokens for use
                request_data["github_token"] = decrypting_token(request_data.get("github_token", ""))
                request_data["egpt_token"] = decrypting_token(request_data.get("egpt_token", ""))

                # Step 6: Initialize project and start analysis
                assistant_id, chatbot_name = _initialize_project(user_id, task_id, request_data, old_task_id, uploaded_file_count, version,data.get('project_id'))
                _start_analysis_thread(user_id, task_id, request_data, assistant_id, temp_dir, chatbot_name)

                return jsonify({"status": "success", "message": "Request Submitted"}), 200
            else:
                # Handle rejection by superapprover
                update_status = store_approval_details(task_id=task_id, action_user_id= request_body['approver_user_id'], collection=collection, feedback = request_body.get("feedback"), action_by_super_po=True, action="Rejected")
                delete_pending_ingestion_details(task_id=task_id, collection=collection, rejected=True, rejected_by=request_body["approver_user_id"])

                # Notify the user about rejection via email
                email_body, email_subject = request_rejected_by_superapprover_email_body(
                    github_repos=data.get("github_repos"),  # List of tuples containing (branch, link)
                    project_name=data.get("project_name"),
                    number_of_files=data.get("number_of_files"),
                    total_tokens=data.get("number_of_tokens"), operation_type=operation_type,
                    superapprover_name=superapprover_data_for_control[request_body["approver_user_id"]][1],
                    superapprover_email=superapprover_data_for_control[request_body["approver_user_id"]][0],
                )
                response, status_code = send_email_with_body(recipient_emails=[data.get("user_email")], email_body=email_body, subject=email_subject)
                return jsonify({"status": "success", "message": "Request Submitted"}), 200
        else:
            # Handle actions by approvers
            # data, status = get_pre_ingestion_raw_data(task_id, user_id, check_if_repo_supported=check_if_repo_supported)
            # if not data:
            #     return jsonify({"message":"No data found with given task_id and user_id", "status": "error"}), 404
            if request_body["approved"]:
                update_status = store_approval_details(task_id=task_id, action_user_id= request_body['approver_user_id'], collection=collection, feedback = request_body.get("feedback"), action_by_super_po=False, action="Approved")
                # Notify superapprovers about the approval request
                email_body, email_subject = approval_request_to_superapprover_email_body(
                    user_name=data.get("user_name"),
                    user_email=data.get("user_email"),
                    github_repos=data.get("github_repos"),  # List of tuples containing (branch, link)
                    project_name=data.get("project_name"),
                    number_of_files=data.get("number_of_files"),
                    total_tokens=data.get("number_of_tokens"),
                    po_email=approver_data_for_control[request_body["approver_user_id"]][0],
                    po_name=approver_data_for_control[request_body["approver_user_id"]][1], 
                    task_id = task_id, user_id = user_id, operation_type=request_body.get("operation_type")
                )
                response, status_code = send_email_with_body(
                    recipient_emails=[superapprover_data[0] for superapprover_data in superapprover_data_for_control.values()],
                    email_body=email_body,
                    subject=email_subject
                )

                # Notify the user about approval pending superapprover review
                email_body, email_subject = request_sent_to_user_about_po_approval(
                    github_repos=data.get("github_repos"),  # List of tuples containing (branch, link)
                    project_name=data.get("project_name"),
                    number_of_files=data.get("number_of_files"),
                    total_tokens=data.get("number_of_tokens"), operation_type=operation_type,
                    po_email=approver_data_for_control[request_body["approver_user_id"]][0],
                    po_name=approver_data_for_control[request_body["approver_user_id"]][1]
                )
                response, status_code = send_email_with_body(recipient_emails=[data.get("user_email")], email_body=email_body, subject=email_subject)

                return jsonify(response), status_code
            else:
                # Handle rejection by approvers
                update_status = store_approval_details(task_id=task_id, action_user_id= request_body['approver_user_id'], collection=collection, feedback = request_body.get("feedback"), action_by_super_po=False, action="Rejected")
                delete_pending_ingestion_details(task_id=task_id, collection=collection, rejected=True, rejected_by=request_body["approver_user_id"])
                
                email_body, email_subject = request_rejected_by_po_email_body_to_user(
                    github_repos=data.get("github_repos"),  # List of tuples containing (branch, link)
                    project_name=data.get("project_name"),
                    number_of_files=data.get("number_of_files"),
                    total_tokens=data.get("number_of_tokens"), operation_type=operation_type,
                    po_email=approver_data_for_control[request_body["approver_user_id"]][0],
                    po_name=approver_data_for_control[request_body["approver_user_id"]][1]
                )
                response, status_code = send_email_with_body(recipient_emails=[data.get("user_email")], email_body=email_body, subject=email_subject)
                
                return jsonify({"status": "success", "message": "Approval has been declined"}), 200
    except Exception as e:
        error_message = f"Error occurred in ingest_project_summary_approval: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_message)
        _handle_error(user_id, task_id, error_message)
        return jsonify({"message": "Internal server error", "status":"success"}), 500
    finally:
        if zip_folder: _cleanup_temporary_files(zip_folder, temp_dir)

@blueprint_prefix.route('/ingest_project_re-request', methods=['POST'])
@cross_origin(supports_credentials=True)
def ingest_project_summary_rerequest():
    try:
        task_id, user_id, active_user_id = request.get_json().get("task_id"), request.get_json().get("user_id"), request.get_json().get("active_user_id")
        if not (task_id or user_id): 
            return jsonify({"message": "Missing keys", "status": "error"}), 422
        user_email, user_name = fetch_user_email(active_user_id, user_data=map_email_to_user_id)
        
        # Step 0: Get all the data required for the request 
        data, status = get_pre_ingestion_raw_data(task_id, user_id, check_if_repo_supported)
        if status != 200:
            return jsonify({"message": data.get('message', "No data found with given task_id and user_id"), "status": "error"}), status
        map_analysis_type_to_operation_type={"sync":"sync", "new_analysis":"analyse", "reanalysis": "reanalyze"}
        operation_type = map_analysis_type_to_operation_type.get(data.get("analysis_type"))
        if operation_type is None:
            return jsonify({"message": "Error occurred at our side, data is not present", "status":"error"}), 400
        # Step 1: Check if data is Rejected
        if data.get("status") != "Rejected": 
            return jsonify({"message":"No data found", "status":"error"}), 403
        
        # Step 2: Check who had rejected the request and send mail accordingly
        rejected_by: str=data.get("rejected_by")
        
        if rejected_by in superapprover_data_for_control:
            # Was rejected by superapprover so send mail to all of the superapprovers for approval
            
            # Step 3: Reset the action taken by superapprover, make it pending again. Also reset the overall status as Pending.
            res = project_summary_db.project_analyzer_task.update_one({
                "task_id":task_id, "pre_ingestion_data.request_data.active_user_id": active_user_id}, 
                {"$set":{
                    "pre_ingestion_data.status":"Pending",
                    "pre_ingestion_data.action_by_super_approver.action":"Pending",
                    "pre_ingestion_data.action_by_super_approver.feedback":"",
                    "pre_ingestion_data.created_at": str(datetime.now(timezone.utc)),
                    "pre_ingestion_data.rejected_by":""
                }}, upsert=False)
            if res.modified_count==0:
                return jsonify({"message":"No data found", "status":"error"}), 403
            
            email_body, email_subject = request_sent_to_user_about_po_approval(project_name=data.get("project_name"), 
                number_of_files=data.get("number_of_files"), 
                total_tokens=data.get("number_of_tokens"), github_repos=data.get("github_repos"),
                operation_type=operation_type 
            )
            response, status_code = send_email_with_body(recipient_emails=[user_email], email_body=email_body, subject=email_subject)
            
            if status_code == 200:
                email_body, email_subject = approval_request_to_superapprover_email_body(
                    user_name = user_name, user_email=user_email, user_id=active_user_id, task_id=task_id,
                    project_name=data.get("project_name"), 
                    number_of_files=data.get("number_of_files"), 
                    total_tokens=data.get("number_of_tokens"), github_repos=data.get("github_repos"),
                    operation_type=operation_type, is_request_again=True
                )
                response, status_code = send_email_with_body(
                    recipient_emails=[superapprover_data[0] for superapprover_data in superapprover_data_for_control.values()], 
                    email_body=email_body, subject=email_subject
                )
            return jsonify(response), status_code
        elif rejected_by in approver_data_for_control:
            # Now its time for case when it was rejected by approver (so send the mail to that lady/Gent in that case)
            
            # Step 3: Reset the action taken by approver, make it pending again. Also reset the overall status as Pending.
            res = project_summary_db.project_analyzer_task.update_one({
                "task_id":task_id, "pre_ingestion_data.request_data.active_user_id": active_user_id}, 
                {"$set":{
                    "pre_ingestion_data.status":"Pending",
                    "pre_ingestion_data.action_by_approver.action":"Pending",
                    "pre_ingestion_data.action_by_approver.feedback": "",
                    "pre_ingestion_data.created_at": str(datetime.now(timezone.utc)), # Created at time
                    "pre_ingestion_data.rejected_by":"" 
                }}, upsert=False)
            if res.modified_count==0:
                return jsonify({"message":"No data found", "status":"error"}), 403
            
            # Mail to user that his rerequest is sent to same approver again 🙃
            email_body, email_subject = request_sent_to_user_for_approver_mail(project_name=data.get("project_name"), 
                number_of_files=data.get("number_of_files"), 
                total_tokens=data.get("number_of_tokens"),
                po_name=approver_data_for_control[rejected_by][1], 
                po_email=approver_data_for_control[rejected_by][0],
                operation_type="Re-Request", 
                github_repos=data.get("github_repos")
            )
            response, status_code = send_email_with_body(recipient_emails=[user_email],email_body=email_body, subject=email_subject)
            if status_code == 200:
                # Send mail to approver with required details about rerequest 
                email_body, email_subject = approval_request_to_approver_email_body(project_name=data.get("project_name"),
                    user_name = user_name, user_email=user_email, user_id=active_user_id, task_id=task_id,
                    number_of_files=data.get("number_of_files"), 
                    total_tokens=data.get("number_of_tokens"),
                    operation_type=operation_type, is_request_again=True,
                    github_repos=data.get("github_repos")
                )
                response, status_code = send_email_with_body(
                    recipient_emails=[approver_data_for_control[rejected_by][0]], 
                    email_body=email_body, subject=email_subject
                )
            return jsonify(response), status_code
        else:
            return jsonify({"message": "Previous data not found, try for another project", "status":"error"}), 400
    
    except Exception as e:
        error_message = f"Error occurred in ingest_project_summary_approval: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_message)
        # _handle_error(user_id, task_id, error_message, data['analysis_type'], data)
        return jsonify({"message": "Internal server error", "status":"success"}), 500
        

    
@blueprint_prefix.route('/cs-violations', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_coding_standard_analysis():
    try:

        task_id = request.args.get('task_id')
        github_url = request.args.get('git_url')

        #Check default fields
        if not task_id or not github_url:
            return jsonify({"error": "task_id and github url are neccessary field"}), 400

        category = parse_list_params(request.args.get('category'))
        language = parse_list_params(request.args.get('language'))
        severity = parse_list_params(request.args.get('severity'))


        file_name = request.args.get('file_name', None)
        page_number = int(request.args.get('page_number', 1))
        page_size = int(request.args.get('page_size', 10))


        response = get_vulnerability_checks(task_id, github_url, category, language, severity, file_name, page_number,page_size)

        return jsonify(response),200

    except Exception as e:
            return jsonify({"error":f"An error has occured {str(e)}"}), 500

@blueprint_prefix.route('/generate_description', methods=['POST'])
@cross_origin(supports_credentials=True)
def generate_description():
    try:
        # Get data from request
        data = request.get_json()
        git_url = data.get("git_url")
        branch_name = data.get("branch_name")
        access_token = request.headers.get("git-access-token")

        # Validate request data
        if not git_url or not branch_name or not access_token:
            return jsonify({"error": "Missing required data"}), 400

        # Get the README content
        content, status_code = get_readme_content(git_url, branch_name, access_token)

        # Return the response
        if status_code == 200:
            description = process_readme(content)
            if isinstance(description, str) and description.startswith("Error:"):
                return jsonify({"error": description}), 500
            return jsonify({"readme_content": description}), 200
        else:
            return jsonify({"error": content}), 404

    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@blueprint_prefix.route('/delete_existing_repo', methods=['POST'])
@cross_origin(supports_credentials=True)
def delete_exisiting_repo_of_user():
    try:
        data = request.json
        user_id = data.get("user_id")
        task_id = data.get("task_id")
        repo_data = data.get("repo_data")
        repo_url_list = []
        branch_list = []
        for repo in repo_data:
            repo_url_list.append(repo.get("git_url",""))
            branch_list.append(repo.get("branch",""))
        if not task_id or not user_id:
            return jsonify({"error":"user_id and task_id are required params"})
        result, status_code = delete_repo_from_db2(user_id, repo_url_list, branch_list, task_id)
        return result, status_code
    except Exception as e:
        return jsonify({"error": f"An error has occured: {str(e)}"}), 500
    
@blueprint_prefix.route('/delete_project', methods=['DELETE'])
@cross_origin(supports_credentials=True)
def delete_existing_project():
    try:
        data = request.json
        user_id, project_id = data.get("user_id"), data.get("project_id")
        if not project_id or not user_id:
            return jsonify({"message": "missing fields", "status": "error"}), 422
        message, status_code = delete_project(project_id=project_id, user_id=user_id, 
            filter_assistant_ids=filter_assistant_ids, delete_assistants=delete_assistants)
        return jsonify({"message": message, "status": "success" if status_code == 200 else "error"}), status_code
    except Exception as e:
        print({"error": f"An error has occured: {str(e)}"})
        return jsonify({"message": "Internal server error", "status":"error"}), 500
    
@blueprint_prefix.route('/delete_project_versions', methods=['DELETE'])
@cross_origin(supports_credentials=True)
def delete_project_versions_api():
    try:
        task_ids = request.get_json().get("task_ids", [])
        user_id = request.get_json().get("user_id", None)
        if not user_id or not task_ids:
            return jsonify({"message": "missing fields", "status":"error"}), 422
        message, status_code = delete_project_versions(task_ids, user_id, filter_assistant_ids=filter_assistant_ids, delete_assistants=delete_assistants)
        return jsonify({"message": message, "status":"success" if status_code==200 else "error"}), status_code
    except Exception as e:
        error_message = f"Error occurred in ingest_project_summary_approval: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        logger.error(error_message)
        return jsonify({"message": "Internal server error", "status":"error"}), 500

@blueprint_prefix.route('/get_exisiting_repo', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_exisiting_repos_of_user():
    try:
        user_id = request.args.get("user_id")
        if not user_id:
            return jsonify({"error": "user_id is required param"}), 422
        data = {}
        data = get_all_repo_user_v2(user_id=user_id, map_user_id_to_info=map_user_id_to_info, get_count_of_doc=get_count_of_doc)
        data = get_requested_repo(user_id, user_analyzed_repo_list=data, map_user_id_to_info=map_user_id_to_info)
        data += get_shared_repo_list(user_id, map_user_id_to_info=map_user_id_to_info)
        
        data = _format_data(data)

        #Adding model settings to data from Redis/Mongo collection
        model_keys = ["chat_embedding_model"] #Add the required keys into this list.
        data = add_model_settings_to_data(data=data, model_keys=model_keys)

        response = {"data": data}
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": f"An error has occurred: {str(e)}"}), 500

@blueprint_prefix.route('/update-summary-with-feedback', methods=['POST'])
@cross_origin(supports_credentials=True)
def update_summary_feedback():
    if request.method == "POST":
        try:
            data = request.get_json()
            task_id = data["task_id"]
            user_id = data['user_id']
            feedback = data['feedback']
            return executive_summary_feedback(task_id, user_id, feedback)
        except Exception as e:
            return jsonify({"error": str(e)}), 500


@blueprint_prefix.route('/share_project', methods=['POST'])
@cross_origin(supports_credentials=True)
def share_project_via_email():
    
    data = request.get_json()
    validation_status, validated_data, missed_keys = validate_payload(request_body=data,required_keys=["project_id", "validity", "user_id"], 
    optional_keys=[ "editor_access_list", "viewer_access_list"], key_types={"project_id": str, "validity": str, "user_id":str, "editor_access_list": list, "viewer_access_list": list})
    if not validation_status:
        return jsonify({"error": "Expected keys missing or invalid type", "keys": missed_keys}), 422
    
    info_logger.info("Checking if the shared emails are valid or not")
    
    
    validation_result, validation_status = validaiton_share_pipeline(project_id=data["project_id"], edit_email_ids=validated_data.get("editor_access_list", []), 
        view_email_ids=validated_data.get("viewer_access_list", []), user_database=map_email_to_user_id)
    if validation_status != 200:
        return jsonify(validation_result), validation_status
    
    if not validated_data.get("user_id"): return jsonify({"error": "No project found", "keys": missed_keys}), 404
    original_owner_id, task_id, project_name =  get_recent_versions_from_project_id(validated_data["project_id"])
    user_has_share_access: bool = check_share_access(user_id=validated_data["user_id"], project_id=validated_data["project_id"], original_owner_id=original_owner_id)
    if not user_has_share_access:
        return jsonify({"error": "User doesn't have access to share this project"}), 403
    
    response, status_code = {"error": "Empty Mail list not allowed"}, 400
    if validated_data.get("editor_access_list", []):
        response, status_code, db_data = add_shared_project_to_db(
            project_id=data.get("project_id"), email_ids=list(set(validated_data.get("editor_access_list", []))),
            validity=validated_data.get("validity"), owner_id=original_owner_id, 
            map_email_to_user_id=map_email_to_user_id, access_type="edit"
        )
        if status_code == 200:   
            response, status_code = send_email_to_users(validated_data.get("user_id"), task_id, user_name = get_user_name_from_user_id(validated_data.get("user_id"), map_email_to_user_id), 
                project_name = project_name, email_ids = list(set(validated_data.get("editor_access_list", []))), access_type="edit")
    
    if validated_data.get("viewer_access_list", []):
        response, status_code, db_data = add_shared_project_to_db(
            project_id = data.get("project_id"), email_ids = list(set(validated_data.get("viewer_access_list", []))),
            validity = validated_data.get("validity"), access_type="view",
            owner_id=original_owner_id, map_email_to_user_id=map_email_to_user_id
        )
        if status_code == 200:   
            response, status_code = send_email_to_users(validated_data.get("user_id"), task_id, user_name = get_user_name_from_user_id(validated_data.get("user_id"), map_email_to_user_id), 
                project_name = project_name, email_ids = validated_data.get("viewer_access_list", []), access_type="view")
    
    return response, status_code


@blueprint_prefix.route('/share-dashboard/view', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_shared_dashboard():
    try:
        data = request.args
        validation_status, validated_data, missed_keys = validate_payload(request_body=data,required_keys=["project_id"], key_types={"project_id":str})
        if not validation_status:
            return jsonify({"error": "Expected keys missing or invalid type", "keys": missed_keys}), 422
        
        response, status_code = get_shared_dashboard_data(project_id=validated_data["project_id"], user_details=map_email_to_user_id)
        
        return jsonify(response), status_code
    except Exception as e:
        return jsonify({"error": f"Error fetching shared dashboard data: {str(e)}"}), 500
    
    
@blueprint_prefix.route('/share-dashboard/update', methods=['PUT'])
@cross_origin(supports_credentials=True)
def update_shared_dashboard():
    try:
        data = request.get_json()
        validation_status, validated_data, missed_keys = validate_payload(request_body=data,required_keys=["project_id", "user_id"],optional_keys= ["update_access", "delete_access", "update_validity"], key_types={"project_id":str, "user_id":str, "update_access":list, "delete_access":list, "update_validity" :list})
        if not validation_status:
            return jsonify({"error": "Expected keys missing or invalid type", "keys": missed_keys}), 422
        
                                                                        
        response, status_code = update_shared_dashboard_data_pipeline(project_id=validated_data["project_id"], action_user_id=validated_data["user_id"], update_access_list=validated_data.get("update_access", []), delete_access_list=validated_data.get("delete_access", []), update_validity_list=validated_data.get('update_validity',[]), user_data=map_email_to_user_id)
        
        return jsonify(response), status_code
    except Exception as e:
        return jsonify({"error": f"Error updating shared dashboard data: {str(e)}"}), 500

@blueprint_prefix.route('/send-email-user-support', methods=['POST'])
@cross_origin(supports_credentials=True)
def send_email():
    data = request.get_json()
    user_id = data["user_id"]
    task_id = data["task_id"]
    user_name = data["name"]
    user_email = data["user_email"]

    return send_support_email(user_id, task_id, user_name, user_email)


@blueprint_prefix.route('/update-feature-with-feedback', methods=['POST'])
@cross_origin(supports_credentials=True)
def update_feature_feedback():
    try:
        data = request.get_json()
        task_id = data["task_id"]
        user_id = data['user_id']
        feedback = data['feedback']

        return feature_view_feedback(task_id, user_id, feedback)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/user_edit_feature', methods=['POST'])
@cross_origin(supports_credentials=True)
def feature_edit():
    try:
        data = request.get_json()
        task_id = data.get("task_id", "")
        user_id = data.get('user_id', '')
        action_type = data.get("action_type", "")
        feature_data = data.get("feature_data", {})
        response, status_code = v2_update_edited_feature(task_id=task_id, user_id=user_id, action_type=action_type, feature_data=feature_data)
        return jsonify(response), status_code

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/feature_hierarchy/feature:reorder', methods=['POST'])
@cross_origin(supports_credentials=True)
def feature_order_edit():
    feature_data, status, message, status_code = {}, "success", "", 200
    try:
        data = request.get_json()
        task_id, ordered_list = data.get("task_id", ""), data.get("ordered_list", [])
        if not task_id or not ordered_list:
            return jsonify({"data": feature_data, "status":"error", "message": "task_id and ordered_list required"}), 422
        feature_data, message, status_code = change_order_of_features(task_id, ordered_list)
    except Exception as e:
        logging.error(f"Error occurred in tbe feature_order_edit: {e}")
        feature_data, status, message, status_code = {}, "error", "Something went wrong", 500
    return jsonify({"data":feature_data, "status":status, "message": message}), status_code



@blueprint_prefix.route('/feature_hierarchy/feature:reorder:ai', methods=['POST'])
@cross_origin(supports_credentials=True)
def feature_hierarchy_reorder_with_ai():
    feature_data, status, message, status_code = {}, "success", "", 200
    try:
        data = request.get_json()
        task_id = data.get("task_id", "")
        when_to_apply = data.get("when_to_apply", "")
        where_to_apply = data.get("where_to_apply", "")
        file_ids = data.get("file_ids", [])
        
        if not task_id or not when_to_apply or not where_to_apply or not file_ids:
            return jsonify({"data": feature_data, "status":"error", "message": "task_id, when_to_apply and where_to_apply required"}), 422
        
        feature_data, message, status_code = change_order_of_features_with_ai(task_id, when_to_apply, where_to_apply, file_ids)
    except Exception as e:
        logging.error(f"Error occurred in tbe feature_order_edit: {e}")
        feature_data, status, message, status_code = {}, "error", "Something went wrong", 500
    return jsonify({"data":feature_data, "status":status, "message": message}), status_code

@blueprint_prefix.route('/feature_hierarchy/mmvf:reorder:ai', methods=['POST'])
@cross_origin(supports_credentials=True)
def feature_reordering_with_WTA():
    mmvf_data, status, message, status_code = {}, "success", "", 200
    try:
        data = request.get_json()
        task_id = data.get("task_id", "")
        UserInstructions = data.get("user_instructions", "")
        feature_ids = data.get("file_ids", [])
        
        if not task_id or not UserInstructions or not feature_ids:
            return jsonify({"data": mmvf_data, "status":"error", "message": "task_id and User Instructions required"}), 422
        
        mmvf_data, message, status_code = change_grouping_of_features_with_ai(task_id, UserInstructions, feature_ids)
    except Exception as e:
        logging.error(f"Error occurred in tbe feature_order_edit: {e}")
        mmvf_data, status, message, status_code = {}, "error", "Something went wrong", 500
    return jsonify({"data":mmvf_data, "status":status, "message": message}), status_code



@blueprint_prefix.route('/feature_hierarchy/miscellaneous:move', methods=['POST'])
@cross_origin(supports_credentials=True)
def miscellaneous_features_move():
    status, message, status_code = "success", "", 200
    try:
        data = request.get_json()
        task_id = data.get("task_id")
        user_id = data.get("user_id")
        miscellaneous_sub_feature_ids: List = data.get("miscellaneous_sub_feature_id", [])
        feature_to_move: List = data.get("feature_to_move", [])
        
        if not task_id or not user_id or not miscellaneous_sub_feature_ids or not feature_to_move:
            return jsonify({"status": "error", "message": "task_id, user_id, miscellaneous_sub_feature_id and feature_to_move are required"}), 422
        if not isinstance(miscellaneous_sub_feature_ids, list) or not isinstance(feature_to_move, list):
            return jsonify({"status": "error", "message": "miscellaneous_sub_feature_id and feature_to_move should be an array"}), 422
        
        result, message, status_code = move_miscellaneous_to_sub_features(user_id, task_id, miscellaneous_sub_feature_ids, feature_to_move)
        
    except Exception as e:
        error_logger.error(f"Error occurred in the miscellaneous_features_move: {e}")
        status, status_code = "error", 500
    
    finally:
        return jsonify({"status": status, "message": message}), status_code
        
    
        

@blueprint_prefix.route('/feature_hierarchy/subfeatures:move', methods=['POST'])
@cross_origin(supports_credentials=True)
def move_subfeatures():
    status, message, status_code = "success", "", 200
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        task_id = data.get("task_id")
        source_feature_id = data.get("source", {}).get("feature_id")
        target_feature_id = data.get("target", {}).get("feature_id")
        sub_feature_ids:List = data.get("source", {}).get("sub_feature_ids", []) # [{"sub_feature_id": "id1", "position": ""}] later we will add position
        
        sub_feature_ids_list = []
        
        for sub_feature in sub_feature_ids:
            if not isinstance(sub_feature, dict):
                status = "error"
                message = "sub_feature_ids should be an array of objects"
                status_code = 422
                break
            
            if not sub_feature.get("sub_feature_id"):
                status = "error"
                message = "source.sub_feature_ids.$.sub_feature_id is required"
                status_code = 422
            else:
                sub_feature_ids_list.append(sub_feature.get("sub_feature_id"))
        if not user_id or (not task_id) or (not source_feature_id) or (not target_feature_id) or (not sub_feature_ids):
            status = "error"
            message = "user_id, task_id, source.feature_id, target.feature_id and source.sub_features are required in payload"
            status_code = 422
        
        if status_code == 200:
            message, status_code = move_subfeatures_from_source_to_target_feature(task_id, user_id, source_feature_id, target_feature_id, sub_feature_ids_list)
    except Exception as e:
        logging.error(f"Error occurred in the feature_order_edit: {e}")
        status, message, status_code = "error", "Something went wrong", 500
    return jsonify({"status":status, "message": message}), status_code


@blueprint_prefix.route('/set_email_status', methods=['POST'])
@cross_origin(supports_credentials=True)
def set_email_status():
    if request.method == 'POST':
        try:
            data = request.get_json()
            task_id = data["task_id"]
            email_status = data['email_status']
            email_id = data['email_id']

            result = update_email_status(task_id=task_id, email_id=email_id, email_status=email_status)

            return jsonify(result), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500


@blueprint_prefix.route('/edit-file-summary', methods=['POST'])
@cross_origin(supports_credentials=True)
def edit_file_summary():
    try:
        data = request.get_json()
        task_id = data["task_id"]
        user_id = data['user_id']
        file_path = data["file_path"]
        feedback_or_edit = data["feedback_or_edit"]
        updated_summary = data["updated_summary"]
        repo_url = data["repo_url"]
        if not task_id or not user_id or not file_path or not feedback_or_edit or not updated_summary or not repo_url:
            return jsonify({"error": "task_id, user_id, file_path, feedback_or_edit, updated_summary, repo_url are required"}), 400


        return file_summary_edit(task_id, user_id, file_path, feedback_or_edit, updated_summary, repo_url)
    except Exception as e:
        print("Error in Edit file summary API", e)
        return jsonify({"error": str(e)}), 500




@blueprint_prefix.route('/user-edit-summary', methods=['POST'])
@cross_origin(supports_credentials=True)
def summary_edit():
    try:
        data = request.get_json()
        task_id = data["task_id"]
        user_id = data['user_id']
        view = data["view"]
        edited_summary = data["edited_summary"]

        return update_edited_summary(user_id, task_id, view, edited_summary)

    except Exception as e:
        print("Error in user summary edit API", e)
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/approve_rlef_req', methods=['POST'])
@cross_origin(supports_credentials=True)
def approve_rlef_req_api():
    try:
        request_data = request.get_json()
        return approve_rlef_req(request_data)

    except Exception as e:
        print("Error in user summary edit API", e)
        return jsonify({"error": str(e)}), 500


@blueprint_prefix.route('/get_project_summary_data', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_project_summary_data():
    user_id = request.args.get('user_id')
    task_id = request.args.get('task_id')
    git_token = request.args.get('git_token')

    if not user_id or not task_id:
        return jsonify({"error": "user_id and task_id are required"}), 400
    if not git_token:
        return jsonify({"error": "git_token is required"}), 400

    # Fetch data from the database
    data = project_summery_db.project_analyzer_task.find_one({"user_id": user_id, "task_id": task_id}, {'_id': 0})

    if not data:
        return jsonify({"error": "No data found"}), 404

    # Structure the response as specified
    if data.get("completion_status") is False:
        response = { "user_id":user_id, "task_id":task_id, "completion_status": False}
        return jsonify(response)
    else:
        project_data = project_summery_db.projects.find_one({"task_id": task_id}, {'_id': 0})
        if not project_data:
            return jsonify({"error": "No data found"}), 404

    github_access = check_github_access(git_token=git_token, github_urls=project_data.get("repo_urls", []))

    if github_access == 401:
        return f"Error : Invalid Token", 403
    elif github_access == 404:
        return f"Error : Access Denied", 403

    # Extract the dashboard data correctly
    dashboard_data = project_data.get("dashboard", {})

    feature_view = calculate_line_counts(project_data=project_data)
    feature_view = mapping_service_name(project_data = project_data)
    feature_view_data = feature_view.get("feature_view", {})
    print("$"*50,"feature_view_data","$"*50)
    print(feature_view_data)
    feature_view_data["feature_view_loc"] = feature_view_data.get("feature_view_loc", {})


    response = {
        "user_id": project_data.get("user_id", ""),
        "task_id": project_data.get("task_id", ""),
        "completion_status": project_data.get("completion_status", ""),
        "repo_urls": project_data.get("repo_urls", []),
        "branch_names":  project_data.get("branch_names", []),
        "project_name": project_data.get("project_name", ""),
        "updated_at": project_data.get("updated_at", ""),
        "assistant_name": project_data.get("assistant_id", ""),
        "feature_view": feature_view_data,

        "dashboard": {
                "executive_summary": dashboard_data.get("updated_executive_summary", ""),
                "language": dashboard_data.get("language", []),
                "vulnerability_check": filtered_vulnerability_checks,
                "content_type": dashboard_data.get("content_type", None),
                "architecture_diagram": dashboard_data.get("architecture_diagram", ""),
                "system_prompt": dashboard_data.get("system_prompt",""),
                "executive_summary_score": dashboard_data.get("executive_summary_score",""),
            }

    }

    return jsonify(response)



@blueprint_prefix.route('/v2/get_project_summary_data', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_project_summary_data_v2():
    try:
        user_id = request.args.get('user_id')
        task_id = request.args.get('task_id')
        git_token = request.args.get('git_token')
        public = request.args.get('public', "false").lower() == "true"
        is_shared = request.args.get('is_shared', "false").lower() == "true"
        
        if is_shared:
            shared_repo_result , owner_user_id, access_type = check_if_project_shared(user_id, task_id)
            # info_logger.info(f"Shared Repo Result: {shared_repo_result}")
            if not shared_repo_result:
                return jsonify({"error": "No data found"}), 404
        
        owner_user_id = owner_user_id if is_shared else user_id
        access_type = access_type if is_shared else "owner"
        # info_logger.info(f"User ID: {user_id}, Task ID: {task_id}, Public: {public}, Is Shared: {is_shared}, Owner User ID: {owner_user_id}, Access Type: {access_type}")
        
        if not user_id or not task_id:
            return jsonify({"error": "user_id and task_id are required"}), 400
        # Fetch data from the database
        data = project_summery_db.project_analyzer_task.find_one({"user_id": owner_user_id, "task_id": task_id}, {'_id': 0})
        # info_logger.info(f"Data fetched from project_analyzer_task: {data}")
        if not data:
            return jsonify({"error": "No data found"}), 404

        project_data = project_summery_db.projects.find_one({"task_id": task_id}, {'_id': 0})
        if not project_data:
            return jsonify({"error": "No data found"}), 404
        
        if not public and access_type != "viewer" and access_type != "view":
            if not git_token:
                return jsonify({"error": "git_token is required"}), 400

            # github_access = check_github_access(git_token=git_token, github_urls=project_data.get("repo_urls", []))

            # if github_access == 401:
            #     return "Error : Invalid Token", 403
            # elif github_access == 404:
            #     return "Error : Access Denied", 403


        dashboard_data = project_data.get("dashboard", {})
        project_data = fetch_fetch_feature_hierarchy_gcp(project_data = project_data, completion_data=data.get("completion_status", []))
        feature_view = calculate_line_counts(project_data=project_data)
        feature_view = mapping_service_name(project_data = project_data)
        
        feature_view_data = feature_view.get("feature_view", {})  

        if not feature_view_data:
            feature_view_data = {
                "feature_hierarchy": {
                    "features": [],
                    "miscellaneous": []
                },
                "feature_summary" : "",
                "feature_summary_score": 0,
                "model_used_in_hierarchy" : "",
                "model_used_in_summary" : ""
            }
        feature_hierarchy = feature_view_data.get("feature_hierarchy", {})
        if isinstance(feature_hierarchy, dict) and not feature_view_data.get("feature_hierarchy", {}).get("miscellaneous"):
            # print("Miscellaneous not found")
            feature_view_data.setdefault("feature_hierarchy", {})["miscellaneous"] = []


        
        vulnerability_checks = dashboard_data.get("vulnerability_check", [])
        formatted_data_list = []
        category_set = set(["Code Readability and Maintainability","Security"])
        severity_set = set(["Error", "Warning", "Info", "Hint"])
        for check in vulnerability_checks:
            formatted_data_dict = {}
            formatted_data_dict["repo_url"] = check.get("repo_url", "")
            for value in check.get("value", []):
                if value.get("suggestions_set", []):
                    category = value.get("category", False)
                    severity = value.get("severity", False)
                    if category and severity:
                        category_set.add(category)
                        severity_set.add(severity)
                        if category not in formatted_data_dict:
                            formatted_data_dict[category] = {}

                        if severity in formatted_data_dict[category]:
                            formatted_data_dict[category][severity] += 1
                        else:
                            formatted_data_dict[category][severity] = 1

            formatted_data_list.append(formatted_data_dict)

        formatted_vulnerability_checks = {"category_variance": list(category_set),"severity_variance": list(severity_set), "data": formatted_data_list}

        version_object = get_version_list_v2(task_id, owner_user_id, project_data.get("project_id")) 

        final_arch_diagram, architecture_versions, is_React = handle_fetch_different_architecture_diagram(dashboard_data, task_id, project_data, owner_user_id)
        
        assistant_name = project_data.get("assistant_id", "")
        chatbot_id = get_chatbot_id(assistant_name=assistant_name)
        
        
        feature_hierarchy = feature_view_data.get("feature_hierarchy", {})
        
        response = {
            "user_id": user_id,
            "owner_user_id": owner_user_id,
            "owner_name": map_user_id_to_info.get(owner_user_id)[1],
            "owner_email": map_user_id_to_info.get(owner_user_id)[0], 
            "access_type": access_type,
            "task_id": project_data.get("task_id", ""),
            "completion_status": data.get("completion_status", []),
            "repo_urls": project_data.get("repo_urls", []),
            "branch_names":  project_data.get("branch_names", []),
            "project_name": project_data.get("project_name") or data.get("project_name") or "",
            "updated_at": project_data.get("updated_at", ""),
            "assistant_name": project_data.get("assistant_id", ""),
            "attempts": project_data.get("attempts", {
              "executive_summary": 0,
              "feature_hierarchy": 0,
              "vulnerability_check": 0,
              "services_used": 0,
              "feature_summary": 0
            }),
            "feature_view": feature_view_data,
            "version_history": format_version_history( [{
                    "user_id": user_id, 
                    "task_id": task_id, 
                    "version": project_data.get("version") or 0,
                    "created_at": project_data.get("created_at"),
                    "status": check_completion_status(data.get("completion_status", True)),
                    "analysis_type": data.get("analysis_type", "old_analysis"),
                }] + version_object),
            "chatbot_id": chatbot_id,
            "email_status": data.get("email_status", None),
            "analysis_type": data.get("analysis_type", "old_analysis"),
            "dashboard": {
                    "executive_summary": dashboard_data.get("updated_executive_summary", ""),
                    "language": dashboard_data.get("language", []),
                    "vulnerability_check": formatted_vulnerability_checks,
                    "content_type": dashboard_data.get("content_type", None),
                    "architecture_diagram": final_arch_diagram,
                    "architecture_versions": architecture_versions,
                    "default_version": dashboard_data.get("default_version", config.ARCHITECTURE_DEFAULT_VERSION),
                    "attempts_count": dashboard_data.get("attempts_count", config.ARCHITECTURE_ATTEMPTS_LIMIT ),
                    "executive_summary_score": dashboard_data.get("executive_summary_score",""),
                    "repo_services": project_data.get("repo_services", dashboard_data.get("repo_services", {})),
                    "model_used_in_repo_services": dashboard_data.get("model_used_in_repo_services", ""),
                    "model_used_in_executive_summary": dashboard_data.get("model_used_in_executive_summary", ""),
                    "model_used_in_architecture": dashboard_data.get("model_used_in_architecture", ""),
                    "dependency_graph_support": dashboard_data.get("dependency_graph_support", {}),
                    "is_reactflow_architecture": is_React

                },
            "ingestion_model_name" : project_data.get("ingestion_model_name", None),
            "chat_model_name" : project_data.get("chat_model_name", None),
            "datasource": project_data.get("datasource", None),
        }
        keys_to_remove = ['system_prompt_used_in_hierarchy', "system_prompt_used_in_summary", "user_prompt_used_in_hierarchy", "user_prompt_used_in_summary"]
        formatted_response = filter_nested_json(response, keys_to_remove)

        return jsonify(formatted_response)

    except Exception as e:
        print(f"Error in get_project_summary_data_v2: {str(e)} Traceback: {traceback.format_exc()}")
        log_into_bigquery("get_project_summary_data", user_id, task_id, str(e), 500)
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/real-time-update', methods=['GET'])
@cross_origin(supports_credentials=True)
def real_time_update():
    try:

        task_id = request.args.get('task_id')
        git_token = request.args.get('git_token')

        if not git_token:
            return jsonify({"error": "git_token is required"}), 400

        project_data = project_summery_db.projects.find_one({"task_id": task_id}, {'_id': 0})
        if not project_data:
            return jsonify({"error": "No data found"}), 404

        github_access = check_github_access(git_token=git_token, github_urls=project_data.get("repo_urls", []))

        if github_access == 401:
            return f"Error : Invalid Token", 403
        elif github_access == 404:
            return f"Error : Access Denied", 403

        from real_time_ingetion import get_currant_project_ingestion_percentage
        def generate():
            while True:
                response = get_currant_project_ingestion_percentage(task_id)
                yield f"data: {json.dumps(response)}\n\n"
                if response == 100:
                    break
                time.sleep(3)

        return Response(stream_with_context(generate()), mimetype='text/event-stream')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/regenerate/architecture-diagram', methods=['POST'])
@cross_origin(supports_credentials=True)
def regenerate_architecture_diagram():
    try:
        data = request.get_json()
        task_id = data.get("task_id")
        user_id = data.get('user_id')
        user_feedback = data.get('user_feedback')
        version_id = data.get("version_id", 0)
        if not task_id or not user_id or not user_feedback:
            return jsonify({"error": "task_id, user_id, user_feedback are required"}), 400

        db_result = project_summery_db.projects.find_one({"task_id": task_id, "user_id": user_id}, {"_id": 0})
        if not db_result:
            return jsonify({"error": "No data found"}), 404
        attempts_count = db_result.get("dashboard", {}).get("attempts_count", 0)

        python_architecture_code = db_result.get("dashboard", {}).get("python_architecture_code", "")
        print("python_architecture_code api call: ", python_architecture_code)
        if attempts_count <= 0:
            return jsonify({"error": f"You have already exceeded {config.ARCHITECTURE_ATTEMPTS_LIMIT} attemps"}), 400
        response, status_code  = start_regeneration_process(task_id, user_id, python_architecture_code, user_feedback, version_id)
        if status_code == 200:

            return jsonify({"architecture_diagram": response, "attemps_left": attempts_count - 1}), status_code
        else:
            print(response)
            return jsonify({"error": response}), status_code

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/approve/architecture-diagram', methods=['PUT'])
@cross_origin(supports_credentials=True)
def approve_architecture_diagram():
    try:
        data = request.get_json()
        task_id = data.get("task_id")
        user_id = data.get("user_id")
        svg_code = data.get("svg_code")
        reactflow_code = data.get("reactflow_diagram", {})

        if not task_id:
            return jsonify({"error": "task_id are required"}), 400

        project_data = project_summery_db.projects.find_one({"task_id": task_id, "user_id": user_id}, {"_id": 0})
        if not project_data:
            return jsonify({"error": "No project found with the given task_id"}), 404

        dashboard_data = project_data.get("dashboard", {})
        architecture_data = dashboard_data.get("architecture_diagram", [])
        reactflow_architecture_diagram_present = project_data.get("dashboard", {}).get("reactflow_architecture_diagram_present", False)
        if reactflow_architecture_diagram_present:
            if len(architecture_data) > 4:
                return jsonify({"message":"Consumed all attempts"}), 405
            version = 0
            # finding the last version
            for architecture_diagram in architecture_data:
                version = max(architecture_diagram.get("version"), version)
            new_version_id = str(uuid.uuid4())
            #creating new architecture diagram data
            new_architecture = {
                "reactflow_diagram": reactflow_code,
                "version": version+1,
                "version_id": new_version_id,
                "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "is_dagre_used":True # believing that the approved architecture will always be beautified by dagre from frontend and then only sent to save in db
            }   
            # Pushing the data into the array
            project_summery_db.projects.update_one(
                {"task_id": task_id, "user_id": user_id},
                {
                    "$push": {"dashboard.architecture_diagram": new_architecture},
                }
            )
            return jsonify({"task_id": task_id, "version_id": new_version_id,"message": "Architecture diagram updated successfully"}), 200

        if not isinstance(architecture_data, list):
            architecture_data = [{
                "svg_code": architecture_data,
                "version": config.ARCHITECTURE_DEFAULT_VERSION,
                "version_id": str(uuid.uuid4()),
                "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            }]

        max_version = max([item.get("version", 0) for item in architecture_data], default=0)
        new_version = max_version + 1

        new_entry = {
            "svg_code": svg_code,
            "version": new_version,
            "version_id": str(uuid.uuid4()),
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }

        architecture_data.append(new_entry)

        # Initialize default_version and attempts_count if not present
        if "default_version" not in dashboard_data:
            dashboard_data["default_version"] = config.ARCHITECTURE_DEFAULT_VERSION
        if "attempts_count" not in dashboard_data:
            dashboard_data["attempts_count"] = config.ARCHITECTURE_ATTEMPTS_LIMIT - 1

        project_summery_db.projects.update_one(
            {"task_id": task_id},
            {"$set": {"dashboard.architecture_diagram": architecture_data, "dashboard.default_version": dashboard_data["default_version"], "dashboard.attempts_count": dashboard_data["attempts_count"]}}
        )

        return jsonify({"task_id": task_id, "version_id": new_entry["version_id"],"message": "Architecture diagram updated successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/fetch/architecture-diagram', methods=['GET'])
@cross_origin(supports_credentials=True)
def fetch_architecture_diagram():
    try:
        task_id = request.args.get('task_id')
        user_id = request.args.get('user_id')
        version_id = request.args.get('version_id')

        if not task_id or not version_id:
            return jsonify({"error": "task_id and version_id are required"}), 400

        project_data = project_summery_db.projects.find_one(
            {"task_id": task_id, "user_id": user_id},
            {'_id': 0}
        )
        if not project_data:
            return jsonify({"error": "No data found"}), 404

        architecture_diagrams = project_data.get("dashboard", {}).get("architecture_diagram", [])
        if not architecture_diagrams:
            return jsonify({"error": "No architecture diagram data found"}), 404

        for diagram in architecture_diagrams:
            if diagram.get("version_id") == version_id:
                reactflow_present = project_data.get("dashboard", {}).get("reactflow_architecture_diagram_present", False)
# "label": The name of the node.
#       - "description": Description of the service.
#       - "associated_files": Associated file and used in files in the service node, defaulting to "No File Available" if not present.
#       - "group": The logical group name (e.g., 'Backend', 'Frontend', 'Database',etc..).
#       - "subgroup": The specific subgroup within the group (e.g., Frameworks, NoSQL Databases, State Management, etc.).
#       - "service": The appropriate filename of the icon based on the $icons_metadata.
#       - "position" : The x and y coordinates 

                if reactflow_present:
                    print("Reactflow diagram present")
                    reactflow_diagram = diagram.get("reactflow_diagram", {})
                    nodes = reactflow_diagram.get("nodes", [])
                    collection = project_summery_db["icons_data"]
                    for i,node in enumerate(nodes):
                        data = node.get("data",{})
                        if not data: continue

                        icon = data.get("service")
                        if not icon:
                            icon = data.get("icon")
                        if icon:
                            result = collection.find_one({"label": icon.lower()})
                            drive_link = result["link"] if result else ""
                            data["link"] = drive_link
                    

                    response = {
                        "created_at": diagram.get("created_at"),
                        "version": diagram.get("version"),
                        "version_id": diagram.get("version_id"),
                        "reactflow_diagram": reactflow_diagram,
                        "is_dagre_used": diagram.get("is_dagre_used", False)
                    }

                else:
                    print("Reactflow diagram not present")
                    response = {
                        "svg_code": diagram.get("svg_code"),
                        "created_at": diagram.get("created_at"),
                        "version": diagram.get("version"),
                        "version_id": diagram.get("version_id")
                    }

                project_summery_db.projects.update_one(
                    {"task_id": task_id, "user_id": user_id},
                    {"$set": {"dashboard.default_version": diagram.get("version")}}
                )

                return jsonify(response), 200

        return jsonify({"error": "No matching version_id found"}), 404

    except Exception as e:
        print(f" Error in fetch_architecture_diagram API: {str(e)} \n\n Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/action_on_architecture_diagram', methods=['POST'])
@cross_origin(supports_credentials=True)
def actions_on_architecture_diagram():
    try:
        request_data = request.get_json()

        task_id = request_data.get("task_id")
        user_id = request_data.get("user_id")
        version_id = request_data.get("version_id")
        actions = request_data.get("actions", [])

        if None in set([task_id, user_id, version_id]):
            return jsonify({"error": "Invalid input"}), 400

        collection = project_summery_db["projects"]

        document_filter = {"task_id": task_id, "user_id": user_id}
        document = collection.find_one(document_filter)
        if not document:
            return jsonify({"error": "Document not found"}), 404

        reactflow_architecture_diagram_present = document.get("dashboard", {}).get("reactflow_architecture_diagram_present")
        if not reactflow_architecture_diagram_present:
            return jsonify({"error": "Architecture Not editable"}), 404
        
        architecture_diagram = document.get("dashboard", {}).get("architecture_diagram", [])
        target_diagram = next((d for d in architecture_diagram if d.get("version_id") == version_id), None)

        if not target_diagram:
            return jsonify({"error": "No architecture diagram found with version mentioned"}), 404
        actions = rearrange_actions(actions=actions)
        response = updating_reactflow_architecture_diagram(task_id, user_id, version_id, actions, target_diagram, collection, architecture_diagram)

        return response

    except Exception as e:
        # Handle unexpected exceptions and print the traceback for debugging
        print(f"Error in actions_on_architecture_diagram API: {str(e)} \n\n Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@blueprint_prefix.route('/choose_services/get_icons_for_architecture_diagram', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_icons_in_categories():
    try:
        results = fetch_icons_under_categories()
        return jsonify({"data": results}), 200
    except Exception as e:
        print(f"Error occurred in get_icons_in_categories API: {e}")
        return jsonify({"error": str(e)}), 500
    

@blueprint_prefix.route('/regenerate-component', methods=['POST'])
@cross_origin(supports_credentials=True)
def regenerate_component():
    try:
        # print("API called")
        data = request.get_json()
        task_id = data["task_id"]
        user_id = data['owner_user_id']
        active_user_id = data['user_id']
        component_name = data.get("component_name")
        user_feedback = data.get("user_feedback")

        git_access_token = request.headers.get("git-access-token")

        if not git_access_token:
            return jsonify({"error": "git-access-token is required in the header"}), 400

        if not task_id or not user_id or not component_name or not user_feedback:
            return jsonify({"error": "task_id, user_id, component_name, component_data, user_feedback are required"}), 400

        response, status_code = start_regeneration_component_process(task_id, user_id, component_name, user_feedback, git_access_token)

        return jsonify(response), status_code

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/download/architecture-diagram', methods=['POST'])
@cross_origin(supports_credentials=True)
def download_architecture_diagram():
    try:
        # Parse the request payload
        data = request.json
        version_id = data.get('version_id')
        task_id = data.get('task_id')

        if not version_id or not task_id:
            return jsonify({"error": "Missing version_id or task_id"}), 400

        # Query the MongoDB collection for the corresponding svg_code
        db_result = project_summery_db.projects.find_one({"task_id": task_id})

        if not db_result:
            return jsonify({"error": "No data found"}), 404

        architecture_versions = db_result.get("dashboard", {}).get("architecture_diagram", {})
        svg_code = ""
        if isinstance(architecture_versions, list):
            for versions in architecture_versions:
                if versions.get("version_id") == version_id:
                    svg_code = versions.get("svg_code", "")
                    break
        else:
            svg_code = architecture_versions

        if not svg_code:
            return jsonify({"error": "No SVG code found"}), 404
        print("svg_code: ", svg_code)

        filename = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        filename_with_extension = f"architecture_diagram_{filename}.svg"

        #Writing to temprorary buffer instead of disk.
        #Because writing in disk has some locked in another process permission issues.

        svg_io = BytesIO()
        svg_io.write(svg_code.encode('utf-8'))
        svg_io.seek(0)

        response = send_file(
            svg_io,
            as_attachment=True,
            download_name=filename_with_extension,
            mimetype='image/svg+xml'
        )

        return response

    except Exception as e:
        print("Error in download_architecture_diagram API: ", str(e))
        return jsonify({"error": str(e)}), 500



@blueprint_prefix.route('/get_mermaid_code', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_mermaid_code():
    try:
        task_id = request.args.get('task_id')

        document = project_summery_db.projects.find_one(
            {'task_id': task_id},
            {"_id": 0, "dashboard.mermaid_code": 1, "repo_services": 1}
        )

        print("document: ", document)

        if document:
            mermaid_code = document.get("dashboard", {}).get("mermaid_code")
            if mermaid_code:
                return jsonify({"mermaid_code": mermaid_code}), 200

            repo_services = document.get("repo_services")
            if not repo_services:
                return jsonify({"error": "repo_services field is missing in the document."}), 400

            service_class_json = create_service_class_json(repo_services)

            print("service_class_json: ", service_class_json)
            mermaid_code = anthropic_service_api_call(service_class_json)
            pattern = r'```Mermaid(.*?)```'
            match = re.search(pattern, mermaid_code, re.DOTALL)
            extracted_mermaid_code = ""
            if match:
                extracted_mermaid_code = match.group(1).strip()
            print("\n\nmermaid code: ", extracted_mermaid_code)

            project_summery_db.projects.update_one(
                {"task_id": task_id},
                {"$set": {"dashboard.mermaid_code": extracted_mermaid_code}}
            )

            return jsonify({"mermaid_code": extracted_mermaid_code}), 200

        else:
            return jsonify({"error": "No document found with the given task_id."}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 400

@blueprint_prefix.route('/edit_mermaid_code', methods=['POST'])
@cross_origin(supports_credentials=True)
def edit_mermaid_code():
    try:
        task_id = request.form.get('task_id')
        edited_mermaid_code = request.form.get('mermaid_code')

        if not task_id or not edited_mermaid_code:
            return jsonify({"error": "Both 'task_id' and 'mermaid_code' are required."}), 400

        document = project_summery_db.projects.find_one({'task_id': task_id})
        if 'dashboard' not in document or 'mermaid_code' not in document['dashboard']:
            return jsonify({"error": "'dashboard.mermaid_code' is missing in the document."}), 400

        update_result = project_summery_db.projects.update_one(
            {"task_id": task_id},
            {"$set": {"dashboard.mermaid_code": edited_mermaid_code}}
        )

        if update_result.matched_count == 0:
            return jsonify({"error": "No document found with the given 'task_id'."}), 404

        service_class_json = anthropic_mermaid_api_call(edited_mermaid_code)
        print("service_class_json: ", service_class_json)
        pattern = r'```json(.*?)```'
        match = re.search(pattern, service_class_json, re.DOTALL)
        extracted_service_json = ""
        if match:
            extracted_service_json = match.group(1).strip()
        project_summery_db.projects.update_one(
            {"task_id": task_id},
            {"$set": {"dashboard.service_class_json": json.loads(extracted_service_json)}}
        )

        return jsonify({"status": "Code updated successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/get_react_flow', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_reactflow_code():
    try:
        task_id = request.args.get('task_id')

        if not task_id:
            return jsonify({"error": "task_id parameter is missing."}), 400

        document = project_summery_db.projects.find_one(
            {'task_id': task_id}
        )

        if not document:
            return jsonify({"error": "No document found with the given task_id."}), 404

        repo_services = document.get("repo_services")
        reactflow_code = document.get("reactflow_code")
        print(repo_services)

        # Check if reactflow_code already exists
        if reactflow_code:
            print("-------------")
            print("Already there")
            return jsonify({"reactflow_code": reactflow_code}), 200

        if repo_services is None:
            return jsonify({"error": f"Services data not available for {task_id}."}), 400

        allowed_keys = {'external_services': [], 'thirdparty_services': []}

        # Check if both keys are missing
        if not any(key in repo_services for key in allowed_keys.keys()):
            return jsonify({"error": f"Services are not available for task id: {task_id}"}), 203

        # Filter repo_services to include only the allowed keys
        filtered_repo_services = {key: repo_services.get(key, []) for key in allowed_keys.keys()}

        # Check if filtered services are empty
        if not filtered_repo_services['external_services'] and not filtered_repo_services['thirdparty_services']:
            return jsonify({"error": "Services are empty."}), 203

        # Call external service to generate reactflow code
        reactflow_code = anthropic_service_reactflow_api_call(filtered_repo_services)

        if reactflow_code:
            project_summery_db.projects.update_one(
                {'task_id': task_id},
                {'$set': {'reactflow_code': [reactflow_code]}}
            )
            return jsonify({"reactflow_code": reactflow_code}), 200
        else:
            print("No reactflow code received.")
            return jsonify({"error": "Failed to generate reactflow code"}), 500

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"error": "An unexpected error occurred."}), 500


@blueprint_prefix.route('/get_assistant_id', methods=['POST'])
@cross_origin(supports_credentials=True)
def get_assistant_id():

    payload = request.get_json()
    print("api /get_assistant_id payload: ", payload)

    git_url = payload.get('git_url')

    if not git_url or not isinstance(git_url, str):
        return jsonify({"error": "git_url must be a string"}), 400

    # Create a list to hold both versions of the URL (with and without .git)
    normalized_urls = [git_url]
    if not git_url.endswith('.git'):
        normalized_urls.append(git_url + '.git')
    else:
        normalized_urls.append(git_url[:-4])

    document = project_summery_db.projects.find_one({"repo_urls": {"$in": normalized_urls}})

    if not document:
        #check with repo name if not found with git_url
        if git_url.endswith('.git'):
            repo_name = git_url.split('/')[-1][:-4]
        else:
            repo_name = git_url.split('/')[-1]
        print("repo_name: ", repo_name)
        document = project_summery_db.projects.find_one({"repo_urls": {"$regex": repo_name, "$options": "i"}})
        print("document: ", document)
        if not document:
            return jsonify({"error": "No matching git_url found"}), 404

    assistant_id = document.get('assistant_id')
    task_id = document.get('task_id')
    chatbot_id = get_chatbot_id(assistant_name=assistant_id)
    # from egpt_ops import empty_agents_list_from_chatbot
    # is_empty_agent = empty_agents_list_from_chatbot(assistant_id)
    # print("Agents Deleted from chatbots : ",is_empty_agent)

    # Accessing exec_summary from within the dashboard object
    exec_summary = document.get('dashboard', {}).get('original_executive_summary')
    # from egpt_ops import add_sys_prompt,toggle_copilot,chnage_model
    # add_sys_prompt(assistant_id)
    # toggle_copilot(assistant_id)
    # chnage_model(assistant_id)
    return jsonify({
        "assistant_id": assistant_id,
        "task_id": task_id,
        "exec_summary": exec_summary,
        "chatbot_id": chatbot_id
    }), 200

@blueprint_prefix.route('/v2/get_assistant_id', methods=['POST'])
@cross_origin(supports_credentials=True)
def get_assistant_id_v2():
    try:
        payload = request.get_json()
        git_url = payload.get('git_url')
        branch_name = payload.get('branch_name')

        if not git_url or not isinstance(git_url, str):
            return jsonify({"error": "git_url must be a string"}), 400

        # Normalize git URL variants
        def normalize_git_urls(url):
            url = url.strip()
            if url.startswith('git@'):
                url = url.replace(':', '/').replace('git@', 'https://')
            url = re.sub(r'\.git$', '', url)
            return list(set([url, url + '.git']))

        normalized_urls = normalize_git_urls(git_url)

        # Direct match by URL
        docs = list(project_summery_db.projects.find({"repo_urls": {"$in": normalized_urls}}))

        # Fallback: match by repo name
        if not docs:
            repo_name_match = re.search(r'/([^/]+?)(?:\.git)?$', git_url)
            repo_name = repo_name_match.group(1) if repo_name_match else ""
            if repo_name:
                docs = list(project_summery_db.projects.find({
                    "repo_urls": {"$regex": repo_name, "$options": "i"}
                }))
            if not docs:
                res = []
                return jsonify(res), 200

        # Deduplicate by task_id
        seen_task_ids = set()
        final_docs = []
        for doc in docs:
            task_id = doc.get('task_id')
            if task_id and task_id not in seen_task_ids:
                seen_task_ids.add(task_id)
                final_docs.append(doc)

        if not final_docs:
            return jsonify([]), 200

        result = []
        for doc in final_docs:
            branches = doc.get('branch_names', [])
            if isinstance(branches, list) and len(branches) == 1:
                repo_urls = doc.get('repo_urls', [])
                match = re.search(r'github\.com[:/][^/]+/([^/]+?)(?:\.git)?$', repo_urls[0]) if repo_urls else None
                repo_name = match.group(1) if match else ""

                git_info = doc.get("git_info", [])
                commit_hash = git_info[0].get("commit_hash", "") if git_info else ""

                result.append({
                    "assistant_id": doc.get("assistant_id"),
                    "task_id": doc.get("task_id"),
                    "repo_name": repo_name,
                    "branch_name": branches[0],
                    "project_name": doc.get("project_name"),
                    "project_id": doc.get("project_id", ""),
                    "commit_hash": commit_hash,
                    "created_at": doc.get("created_at"),
                    "emb_model_config": get_emb_model_config(task_id=doc.get("task_id")),
                })

        result.sort(key=lambda x: x['created_at'], reverse=True)
        return jsonify(result), 200

    except Exception as e:
        print(f"Error in get_assistant_id API: {str(e)}")
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/create_module_branch', methods=['POST'])
@cross_origin(supports_credentials=True)
def create_module_branch_api():
    try:
        data = request.get_json()
        file_paths = data.get('file_path', [])
        branch_name = data.get('branch_name')
        access_token = request.headers.get('git-access-token')

        if not file_paths or not branch_name or not access_token:
            return jsonify({"error": "Missing required parameters or header"}), 400

        results = create_module_branch(file_paths, branch_name, access_token)

        return jsonify(results), 200

    except Exception as e:
        print(f"Error in create_module_branch_api: {str(e)}")
        return jsonify({"error": str(e)}), 500

@blueprint_prefix.route('/get_project_services', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_project_services():
    user_id = request.args.get('user_id')
    task_id = request.args.get('task_id')
    git_token = request.args.get('git_token')
    if not user_id or not task_id:
        return jsonify({"error": "user_id and task_id are required"}), 400
    if not git_token:
        return jsonify({"error": "git_token is required"}), 400

    # Fetch data from the database
    data = project_summery_db.project_analyzer_task.find_one({"user_id": user_id, "task_id": task_id}, {'_id': 0})

    if not data:
        return jsonify({"error": "No data found"}), 404

    # Structure the response as specified
    if data.get("completion_status") is False:
        response = { "user_id":user_id, "task_id":task_id, "completion_status": False}
        return jsonify(response)
    else:
        project_data = project_summery_db.projects.find_one({"task_id": task_id}, {'_id': 0})
        if not project_data:
            return jsonify({"error": "No data found"}), 404

    github_access = check_github_access(git_token=git_token, github_urls=project_data.get("repo_urls", []))

    if github_access == 401:
        return f"Error : Invalid Token", 403
    elif github_access == 404:
        return f"Error : Access Denied", 403

    repo_services = project_data.get("repo_services", None)
    if repo_services is None:
        return jsonify({"error":"Services are not generated"}),400

    service_list = []

    service_type_mapping = {
        "external_services": "External Service",
        "thirdparty_services": "Thirdparty Service",
    }

    for service_category, services in repo_services.items():
        service_type = service_type_mapping.get(service_category, "Unknown Service Type")

        for service in services:
            document = {
                "associated_files": service.get("associated_files", []),
                "description": service.get("description", ""),
                "name": service.get("external_service_name") or service.get("thirdparty_service_name", ""),
                "service_class": service.get("service_class", ""),
                "service_type": service_type
            }

            service_list.append(document)

    return jsonify({"service_names":service_list}),200

@blueprint_prefix.route('/architecture/save-architecture', methods=['POST'])
@cross_origin(supports_credentials=True)
def update_architecture_and_services_data():
    """This API will save the architecture code and generate updated services from it.
    The updated serives json will be updated in database
    """
    try:
        data = request.json
        task_id = data.get("task_id")
        user_id = data.get("user_id")
        language = data.get("language", "default")
        reactflow_code = data.get("design")
        status = data.get("status", "pending")

        allowed_status = ["approved", "pending", "denied"]

        # Validate the payload
        if not task_id or not isinstance(task_id, str):
            return jsonify({"error": "Missing or invalid task id"}), 400

        if not reactflow_code or not isinstance(reactflow_code, dict):
            return jsonify({"error": "Missing or invalid reactflow_code"}), 400

        if not user_id or not isinstance(user_id, str):
            return jsonify({"error": "Missing or invalid user_id"}), 400

        if not isinstance(language, str) or language == "":
            return jsonify({"error": "language parameter is invalid."}), 400

        # Clean payload
        language = language.lower().strip()
        user_id = user_id.strip()
        task_id = task_id.strip()

        if status not in allowed_status:
            return jsonify({"error": f"Invalid status passed. Following are allowed: {', '.join(allowed_status)}"}), 400

        # Fetch required data from project collection
        document = project_summery_db.projects.find_one(
            {'task_id': task_id},
            {"_id": 0, "repo_services": 1, "task_id": 1}
        )
        # Check if document exists
        if not document:
            return jsonify({"error": f"Data not found for task id: {task_id}"}), 404

        # Outdated/Current services data
        prev_services = document.get('repo_services', {})
        if not prev_services:
            return jsonify({"error": f"Services data not found for task id: {task_id}"}), 404

        # Fetch updated services from reactflow data
        updated_services = anthropic_reactflow_to_services_call(reactflow_code, prev_services)
        print("updated services: ", updated_services)

        if updated_services is None:
            return jsonify({"error": "Failed to generate the services using AI"}), 500

        # Save and update the data in required collections in db
        operation_status = save_architecture_and_services(reactflow_code, updated_services, task_id, user_id, status, language)

        if operation_status is False:
            return jsonify({"error": "Failed to save and update the data in database"}), 500

        return jsonify({"message": "Data successfully saved"}), 200

    except Exception as e:
        traceback.print_exc()
        print(f"Error in save_reactflow_code API: {str(e)}")
        return jsonify({"error": str(e)}), 500


@blueprint_prefix.route('/architecture/v2/save-architecture', methods=['POST'])
@cross_origin(supports_credentials=True)
def update_architecture_and_services_data_v2():
    """This API will save the architecture code and generate updated services from it.
    The updated serives json will be updated in database
    """
    try:
        print("\nExecuting save-architecture endpoint")
        data = request.json
        repo_url = data.get('repo_url', '')
        branch_name = data.get('branch_name', '')
        task_id = data.get("task_id")
        user_id = data.get("user_id")
        language = data.get("language", "default")
        reactflow_code = data.get("design")
        prev_services = data.get("previous_services")
        status = data.get("status", "pending")

        allowed_status = ["approved", "pending", "denied"]

        # Validate the payload
        if not task_id or not isinstance(task_id, str):
            return jsonify({"error": "Missing or invalid task id"}), 400
        if not reactflow_code or not isinstance(reactflow_code, dict):
            return jsonify({"error": "Missing or invalid reactflow_code"}), 400
        if not user_id or not isinstance(user_id, str):
            return jsonify({"error": "Missing or invalid user_id"}), 400
        if not isinstance(language, str) or language == "":
            return jsonify({"error": "language parameter is invalid."}), 400
        if not repo_url or not isinstance(repo_url, str):
            return jsonify({"error": "'repo_url' missing or invalid"}), 400
        if not branch_name or not isinstance(branch_name, str):
            return jsonify({"error": "'branch_name' missing or invalid"}), 400
        if not prev_services or not isinstance(prev_services, dict):
            return jsonify({"error": "'previous_services' missing or invalid. Should be a json object."}), 400

        # Clean payload
        repo_url = repo_url.strip()
        branch_name = branch_name.strip()
        language = language.lower().strip()
        user_id = user_id.strip()
        task_id = task_id.strip()

        if status not in allowed_status:
            return jsonify({"error": f"Invalid status passed. Following are allowed: {', '.join(allowed_status)}"}), 400

        # Fetch required data from project collection
        exists = project_summery_db.projects.count_documents({'task_id': task_id}, limit=1) > 0

        # Check if document exists
        if not exists:
            return jsonify({"error": f"Analysis data not found for task id: {task_id}"}), 404

        # Fetch updated services from reactflow data
        updated_services = anthropic_reactflow_to_services_call(reactflow_code, prev_services)
        # print("updated services: ", updated_services)

        if updated_services is None:
            return jsonify({"error": "Failed to generate the services using AI"}), 500

        # Save and update the data in required collections in db
        operation_status = save_architecture_and_services_v2(
            reactflow_code, updated_services,
            repo_url, branch_name,
            task_id, user_id,
            status, language,
            update_project_analyzer=False
            )

        if operation_status is False:
            return jsonify({"error": "Failed to save and update the data in database"}), 500

        return jsonify({"message": "Data successfully saved"}), 200

    except Exception as e:
        traceback.print_exc()
        print(f"Error in save_reactflow_code API: {str(e)}")
        return jsonify({"error": str(e)}), 500


@blueprint_prefix.route('/architecture/get_architecture', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_architecture_data():
    """API to fetch or generate the architecture data
    """
    try:
        task_id = request.args.get('task_id')
        user_id = request.args.get('user_id')
        language = request.args.get('language', 'default')
        reload_architecture = request.args.get('reload', "false")

        # Validate payload
        if not task_id or not isinstance(task_id, str):
            return jsonify({"error": "task_id parameter is missing or invalid."}), 400
        if not user_id or not isinstance(user_id, str):
            return jsonify({"error": "task_id parameter is missing or invalid."}), 400
        if not isinstance(language, str) or language.strip().lower() == "":
            return jsonify({"error": "language parameter is invalid."}), 400
        if not isinstance(reload_architecture, str) or reload_architecture.strip().lower() == "" or reload_architecture.strip().lower() not in ('true', 'false'):
            return jsonify({"error": "reload_architecture parameter is invalid, should be true or false as string"}), 400

        # Clean payload
        language = language.lower().strip()
        user_id = user_id.strip()
        task_id = task_id.strip()
        reload_architecture = reload_architecture.lower().strip()

        # If reload_architecture True then saved architectures will be ignored and new one will be regenerated
        if reload_architecture == "false":
            # Get the latest 5 documents sorted by 'approved_at' in descending order
            latest_documents = list(project_summery_db.architecture.find(
                {"task_id": task_id, "approved_at": {"$exists": True}, "language": language},
                {"_id": 0}
            ).sort("approved_at", DESCENDING).limit(5))

            if len(latest_documents) > 0:
                print("Designs found in db")

                # # Update the repo_services in project collection with latest data (switch target language flow)
                # latest_services_data = latest_documents[0].get("services")
                # result2 = project_summery_db.projects.update_one(
                #         {"task_id": task_id},
                #         {"$set": {"repo_services": latest_services_data}},
                #     )
                # # check if successful
                # if result2.modified_count > 0:
                #     print("Project document found and updated")
                # else:
                #     print("Project document found but not modified")

                return jsonify(latest_documents), 200

        # ------------------------------------------------------------
        print("\nCreating new architecture.")

        # Fetch the latest default services json from architecture collection
        latest_documents = list(project_summery_db.architecture.find(
            {"task_id": task_id, "approved_at": {"$exists": True}, "language": "default"},
            {"_id": 0}
        ).sort("approved_at", DESCENDING))

        first_source_services = None
        recent_source_services = None
        final_repo_services = None

        if len(latest_documents) > 0:
            first_source_services = latest_documents[-1].get("services")
            recent_source_services = latest_documents[0].get("services")

        if not recent_source_services:
            if language != "default":
                return jsonify({"error": f"Source services not found, please create the Source architecture first for task id: {task_id}."}), 400
            else:
                # Case when language is default and no architecture for default already exist
                document = project_summery_db.projects.find_one(
                    {'task_id': task_id}
                )
                if not document:
                    return jsonify({"error": f"No project analysis task data found related to provided task id: {task_id}"}), 404

                # language is default and source services are not present. Create a new architecture! Proceed with usual flow.
                repo_services = document.get("repo_services")
                if repo_services is None:
                    return jsonify({"error": f"Services data not available for {task_id}."}), 400

                allowed_keys = {'external_services': [], 'thirdparty_services': []}
                # Check if both keys are missing
                if not any(key in repo_services for key in allowed_keys.keys()):
                    return jsonify({"error": f"Services are not available for task id: {task_id}"}), 203

                filtered_repo_services = {key: repo_services.get(key, []) for key in allowed_keys.keys()}
                if not filtered_repo_services['external_services'] and not filtered_repo_services['thirdparty_services']:
                    return jsonify({"error": "Services are empty."}), 203

                final_repo_services = filtered_repo_services
                reactflow_code = anthropic_service_reactflow_api_call(final_repo_services)
        else:
            print("Source(Default) services found in architecture collection")
            # Case when reload_architecture is True for language "default"
            if language == "default":
                final_repo_services = first_source_services
                print("\nUsing the firstmost source architecture:", final_repo_services)
                reactflow_code = anthropic_service_reactflow_api_call(final_repo_services)
            else:
                final_repo_services = recent_source_services
                print("\nUsing the most recent source architecture:", final_repo_services)
                reactflow_code = anthropic_service_reactflow_api_call(final_repo_services)
                print(f"\nConverting reactflow for target language: {language}")
                reactflow_code = anthropic_reactflow_code_conversion(reactflow_code, language)
                print("\nConverted reactflow code: ", reactflow_code)

        # Get updated services json from the reactflow_code generated
        updated_services = anthropic_reactflow_to_services_call(reactflow_code, final_repo_services)

        # Save in db if generated successfully
        if reactflow_code:
            check = save_architecture_and_services(
                design_code=reactflow_code,
                updated_services=updated_services,
                task_id=task_id,
                user_id=user_id,
                status="approved",
                language=language,
                update_project_analyzer=True
            )
            if check is True:
                # Fetch 5 most recent architectures
                latest_documents = list(project_summery_db.architecture.find(
                    {"task_id": task_id, "approved_at": {"$exists": True}, "language": language},
                    {"_id": 0}
                ).sort("approved_at", DESCENDING).limit(5))

                return jsonify(latest_documents), 200
            else:
                return jsonify({"error": "Failed to save the genrated design data in database"}), 500
        else:
            print("No reactflow code received.")
            return jsonify({"error": "Failed to generate architecture design"}), 500

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500
    
    
# @blueprint_prefix.route('/architecture/v2/get_architecture', methods=['GET'])
# @cross_origin(supports_credentials=True)
# def v2_get_architecture_data():
#     """API to fetch or generate the architecture data
#     """
#     try:
#         task_id = request.args.get('task_id')
#         user_id = request.args.get('user_id')
#         language = request.args.get('language', 'default')
#         reload_architecture = request.args.get('reload', "false")

#         # Validate payload
#         if not task_id or not isinstance(task_id, str):
#             return jsonify({"error": "task_id parameter is missing or invalid."}), 400
#         if not user_id or not isinstance(user_id, str):
#             return jsonify({"error": "task_id parameter is missing or invalid."}), 400
#         if not isinstance(language, str) or language.strip().lower() == "":
#             return jsonify({"error": "language parameter is invalid."}), 400
#         if not isinstance(reload_architecture, str) or reload_architecture.strip().lower() == "" or reload_architecture.strip().lower() not in ('true', 'false'):
#             return jsonify({"error": "reload_architecture parameter is invalid, should be true or false as string"}), 400

#         # Clean payload
#         language = language.lower().strip()
#         user_id = user_id.strip()
#         task_id = task_id.strip()
#         reload_architecture = reload_architecture.lower().strip()

#         # If reload_architecture True then saved architectures will be ignored and new one will be regenerated
#         if reload_architecture == "false":
#             # Get the latest 5 documents sorted by 'approved_at' in descending order
#             latest_documents = list(project_summery_db.architecture.find(
#                 {"task_id": task_id, "approved_at": {"$exists": True}, "language": language},
#                 {"_id": 0}
#             ).sort("approved_at", DESCENDING).limit(5))

#             if len(latest_documents) > 0:
#                 print("Designs found in db")
#                 info_logger.info(f"Designs found in db for task_id: {task_id} \n {latest_documents}")
                
                # improved_struct_data, status = get_formatted_architecture_data(latest_documents)
                
#                 info_logger.info(f"Improved structured data: {improved_struct_data}")
#                 if status != 200:
#                     return jsonify({
#                         "error": "Failed to get formatted architecture data"
#                     }), status
#                 return jsonify(improved_struct_data), 200


#         # ------------------------------------------------------------
#         # print("\nCreating new architecture.")

#         # Fetch the latest default services json from architecture collection
#         info_logger.info(f"Fetching the latest default services json from architecture collection for task_id: {task_id}")
        
        
#         latest_documents = list(project_summery_db.architecture.find(
#             {"task_id": task_id, "approved_at": {"$exists": True}, "language": "default"},
#             {"_id": 0}
#         ).sort("approved_at", DESCENDING))
#         info_logger.info(f"Latest documents: {latest_documents}")
#         first_source_services = None
#         recent_source_services = None
#         final_repo_services = None
#         # print("latest_documents: ", latest_documents)
#         if len(latest_documents) > 0:
#             first_source_services = latest_documents[-1].get("services")
#             recent_source_services = latest_documents[0].get("services")

#         if not recent_source_services:
#             if language != "default":
#                 return jsonify({"error": f"Source services not found, please create the Source architecture first for task id: {task_id}."}), 400
#             else:
#                 # Case when language is default and no architecture for default already exist
#                 document = project_summery_db.projects.find_one(
#                     {'task_id': task_id}
#                 )
#                 if not document:
#                     return jsonify({"error": f"No project analysis task data found related to provided task id: {task_id}"}), 404

#                 # language is default and source services are not present. Create a new architecture! Proceed with usual flow.
#                 repo_services = document.get("repo_services")
#                 info_logger.info(f"Services data fetched from project collection: {repo_services}")
#                 if repo_services is None:
#                     return jsonify({"error": f"Services data not available for {task_id}."}), 400

#                 allowed_keys = {'external_services': [], 'thirdparty_services': []}
#                 # Check if both keys are missing
#                 if not any(key in repo_services for key in allowed_keys.keys()):
#                     return jsonify({"error": f"Services are not available for task id: {task_id}"}), 203

#                 filtered_repo_services = {key: repo_services.get(key, []) for key in allowed_keys.keys()}
#                 info_logger.info(f"Filtered repo services: {filtered_repo_services}")
#                 if not filtered_repo_services['external_services'] and not filtered_repo_services['thirdparty_services']:
#                     info_logger.info(f"Services are empty.")
#                     # return jsonify({"error": "Services are empty."}), 203
#                     reactflow_code = []
#                     improved_struct_data, status = get_formatted_architecture_data(latest_documents)
#                     info_logger.info(f"Improved structured data: {improved_struct_data}")
#                     info_logger.info(f"Status: {status}")
#                     if status != 200:
#                         return jsonify({
#                             "error": "Failed to get formatted architecture data"
#                         }), status
#                     return jsonify(improved_struct_data), 200

#                 else:
#                     info_logger.info(f"Filtered services: {filtered_repo_services}")    
#                     final_repo_services = filtered_repo_services
#                     reactflow_code = anthropic_service_reactflow_api_call(final_repo_services)
#         else:
#             print("Source(Default) services found in architecture collection")
#             # Case when reload_architecture is True for language "default"
#             if language == "default":
#                 final_repo_services = first_source_services
#                 # print("\nUsing the firstmost source architecture:", final_repo_services)
#                 reactflow_code = anthropic_service_reactflow_api_call(final_repo_services)
#             else:
#                 final_repo_services = recent_source_services
#                 # print("\nUsing the most recent source architecture:", final_repo_services)
#                 reactflow_code = anthropic_service_reactflow_api_call(final_repo_services)
#                 # print(f"\nConverting reactflow for target language: {language}")
#                 reactflow_code = anthropic_reactflow_code_conversion(reactflow_code, language)
#                 # print("\nConverted reactflow code: ", reactflow_code)

#         updated_services = anthropic_reactflow_to_services_call(reactflow_code, final_repo_services)

#         # Save in db if generated successfully
#         if reactflow_code:
#             check = save_architecture_and_services(
#                 design_code=reactflow_code,
#                 updated_services=updated_services,
#                 task_id=task_id,
#                 user_id=user_id,
#                 status="approved",
#                 language=language,
#                 update_project_analyzer=True
#             )
#             if check is True:
#                 # Fetch 5 most recent architectures
#                 latest_documents = list(project_summery_db.architecture.find(
#                     {"task_id": task_id, "approved_at": {"$exists": True}, "language": language},
#                     {"_id": 0}
#                 ).sort("approved_at", DESCENDING).limit(5))

#                 return jsonify(latest_documents), 200
#             else:
#                 return jsonify({"error": "Failed to save the genrated design data in database"}), 500
#         else:
#             print("No reactflow code received.")
#             return jsonify({"error": "Failed to generate architecture design"}), 500

#     except Exception as e:
#         print(f"An error occurred: {e}")
#         return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@blueprint_prefix.route('/architecture/v2/get_architecture', methods=['GET'])
@cross_origin(supports_credentials=True)
def v2_get_architecture_data():
    try:
        task_id = request.args.get('task_id')
        user_id = request.args.get('user_id')
        
        if not task_id or not user_id:
            return jsonify({"error": "task_id and user_id are required"}), 400

        
        architecture_data, status = get_updated_services_modernisation(task_id, user_id)
        
        return jsonify(architecture_data), status
    except Exception as e:
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500
    

    



@blueprint_prefix.route('/architecture/services', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_catalog_services():
    try:
        # Fetch the data from the db
        docs = list(project_summery_db.architecture_services.find())

        for category in docs:
            doc_id = category.pop("_id", None)
            if doc_id:
                category["id"] = str(doc_id)

        if docs:
            return jsonify(docs), 200
        else:
            return jsonify({"error": "No services found in the database"}), 404

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@blueprint_prefix.route('/architecture/chat-bot', methods=['POST'])
@cross_origin(supports_credentials=True)
def architecture_chatbot():
    try:
        data = request.json
        task_id = data.get("task_id", None)
        language = data.get("language", None)
        chats = data.get("messages", [])

        # Validate payload
        if not chats or not isinstance(chats, list):
            return jsonify({"error": "Messages not found or invalid (should be an array of json)"}), 400
        if not language or not isinstance(language, str):
            return jsonify({"error": "Language not found or invalid"}), 400
        if not task_id or not isinstance(task_id, str):
            return jsonify({"error": "task_id parameter is missing or invalid."}), 400

        # Fetch the services json for the provided language to be passed to ai for reference
        doc = project_summery_db.architecture.find_one(
            {"task_id": task_id, "approved_at": {"$exists": True}, "language": language},
            sort=[("approved_at", DESCENDING)],
            projection={"services": 1}
        )

        if not doc:
            return jsonify({"error": f"Architecture data not found for given task id and target language"}), 404

        services = doc.get("services", None)
        if not services:
            print(f"No services found for task_id: {task_id} and language: {language}")

        # Generate the ai response
        answer = anthropic_architecture_chatbot_call(chats, services)

        if not answer:
            ai_response = {"role": "assistant", "content": "I'm sorry, I wasn't able to process your request at the moment. Please try again later."}
        else:
            ai_response = {"role": "assistant", "content": answer}

        # Append in the messages (chat history)
        chats.append(ai_response)

        final_response = {"messages": chats}

        return jsonify(final_response), 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@blueprint_prefix.route('/architecture/services-autosuggest', methods=['GET'])
@cross_origin(supports_credentials=True)
def suggest_service_names():
    try:
        # Define the aggregation pipeline
        pipeline = [
            {
                "$project": {
                    "_id": 0,      # Exclude the _id field
                    "services": 1   # Include the services field
                }
            }
        ]

        # Execute the aggregation pipeline
        matching_services_cursor = project_summery_db.architecture_services.aggregate(pipeline)

        # Convert the cursor to a list and process the result
        matching_services = []
        for doc in matching_services_cursor:
            matching_services.extend(doc.get("services", []))  # Flatten the services into the list

        # If no matching services are found, return an empty list
        if not matching_services:
            return jsonify({"services": []}), 200

        # Return the matching services as JSON
        return jsonify({"services": matching_services}), 200

    except Exception as e:
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500
    


@blueprint_prefix.route('/architecture/v2/services-autosuggest', methods=['POST'])
@cross_origin(supports_credentials=True)
def v2_suggest_service_names():
    try:
        data = request.json
        target_languages = data.get("target_languages", [])

        collection = project_summery_db.architecture_services
        
        result = collection.find_one({"category": "modularization_new"}, {"_id": 0, "services": 1})
        groups = result.get("services", []) if result else []


        filtered_groups = []
        for group in groups:
            filtered_services = [
                service for service in group.get("services", [])
                if not service.get("languages") or any(lang in target_languages for lang in service.get("languages", []))
            ]
            if filtered_services:
                filtered_groups.append({
                    "group": group["group"],
                    "services": filtered_services
                })

        return jsonify(filtered_groups), 200

    except Exception as e:
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


    
    


@blueprint_prefix.route('/architecture/suggest-services', methods=['POST'])
@cross_origin(supports_credentials=True)
def suggest_service_names_description():
  try:
    data = json.loads(request.data)
    description = data.get("description","")

    # Check if description is either not an instance of str or if it is an empty string
    if not isinstance(description, str) or len(description) == 0:
        return jsonify({"error": "Description is compulsory and must be a string"}), 400

    ai_suggestion=get_service_names(description)

    if ai_suggestion is None:
            return jsonify({"error": "Failed to generate suggestions"}), 500

    return ai_suggestion,200

  except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON format in request data."}), 400

  except Exception as e:
    return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@blueprint_prefix.route('/generate-sql-file', methods=['POST'])
@cross_origin(supports_credentials=True)
def generate_dql_file():
    try:
        data = request.get_json()

        sql_data = data.get("sql_data")

        if not sql_data:
            return jsonify({"error": "dql_data are required"}), 400
        from sql_dump_file import get_sql_query
        result = get_sql_query(sql_data)
        unique_id = uuid.uuid4()
        file_path = os.getcwd() + f"/{unique_id}.sql"

        with open(file_path, "w") as file:
            file.write(result)
        return send_file(file_path, as_attachment=True, download_name=f"{unique_id}.sql")


    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@blueprint_prefix.route("/generate-uml-document", methods=['POST'])
def generate_feature_documentation():
    try:
        data = request.json
        logger.info(f"Received data: {data}")

        task_id = data.get("task_id", None)
        feature_id = data.get("feature_id", None)
        f_id = str(uuid.uuid4())
        if feature_id is None:
            feature_id = f_id
        print(f"Feature ID: {feature_id} and Task ID: {task_id}")
        if not task_id or not feature_id:
            return jsonify({"status": "error", "message": "task_id and feature_id are required"}), 400
        
        logger.info(f"Starting step 1")
        uml_id = str(uuid.uuid4())


        git_pat_token = request.headers.get("git-access-token", None)
        if not git_pat_token:
            return jsonify({"status": "error", "message": "'git-access-token' is required header field"}), 400

        logger.info(f"Starting step 2")
        existing_uml_id = check_document_already_generated(task_id, feature_id)

        logger.info(f"Starting step 3")
        if existing_uml_id:
            return  jsonify({"uml_id": existing_uml_id, "message":"Document already generated"}),200

        module_data = get_module_data_from_db(task_id, feature_id)
        logger.info((f"Module data: {module_data}"))
        update_uml_documents_in_db(task_id, uml_id, feature_id, module_data)
        logger.info(f"Starting step 4")
        threading.Thread(target=generate_uml_documentation, args=(module_data, git_pat_token,uml_id)).start()

        return jsonify({"uml_id": uml_id}), 200

    except Exception as e:
        logger.error(f"Error occurred: {str(e)} Traceback: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500

@blueprint_prefix.route("/regenerate-document", methods=['POST'])
def regenerate_feature_documentation():
    try:
        data = request.json
        uml_id = data.get("uml_id")
        uml_document = project_summery_db.uml_documents.find_one({"uml_id":uml_id})
        module_data = uml_document.get("module_data", [])
        git_pat_token = request.headers.get("git-access-token", None)
        if not git_pat_token:
            return jsonify({"status": "error", "message": "'git-access-token' is required header field"}), 400

        if not module_data:
            return jsonify({"Documentation not generated for this uml_id"}), 400

        print("1")
        reset_db_for_regenerate(uml_id)
        print("2")
        threading.Thread(target=generate_uml_documentation, args=(module_data, git_pat_token,uml_id)).start()

        return jsonify({"uml_id": uml_id}), 200

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@blueprint_prefix.route("/get-uml-details", methods = ['GET'])
def fetching_uml_details():
    try:
        uml_id = request.args.get("uml_id")

        if not uml_id:
            return jsonify({"status":"error","message":"uml_id is not given as url parameters"}), 400

        uml_document = project_summery_db.uml_documents.find_one({"uml_id":uml_id})


        if not uml_document:
            return jsonify({"status": "error", "message": "UML document not found"}), 404

        error_logs = uml_document.get("error_logs", {})

        status_code = error_logs.get("status_code", 500)
        error_message = error_logs.get("error_message", "An unknown error occurred")
        error_type = error_logs.get("error_type", "UnknownError")
        stack_trace = error_logs.get("stack_trace", "NoStackTrace")

        if status_code != 200:
            logger.error(f"Error occurred: {error_message} Traceback: {stack_trace}")
            return jsonify ({
                "status": "error",
                "error_type": error_type,
                "error_message": error_message,
                "stack_trace": stack_trace
            }), status_code

        # Remove '_id' from the document
        if '_id' in uml_document:
            del uml_document['_id']

        return jsonify(uml_document),200

    except Exception as e:
        print(f"Error occurred: {str(e)} Traceback: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500

@blueprint_prefix.route("/update-uml-document", methods=['POST'])
def updating_uml_documentation():
    try:
        data = request.json
        print(f"Received data: {data}")  # Debugging line
        uml_id = data.get("uml_id")
        mode  = data.get("mode")


        print("mode: ", mode)
        if mode not in ["developer", "designer"]:
            return jsonify({"status": "error", "message": "Invalid mode. Allowed values: 'developer' or 'designer'"}), 400

        updated_uml_document = data.get("updated_document")

        if not uml_id:
            raise ValueError("uml_id is missing or None")

        update_uml_document(uml_id, updated_uml_document, mode)

        return jsonify({"uml_id": uml_id, "message": "UML documents updated successfully"}), 200
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@blueprint_prefix.route('/set-uml-email-status', methods=['POST'])
@cross_origin(supports_credentials=True)
def set_documentation_status_and_send_email():
    if request.method == 'POST':
        try:
            data = request.json
            uml_id = data.get("uml_id")
            email_id = data.get('email_id')

            result, status_code = update_uml_email_status(uml_id=uml_id, email_id=email_id)

            return jsonify(result), status_code

        except Exception as e:
            return jsonify({"error": str(e)}), 500


@blueprint_prefix.route('/architecture/list-versions', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_task_id_list():
    try:
        print("\nExecuting list-version endpoint")
        repo_url = request.args.get('repo_url', '')
        branch_name = request.args.get('branch_name', '')

        # Clean the parameters:
        repo_url = repo_url.strip()
        branch_name = branch_name.strip()

        # Validate the parameters:
        if not repo_url or not isinstance(repo_url, str):
            return jsonify({"error": "'repo_url' missing or invalid"}), 400
        if not branch_name or not isinstance(branch_name, str):
            return jsonify({"error": "'branch_name' missing or invalid"}), 400

        # Fetch the data from the db
        docs = list(
            project_summery_db.projects.find(
                {"repo_urls": repo_url, "branch_names": branch_name},
                {"task_id": 1, "created_at": 1}
                ).sort("created_at", -1)
            )

        # Extract list of task_ids from the sorted documents
        task_ids = [doc["task_id"] for doc in docs]
        if not task_ids:
            return jsonify({"error": f"Data not foud for repo_url: '{repo_url}' and branch_name: '{branch_name}'"}), 404
        print("\nTotal task ids: ", task_ids)

        # check which task ids have successfully completed generation of repo_services
        query = {
            "task_id": {"$in": task_ids},
            "completion_status.dashboard.repo_services.status": "completed"
        }
        # Fetch the matching documents
        matching_docs = list(project_summery_db.project_analyzer_task.find(query, {"task_id": 1, "completion_status.dashboard.repo_services": 1}))
        print("\nStatus docs matched for task ids: ", matching_docs)

        # Extract the task_id from the matching documents
        completed_task_ids = [doc["task_id"] for doc in matching_docs if "task_id" in doc]
        if not completed_task_ids:
            print("No task ids found with repo_services generation status 'completed'")
            return jsonify(
                {"error": f"No task ids found with repo_services generation status 'completed' for repo_url: '{repo_url}' and branch_name: '{branch_name}'"}
                ), 404

        print("\nFinal list of task ids:", completed_task_ids)
        final_response = {
            "task_ids": completed_task_ids
        }
        return jsonify(final_response), 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@blueprint_prefix.route('/architecture/v2/chat-bot', methods=['POST'])
@cross_origin(supports_credentials=True)
def architecture_chatbot_v2():
    try:
        data = request.json
        task_id = data.get("task_id", None)
        language = data.get("language", None)
        chats = data.get("messages", [])
        repo_url = data.get("repo_url")
        branch_name = data.get("branch_name")

        # Validate payload
        if not chats or not isinstance(chats, list):
            return jsonify({"error": "Messages not found or invalid (should be an array of json)"}), 400
        if not language or not isinstance(language, str):
            return jsonify({"error": "Language not found or invalid"}), 400
        if not task_id or not isinstance(task_id, str):
            return jsonify({"error": "task_id parameter is missing or invalid."}), 400
        if not repo_url or not isinstance(repo_url, str):
            return jsonify({"error": "repo_url not found or invalid"}), 400
        if not branch_name or not isinstance(branch_name, str):
            return jsonify({"error": "branch_name parameter is missing or invalid."}), 400
        repo_url = repo_url.strip()
        branch_name = branch_name.strip()
        language = language.strip().lower()
        # Fetch the services json for the provided language to be passed to ai for reference
        doc = project_summery_db.architecture.find_one(
            {"task_id": task_id, "approved_at": {"$exists": True}, "language": language, "repo_urls": {"$in": [repo_url]}, "branch_names":{"$in": [branch_name]}},
            sort=[("approved_at", DESCENDING)],
            projection={"services": 1}
        )


        if not doc:
            return jsonify({"error": f"Architecture data not found for given task id and target language"}), 404

        services = doc.get("services", None)
        if not services:
            print(f"No services found for task_id: {task_id} and language: {language}")

        answer = anthropic_architecture_chatbot_call(chats, services)

        if not answer:
            ai_response = {"role": "assistant", "content": "I'm sorry, I wasn't able to process your request at the moment. Please try again later."}
        else:
            ai_response = {"role": "assistant", "content": answer}

        # Append in the messages (chat history)
        chats.append(ai_response)

        final_response = {"messages": chats}

        return jsonify(final_response), 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@blueprint_prefix.route('/architecture/validate-code-services', methods=['POST'])
@cross_origin(supports_credentials=True)
def validate_services_v2():
  try:
    data = json.loads(request.data)
    user_id = data.get("user_id")
    user_prompt = data.get("user_prompt")
    repo_url = data.get("repo_urls")
    branch_name = data.get("branch_name")
    language = data.get("language")

    missing_fields = []
    if user_id is None:
          missing_fields.append("user_id")
    if user_prompt is None:
          missing_fields.append("user_prompt")
    if repo_url is None:
          missing_fields.append("repo_urls")
    if branch_name is None:
          missing_fields.append("branch_name")
    if language is None:
          missing_fields.append("language")
    if missing_fields:
          return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), 400

    repo_url = repo_url.strip()
    branch_name = branch_name.strip()
    language = language.strip().lower()

    if not all(isinstance(value, str) for value in [user_id, user_prompt,repo_url]):
        return jsonify({"error":"Fields must be string"})

    response = project_summery_db.projects.find_one(
    {"repo_urls": {"$in": [repo_url]}, "branch_names": {"$in": [branch_name]}},
    {"task_id": 1},
    sort=[("created_at", -1)]
)

    if response:
      task_id = response.get("task_id")
      print("task_id",task_id)
      check = project_summery_db.architecture.find_one(
        {"task_id": task_id, "language": language,"approved_at": {"$exists": True}, "repo_urls": {"$in": [repo_url]}, "branch_names": {"$in": [branch_name]}},
        sort=[("approved_at", -1)]
       )


      if check is None:
          return jsonify({"error":"Please connect to architecture builder"}),452

      services= check['services']

      if services is None:
          return jsonify({"error":"Please reanalyze your repo"}),453

      if not any(services.values()):
           return jsonify({"error": "No services found in the project"}), 454

      correct_services = {"external_services": [], "thirdparty_services": []}
      count_service_applicable_key = 0

        # Iterate through external_services and thirdparty_services
      for service_type, service_dict in services.items():
          for service_name, service_details in service_dict.items():  # service_dict is a dictionary
                # Check if the 'service_applicable' key exists in service_details
            if "service_applicable" in service_details:
                count_service_applicable_key += 1
                if service_details.get("service_applicable"):  # Check if it's True
                    correct_services[service_type].append(service_details)

        # If no service_applicable key was found in any service
      if count_service_applicable_key == 0:
            return jsonify({"error": "Please reanalyze your repo"}), 453

      if any(correct_services.values()):
          result = get_validated_service_ai_response(user_prompt,correct_services, user_prompt)

          return result,200
      else:
           return jsonify({"error": "No services found in the project"}), 454

    return jsonify({"error": "Project not found"}), 404

  except Exception as e:
    print(f"Error creating design service: {str(e)}")
    return jsonify({"error": f"Internal server error: {str(e)}"}), 500


# @app.before_request
# def log_request_info():
#     g.start_time = datetime.now()
#     # Check if request contains query parameters
#     if request.args:
#         g.request_data = request.args.to_dict()  # Convert request.args to a dictionary
#         print("Using query parameters from request.args")
#     elif request.is_json:
#         try:
#             g.request_data = request.get_json()
#             print("Using JSON data from request")
#         except Exception as e:
#             g.request_data = {}  # Fallback to empty dict
#             print(f"Failed to decode JSON: {str(e)}")  # Log the error for debugging
#     else:
#         g.request_data = request.data or b''  # Fallback to raw request data
#     print(g.request_data)
#     g.task_id, g.user_id = extract_task_and_user_id()
#
# @app.after_request
# def log_response_info(response):
#     if response.status_code in error_codes:
#         response_data = response.get_data(as_text=True) if isinstance(response.get_data(), bytes) else response.get_data()
#         try:
#             response_dict = json.loads(response_data)
#         except json.JSONDecodeError:
#             response_dict = None
#
#         error_message = None
#         if isinstance(response_dict, dict):
#             error_message = ', '.join(f"'{k}': '{v}'" for k, v in response_dict.items())
#
#         log_data = {
#             "timestamp": g.start_time,
#             "api_endpoint": request.path,
#             "request": g.request_data.decode('utf-8') if isinstance(g.request_data, bytes) else g.request_data,
#             "response": response_data,
#             "status": response.status_code,
#             "task_id": g.task_id,
#             "user_id": g.user_id,
#             "error_message": error_message
#         }
#         print("log wala")
#         # Insert log into BigQuery
#         insert_log(log_data)
#     return response
#
# from datetime import datetime
# @app.errorhandler(Exception)
# def handle_exception(e):
#     # Capture the error traceback and other error information
#     error_traceback = str(e)  # Get the error message from the exception object
#     log_data = {
#         "timestamp": datetime.now(),
#         "api_endpoint": request.path,
#         "request": g.request_data.decode('utf-8') if isinstance(g.request_data, bytes) else g.request_data,  # Convert bytes to string if necessary
#         "response": None,  # No response is available due to the error
#         "status": 500,
#         "task_id": g.task_id,  # Include task_id if available
#         "user_id": g.user_id,
#         "error_traceback": error_traceback
#     }
#
#     # Log the error details in BigQuery
#     print("Error wala")
#     insert_log(log_data)
#
#     # Return a detailed JSON response with error information
#     response = {
#         "status": "error",
#         "message": str(e),
#         "error_traceback": error_traceback,
#         "timestamp": datetime.now().isoformat(),
#         "api_endpoint": request.path
#     }
#     return jsonify(response), 500


@blueprint_prefix.route('/crashanalytics', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_crash_data():
    """
    Fetch paginated crash analytics data from BigQuery and return as JSON,
    along with common crash metrics such as total number of crashes, most
    common error messages, and most affected APIs.
    """
    try:
        # Fetch pagination parameters from the query string
        page = int(request.args.get('page', 1))  # Default to page 1
        page_size = int(request.args.get('page_size', 10))  # Default to 10 records per page
        offset = (page - 1) * page_size  # Calculate offset for pagination

        # Fetch filtering parameters from the query string
        task_id_filter = request.args.get('task_id')
        user_id_filter = request.args.get('user_id')
        api_name_filter = request.args.get('api_name')
        tag_filter = request.args.get('tag')
        from_timestamp = request.args.get('from')  # Expected in ISO format
        to_timestamp = request.args.get('to')  # Expected in ISO format
        search_term = request.args.get('search')
        sort_order = request.args.get('sort', 'desc').lower()  # Default to descending order

        # Convert to BigQuery compatible format if `from` and `to` are provided
        if from_timestamp:
            from_date = datetime.strptime(from_timestamp, "%b %d, %Y, %H:%M:%S")
            from_date = from_date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            from_date = None

        if to_timestamp:
            to_date = datetime.strptime(to_timestamp, "%b %d, %Y, %H:%M:%S")
            to_date = to_date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            to_date = None

        # Validate sort order
        if sort_order not in ['asc', 'desc']:
            return jsonify({"status": "error", "message": "Invalid sort parameter. Use 'asc' or 'desc'."}), 400

        # Construct the WHERE clause for filtering
        filters = []
        if task_id_filter:
            filters.append(f"LOWER(task_id) = '{task_id_filter.lower()}'")
        if user_id_filter:
            filters.append(f"LOWER(user_id) = '{user_id_filter.lower()}'")
        if tag_filter:
            tag_list = [tag.strip().lower() for tag in tag_filter.split(',')]
            tag_conditions = " OR ".join([f"LOWER(tag) = '{tag}'" for tag in tag_list])
            filters.append(f"({tag_conditions})")
        if api_name_filter:
            api_list = [api.strip().lower() for api in api_name_filter.split(',')]
            api_conditions = " OR ".join([f"LOWER(api_name) = '{api}'" for api in api_list])
            filters.append(f"({api_conditions})")
        if from_timestamp:
            filters.append(f"timestamp >= '{from_date}'")
        if to_timestamp:
            filters.append(f"timestamp <= '{to_date}'")
        if search_term:
            search_conditions = [
                f"LOWER(task_id) LIKE '%{search_term.lower()}%'",
                f"LOWER(user_id) LIKE '%{search_term.lower()}%'",
                f"LOWER(api_name) LIKE '%{search_term.lower()}%'",
                f"LOWER(username) LIKE '%{search_term.lower()}%'",
                f"LOWER(tag) LIKE '%{search_term.lower()}%'",
                f"LOWER(error_message) LIKE '%{search_term.lower()}%'"
            ]
            filters.append(f"({' OR '.join(search_conditions)})")
        filter_clause = ""
        if filters:
            filter_clause = " AND " + " AND ".join(filters)

        print(f"Filter clause: {filter_clause}")
        # Define the SQL query to fetch paginated rows from the table
        query = f"""
        SELECT api_name, user_id, task_id, error_message, timestamp, status, tag, username
        FROM `{table_id}`
        WHERE status != 200 {filter_clause}
        ORDER BY timestamp {sort_order.upper()}
        LIMIT {page_size} OFFSET {offset}
        """

        # Execute the query for paginated data
        query_job = bigquery_client.query(query)
        results = query_job.result()  # Wait for the job to complete

        # Prepare data in JSON format
        data = []
        for row in results:
            dt_object = datetime.fromisoformat(row.timestamp.isoformat())
            formatted_date = dt_object.strftime("%b %d, %Y, %H:%M:%S")
            data.append({
                "api_name": row.api_name,
                "user_id": row.user_id,
                "task_id": row.task_id,
                "username": row.username,
                "tag": row.tag,
                "error_message": row.error_message,
                "status": row.status,
                "timestamp": formatted_date
            })

        # Fetch total number of rows for pagination metadata
        count_query = f"SELECT COUNT(*) as total_rows FROM `{table_id}` WHERE status != 200 {filter_clause}"
        count_job = bigquery_client.query(count_query)
        total_rows = list(count_job.result())[0].total_rows  # Get the total number of rows
        total_pages = (total_rows // page_size) + (1 if total_rows % page_size != 0 else 0)

        # Define SQL query to get crash-related metrics (status != 200)
        metrics_query = f"""WITH crashes AS (
            SELECT api_name, error_message, tag, COUNT(*) AS crash_count
            FROM `{table_id}`
            WHERE status != 200
            AND error_message IS NOT NULL  -- Exclude NULL error messages
            AND api_name != '/analyzer/crashanalytics'  -- Exclude specific API
            {filter_clause}
            GROUP BY api_name, error_message, tag
        )
        SELECT
            (SELECT COUNT(*) FROM `{table_id}` WHERE status != 200 {filter_clause}) AS total_crashes,
            (SELECT error_message FROM crashes ORDER BY crash_count DESC LIMIT 1) AS most_common_error,
            (SELECT api_name FROM crashes ORDER BY crash_count DESC LIMIT 1) AS most_affected_api,
            (SELECT tag FROM crashes ORDER BY crash_count DESC LIMIT 1) AS most_affected_tag
        """

        # Execute the query for crash metrics
        metrics_job = bigquery_client.query(metrics_query)
        metrics_results = list(metrics_job.result())[0]  # Convert to list and get the first row

        # Prepare metrics data
        total_crashes = metrics_results.total_crashes
        most_common_error = metrics_results.most_common_error
        most_affected_api = metrics_results.most_affected_api
        most_affected_tag = metrics_results.most_affected_tag
        api_list = list(tags_mapper.keys())
        tag_list = list(set(tags_mapper.values()))
        # Return the data as a JSON response, along with pagination and crash metrics
        return jsonify({
            "status": "success",
            "data": data,
            "page": page,
            "page_size": page_size,
            "api_list": api_list,
            "tag_list": tag_list,
            "sort_order": sort_order,
            "total_pages": total_pages,
            "total_rows": total_rows,
            "crash_metrics": {
                "total_crashes": total_crashes,
                "most_common_error": most_common_error,
                "most_affected_api": most_affected_api,
                "most_affected_tag": most_affected_tag
            }
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@blueprint_prefix.route('/check-repo-support', methods=['POST'])
@cross_origin(supports_credentials=True)
def check_repo_support_analyser():
    try:
        data = json.loads(request.data)

        repos = data.get("repos", [])

        git_access_token = request.headers.get("git-access-token", None)

        if not git_access_token:
            return jsonify({"error": "Missing git-access-token"}), 400

        if not repos or len(repos) == 0:
            return jsonify({"error": "No repositories provided"}), 400

        results = []
        for repo in repos:
            repo_url = repo.get("url", "")
            branch_name = repo.get("branch", "")

            if not repo_url or not branch_name:
                return jsonify({"error": "Invalid repository data"}), 400

            repo_check = check_repo_analysis_support(git_url=repo_url, branch_name=branch_name, git_token=git_access_token)
            if repo_check:
                results.append({"repo_url": repo_url, "status": True})
            else:
                results.append({"repo_url": repo_url, "status": False})


        return jsonify(results), 200

    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON input"}), 400

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@blueprint_prefix.route('/send_report_email', methods=['POST'])
@cross_origin(supports_credentials=True)
def send_report_email_api():
    try:
        data = json.loads(request.data)
        user_name = data.get("user_name")
        report_type = data.get("report_type", []) 
        user_email = data.get("user_email")
        description = data.get("description" , None)
        
        if not user_name or not user_email:
            return jsonify({"error": "Missing required fields: user_name, user_email"}), 400
            
        result, status = send_report_email(user_name, report_type, user_email, description)
        return result, status
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    
@blueprint_prefix.route('/send-language-request-email', methods=['POST'])
@cross_origin(supports_credentials=True)
def send_request_email():
    try:
        data = json.loads(request.data)
        user_name = data.get("user_name")
        request_type = data.get("request_type") # analyser-support or dependency-graph-support
        user_email = data.get("user_email")
        language_required = data.get("language_required")
        result, status = send_language_support_email(user_name, request_type, user_email, language_required)
        return result, status
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@blueprint_prefix.route('/architecture/v3/chat-bot', methods=['POST'])
@cross_origin(supports_credentials=True)
def architecture_chatbot_v3():
    try:
        data = request.json
        task_id = data.get("task_id", None)
        language = data.get("language", None)
        chats = data.get("messages", [])

        if not chats or not isinstance(chats, list):
            return jsonify({"error": "Messages not found or invalid (should be an array of json)"}), 400
        if not language or not isinstance(language, str):
            return jsonify({"error": "Language not found or invalid"}), 400
        if not task_id or not isinstance(task_id, str):
            return jsonify({"error": "task_id parameter is missing or invalid."}), 400

        language = language.strip().lower()
        doc = project_summery_db.architecture.find_one(
            {"task_id": task_id, "approved_at": {"$exists": True}, "language": language},
            sort=[("approved_at", DESCENDING)],
            projection={"services": 1}
        )
        if not doc:
            return jsonify({"error": f"Architecture data not found for given task id and target language"}), 404
        services = doc.get("services", None)
        if not services:
            print(f"No services found for task_id: {task_id} and language: {language}")

        answer,isService = anthropic_architecture_chatbot_call_v3(chats, services)

        if not answer:
            ai_response = {"role": "assistant", "content": "I'm sorry, I wasn't able to process your request at the moment. Please try again later."}
        else:
            ai_response = {"role": "assistant", "content": answer,"isService":isService}
        chats.append(ai_response)
        final_response = {"messages": chats}

        return jsonify(final_response), 200
    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@blueprint_prefix.route('/get-extension-analytics', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_extension_analytics():
    try:
        page = int(request.args.get('page', 1))  # Default to page 1
        page_size = int(request.args.get('page_size', 10))  # Default to 10 records per page
        offset = (page - 1) * page_size  # Calculate offset for pagination
        table_id = f'{dataset_id}.coding_standards'

        limit_clause = f"LIMIT {page_size} OFFSET {offset}" if page_size != -1 else ""
        query = user_analytics_extraction_query.format(table_id=table_id, limit_clause=limit_clause)
        query_job = bigquery_client.query(query)
        results = query_job.result()

        data = []
        total_users = 0
        disabled_users = 0
        current_time = datetime.now(timezone.utc)

        for row in results:
            last_used_time = row.last_used_date

            # If the last_used_time is naive (lacking timezone), make it timezone-aware (UTC)
            if last_used_time and last_used_time.tzinfo is None:
                last_used_time = last_used_time.replace(tzinfo=timezone.utc)

            # Determine is_disabled based solely on last_used_date (more than 36 hours ago)
            if last_used_time:
                is_disabled = (current_time - last_used_time) > timedelta(hours=36)
                is_active = (current_time - last_used_time) <= timedelta(hours=1.5)
            else:
                is_disabled = True
                is_active = False

            # Calculate total cost for input and output tokens
            input_cost = (row.total_input_tokens / 1_000_000) * 3 if row.total_input_tokens else 0
            output_cost = (row.total_output_tokens / 1_000_000) * 15 if row.total_output_tokens else 0
            total_cost = round(input_cost + output_cost, 2)
            total_users += 1
            if is_disabled:
                disabled_users += 1

            # Calculate average time per API call
            average_time_per_call = (
                round(row.total_time_taken / row.total_api_calls, 2)
                if row.total_api_calls and row.total_time_taken
                else 0
            )

            data.append({
                "user_name": row.user_name,
                "version": row.version,
                "last_used_date": row.last_used_date.isoformat() if row.last_used_date else None,
                "is_active": is_active,
                "is_disabled": is_disabled,
                "repo_used": row.repo_used,
                "model_used": row.model_used,
                "file_path_used": row.file_path_used,
                "total_code_sensei_code_gen": row.total_code_sensei_code_gen,
                "total_code_sensei_generic_chat": row.total_code_sensei_generic_chat,
                "total_code_sensei_unit_test": row.total_code_sensei_unit_test,
                "total_cs_check_api_call": row.total_cs_check_api_call,
                "total_org_apply_fix": row.total_org_apply_fix,
                "total_org_skip_suggestions": row.total_org_skip_suggestions,
                "total_org_suggestion_count": row.total_org_suggestion_count,
                "total_project_apply_fix": row.total_project_apply_fix,
                "total_project_skip_suggestions": row.total_project_skip_suggestions,
                "total_project_suggestion_count": row.total_project_suggestion_count,
                "total_service_check": row.total_service_check,
                "total_service_violation_count": row.total_service_violation_count,
                "total_cost": total_cost,
                "average_time_per_api_call": average_time_per_call
            })

        # Fetch total number of rows for pagination metadata
        count_query = f"SELECT COUNT(DISTINCT user_name) as total_rows FROM `{table_id}`"
        total_rows = list(bigquery_client.query(count_query).result())[0].total_rows
        total_pages = (total_rows // page_size) + (1 if total_rows % page_size != 0 else 0) if page_size != -1 else 1

        # Return the data as a JSON response, along with pagination and user metrics
        return jsonify({
            "data": data,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "total_rows": total_rows,
            "user_metrics": {
                "total_users": total_users,
                "disabled_users": disabled_users
            }
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@blueprint_prefix.route('/get-features', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_architecture_features():
    try:
        task_id = request.args.get("task_id", None)
        if not task_id or not isinstance(task_id, str):
            return jsonify({"error": "task_id parameter is missing or invalid."}), 400
        task_id = task_id.strip()

        cursor = project_summery_db.projects.find(
            {"task_id": task_id},
            projection={"feature_view": 1, "repo_services": 1, "created_at": 1, "is_gcp_support": 1}
        ).sort("created_at", -1).limit(1)

        latest_document = None
        try:
            if cursor.alive:
                latest_document = cursor.next()
        except Exception as db_error:
            print(f"Database query failed: {db_error}")
            return jsonify({"error": "Please analyze the repo"}), 400

        if not latest_document:
            return jsonify({"error": "No data found for the given task_id"}), 404

        is_gcp_support = latest_document.get("is_gcp_support", False)
        
        features = []
        
        if is_gcp_support:
            try:
                feature_hierarchy_gcp_path = latest_document.get("feature_view", {}).get("feature_hierarchy")
                
                if not feature_hierarchy_gcp_path:
                    return jsonify({"error": "GCP path for feature hierarchy not found"}), 404
                
                feature_hierarchy_data = read_json_from_bucket(
                    bucket_name=config.GCP_BUCKET_BASE_PATH, 
                    blob_path=feature_hierarchy_gcp_path
                )
                
                features = feature_hierarchy_data.get("feature_hierarchy", {}).get("features", [])
            except Exception as gcp_error:
                print(f"Error retrieving data from GCP: {gcp_error}")
                return jsonify({"error": f"Failed to retrieve data from GCP: {str(gcp_error)}"}), 500
        else:
            features = latest_document.get("feature_view", {}).get("feature_hierarchy", {}).get("features", [])
        
        if not features:
            return jsonify({"error": "Features not found. Please reanalyze the repo."}), 404

        return jsonify({"features": features}), 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@blueprint_prefix.route('/get-graph-analytics', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_graph_analytics():
    try:
        # Example BigQuery table ID and configuration
        table_id = f'{dataset_id}.coding_standards'

        # Extract year and month from query parameters (if provided)
        year = request.args.get('year', type=int)
        month = request.args.get('month', type=int)
        user_name = request.args.get('user_name', type=str, default=None)

        # Base SQL query for daily data
        query = f"""
            SELECT DATE(last_used_date) AS date,
                SUM(COALESCE(org_suggestion_count, 0) + COALESCE(project_suggestion_count, 0)) AS total_ai_suggestions,
                SUM(CASE WHEN cs_check_api_call = TRUE THEN 1 ELSE 0 END) AS total_cs_check_api_call
            FROM `{table_id}`
        """

        # Apply filtering based on year and month
        filters = []
        if year:
            filters.append(f"EXTRACT(YEAR FROM last_used_date) = {year}")
        if month:
            filters.append(f"EXTRACT(MONTH FROM last_used_date) = {month}")
        if user_name:
            filters.append(f"user_name = '{user_name}'")

        # Add WHERE clause if filters are specified
        if filters:
            query += " WHERE " + " AND ".join(filters)

        # Finalize the query for daily data with GROUP BY and ORDER BY clauses
        query += """
            GROUP BY date
            ORDER BY date
        """

        # Query for monthly totals
        total_query = f"""
        SELECT
            SUM(COALESCE(org_suggestion_count, 0) + COALESCE(project_suggestion_count, 0)) AS monthly_total_ai_suggestions,
            SUM(CASE WHEN cs_check_api_call = TRUE THEN 1 ELSE 0 END) AS monthly_total_cs_check_api_call
            FROM `{table_id}`
        """
        if filters:
            total_query += " WHERE " + " AND ".join(filters)

        # Execute the daily data query
        query_job = bigquery_client.query(query)
        results = query_job.result()

        # Execute the monthly totals query
        total_query_job = bigquery_client.query(total_query)
        total_results = total_query_job.result()
        total_row = list(total_results)[0]  # Get the first result (only one row)


        date_query = f"""
            SELECT DISTINCT EXTRACT(YEAR FROM last_used_date) AS year,
                            EXTRACT(MONTH FROM last_used_date) AS month
            FROM `{table_id}`
            ORDER BY year, month
        """
        date_query_job = bigquery_client.query(date_query)
        date_results = date_query_job.result()

        # Process results into a JSON-compatible structure
        daily_data = []
        for row in results:
            if row.date:
                daily_data.append({
                    "date": row.date.day,
                    "total_cs_check_api_call": row.total_cs_check_api_call or 0,
                    "total_ai_suggestions": row.total_ai_suggestions or 0
                })
        available_dates = [{"year": row.year, "month": row.month} for row in date_results]

        # Prepare response JSON
        response = {
            "daily_data": daily_data,
            "monthly_totals": {
                "total_cs_check_api_call": total_row.monthly_total_cs_check_api_call,
                "total_ai_suggestions": total_row.monthly_total_ai_suggestions
            },
            "available_dates": available_dates
        }

        # Return JSON response with both daily data and monthly totals
        return make_response(jsonify(response), {'Content-Type': 'application/json'}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@blueprint_prefix.route('/check-big-query-upload', methods=['GET'])
@cross_origin(supports_credentials=True)
def check_big_query_upsert():
    try:
        log_into_bigquery("check-big-query-upload", user_id = "66cdc396905e191e94effd21", task_id = "yusharth_test",error_message="None", status=500)
        return jsonify({"status":"success"}),200
    except Exception as e:
        print(e)
        log_into_bigquery("check-big-query-upload", user_id = "yusharth_test", task_id = "yusharth_test",error_message="None", status=500)
        return jsonify({"status":f"{e}"}),500



@blueprint_prefix.route("/get-analyzed-repos", methods=["GET"])
@cross_origin(supports_credentials=True)
def get_analyzed_repos():
    try:
        # Get page and limit from request arguments, default to page 1 and limit 10
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 10))
        search_query = request.args.get('search', None)
        user_id = request.args.get('user_id', None)

        if not user_id:
            return jsonify({"error": "user_id is required", "results": [], "total_count": 0}), 400

        output = fetch_analyzed_repos(user_id, page=page, limit=limit, search_query=search_query)
        output["analyzed_count"] = output.get("total_count", 0)
        shared_repo_output = fetch_user_edit_access_repos(user_id)
        output["shared_repos_count"] = shared_repo_output.get("total_count", 0)
        # print("shared repo output", shared_repo_output)
        output["results"].extend(shared_repo_output.get("results", []))
        output["total_count"] += shared_repo_output.get("total_count", 0)
        print("length of output", len(output.get("results", [])))

        return jsonify(output), 200
    except Exception as e:
        print(traceback.print_exc())
        return jsonify({"error": str(e), "results": [], "total_count": 0}), 500



@blueprint_prefix.route('/sdlc/conversion_modification_featureaddition', methods=['POST'])
@cross_origin(supports_credentials=True)
def handle_conversion_modification_feature_addition():
    try:
        if not request.is_json:
            print("Invalid content type: Request is not JSON")
            return jsonify({"error": "Invalid content type. JSON expected."}), 400
        data = request.json
        feature_name = data.get("feature_name", "")
        task_id = data.get("task_id", "")
        user_prompt = data.get("user_prompt", "")
        operation_type = data.get("operation_type", "")
        feature = data.get("feature", None)
        user_chat = data.get("user_chat", [])
        file_paths = data.get("file_paths", [])
        subfeature_name = data.get("subfeature_name", "")
        feature_check = feature
        print(f"Received operation '{operation_type}' for feature '{feature_name}' with task ID '{task_id}'")
        if operation_type == 'conversion':
            return handle_cmf(user_prompt, feature_check, task_id, feature_name, subfeature_name, user_chat, True, [])
        elif operation_type == 'modification':
            return handle_cmf(user_prompt, feature_check, task_id, feature_name, subfeature_name, user_chat, False, [])
        elif operation_type == 'feature_addition':
            if feature_name:
                print(feature_check)
                return handle_cmf(user_prompt, False, task_id, feature_name, subfeature_name, user_chat, False, file_paths)
            else:
                print("feature_name is required for feature_addition operation")
                return jsonify({"error": "feature_name is required for feature_addition operation"}), 400
        else:
            print(f"Invalid operation_type: {operation_type}")
            return jsonify({"error": "Invalid operation type"}), 400
    except KeyError as ke:
        print(f"KeyError: Missing field in input - {str(ke)}")
        return jsonify({"error": f"Missing field: {str(ke)}"}), 400
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@blueprint_prefix.route('/recommend-services', methods=['POST'])
@cross_origin(supports_credentials=True)
def get_service_recommendations():
    try:
        data = request.get_json()
        if not data or not isinstance(data, dict):
            return jsonify({"error": "Invalid request data - must be JSON object"}), 400
        if "target_languages" not in data:
            return jsonify({"error": "Missing required fields in request: target_languages"}), 400
        target_languages = data["target_languages"]
        task_id = data["task_id"]

        repo_services_db = project_summery_db.projects.find_one({"task_id": task_id}, {"repo_services": 1, "_id": 0})
        current_target_architecture=[]
        if repo_services_db:
            current_target_architecture = repo_services_db.get("repo_services")
            combined_services = current_target_architecture.get("external_services", []) + current_target_architecture.get("thirdparty_services", [])
        else:
            return {"Error":"No Architecture found with the given task_id"},400
        user_message = data['user_message']
        if not isinstance(target_languages, list):
            return jsonify({"error": "target_languages must be list."}), 400
        response,status_code = get_suggested_services(combined_services, target_languages,user_message)
        transformed_response = [
            {"data": obj, "id": obj.get("label", ""), "type": "service"}
            for obj in response
        ]
        result = [{"approved_at": "", "created_at": "", "design": {"edges": [], "nodes": transformed_response}, "status": "approved", "task_id": "", "id": str(uuid.uuid4()),"language": "default","user_id": "","services":{} }]

        if status_code!=200:
            return jsonify(response), 500
        return jsonify(result), 200
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500
    
    
    
@blueprint_prefix.route('/v2/recommend-services', methods=['POST'])
@cross_origin(supports_credentials=True)
def v2_get_service_recommendations():
    try:
        data = request.get_json()
        if not data or not isinstance(data, dict):
            return jsonify({"error": "Invalid request data - must be JSON object"}), 400
        if "target_languages" not in data:
            return jsonify({"error": "Missing required fields in request: target_languages"}), 400
        target_languages = data["target_languages"]
        task_id = data["task_id"]

        repo_services_db = project_summery_db.projects.find_one({"task_id": task_id}, {"repo_services": 1, "_id": 0})
        # print(f"Repo services DB: {repo_services_db}")
        current_target_architecture=[]
        if repo_services_db:
            current_target_architecture = repo_services_db.get("repo_services")
            # print(f"Current target architecture: {current_target_architecture}")
            combined_services = current_target_architecture.get("external_services", []) + current_target_architecture.get("thirdparty_services", [])
            # print(f"Combined services: {combined_services}")
        else:
            return {"Error":"No Architecture found with the given task_id"},400
        user_message = data['user_message']
        if not isinstance(target_languages, list):
            return jsonify({"error": "target_languages must be list."}), 400
        response,status_code = get_suggested_services(combined_services, target_languages, user_message, api_version="v2")

        # print(f"Response: {response}")


        if status_code!=200:
            return jsonify(response), 500
        
        return jsonify(response), 200
    except Exception as e:
        print(f"Error processing request: {str(e)} Trackeback: {traceback.print_exc()}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@blueprint_prefix.route('/v2/regenerate/architecture-diagram', methods=['POST'])
@cross_origin(supports_credentials=True)
def v2_regenerate_architecture_diagram():
    try:
        data = request.get_json()
        if not data or not isinstance(data, dict):
            return jsonify({"error": "Invalid request data - must be JSON object"}), 400
        if "services_selected" not in data:
            return jsonify({"error": "Missing required fields in request: services_selected"}), 400
        
        
        services_selected = data["services_selected"]
        task_id = data["task_id"]

        response = v2_egenerate_architecture(selected_services=services_selected, task_id=task_id)
        if isinstance(response[-1], int):
            if response[-1]!=200:
                return jsonify(response[0]), response[-1]
            return jsonify({"is_svg": response[0], "old_architecture_diagram": response[1], "final_architecture": response[2]}), 200
            
        is_svg, old_architecture_diagram, final_architecture, status = response
        return jsonify({"is_svg": is_svg, "old_architecture_diagram": old_architecture_diagram, "final_architecture": final_architecture}), 200
        
    except Exception as e:
        print(f"Error processing request: {str(e)} {traceback.format_exc()}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@blueprint_prefix.route("/dependency_classifier", methods=['POST'])
@cross_origin(supports_credentials=True)
def dependency_classifier():
    try:
        data = request.get_json()  # Fetch JSON once

        # Validate required fields
        github_url = data.get("github")
        branch = data.get("branch")
        language = data.get("language")

        if not isinstance(github_url, str):
            return jsonify({"error": "'github' must be a valid string"}), 422

        
            # dependencies, status_code = absolute_parser(github_url, branch, language)
            # if status_code != 200:
            #     return dependencies, status_code
            # return jsonify(dependencies), status_code
        language: list[str]= [language] if isinstance(language, str) else language
        
        if isinstance(language, list):
            if not all(isinstance(lang, str) for lang in language):
                return jsonify({"error": "All items in 'language' list must be valid strings"}), 422

            dependencies, status_code = absolute_parser(github_url, branch, language, repo_dependency=[], local_sync_changes=[])
            if status_code == 200:
                return jsonify(dependencies), status_code
            else:
                return jsonify(dependencies), 422

        else:
            return jsonify({"error": "'language' must be a string or a list of strings"}), 422

    except Exception as e:
        # Log error message for debugging and monitoring purposes
        error_message = f"Error on dependency classification: {str(e)}"
        print({"error": error_message})  # Replace with proper logging in production
        return jsonify({"error": error_message}), 500


@blueprint_prefix.route("/project-summary/sync-request", methods=["POST"])
@cross_origin(supports_credentials=True)
def project_summary_sync_request():
    if request.method != "POST":
        return jsonify({"error": "Method not allowed"}), 405

    try:
        request_data = request.get_json()
        task_id = request_data.get("task_id", None)
        user_id = request_data.get("owner_user_id", None)
        active_user_id = request_data.get("user_id", None)
        sync_type = request_data.get("sync_type", None)
        request_data['analysis_type'] = "sync"
        request_data['active_user_id'] = active_user_id
        git_token = request.headers.get("git-access-token", None)
        resync_task_id = str(uuid.uuid4())
        
        validated_request_body, body, missing_keys = (
            _validate_request_v2_ingest_project_summary(
                request_body=request.get_json(),
                keys=["user_id", "task_id", "sync_type"],
            )
        )
        if not validated_request_body:
            error_logger.error(f"Expected keys missing or invalid type: {missing_keys}")
            return jsonify(
                {"error": "Expected keys missing or invalid type", "keys": missing_keys}
            ), 422

        info_logger.info(
            f"### Received project summary sync request for task_id: {task_id}, user_id: {user_id}, sync_type: {sync_type} ###"
        )

        sync_support, message, data, status_code = check_if_resync_supported(task_id, user_id)

        info_logger.info(f"Sync support: {sync_support}, Message: {message}")
        if not sync_support:
            return make_response(jsonify({"error": message}), status_code)


        user_analysis_stats = project_summary_db.user_analysis_stats
        op_allowed, user_role = process_request(form_data=request_data, superapprover_data=superapprover_data_for_control, approver_data=approver_data_for_control, collection=user_analysis_stats)
        info_logger.info(f"Operation allowed: {op_allowed}, User role: {user_role}")
        if not op_allowed:
            
        
            data_to_store_status: bool = _store_pre_sync_data(request_data_recieved=request_data, old_data=data, new_task_id=resync_task_id, user_id=user_id, user_data=map_email_to_user_id, number_of_files=10,number_of_tokens=100,github_token=git_token, active_user_id = active_user_id)
            
            if not data_to_store_status:
                return jsonify({"error": "Error storing pre-sync data"}), 500
        
            approvers_mail = [approver_data[0] for approver_data in list(approver_data_for_control.values())]
            if user_id in approver_data_for_control:
                approvers_mail = [superapprover_data[0] for superapprover_data in list(superapprover_data_for_control.values())]
           
            print(f"Analysis needs approval started: {user_role}")
            approvers_data = []
            for key in approver_data_for_control:
                mailid = approver_data_for_control[key][0]
                proj_data = get_project_details(email = mailid)
                if proj_data:
                    approvers_data+=proj_data
                
            # Get the data here to pass in 
            return jsonify({
                "user_id": request_data['active_user_id'],
                "analysis_type": "sync",
                "task_id": resync_task_id,
                "status": False,
                "approval_required": True,
                "approvers_data": approvers_data,
                "user_role": "approver" if user_id in approver_data_for_control else "normal"
            }), 200
            
        resync_task_id = _initialise_resync_project(resync_task_id,user_id, task_id, sync_type, data)

        _start_resync_thread(resync_task_id, user_id, data, sync_type, git_token, active_user_id)
        
        return jsonify(
            {"user_id": user_id, "task_id": resync_task_id, "status": False}
        ), 200


    except Exception as e:
        error_logger.info(f"Error occurred in Sync request: {str(e)}")
        error_message = f"Error occurred in Sync request: {str(e)} Traceback: {traceback.format_exc()}"
        _handle_error(user_id, task_id, error_message, "sync", request_data)
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@blueprint_prefix.route('/ecg/local-sync', methods=['POST'])
@cross_origin(supports_credentials=True)
def local_sync_ecg():
    """
    Endpoint to handle local sync requests for ECG (Enterprise Code Generation).
    """
    #validate the request body
    try:
        request_data = request.get_json()
        required_keys = ["user_id", "workspace_id", "repo_url", "branch_name", "base_branch_name", "base_commit_hash", "sync_id", "sync_version_id", "operations", "sync_meta"]
        
        for key in required_keys:
            if key not in request_data:
                return jsonify({"error": f"Missing required field: {key}"}), 422
        
        # Validate operations
        for operation in request_data["operations"]:
            if not isinstance(operation, dict):
                return jsonify({"error": "Each operation must be a dictionary"}), 422
            if "type" not in operation or "file_path" not in operation or "content" not in operation or "git_diff" not in operation:
                return jsonify({"error": "Each operation must contain 'type', 'file_path', 'content', and 'git_diff'"}), 422

        # Process the local sync request
        user_id = request_data["user_id"]
        workspace_id = request_data["workspace_id"]
        repo_url = request_data["repo_url"]
        branch_name = request_data["branch_name"]
        base_branch_name = request_data["base_branch_name"]
        base_commit_hash = request_data["base_commit_hash"]
        sync_id = request_data["sync_id"]
        sync_version_id = request_data["sync_version_id"]
        operations = request_data["operations"]
        sync_meta = request_data["sync_meta"]

        ## Create a unique identifier for the sync
        sync_request_id = str(uuid.uuid4())
        ## get the task id for the base branch 
        task_id = get_task_id_local_sync(repo_url= repo_url, branch_name= base_branch_name)

        # Validation to check, sync is already in progress or not
        collection = ecg_db['user_local_syncs']

        existing_document = collection.find_one(
            {"sync_id": sync_id},
            # {"complete_status": 1},
            sort=[("created_at", -1)]
        )
        # input("check")

        if existing_document:
            print(f"Existing document found, is_completed: {existing_document.get('complete_status', {}).get('is_completed')}, is_failed: {existing_document.get('complete_status', {}).get('is_failed')}")
            complete_status = existing_document.get("complete_status", {})
            is_completed = complete_status.get("is_completed")
            is_failed = complete_status.get("is_failed")

            if complete_status and is_completed is None:
                # TODO I can check the time difference between the current time and the created_at time of the existing document
                # TODO and can allow to proceed if time difference is large and can set status False to the complete_status and True to Failed status considering the sync is failed
                return jsonify({"error": f"A sync with this ID is already in progress. {sync_id}" }), 409

        # input("can proceedd with sync")

        # insert document with None values
        collection_id = collection.insert_one(
            {
                "user_id": user_id,
                "workspace_id": workspace_id, 
                "task_id": task_id, 
                "repo_url": repo_url,
                "branch_name": branch_name,
                "base_branch_name": base_branch_name,
                "base_commit_hash": base_commit_hash,
                "sync_id": sync_id,
                "sync_version_id": sync_version_id,
                "diff_view_path": None,
                "dependency_graph_path": None,
                "feature_hierarchy_path": None,
                "folder_paths": None,
                "sync_meta": sync_meta,
                "complete_status": {
                    "is_completed": None,
                    "is_failed": None,
                    "message": None
                },
                "sync_request_id": sync_request_id,
                "created_at": datetime.now(timezone.utc)
            }
        ).inserted_id

        print("starting the local sync for sync id", sync_id, "collection id", collection_id, "sync_request_id", sync_request_id)
        # input("check")

        ## Handle the file path conflict in local sync data
        ## Repo name should be present in the file path of local sync changes
        repo_name= repo_url.rsplit('/',1)[-1].replace('.git','')
        for i, _ in enumerate(operations):
            operations[i]['file_path'] = os.path.join(repo_name, operations[i].get('file_path'))

        embed_creation_result = []
        embed_creation_thread = threading.Thread(
            target=lambda: embed_creation_result.append(bg_thread_embedding_creation(operations, task_id, sync_id, repo_url, branch_name))
        )
        # thread.daemon = True  # Thread will exit when main program exits
        embed_creation_thread.start()

        # print("my thread task is completed")

        # input("function executed successfully")

        # Here you can add logic to handle the local sync, e.g., store it in a database or process it further
        FILE_PATH = f"{config.INGESTION_DATA_BLOB_PATH}/{config.ENV}/ecg_syncs/{user_id}/{workspace_id}/{sync_id}.json"
        FEATURE_HIERARCHY_FILE_PATH = f"{config.INGESTION_DATA_BLOB_PATH}/{config.ENV}/ecg_syncs/{user_id}/{workspace_id}/{sync_id}/feature_hierarchy.json"
        DEPENDENCY_GRAPH_FILE_PATH = f"{config.INGESTION_DATA_BLOB_PATH}/{config.ENV}/ecg_syncs/{user_id}/{workspace_id}/{sync_id}/dependency_graph.json"

        upload_json_to_bucket_from_memory(
            bucket_name=config.GCP_BUCKET_BASE_PATH,
            blob_path=FILE_PATH,
            json_data=operations
        )

        print("can start other threads")

        ## Process the dependency graph and feature hierarchy for the local sync in parallel threads
        dependency_result = []
        feature_result = []

        dependency_thread = threading.Thread(
            target=lambda: dependency_result.append(local_sync_dependency_graph(user_id, task_id, repo_url, base_branch_name, operations, existing_document))
        )

        feature_thread = threading.Thread(
            target=lambda: feature_result.append(local_sync_feature_hierarchy(task_id, operations, existing_document))
        )

        dependency_thread.start()
        feature_thread.start()

        embed_creation_thread.join()
        dependency_thread.join()
        feature_thread.join()


        # input("check check")

        updated_dependency_graph, dep_status = dependency_result[0] 
        updated_feature_hierarchy, feat_status = feature_result[0]
        embed_creation_res, embed_creation_status = embed_creation_result[0]

        print(f"dependency graph status {dep_status}, feature hierarchy status {feat_status}, embedding creation status {embed_creation_status}")

        # input("check check check")

        if dep_status == 200:
            upload_json_to_bucket_from_memory(
                bucket_name=config.GCP_BUCKET_BASE_PATH,
                blob_path=DEPENDENCY_GRAPH_FILE_PATH,
                json_data=updated_dependency_graph
            )
        if feat_status == 200:
            upload_json_to_bucket_from_memory(
                bucket_name=config.GCP_BUCKET_BASE_PATH,
                blob_path=FEATURE_HIERARCHY_FILE_PATH, 
                json_data=updated_feature_hierarchy
            )

        ## update the folder structurer
        updated_folder_structure, _ = local_sync_update_folder_paths(operations)

        message = "Failed"
        is_completed = False
        is_failed = True
        if dep_status == 200 and feat_status == 200 and embed_creation_status==200:
            message = "Success"
            is_completed = True
            is_failed = False
        else:
            message = f"dependency graph status {dep_status}, feature hierarchy status {feat_status}, embedding creation status {embed_creation_status}"

        # update the collection with computed values
        collection.update_one(
            {
                "sync_request_id": sync_request_id
            },
            {
                "$set": {
                    "diff_view_path": FILE_PATH,
                    "dependency_graph_path": DEPENDENCY_GRAPH_FILE_PATH,
                    "feature_hierarchy_path": FEATURE_HIERARCHY_FILE_PATH,
                    "folder_paths": updated_folder_structure,
                    "complete_status.is_completed": is_completed,
                    "complete_status.is_failed": is_failed,
                    "complete_status.message": message
                }
            }
        )

        return jsonify(
            {
                "status": is_completed,
                "message": message,
                "sync_request_id": sync_request_id
            }
        ), 200

    except ValueError as ve:
        traceback.print_exc()
        error_message = f"Invalid value error: {str(ve)}"
        error_logger.error(error_message)
        return jsonify({"error": error_message}), 404

    except Exception as e:
        traceback.print_exc()
        error_message = f"Error processing local sync request: {str(e)}"
        error_logger.error(error_message)
        return jsonify({"error": error_message}), 500



@blueprint_prefix.route('/ecg/local-sync/v2', methods=['POST'])
@cross_origin(supports_credentials=True)
def local_sync_ecg_v2():
    """
    Endpoint to handle local sync requests for ECG (Enterprise Code Generation).
    """
    
    try:
        request_data = request.get_json()
        required_keys = ["user_id", "workspace_id", "repo_url", "branch_name", "base_branch_name", "base_commit_hash", "sync_id", "sync_version_id", "operations", "sync_meta"]
        
        # Validate that all required keys are present in the request data
        for key in required_keys:
            if key not in request_data:
                return SyncEventFormat(
                    status="failed",
                    message=f"Missing required field: {key}",
                    event="error",
                    status_code=422
                ).model_dump()
                # return jsonify({"error": f"Missing required field: {key}"}), 422

        # Extracting values
        user_id = request_data["user_id"]
        workspace_id = request_data["workspace_id"]
        repo_url = request_data["repo_url"]
        branch_name = request_data["branch_name"]
        base_branch_name = request_data["base_branch_name"]
        base_commit_hash = request_data["base_commit_hash"]
        sync_id = request_data["sync_id"]
        sync_version_id = request_data["sync_version_id"]
        operations = request_data["operations"]
        sync_meta = request_data["sync_meta"]

        if not isinstance(operations, list) or not operations:
            return SyncEventFormat(
                status="failed",
                message="Error in `operations` JSON format: `operations` must be a non-empty list",
                event="error",
                status_code=422
            ).model_dump()
            # return jsonify({"error": "`operations` must be a non-empty list"}), 422

        # Validate operations dict
        for operation in operations:
            if not isinstance(operation, dict):
                return SyncEventFormat(
                    status="failed",
                    message="Error in `operations` JSON format: Each operation must be a dictionary",
                    event="error",
                    status_code=422
                ).model_dump()
                # return jsonify({"error": "Each operation must be a dictionary"}), 422

            if "type" not in operation or "file_path" not in operation or "content" not in operation or "git_diff" not in operation:
                return SyncEventFormat(
                    status="failed",
                    message="Each operation must contain 'type', 'file_path', 'content', and 'git_diff'",
                    event="error",
                    status_code=422
                ).model_dump()
                # return jsonify({"error": "Each operation must contain 'type', 'file_path', 'content', and 'git_diff'"}), 422

        return Response(
            stream_with_context(
                local_sync_sse_combined(
                    user_id,
                    workspace_id,
                    repo_url,
                    branch_name,
                    base_branch_name,
                    base_commit_hash,
                    sync_id,
                    sync_version_id,
                    operations,
                    sync_meta
                )
            ),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Cache-Control'
            }
        )

    except ValueError as ve:
        traceback.print_exc()
        error_message = f"Invalid value error: {str(ve)}"
        error_logger.error(error_message)
        return SyncEventFormat(
            status="failed",
            message=error_message,
            event="error",
            status_code=404
        ).model_dump()
        # return jsonify({"error": error_message}), 404

    except Exception as e:
        traceback.print_exc()
        error_message = f"Error processing local sync request: {str(e)}"
        error_logger.error(error_message)
        return SyncEventFormat(
            status="failed",
            message=error_message,
            event="error",
            status_code=500
        ).model_dump()
        # return jsonify({"error": error_message}), 500





@blueprint_prefix.route("/local-sync-event-sse", methods=["GET"])
@cross_origin(supports_credentials=True)
def local_sync_event_sse_route():
    try:
        sync_request_id = request.args.get('sync_request_id')

        # Validation checks
        if not sync_request_id:
            return SyncEventFormat(
                status="failed",
                message="sync_request_id is required",
                event="error",
                status_code=400
            ).model_dump()

        return Response(
            stream_with_context(generate_sse_local_sync(sync_request_id)),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Cache-Control'
            }
        )

    except Exception as e:
        print(f"Error in trigger_endpoint_ecg_sse_route: {str(e)}")
        traceback.print_exc()
        return SyncEventFormat(
            status="failed",
            message=f"Internal server error: {str(e)}",
            event="error",
            status_code=500
        ).model_dump()


@blueprint_prefix.route('/analyzed-branch', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_branches():
    try:
        repo_url = request.args.get('repo_url')
        if not repo_url:
            return jsonify({"error": "repo_url parameter is required"}), 400

        repo_url_no_git = repo_url.removesuffix(".git")

        projects = project_summery_db.projects.find(
            {"repo_urls": {"$in": [repo_url, repo_url_no_git]}},
            {"repo_urls": 1, "branch_names": 1, "project_name": 1, "_id": 0}
        )

        result = {}
        for project in projects:
            for repo, branch in zip(project.get('repo_urls', []), project.get('branch_names', [])):
                if repo in (repo_url, repo_url_no_git):
                    normalized_repo = repo.removesuffix(".git")
                    if normalized_repo not in result:
                        result[normalized_repo] = []
                    result[normalized_repo].append(branch)

        if not result:
            return jsonify({"error": "No projects found with the given repo_url"}), 404

        return jsonify({"branch": result[repo_url_no_git]}), 200

    except Exception as e:
        error_message = f"Error retrieving branch names: {str(e)}"
        error_logger.error(error_message)
        return jsonify({"error": "Internal server error"}), 500


@blueprint_prefix.route("/sync-request/get-status", methods=["GET"])
@cross_origin(supports_credentials=True)
def sync_request_get_status():
    if request.method != "GET":
        return jsonify({"error": "Method not allowed"}), 405

    try:
        task_id = request.args.get("task_id", None)
        user_id = request.args.get("user_id", None)
        
        analyzer_results = project_summary_db.project_analyzer_task.find_one({"user_id": user_id, "task_id": task_id})
        info_logger.info(f"Analyzer results: {analyzer_results}")
        if analyzer_results is None:
            analyzer_results = {}
        analysis_status = check_completion_status(analyzer_results.get("completion_status", {}))
        analysis_type = analyzer_results.get("analysis_type", "new_analysis")
        info_logger.info(f"Analysis status: {analysis_status}, Analysis type: {analysis_type}")
        
        return jsonify({"status": analysis_status, "analysis_type": analysis_type}), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@blueprint_prefix.route("/get-analysed-data/languages", methods=["GET"])    
@cross_origin(supports_credentials=True)
def get_languges_repowise():
    if request.method != "GET":
        return jsonify({"error": "Method not allowed"}), 405
    
    try:
        task_id = request.args.get("task_id", None)
        if not task_id:
            return jsonify({"error": "task_id is required"}), 400
        
        query = {"task_id": task_id}
        projection = {"_id": 0, "dashboard": 1}
        analyzer_result = project_summary_db.projects.find_one(query,projection)
        if not analyzer_result:
            return jsonify({"error": "No data found for the given task_id"}), 404
        
        langauges  = analyzer_result.get("dashboard", {}).get("language", [])
        
        return jsonify(langauges), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@blueprint_prefix.route("/access_dashboard", methods=["GET"])
@cross_origin(supports_credentials=True)
def access_dashboard():
    try:
        user_id = request.headers.get("X-Secure-UserID")
        if not user_id:
            return jsonify({"status": "error", "message": "User ID not provided"}), 401

        validated_payload, message, status_code = validate_payload_dashboard(request.args.to_dict())
        if status_code != 200:
            return jsonify({"status": "error", "data": [], "message": message}), status_code
        
        map_role_to_function = {
            "superapprover": lambda: get_data_for_approver(user_id, validated_payload, map_user_id_to_info, is_super_approver=True),
            "approver": lambda: (
                get_data_for_normal_user(user_id, validated_payload, map_user_id_to_info)
                if validated_payload.get("self")=="true"
                else get_data_for_approver(user_id, validated_payload, map_user_id_to_info)
            ),
            "normal": lambda: get_data_for_normal_user(user_id, validated_payload, map_user_id_to_info)
        }

        # Determine user role
        user_role = (
            "superapprover" if user_id in superapprover_data_for_control
            else "approver" if user_id in approver_data_for_control
            else "normal" if user_id in map_user_id_to_info
            else None
        )

        if not user_role:
            return jsonify({"status": "error", "message": "Unauthorized access"}), 403
        # Get data based on role
        response, message, status_code = map_role_to_function[user_role]()

        if response and user_id in map_user_id_to_info:
            response["meta"]["accessor"]["email"] = map_user_id_to_info[user_id][0]
            response["meta"]["accessor"]["role"] = user_role
            response["meta"]["accessor"]["user_name"] = map_user_id_to_info[user_id][1]
            if user_role in ["approver", "superapprover"]:
                if response.get("meta", {}).get("accessor", {}):
                    response["meta"]["accessor"]["all_projects"] = config.PO_MAIL_MAPPING.get(map_user_id_to_info.get(user_id, [""])[0], []) if user_role=="approver" else get_all_projects()
                    response["meta"]["accessor"]["all_projects"] = response["meta"]["accessor"]["all_projects"] or ["appmod"]
                if user_role == "approver" and validated_payload.get("self", "false")=="true":
                    response["meta"]["accessor"]["self"] = True
        return jsonify({"result": response,"message": message,"status": "success" if status_code == 200 else "error"}), status_code

    except Exception as e:
        logger.error(f"Dashboard access error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({"status": "error","message": "Internal server error"}), 500


@blueprint_prefix.route("/access_dashboard/base-graphs", methods=["GET"])
@cross_origin(supports_credentials=True)
def access_dashboard_base_graphs():
    try:
        user_id = request.headers.get("active-user-id")
        if not user_id:
            return jsonify({"error": "User ID is required"}), 400

        user_id_data, user_id_validation_status = validate_user_id(
            user_id=user_id,
            super_approvers_data=superapprover_data_for_control,
            approvers_data=approver_data_for_control,
            compelete_user_data=map_user_id_to_info,
        )
        info_logger.info(f"User ID data: {user_id_data}")

        if user_id_validation_status != 200:
            return jsonify(
                {"error": user_id_data["message"]}
            ), user_id_validation_status

        params_dict = {}
        for key, value in request.args.items():
            params_dict[key] = value

        validaton_schema = {
            "projectName": {"type": list, "default": None, "optional": False},
            "graph1month": {
                "type": int,
                "default": int(datetime.now().strftime("%m")),
                "min": 1,
                "max": 12,
            },
            "graph1year": {
                "type": int,
                "default": datetime.now().year,
                "max": datetime.now().year,
            },
            "graph2month": {
                "type": int,
                "default": int(datetime.now().strftime("%m")),
                "min": 1,
                "max": 12,
            },
            "graph2year": {
                "type": int,
                "default": datetime.now().year,
                "max": datetime.now().year,
            },
            "graph2type": {
                "type": str,
                "default": "month",
                "allowed": ["month", "week"],
            },
        }

        validated_params, validation_message, validation_status = (
            validate_payload_dashboard_v2(
                payload=params_dict,
                validation_schema=validaton_schema,
                role=user_id_data["role"],
            )
        )

        info_logger.info(f"Validated params: {validated_params}")
        if validation_status != 200:
            error_logger.error(f"Validation error: {validation_message}")
            return jsonify(validation_message), validation_status

        final_filtered_data = get_base_graphs_data(
            provided_filters=validated_params, active_user_details=user_id_data
        )

        return jsonify(final_filtered_data), 200

    except Exception as e:
        error_logger.error(f"Dashboard access error: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@blueprint_prefix.route("/access_dashboard/user-based-graphs", methods=["GET"])
@cross_origin(supports_credentials=True)
def access_dashboard_user_based_graphs():
    try:
        user_id = request.headers.get("active-user-id")
        target_user_id = request.headers.get("target-user-id")
        info_logger.info(f"User ID: {user_id}, Target user ID: {target_user_id}")

        if not user_id or not target_user_id:
            return jsonify({"error": "User ID and target_user_id are required"}), 400

        user_id_data, user_id_validation_status = validate_user_id(
            user_id=user_id,
            super_approvers_data=superapprover_data_for_control,
            approvers_data=approver_data_for_control,
            compelete_user_data=map_user_id_to_info,
        )
        info_logger.info(f"User ID data: {user_id_data}")

        if user_id_validation_status != 200:
            return jsonify(
                {"error": user_id_data["message"]}
            ), user_id_validation_status

        if user_id_data["role"] == "normal":
            return jsonify({"error": "Unauthorized access"}), 403

        target_user_id_data, target_user_id_validation_status = validate_user_id(
            user_id=target_user_id,
            super_approvers_data=superapprover_data_for_control,
            approvers_data=approver_data_for_control,
            compelete_user_data=map_user_id_to_info,
        )
        info_logger.info(f"Target user ID data: {target_user_id_data}")

        final_filtered_data = get_user_based_graph_data(
            active_user_id_details=user_id_data,
            target_user_id_details=target_user_id_data,
        )

        return jsonify(final_filtered_data), 200

    except Exception as e:
        error_logger.error(f"Dashboard access error: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500
    
    
    
@blueprint_prefix.route("/get_task_ids", methods=["POST"])
@cross_origin(supports_credentials=True)
def get_task_ids():
    try:
        request_body = request.get_json()
        user_id = request_body.get("user_id", None)
        repo_urls = request_body.get("repo_urls", [])
        project_name = request_body.get("project_name")
        if not user_id:
            return jsonify({"error": "User ID is required"}), 422
        list_of_task_ids, flag = get_task_ids_from_repo_urls(user_id, repo_urls, project_name)
        if not list_of_task_ids:
            if flag:
                return jsonify({"error": "Project analysis is in progress"}), 423
            return jsonify({"error": "You don't have permission to view this as it doesn't belongs to your project."}), 404
        
        return jsonify({"task_ids": list_of_task_ids}), 200
    except Exception as e:
        error_logger.error(f"Error occurred in get_task_ids: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@blueprint_prefix.route("/v2/get_task_ids", methods=["POST"])
@cross_origin(supports_credentials=True)
def get_task_ids_v2():
    try:
        request_body = request.get_json()
        user_id = request_body.get("user_id", None)
        repo_urls = request_body.get("repo_urls", [])
        project_name = request_body.get("project_name")
        if not user_id:
            return jsonify({"error": "User ID is required"}), 422
        response, status = get_task_ids_from_repo_urls_v2(user_id, repo_urls[0], project_name)
        if status == 200:
            return jsonify(response), 200
        
        return jsonify({"error": response.get("error", "An unexpected error occurred")}), status
    except Exception as e:
        error_logger.error(f"Error occurred in get_task_ids: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@blueprint_prefix.route("/architecture-builder/get-services", methods=["GET"])
@cross_origin(supports_credentials=True)
def arch_builder_get_architecture():
    try:
        task_id = request.args.get("task_id", None)
        if not task_id:
            return jsonify({"error": "task_id is required"}), 400
        architecture_data = get_architecture_versions_list(task_id)
        grouped_architecture_data = merge_duplicate_groups(architecture_data)
        return jsonify(grouped_architecture_data), 200
    except Exception as e:
        error_logger.error(f"Error occurred in arch_builder_get_architecture: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@blueprint_prefix.route("/architecture-builder/update-services", methods=["POST"])
@cross_origin(supports_credentials=True)
def arch_builder_update_services():
    try:
        data = request.get_json()
        task_id = data.get("task_id", None)
        updated_services = data.get("updated_services", [])
        if not task_id or not updated_services:
            return jsonify({"error": "task_id and services are required"}), 400

        update_status = update_services_in_architecture(task_id, updated_services)
        if update_status:
            return jsonify({"status": "success"}), 200
        return jsonify({"error": "Failed to update services"}), 500
    except Exception as e:
        error_logger.error(f"Error occurred in arch_builder_update_services: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@blueprint_prefix.route("/v2/architecture-builder/update-services", methods=["POST"])
@cross_origin(supports_credentials=True)
def arch_builder_update_services_v2():
    try:
        data = request.get_json()
        task_id = data.get("task_id", None)
        updated_services = data.get("updated_services", [])
        if not task_id or not updated_services:
            return jsonify({"error": "task_id and services are required"}), 400

        update_status = update_services_in_architecture_v2(task_id, updated_services)
        if update_status:
            return jsonify({"status": "success"}), 200
        return jsonify({"error": "Failed to update services"}), 500
    except Exception as e:
        error_logger.error(f"Error occurred in arch_builder_update_services: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@blueprint_prefix.route('/github/get_complete_dependency', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_complete_dependency():
    try:
        task_id = request.args.get('task_id')
        data_type = request.args.get("data-type", "all")
        if not task_id:
            return jsonify({'error': 'task_id is required'}), 400
        
        info_logger.info(f"Fetching complete dependency data for task ID: {task_id} with request_type {data_type}")
                
        dependancy_data, status = get_complete_dependency_data(task_id, data_type)

        return jsonify(dependancy_data), status
    except Exception as e:
        print(f"Error fetching dependency data: {e}")
        return jsonify({'error': 'An error occurred while fetching dependency data'}), 500
        
@blueprint_prefix.route('/github/get_particular_dependency', methods=['POST'])
@cross_origin(supports_credentials=True)
def get_particular_dependency():
    try:
        data = request.get_json()
        task_id, file_paths, giturl = data.get("task_id"), data.get("file_paths", []), data.get("giturl")
        
        if not task_id or not file_paths or not giturl:
            return jsonify({"error": "task_id or file_paths or giturl is missing"}), 422
            
        if not isinstance(file_paths, list):
            return jsonify({"error": "file_paths should be a list of strings"}), 422
            
        return get_file_summaries(task_id, giturl, file_paths)
        
    except Exception as e:
        logging.error(f"Error occurred in get_particular_dependency: {str(e)}, {traceback.format_exc()}")
        return jsonify({"error": "Internal server error"}), 500        
        
@blueprint_prefix.route("/get_supported_languages", methods=["GET"])
@cross_origin(supports_credentials=True)
def get_supported_languages():
    try:
        data, status = get_supported_languages_dict()

        if status != 200:
            return jsonify({"error": "Failed to fetch supported languages"}), status

        return jsonify(data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@blueprint_prefix.route("/update_supported_languages", methods=["POST"])
@cross_origin(supports_credentials=True)
def update_supported_languages():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid request data"}), 400

        status = upload_supported_languages_dict(data)
        if status:
            return jsonify({"status": "success"}), 200

        return jsonify({"error": "Failed to update supported languages"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@blueprint_prefix.route("/update_orchestrator_model", methods=["POST"])
@cross_origin(supports_credentials=True)
def update_orchestrator_model():
    try:
        data = request.get_json()
        task_id = data.get("task_id")
        model_data = data.get("model_data")

        if not task_id or not model_data:
            return jsonify({"error": "task_id and model_name are required"}), 400

        project_doc = project_summary_db.projects.find_one(
            {"task_id": task_id},
            {"name": 1, "dashboard.updated_executive_summary": 1, "_id": 0}
        )

        if not project_doc:
            return jsonify({"error": "Project not found"}), 404

        executive_summary = project_doc.get("dashboard", {}).get("updated_executive_summary", "")
        name = project_doc.get("name", "")

        status = set_agent_settings(
            chatbot_name=name,
            executive_summary=executive_summary,
            model_data=model_data
        )

        if status:
            chatbot_id = get_chatbot_id(assistant_name=name)
            RAG_AGENT_URL = f"{config.EGPT_DOMAIN}/utility/rag-agent/"

            model_type = model_data['primary_model']['model_type']
            model_name = model_data['primary_model']['model_name']
            if model_type == "Google VertexAI":
                query = "tools.ContextAwareResponseTool.integrations.VertexAIIntegration.properties.vertex_model.value"
            elif model_type == "VertexAI Anthropic":
                query = "tools.ContextAwareResponseTool.integrations.AnthropicIntegration.properties.model.value"
            # elif llm_call_type == "openai":
            #     query = "tools.ContextAwareResponseTool.integrations.OpenAIIntegration.properties.model.value"

            agent_doc = egpt_agents_collection.update_one(
                {"chatbotId": ObjectId(chatbot_id), "utilityFunctionLink": RAG_AGENT_URL},
                {"$set": {query: model_name, "tools.ContextAwareResponseTool.properties.llm_call_type.value": model_type}}
            )

            # query = { "task_id": task_id }
            # update_fields = { "chat_model_name": model_name }
            # project_summary_db.projects.update_one(query, {"$set": update_fields}, upsert=True)
            
            return jsonify({"status": "success"}), 200
        else:
            return jsonify({"error": "Failed to update model settings"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    
@blueprint_prefix.route("/enable_folder_maping", methods=["PUT"])
@cross_origin(supports_credentials=True)
def enable_folder_mapping_analysis():
    try:
        task_id = request.args.get('task_id')
        user_id = request.args.get('user_id')
        git_pat_token = request.headers.get('git-access-token')
        if not task_id or not user_id or not git_pat_token:
            return jsonify({"error": "task_id, user_id and git-access-token are required"}), 400
        
        completion_status, completion_message = enable_folder_mapping(task_id,user_id, git_pat_token)
        info_logger.info(f"Enable folder mapping status: {completion_status}, Message: {completion_message}")
        if not completion_status:
            return jsonify({"error": completion_message}), 500
        
        return jsonify({"status": completion_message}), 200
    except Exception as e:
        error_logger.error(f"Error occurred in enable_folder_mapping_analysis: {str(e)} {traceback.format_exc()}")
        return jsonify({"error": "Internal server error"}), 500


@blueprint_prefix.route("/get_executive_summary_prompt", methods=["GET"])
@cross_origin(supports_credentials=True)
def get_executive_summary_prompt():
    try:
        # Get database_index parameter from request
        database_index = request.args.get('database_index', '')
        
        # Extract the project name from database_index if it follows the pattern
        project_name = ''
        if '-' in database_index:
            parts = database_index.split('-')
            # If pattern matches something like 84lumber-appmodcdd9fdev
            if len(parts) >= 2:
                project_name = parts[-1]  # Get the last part after the dash
        else:
            project_name = database_index
            
        # If we couldn't extract a project name, return empty
        if not project_name:
            return jsonify({"summary": ""})
            
        # Query the projects collection with project name
        project = project_summery_db.projects.find_one(
            {"name": project_name},
            {"dashboard.updated_executive_summary": 1}
        )
        
        # If project exists and has executive summary
        if project and "dashboard" in project and "updated_executive_summary" in project["dashboard"]:
            summary = project["dashboard"]["updated_executive_summary"]
            # summary_prompt = contextaware_tool_settings_orchestrator_prompt.replace("{executive_summary}", summary)
            return jsonify({"summary": summary})
        else:
            # Return empty summary if not found
            return jsonify({"summary": ""})
            
    except Exception as e:
        error_logger.error(f"Error occurred in get_executive_summary_prompt: {str(e)} {traceback.format_exc()}")
        # Return empty summary on error
        return jsonify({"summary": ""})


@blueprint_prefix.route("/add_summarization_rule", methods=["POST"])
@cross_origin(supports_credentials=True)
def add_summarization_rule():
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({"error": "Invalid request data"}), 400

        language = payload.get("language")
        rule = payload.get("rule")

        if not language or not rule:
            return jsonify({"error": "Language and rule must be provided"}), 400

        supported_languages, status = get_supported_languages_dict()

        
        if status != 200:
            return jsonify({"error": "Could not validate supported languages"}), 500

        if language not in supported_languages:
            return jsonify({"error": f"Provided language {language} is not a supported language, kindly add to the supported languages first using update_supported_languages API"}), 400

        rules_data, rules_file_path = load_summarization_rules()

        if not rules_file_path:        
            rules_data = {key: "" for key in supported_languages.keys()}
            rules_file_path = 'sumarization_rules.json'
        else:
            for key in supported_languages.keys():
                if key not in rules_data.keys():
                    rules_data[key] = ""

        if language in rules_data and rules_data[language]:
            return jsonify({"error": "Rule already exists for this language"}), 400

        rules_data[language] = rule
        save_summarization_rules(rules_data, rules_file_path)

        return jsonify({"status": "Rule added successfully"}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@blueprint_prefix.route("/get_summarization_rule", methods=["GET"])
@cross_origin(supports_credentials=True)
def get_summarization_rule():
    try:
        language = request.args.get('language', None)
        rules_data, _ = load_summarization_rules()

        if language:
            rule = rules_data.get(language, None)
            if rule is None:
                return jsonify({"error": "Rule not found for this language"}), 404
            return jsonify({"language": language, "rule": rule}), 200
        else:
            rule = rules_data
            if rule is None:
                return jsonify({"error": "No ruleset found"}), 404
            return jsonify({"rule": rule}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@blueprint_prefix.route("/update_summarization_rule", methods=["PUT"])
@cross_origin(supports_credentials=True)
def update_summarization_rule():
    try:
        payload = request.get_json()
        if not payload or "rule" not in payload:
            return jsonify({"error": "Rule must be provided in request"}), 400
        
        language = payload.get('language', None)
        if not language:
            return jsonify({"error": "Language must be provided in request"}), 400

        supported_languages, status = get_supported_languages_dict()
        if status != 200:
            return jsonify({"error": "Could not validate supported languages"}), 500

        if language not in supported_languages:
            return jsonify({"error": f"Language '{language}' is not supported"}), 400

        rules_data, rules_file_path = load_summarization_rules()
        
        if not rules_file_path:        
            return jsonify({"error": f"No summarization rules maintained as of now"}), 400
        else:
            for key in supported_languages.keys():
                if key not in rules_data.keys():
                    rules_data[key] = ""

        if language not in rules_data:
            return jsonify({"error": "No existing rule to update"}), 404

        rules_data[language] = payload["rule"]
        save_summarization_rules(rules_data, rules_file_path)

        return jsonify({"status": "Rule updated successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@blueprint_prefix.route("/delete_summarization_rule", methods=["DELETE"])
@cross_origin(supports_credentials=True)
def delete_summarization_rule():
    try:
        language = request.args.get('language', None)
        if not language:
            return jsonify({"error": "Language must be provided to delete"}), 400
        
        rules_data, rules_file_path = load_summarization_rules()

        if language not in rules_data:
            return jsonify({"error": "No rule found to delete"}), 404

        rules_data[language] = ""
        save_summarization_rules(rules_data, rules_file_path)

        return jsonify({"status": "Rule deleted successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@blueprint_prefix.route("/get_file_details", methods=["GET"])
@cross_origin(supports_credentials=True)
def get_file_details():
    try:
        payload = InputPayload(**request.args)
        query_to_get_basic_data = f"""
            SELECT
                a.metadata->>'text' as content,
                a.row_summary as summary,
                a.metadata->>'file_stats' as file_stats,
                a.metadata->>'chunk_number' as chunk_number
                {",a.metadata->>'chunk_details' as chunk_details" if payload.chunks_search else ""}
            FROM
                "{payload.organization_name + "-" + payload.assistant_name}" AS a
            JOIN
                "{payload.organization_name + "-" + payload.assistant_name + "-filemap"}" AS fm
            ON
                a.id = fm.embed_id
            WHERE
                fm.file_path = '{payload.file_path}'
                and fm.chunk_type = '{payload.chunks_type}';"""
        
        query_to_get_chunk_details_data = f"""
            SELECT
                a.metadata->>'text' as content,
                a.row_summary as summary,
                a.metadata->>'file_stats' as file_stats,
                a.metadata->>'chunk_number' as chunk_number,
                a.metadata->>'chunk_details' as chunk_details
            FROM
                "{payload.organization_name + "-" + payload.assistant_name}" AS a
            JOIN
                "{payload.organization_name + "-" + payload.assistant_name + "-filemap"}" AS fm
            ON
                a.id = fm.embed_id
            WHERE
                fm.file_path = '{payload.file_path}'
                and fm.chunk_type = '{payload.chunks_type}';"""

        
        if not payload.technical_details_only:
            search_query = query_to_get_basic_data
        else:
            search_query = query_to_get_chunk_details_data
        alloydb_connector = AlloyDBConnector(
            database=config.EGPT_ALLOY_DB_NAME,
            username=config.EGPT_ALLOY_DB_USERNAME,
            password=config.EGPT_ALLOY_DB_PASSWORD,
            host=config.EGPT_ALLOY_DB_HOST,
            port=config.EGPT_ALLOY_DB_PORT
        )
        try:
            columns, data = alloydb_connector.run(query=search_query)
        except Exception as e:
            columns = []
            data = []

        
        
        if not data:
            logging.warning("Couldn't find the details for the file in the new data, using the old data to get details.")
            out_dict = check_old_file_data(
                task_id=payload.task_id,
                file_path=payload.file_path,
                repo_url=payload.repo_url,
            )
            out_dict = convert_old_file_data_to_new(
                data_dict=out_dict,
                file_path=payload.file_path
            )
            return jsonify(out_dict), 200
        
        data_dict = [dict(zip(columns, row)) for row in data]
        logging.info(f"Data dict: {data_dict}")
        if getattr(payload, "datasource", None) == "db":
            data_dict = process_data_from_db(data_dict)
            logging.info(f"Processed data dict: {data_dict}")
        
        out_dict = {}
        for d in data_dict:
            if payload.technical_details_only:
                d = OrderedOutputTechnical(**d)
            else:
                d = OrderedOutputBasic(**d)

            out_dict[d.chunk_number] = d.model_dump()

        return jsonify(out_dict), 200

    except ValidationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.error(f" Error while getting file detials: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500
    
@blueprint_prefix.route('/get_project_summary_details', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_project_summary_details():
    try:
        user_id = request.args.get("user_id")
        if not user_id:
            return jsonify({"error": "user_id is required param"}), 422
        data = {}
        data = get_all_summaries_v2(user_id=user_id, map_user_id_to_info=map_user_id_to_info, get_count_of_doc=get_count_of_doc)
        # data = get_requested_repo(user_id, user_analyzed_repo_list=data, map_user_id_to_info=map_user_id_to_info)
        data += get_shared_repo_list_v2(user_id, map_user_id_to_info=map_user_id_to_info)
        
        # data = _format_data(data)
        response = {"data": data}
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": f"An error has occurred: {str(e)}"}), 500




@blueprint_prefix.route("/get_executive_summary", methods=["GET"])
@cross_origin(supports_credentials=True)
def get_executive_summary():
    try:
        task_id = request.args.get('task_id', '')
        if not task_id:
            return jsonify({"summary": ""})            

        project = project_summery_db.projects.find_one(
            {"task_id": task_id},
            {"dashboard.updated_executive_summary": 1}
        )

        # If project exists and has executive summary
        if project and "dashboard" in project and "updated_executive_summary" in project["dashboard"]:
            summary = project["dashboard"]["updated_executive_summary"]
            # summary_prompt = contextaware_tool_settings_orchestrator_prompt.replace("{executive_summary}", summary)
            return jsonify({"summary": summary})
        else:
            # Return empty summary if not found
            return jsonify({"summary": ""})

    except Exception as e:
        error_logger.error(f"Error occurred in get_executive_summary_prompt: {str(e)} {traceback.format_exc()}")
        # Return empty summary on error
        return jsonify({"summary": ""})
    

@blueprint_prefix.route("/get_data_from_taskid", methods=["POST"])
@cross_origin(supports_credentials=True)
def get_data_from_taskid():
    try:
        data = request.get_json()
        task_ids = data.get('task_ids', [])
        print(task_ids)

        if not task_ids or not isinstance(task_ids, list):
            return jsonify({"error": "A non-empty array of task_ids is required"}), 400

        results = project_summery_db.projects.find(
            {"task_id": {"$in": task_ids}},
            {"_id": 0, "name": 1, "task_id" : 1, "project_name" : 1, "updated_at" : 1 }
        )
        results_data = list(results)


        return jsonify(list(results_data)), 200

    except Exception as e:
        error_logger.error(f"Error occurred in get_data_from_taskid: {str(e)} {traceback.format_exc()}")
        return jsonify({"error": "Internal server error"}), 500


@blueprint_prefix.route('/share_default_project', methods=['POST'])
@cross_origin(supports_credentials=True)
def share_default_project():
    data = request.get_json()
    user_id = data.get('user_id')
    validity = data.get('validity', "365 days")
    
    if not user_id:
        return jsonify({"error": "user_id is required"}), 400
    
    is_user_exists = project_summery_db.user_repo_access.find_one({"user_id": user_id})
    
    if is_user_exists:
        does_user_have_projects = project_summery_db.user_repo_access.find_one(
            {"user_id": user_id, "projects": {"$exists": True, "$ne": []}}
        )
        
        if does_user_have_projects:
            # Get existing project IDs
            existing_projects = does_user_have_projects.get("projects", [])
            existing_project_ids = {project.get("project_id") for project in existing_projects}
            
            # Get default project IDs
            default_project_ids = {project["project_id"] for project in new_user_default_data}
            
            # Find missing project IDs
            missing_project_ids = default_project_ids - existing_project_ids
            
            if missing_project_ids:
                # Add missing projects with validity
                validity = convert_validity_to_date(validity)
                missing_projects = []
                
                for project in new_user_default_data:
                    if project["project_id"] in missing_project_ids:
                        project_with_validity = project.copy()
                        project_with_validity["validity"] = validity
                        missing_projects.append(project_with_validity)
                
                # Update user with missing projects
                project_summery_db.user_repo_access.update_one(
                    {"user_id": user_id},
                    {"$push": {"projects": {"$each": missing_projects}}}
                )
                
                response = {"status": "success", "message": "Missing projects added successfully"}
                return jsonify(response), 200
            else:
                response = {"status": "success", "message": "User already has all default projects"}
                return jsonify(response), 200
        else:
            # User exists but has no projects, add all default projects
            validity = convert_validity_to_date(validity)
            projects = []
            for item in new_user_default_data:
                project_with_validity = item.copy()
                project_with_validity["validity"] = validity
                projects.append(project_with_validity)
            
            # Update user with all default projects
            project_summery_db.user_repo_access.update_one(
                {"user_id": user_id},
                {"$set": {"projects": projects}}
            )
            
            response = {"status": "success", "message": "Default projects added to existing user"}
            return jsonify(response), 200
    else:
        # User doesn't exist, create new user
        validity = convert_validity_to_date(validity)
        projects = []
        for item in new_user_default_data:
            project_with_validity = item.copy()
            project_with_validity["validity"] = validity
            projects.append(project_with_validity)
        
        created_user = project_summery_db.user_repo_access.insert_one({
            "user_id": user_id,
            "projects": projects
        })
        
        if created_user:
            response = {"status": "success", "message": "User created and projects shared successfully"}
            status_code = 200
        else:
            response = {"status": "error", "message": "Failed to create user or share projects"}
            status_code = 500
        
        return jsonify(response), status_code
    

@blueprint_prefix.route("/create_cw_user", methods=["POST"])
@cross_origin(supports_credentials=True)
def create_cw_user():
    try:
        data = request.get_json()
        user_id = data.get("user_id", "")
        if not user_id:
            return jsonify({"error" : "User ID is missing"}), 400
        
        result = project_summery_db.cw_user.find_one(
            {"user_id" : user_id}
        )
    
        if result:
            result["_id"] = str(result["_id"])
            return jsonify({"user" : result}), 208


        result = project_summary_db.projects.find_one(
            {"user_id" : user_id}
        )

        if result:
            print("User already has some repos analyzed!")
            update = project_summary_db.cw_user.insert_one(
                {"user_id" : user_id, "onboarding_status" : "True"}
            )

            if update:
                result = project_summary_db.cw_user.find_one(
                    {"user_id"  : user_id}
                )
                result["_id"] = str(result["_id"])

                return jsonify({"user" : result}), 200
            else:
                return jsonify({"error" : "Something went wrong while updating the User"})
                
        update = project_summary_db.cw_user.insert_one(
            {"user_id" : user_id, "onboarding_status" : "False"}
        )

        if update:
            result = project_summary_db.cw_user.find_one(
                {"user_id"  : user_id}
            )
            result["_id"] = str(result["_id"])

            return jsonify({"user" : result}), 200
        else:
            traceback.print_exc()
            raise Exception("Error while inserting user to db")

    except Exception as e:
        traceback.print_exc()
        error_logger.error(f"Error in updating CW User :  {str(e)}")
        return jsonify({"error": f"Internal server error {str(e)}"}), 500

   
@blueprint_prefix.route("/database_analyzer", methods=["POST"])
@cross_origin(supports_credentials=True)
def database_analyzer():
    """
    Endpoint handler for /database_analyzer. Parses request and delegates to main function.
    """
    try:
        req_data = request.get_json()
        if not req_data:
            return jsonify({"error": "Invalid or missing JSON payload"}), 400
        GITHUB_ACCESS_TOKEN_DB = config.GITHUB_ACCESS_TOKEN_DB
        forward_headers = Headers(request.headers)
        if GITHUB_ACCESS_TOKEN_DB:
            forward_headers['git-access-token'] = GITHUB_ACCESS_TOKEN_DB
            
        ingest_params, status_code = data_analyzer_main(req_data)  
        info_logger.info(f"status_code: {status_code}")
        if status_code != 200:
            return jsonify({"error": "Error in data analyzer"}), 500
        info_logger.info(f"Received ingest_params: {ingest_params}")
        with current_app.test_request_context('/ingest_project_request',
                                            method='POST',
                                            data=ingest_params,
                                            content_type='multipart/form-data',
                                            headers = forward_headers
                                            ):
            ingest_response = ingest_project_request()
            
        return {"response": ingest_response.get_json() if hasattr(ingest_response, "get_json") else ingest_response}, 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

app.register_blueprint(blueprint_prefix)

apsec_fetch_status, superapprover_data_for_control , approver_data_for_control, map_email_to_user_id, map_user_id_to_info = fetch_and_store_api_data()
    
info_logger.info(f"Super approvers: {superapprover_data_for_control}")
info_logger.info(f"Approvers: {approver_data_for_control }")
    
############################ MAIN ############################
if __name__ == "__main__":
    # apsec_fetch_status, superapprover_data_for_control , approver_data_for_control, map_email_to_user_id, map_user_id_to_info = fetch_and_store_api_data()
    
    # info_logger.info(f"Super approvers: {superapprover_data_for_control}")
    # info_logger.info(f"Approvers: {approver_data_for_control }")
    

    app.run("0.0.0.0", port=5002, debug=False, use_reloader=False)
    
