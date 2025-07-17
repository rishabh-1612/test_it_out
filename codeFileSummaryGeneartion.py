import os
import sys
import ast
import json
import time
import logging
import traceback
from uuid import uuid4
from typing import Union, List, Literal, Dict
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Uncomment to run locally
from data_extraction.utils.github_repo_prompts import Prompts
from helperClasses.GoogleAI import GoogleAI, GeminiParams
from Enums.model_type import google_model_name, google_region
from data_extraction.utils.github_repo_bigquery_logger import log_into_bigquery
from data_extraction.utils.github_repo_summary_output_format import response_schema, response_schema_claude
from data_extraction.utils.github_repo_utils import extract_accurate_summary
from data_extraction.Enums.github_repo_enums import FileStatus, TaskObject, ChunkOutput
from collections import defaultdict


class CodeSummarizer():
    def __init__(
        self, 
        model_name: str = google_model_name.RESPONSE_GEMINI_2_FLASH.value, 
        region: str = google_region.US_CENTRAL1.value,
        task_id: str = str(uuid4())
    ):
        self.model_name = model_name
        self.region = region
        self.googleai = GoogleAI()
        self.task_id = task_id
        self.response_schema = response_schema
        self.response_schema_claude = response_schema_claude

    def _load_json_data(self, input_text):
        try:
            converted_json_data: dict = json.loads(input_text)
            return converted_json_data
        except json.decoder.JSONDecodeError as e:
            try:
                logging.warning(f"Json parsing failed for the generated summary: trying ast.literal_val")
                converted_json_data: dict = ast.literal_eval(input_text)
                return converted_json_data
            except Exception as e:
                logging.warning(f"Json parsing failed for the generated summary: {input_text}")
                return input_text

    def _generate_small_file_summary(
            self,
            query,
            filename = "", 
            max_word_count = 350
        ) -> Union[str, int]:

        if not query:
            return "", 0.0

        user_prompt = Prompts.smaller_file_summary_user_prompt.format(
            file_summ=query,
            max_word_count=max_word_count,
            filename=filename
        )
        system_prompt = Prompts.smaller_file_summary_sys_prompt.format(
            max_word_count=max_word_count
        )
        
        all_models = [
            google_model_name.RESPONSE_CLAUDE_HAIKU_35.value,
            google_model_name.RESPONSE_CLAUDE_HAIKU.value,
            google_model_name.RESPONSE_CLAUDE_SONNET_35.value,
            google_model_name.RESPONSE_GEMINI_2_FLASH.value,
            google_model_name.RESPONSE_GEMINI_1_5_PRO.value,
            google_model_name.RESPONSE_GEMINI_1_PRO.value,
        ]

        logging.info(f"Primary model: {self.model_name}")

        try:
            primary_model_index = all_models.index(self.model_name)
        except ValueError as e:
            raise ValueError(f"Model not supproted {self.model_name}")
        
        _ = all_models.pop(primary_model_index)
        all_models.insert(0, self.model_name)

        for model in all_models:
            try:
                if 'gemini' in model:
                    generated_response: dict = self.googleai.generate_gemini_tool_response(
                        data=None,
                        context=system_prompt,
                        query=user_prompt,
                        chat_history=[],
                        model_name=model,
                        stream=False,
                        tools=[],
                        parameters=GeminiParams(
                            max_output_tokens=8192,
                            top_p=0.0,
                        )
                    )
                elif 'claude' in model:
                    generated_response: dict = self.googleai.generate_anthropic_tool_response(
                        data=None,
                        context=system_prompt,
                        query=user_prompt,
                        chat_history=[],
                        model_name=model,
                        stream=False,
                        tools=[]
                    )

                response = generated_response.get("content", None)
                cost = generated_response.get("total_tokens_cost", 0)
                logging.info(f"Cost of generating summary of summaries: {cost}")

                if response:
                    logging.debug(
                        f"Response in Github Pipeline - Summary of summaries generated using model {model} in region {self.region} for file: {filename}"
                    )
                    return response, cost
            except Exception as e:
                logging.error(f"Error with model {model} in region {self.region}: {traceback.format_exc()}")
                log_into_bigquery(
                    "generate_summary_of_summaries",
                    "",
                    "",
                    f"Error with model {model} in region {self.egion}: {traceback.format_exc()}",
                    500,
                )
                time.sleep(10)  # Short delay before retrying

        logging.error("Max retries reached, failed to generate summary.")
        return '', 0.0


    def _generate_summary(
            self,
            query, 
            filename = "", 
            max_word_count = 350, 
            previous_chunk_summary = "",
            continuous_summary = "",
            chunk_number=0,
            json_output=True
    ):
        """
        Generates a summary of the provided file content or code.

        Parameters:
            query (str): The content or code of the file to summarize.

        Returns:
            str: Summary of the project file content or code.
        """

        if query == "":
            return "No content to summarize", 0.0

        if previous_chunk_summary:
            user_prompt = Prompts.summary_generation_user_prompt_with_prev_summ_v3.format(
                filename=filename,
                query=query,
                prev_summ=previous_chunk_summary,
                file_summ=f"File Sumamry: {continuous_summary}" if continuous_summary else "",
                max_word_count=max_word_count,
            )
        else:
            user_prompt = Prompts.summary_generation_user_prompt_v3.format(
            query=query,
            filename=filename,
            max_word_count=max_word_count,
        )
        
        system_prompt = Prompts.summary_generation_sys_prompt_v3.format(
            max_word_count=max_word_count
        )

        if 'claude' in self.model_name:
            system_prompt += str(response_schema_claude)
        
        # os.makedirs('prompts', exist_ok=True)
        # with open(f'prompts/{os.path.basename(filename)}_{chunk_number}_sys_prompt.txt', 'w') as f:
        #     f.write(system_prompt)

        # with open(f'prompts/{os.path.basename(filename)}_{chunk_number}_user_prompt.txt', 'w') as f:
        #     f.write(user_prompt)

        all_models = [
            google_model_name.RESPONSE_CLAUDE_HAIKU.value,
            google_model_name.RESPONSE_CLAUDE_HAIKU_35.value,
            google_model_name.RESPONSE_CLAUDE_SONNET_35.value,
            google_model_name.RESPONSE_GEMINI_2_FLASH.value,
            google_model_name.RESPONSE_GEMINI_1_5_PRO.value,
            google_model_name.RESPONSE_GEMINI_1_PRO.value,
        ]

        logging.info(f"Primary model: {self.model_name}")
        
        try:
            primary_model_index = all_models.index(self.model_name)
        except ValueError as e:
            raise ValueError(f"Model not supproted {self.model_name}")
        _ = all_models.pop(primary_model_index)
        all_models.insert(0, self.model_name)
        cost = 0
        response = None
        
        for model in all_models:
            try:
                if 'gemini' in model:
                    model_response: dict = self.googleai.generate_gemini_tool_response(
                        data=None,
                        context=system_prompt,
                        query=user_prompt,
                        chat_history=[],
                        model_name=model,
                        stream=False,
                        tools=[],
                        parameters=GeminiParams(
                            max_output_tokens=8192,
                            top_p=0.0,
                            response_schema=self.response_schema,
                            response_mime_type="application/json"
                        )
                    )
                elif 'claude' in model:
                    model_response: dict = self.googleai.generate_anthropic_tool_response(
                        data=None,
                        context=system_prompt,
                        query=user_prompt,
                        chat_history=[],
                        model_name=model,
                        stream=False,
                        tools=[self.response_schema_claude],
                        tool_choice={"type": "tool", "name": "CodeAnalysisResponse"},
                        convert_tool_format_required=False
                    )

                response = model_response.get('content', "")
                cost += model_response.get('total_tokens_cost', 0)
                
                logging.info(f"Cost of generating summary of summaries: {cost}")
                if json_output:
                    response = self._load_json_data(str(response))
                    if isinstance(response, str):
                        continue
                    else:
                        return response, cost

                if response:
                    logging.debug(
                        f"Response in Github Pipeline - Summary generated using model {model} in region {self.region} for file: {filename}"
                    )
                    return str(response), cost
            except Exception as e:
                logging.error(f"Error with model {model} in region {self.region}: {traceback.format_exc()}")
                log_into_bigquery(
                    "generate_summary",
                    "",
                    "",
                    f"Error with model {model} in region {self.region}: {traceback.format_exc()}",
                    500,
                )
                time.sleep(10)  # Short delay before retrying

        if isinstance(response, str):
            return response, cost
        logging.error("Max retries reached, failed to generate summary.")
        return '', 0.0

    def _generate_summary_of_summaries(
            self,
            continuous_summ,
            next_summ, 
            filename = "", 
            max_word_count = 350,
            chunk_number: int = 0
        ):
        
        if not continuous_summ and not next_summ:
            return "No content to summarize", 0.0

        user_prompt = Prompts.summary_of_summary_generation_user_prompt.format(
            cont_summ=continuous_summ,
            next_summ=next_summ,
            max_word_count=max_word_count,
            filename=filename
        )
        system_prompt = Prompts.summary_of_summary_generation_sys_prompt.format(
            max_word_count=max_word_count
        )
        
        # os.makedirs('prompts', exist_ok=True)
        # with open(f'prompts/{os.path.basename(filename)}_{chunk_number}_ss_sys_prompt.txt', 'w') as f:
        #     f.write(system_prompt)

        # with open(f'prompts/{os.path.basename(filename)}_{chunk_number}_ss_user_prompt.txt', 'w') as f:
        #     f.write(user_prompt)

        all_models = [
            google_model_name.RESPONSE_CLAUDE_HAIKU.value,
            google_model_name.RESPONSE_CLAUDE_HAIKU_35.value,
            google_model_name.RESPONSE_CLAUDE_SONNET_35.value,
            google_model_name.RESPONSE_GEMINI_2_FLASH.value,
            google_model_name.RESPONSE_GEMINI_1_5_PRO.value,
            google_model_name.RESPONSE_GEMINI_1_PRO.value,
        ]

        logging.info(f"Primary model: {self.model_name}")

        try:
            primary_model_index = all_models.index(self.model_name)
        except ValueError as e:
            raise ValueError(f"Model not supproted {self.model_name}")
        _ = all_models.pop(primary_model_index)
        all_models.insert(0, self.model_name)

        for model in all_models:
            try:
                if 'gemini' in model:
                    generated_response: dict = self.googleai.generate_gemini_tool_response(
                        data=None,
                        context=system_prompt,
                        query=user_prompt,
                        chat_history=[],
                        model_name=model,
                        stream=False,
                        tools=[],
                        parameters=GeminiParams(
                            max_output_tokens=8192,
                            top_p=0.0,
                        )
                    )
                elif 'claude' in model:
                    generated_response: dict = self.googleai.generate_anthropic_tool_response(
                        data=None,
                        context=system_prompt,
                        query=user_prompt,
                        chat_history=[],
                        model_name=model,
                        stream=False,
                        tools=[]
                    )

                response = generated_response.get("content", None)
                cost = generated_response.get("total_tokens_cost", 0)
                logging.info(f"Cost of generating summary of summaries: {cost}")

                if response:
                    logging.debug(
                        f"Response in Github Pipeline - Summary of summaries generated using model {model} in region {self.region} for file: {filename}"
                    )
                    return response, cost
            except Exception as e:
                logging.error(f"Error with model {model} in region {self.region}: {traceback.format_exc()}")
                log_into_bigquery(
                    "generate_summary_of_summaries",
                    "",
                    "",
                    f"Error with model {model} in region {self.region}: {traceback.format_exc()}",
                    500,
                )
                time.sleep(10)  # Short delay before retrying

        logging.error("Max retries reached, failed to generate summary.")
        return '', 0.0
    
    def run(
        self,
        chunks: List[TaskObject],
        max_word_count: int = 1000,
    ):
        try:
            if not chunks:
                return []

            continuous_summary = ''
            prev_chunk_summary = ''
            all_results = []
            code_artifacts = []
            services = []
            cost = 0.0
            last_filename=""
            full_content = ''
            file_start_time = time.time()
            for task in chunks:
                task: TaskObject
                start_time = time.time()
                chunk_summary_results = ChunkOutput(
                    task_id=task.task_id,
                    git_url=task.github_url,
                    file=task.file_path,
                    content=task.file_content,
                    file_id=task.file_id,
                    chunk_id=task.chunk_id,
                    update_type=task.file_status,
                    is_chunked=task.is_chunked,
                    chunk_number=task.chunk_number,
                    chunk_type = "chunk",
                )
                chunk_cost = 0.0
                last_filename = task.file_path
                full_content += task.file_content

                if task.file_status == FileStatus.end.value:
                    logging.info(f"Received the {FileStatus.end.value} value for task_id :{self.task_id}")
                    all_results.append(ChunkOutput(
                        task_id=task.task_id,
                        git_url='',
                        file=FileStatus.end.value,
                        file_id=FileStatus.end.value,
                        chunk_id=FileStatus.end.value,
                        update_type=FileStatus.end.value,
                        summary=FileStatus.end.value,
                        content=FileStatus.end.value,
                        chunk_details={
                            "code_artifacts": {},
                            "services": {}
                        },
                        chunk_number=-1,
                        cost=0.0,
                        time_taken=0.0,
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
                        chunk_type="file"
                    ).model_dump())
                    continue
                
                stripped_file_path = str(
                    task.file_path.split(self.task_id, 1)[1].lstrip('/') if self.task_id in str(task.file_path) else str(task.file_path))

                chunk_summary, cost = self._generate_summary(
                    query=task.file_content,
                    filename=stripped_file_path,
                    max_word_count=max_word_count,
                    previous_chunk_summary=prev_chunk_summary,
                    continuous_summary=continuous_summary,
                    chunk_number=task.chunk_number,
                    json_output=True
                )

                chunk_cost += cost

                if prev_chunk_summary:
                    continuous_summary, cost = self._generate_summary_of_summaries(
                        continuous_summ=continuous_summary if continuous_summary else prev_chunk_summary,
                        next_summ=json.dumps(chunk_summary),
                        filename=stripped_file_path,
                        max_word_count=max_word_count,
                        chunk_number=task.chunk_number
                    )
                    chunk_cost += cost

                chunk_summary_results.cost = chunk_cost
                
                prev_chunk_summary = json.dumps(chunk_summary)
                
                if isinstance(chunk_summary, str):
                    all_results.append(
                        ChunkOutput(
                            task_id=task.task_id,
                            git_url=task.github_url,
                            file=stripped_file_path,
                            file_id=task.file_id,
                            chunk_id=task.chunk_id,
                            update_type=FileStatus.new.value,
                            summary=f"File: {stripped_file_path}\n\nFile Summary:\n<!file_summary!>\n\nChunk Summary:\n```summary\n{extract_accurate_summary(chunk_summary)}\n```",
                            content=task.file_content,
                            chunk_details={
                                "code_artifacts": {},
                                "services": {}
                            },
                            chunk_number=task.chunk_number,
                            cost=chunk_cost,
                            time_taken=time.time() - start_time,
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
                            chunk_type="chunk",
                            is_chunked=task.is_chunked
                        ).model_dump()
                    )
                    continue
                
                summary = chunk_summary.pop('accurate_summary', '')

                chunk_summary_results.summary = f"File: {stripped_file_path}\n\nFile Summary:\n<!file_summary!>\n\nChunk Summary:\n```summary\n{summary}\n```"
                chunk_summary_results.chunk_details = chunk_summary


                if not isinstance(chunk_summary.get("code_artifacts", {}), dict):
                    chunk_summary["code_artifacts"] = {}
                if not isinstance(chunk_summary.get("services", {}), dict):
                    chunk_summary["services"] = {}

                code_artifacts.append(chunk_summary.get("code_artifacts", {}))
                services.append(chunk_summary.get("services", {}))
                
                chunk_summary_results.file_stats = {
                    "functions_count":len(chunk_summary.get("code_artifacts", {}).get('functions', [])),
                    "classes_count":len(chunk_summary.get("code_artifacts", {}).get('classes', [])),
                    "markup_tags_count":len(chunk_summary.get("code_artifacts", {}).get('markup_tags', [])),
                    "global_variables_used_count":len(chunk_summary.get("code_artifacts", {}).get('global_variables', {}).get('used', [])),
                    "global_variables_not_used_count":len(chunk_summary.get("code_artifacts", {}).get('global_variables', {}).get('not_used', [])),
                    "code_lines_count":len(task.file_content.splitlines()),
                    "total_chunks_count":len(all_results),
                    "tables_count": len(chunk_summary.get('tables', []))
                }
                all_results.append(chunk_summary_results.model_dump())

            if task.is_chunked:
                smaller_file_summary, cost = self._generate_small_file_summary(
                    query=continuous_summary,
                    filename=last_filename,
                    max_word_count=max_word_count//4
                )

                all_results[-1]['cost'] += cost
                
                for i in range(len(all_results)):
                    if all_results[i].get('summary', ""):
                        all_results[i]['summary'] = all_results[i]['summary'].replace('<!file_summary!>', smaller_file_summary)
                

                code_artifacts=merge_code_artifacts_dicts(code_artifacts)
                services=merge_services_dicts(services)
                file_summary_chunk = ChunkOutput(
                        task_id=task.task_id,
                        git_url=task.github_url,
                        file=task.file_path,
                        file_id=task.file_id,
                        chunk_id=f"{task.file_id}_{task.chunk_number}",
                        update_type=FileStatus.new.value,
                        summary=f"File: {os.path.basename(task.file_path)}\n\nFull File Summary:\n```summary\n{continuous_summary}\n```",
                        content=full_content,
                        chunk_details={
                            "code_artifacts": code_artifacts,
                            "services": services
                        },
                        file_stats={
                            "functions_count":len(code_artifacts.get('functions', [])),
                            "classes_count":len(code_artifacts.get('classes', [])),
                            "markup_tags_count":len(code_artifacts.get('markup_tags', [])),
                            "global_variables_used_count":len(code_artifacts.get('global_variables', {}).get('used', [])),
                            "global_variables_not_used_count":len(code_artifacts.get('global_variables', {}).get('not_used', [])),
                            "code_lines_count":len(full_content.splitlines()),
                            "total_chunks_count":len(all_results),
                            "tables_count": len(code_artifacts.get('tables', []))
                        },
                        chunk_type="file",
                        is_chunked=True,
                        time_taken=time.time() - file_start_time
                    ).model_dump()
                all_results.append(file_summary_chunk)

            return all_results
        except Exception as e:
            logging.error(f"Error in Code Summarizer: {traceback.format_exc()}")
            raise e
        
def merge_services_dicts(services: List[Dict[Literal["internal_services", "external_services"], list]]):
    """
    Merges a list of service dictionaries into a single dictionary with unique services.

    Args:
        services (List[Dict]): A list of dictionaries containing "internal_services" and "external_services".

    Returns:
        Dict: A dictionary with merged "internal_services" and "external_services".
    """
    merged_services = {
        "internal_services": [],
        "external_services": []
    }
    internal_services_covered = set()
    external_services_covered = set()
    for service in services:
        # Process internal services
        for value in service.get("internal_services", []):
            name = value.get('name')
            if name and name not in internal_services_covered:
                internal_services_covered.add(name)
                merged_services["internal_services"].append(value)

        # Process external services
        for value in service.get("external_services", []):
            name = value.get('name')
            if name and name not in external_services_covered:
                external_services_covered.add(name)
                merged_services["external_services"].append(value)
        
    return merged_services


def merge_code_artifacts_dicts(services: List[Dict[Literal["internal_services", "external_services"], list]]):
    """
    Merges a list of service dictionaries into a single dictionary with unique services.

    Args:
        services (List[Dict]): A list of dictionaries containing "internal_services" and "external_services".

    Returns:
        Dict: A dictionary with merged "functions", "classes", "markup_tags", "global_variables", "tables", and "procedures".
    """
    merged_services = {
        "functions": [],
        "classes": [],
        "markup_tags": [],
        "global_variables": {
            "used": [],
            "not_used": []
        },
        "tables": [],
        "procedures": []
    }

    functions_covered = set()
    classes_covered = set()
    markup_tags_covered = set()
    global_variables_covered_used = set()
    global_variables_covered_not_used = set()
    tables_covered = set()
    procedures_covered = set()

    for service in services:
        # Process functions
        for value in service.get("functions", []):
            name = value.get('name')
            if name and name not in functions_covered:
                functions_covered.add(name)
                merged_services["functions"].append(value)

        # Process classes
        for value in service.get("classes", []):
            name = value.get('name')
            if name and name not in classes_covered:
                classes_covered.add(name)
                merged_services["classes"].append(value)

        # Process markup tags
        for value in service.get("markup_tags", []):
            name = value.get('name')
            if name and name not in markup_tags_covered:
                markup_tags_covered.add(name)
                merged_services["markup_tags"].append(value)

        # Process global variables
        for value in service.get("global_variables", {}).get('used', []):
            name = value.get('name')
            if name and name not in global_variables_covered_used:
                global_variables_covered_used.add(name)
                merged_services["global_variables"]['used'].append(value)
        
        for value in service.get("global_variables", {}).get('not_used', []):
            name = value.get('name')
            if name and name not in global_variables_covered_not_used and name not in global_variables_covered_used:
                global_variables_covered_not_used.add(name)
                merged_services["global_variables"]['not_used'].append(value)

        # Process tables
        for value in service.get("tables", []):
            name = value.get('name')
            if name and name not in tables_covered:
                tables_covered.add(name)
                merged_services["tables"].append(value)

        # Process procedures
        for value in service.get("procedures", []):
            name = value.get('name')
            if name and name not in procedures_covered:
                procedures_covered.add(name)
                merged_services["procedures"].append(value)

    return merged_services


def merge_global_variables(gv_a, gv_b):
    used_dict = {}
    not_used_dict = {}
    other_keys = defaultdict(list)

    def add_used(item):
        name = item["name"]
        if name not in used_dict:
            used_dict[name] = item.copy()

    def add_not_used(item):
        name = item["name"]
        if name not in used_dict and name not in not_used_dict:
            not_used_dict[name] = item.copy()

    for source in [gv_a, gv_b]:
        for item in source.get("used", []):
            add_used(item)
        for item in source.get("not_used", []):
            add_not_used(item)
        for key, val in source.items():
            if key not in {"used", "not_used"} and isinstance(val, list):
                other_keys[key].extend(val)

    return {
        "used": list(used_dict.values()),
        "not_used": list(not_used_dict.values()),
        **other_keys
    }


def merge_dicts(dict_a, dict_b):
    merged = {}
    all_keys = set(dict_a.keys()).union(dict_b.keys())
    
    for key in all_keys:
        val_a = dict_a.get(key, {})
        val_b = dict_b.get(key, {})

        if isinstance(val_a, dict) and isinstance(val_b, dict):
            merged[key] = {}
            inner_keys = set(val_a.keys()).union(val_b.keys())
            for inner_key in inner_keys:
                inner_val_a = val_a.get(inner_key, [])
                inner_val_b = val_b.get(inner_key, [])
                if inner_key == "global_variables":
                    merged[key][inner_key] = merge_global_variables(inner_val_a, inner_val_b)
                elif isinstance(inner_val_a, list) and isinstance(inner_val_b, list):
                    merged[key][inner_key] = inner_val_a + inner_val_b
                elif isinstance(inner_val_a, list):
                    merged[key][inner_key] = inner_val_a.copy()
                elif isinstance(inner_val_b, list):
                    merged[key][inner_key] = inner_val_b.copy()
                else:
                    merged[key][inner_key] = inner_val_b if inner_key in val_b else inner_val_a
        elif isinstance(val_a, list) and isinstance(val_b, list):
            merged[key] = val_a + val_b
        elif isinstance(val_a, list):
            merged[key] = val_a.copy()
        elif isinstance(val_b, list):
            merged[key] = val_b.copy()
        else:
            merged[key] = val_b if key in dict_b else val_a

    return merged


def merge_multiple_dicts(dict_list):
    merged = {}
    for current_dict in dict_list:
        merged = merge_dicts(merged, current_dict)

    # used_dict = {}
    # not_used_dict = {}
    # used_lst =[]
    # not_used_lst=[]
    # for key,value in merged.get('global_variables', {}).items():
    #     if key == 'used':
    #         for item in value:
    #             name = item["name"]
    #             used_dict[name] = item.copy()
    #     else:
    #         for item in value:
    #             name = item["name"]
    #             not_used_dict[name] = item.copy()

    # for key,value in used_dict.items():
    #     used_lst.append(value)
    # for key,value in not_used_dict.items():
    #     not_used_lst.append(value)
    # if merged.get('global_variables', {}).get('used', None):
    #     merged['global_variables']['used'] = used_lst
    # if merged.get('global_variables', {}).get('not_used', None):
    #     merged['global_variables']['not_used'] = not_used_lst
    return merged