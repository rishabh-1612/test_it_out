import os
import re
import sys
import ast
import json
import time
import logging
import traceback
from uuid import uuid4
from typing import Union, List
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Uncomment to run locally
from data_extraction.utils.github_repo_prompts import Prompts
from helperClasses.GoogleAI import GoogleAI, GeminiParams
from Enums.model_type import google_model_name, google_region
from data_extraction.utils.github_repo_bigquery_logger import log_into_bigquery
from data_extraction.utils.github_repo_summary_output_format import response_schema, response_schema_claude
from data_extraction.utils.github_repo_utils import extract_accurate_summary
from data_extraction.Enums.github_repo_enums import FileStatus, TaskObject, ChunkOutput
from data_extraction.utils.db_summary_output_format import ColumnsResponse, TableResponse
from collections import defaultdict


class DBSummarizer():
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


    def _generate_summary(
            self,
            filename = "", 
            table_info = "", 
            previous_chunk_summary = "",
            column_chunk_info = "",
            metadata="",
            type="column"
            # max_word_count = 350, 
            # previous_chunk_summary = "",
            # continuous_summary = "",
            # chunk_number=0
    ):
        """
        Generates a summary of the provided file content or code.

        Parameters:
            query (str): The content or code of the file to summarize.

        Returns:
            str: Summary of the project file content or code.
        """

        # if query == "":
        #     return "No content to summarize", 0.0

        logging.info(f"\n\nColumn Chunk {column_chunk_info}\n\n")
        if type=="column" : 
            user_prompt = Prompts.description_user_prompt.format(
                table_info=table_info,
                previous_chunk_summary=previous_chunk_summary,
                column_chunk=column_chunk_info,
            )

            system_prompt = Prompts.chunked_description_system_prompt.format(
                metadata = metadata
            )

            response_schema = ColumnsResponse

            logging.info(f"Response Schema for Gemini (Column level summary): \n{response_schema.model_json_schema()}")

        elif type=="table" : 
            user_prompt = Prompts.table_description_user_prompt.format(
                content = previous_chunk_summary,
            )

            system_prompt = Prompts.table_description_system_prompt.format(
                metadata = metadata
            )

            response_schema = TableResponse
            logging.info(f"Response Schema for Gemini (Table level summary): \n{response_schema.model_json_schema()}")

        # log prompts locally
        os.makedirs('prompts', exist_ok=True)
        with open(f'prompts/{os.path.basename(filename)}_0_sys_prompt.txt', 'w') as f:
            f.write(system_prompt)

        with open(f'prompts/{os.path.basename(filename)}_0_user_prompt.txt', 'w') as f:
            f.write(user_prompt)
        
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

        logging.info("\n\n===========\n\n User Prompt: \n\n" + user_prompt + "\n\n=======\n\n")
        logging.info("\n\n===========\n\n System Prompt: \n\n" + system_prompt + "\n\n=======\n\n")

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
                            response_schema=response_schema.model_json_schema(),
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

                cost = model_response.get('total_tokens_cost', 0)
                logging.info(f"Cost of generating summary of summaries: {cost}")

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

        logging.error("Max retries reached, failed to generate summary.")
        return '', 0.0

    def post_process_chunk(self, all_results, task, chunk_cost, chunk_summary_dict):
        try:
            chunk_summary_dict: dict = json.loads(chunk_summary_dict)
        except json.decoder.JSONDecodeError as e:
            try:
                logging.warning(f"Json parsing failed for the generated summary: trying ast.literal_val")
                chunk_summary_dict: dict = ast.literal_eval(chunk_summary_dict)
            except Exception as e:
                logging.warning(f"Json parsing failed for the generated summary: {chunk_summary_dict}")
                all_results.append(
                    ChunkOutput(
                        task_id=task.task_id,
                        git_url='',
                        file=task.file_path,
                        file_id=task.file_id,
                        chunk_id=task.chunk_id,
                        update_type=FileStatus.new.value,
                        summary=f"{chunk_summary_dict}",
                        content=task.file_content,
                        chunk_details={
                            "code_artifacts": {},
                            "services": {}
                        },
                        chunk_number=task.chunk_number,
                        cost=chunk_cost,
                        time_taken=time.time() - self.chunk_start_time,
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
                        chunk_type="chunk"
                    ).model_dump()
                )

                return all_results
            
    def _combine_all_summaries(
        self,
        all_results: str,
    ) -> List[dict]:

        combined_result = {
            "task_id": str(uuid4()),
            "git_url": "",
            "file": "",
            "file_id": "",
            "chunk_id": "",
            "update_type": "new",
            "summary": "json_formatted_string goes here",
            "content": "original_file_content",
            "chunk_details": {},
            "is_chunked": False,
            "chunk_number": int,
            "cost": float,
            "time_taken": 0.0,
            "file_stats": {
                "functions_count": 0,
                "classes_count": 0,
                "markup_tags_count": 0,
                "global_variables_used_count": 0,
                "global_variables_not_used_count": 0,
                "code_lines_count": 0,
                "total_chunks_count": 0,
                "tables_count": 0
            },
            "chunk_type": "chunk"
        }

        combined_result["git_url"] = all_results[-1].get("git_url", "")            
        combined_result["file"] = all_results[-1].get("file", "")
        combined_result["file_id"] = all_results[-1].get("file_id", "")
        combined_result["chunk_id"] = all_results[-1].get("chunk_id", "")
        combined_result["update_type"] = all_results[-1].get("update_type", FileStatus.new.value)
        combined_result["cost"] = int(sum(result.get("cost", 0.0) for result in all_results))
        combined_result["time_taken"] = sum(result.get("time_taken", 0.0) for result in all_results)

        combined_result["chunk_number"] = all_results[-1].get("chunk_number", 0)

        file_stats = all_results[0].get("file_stats", {})
        combined_result["file_stats"] = {
            key: sum([d.get("file_stats", {}).get(key, 0) for d in all_results])
            for key in file_stats.keys()
        }
        
        combined_result["content"] = ", ".join([result.get("content", "") for result in all_results])

        logging.info(f"All Results : {all_results}")

        combined_columns = []
        for result in all_results:
            result = json.loads(result) if isinstance(result, str) else result
            summary_temp = result.get("summary", "{}")
            summary_json = json.loads(summary_temp) if isinstance(summary_temp, str) else summary_temp
            combined_columns.extend(summary_json.get("columns", []))
        
        # logging.info(f"ALL RESULTS : {all_results}\n\n")
        # logging.info(f"ALL RESULTS TYPE : {type(all_results)}\n\n")
        # logging.info(f"All Results [-1]: {all_results[-1]}\n\n")
        # logging.info(f"Type of All Results [-1] : {type(all_results[-1])}\n\n")
        # logging.info(f"All Results [-1] Summary: {all_results[-1]['summary']}\n\n")
        # logging.info(f"JSON LOADED All Results [-1] SUMMARY : {json.loads(all_results[-1]['summary'])}\n\n")
        # logging.info(f"Type of All Results [-1] SUMMARY : {type(all_results[-1]['summary'])}\n\n")
        # logging.info(f"AST LITERAL EVAL All Results [-1] SUMMARY : {ast.literal_eval(all_results[-1]['summary'])}\n\n")
        # logging.info(f"TYPE OF AST LITERAL EVAL All Results [-1] SUMMARY : {type(ast.literal_eval(all_results[-1]['summary']))}\n\n")

        if isinstance(all_results[-1]["summary"], str):
            summary_json = json.loads(all_results[-1]["summary"])
        elif isinstance(all_results[-1]["summary"], dict):
            summary_json = all_results[-1]["summary"]

        logging.info(f"Summary JSON : {summary_json}\n\n")
        logging.info(f"ALL RESULTS : {all_results}\n\n")
        logging.info(f"Last Element of ALL RESULTS : {all_results[-1]}\n\n")

        combined_result["summary"] = {
            "columns": combined_columns,
            "table_description": summary_json.get("table_description"),
            "table_name": summary_json.get("table_name"),
            "indexed_columns": summary_json.get("indexed_columns"),
            "foreign_keys": summary_json.get("foreign_keys"),
            "primary_keys": summary_json.get("primary_keys"),
            }
        
        combined_result["chunk_details"] = {
            "indexed_columns": summary_json.get("indexed_columns"),
            "foreign_keys": summary_json.get("foreign_keys"),
            "primary_keys": summary_json.get("primary_keys"),
            "services": {
                "external_services": [],
                "internal_services": [],
            }
        }

        combined_result["summary"] = json.dumps(combined_result["summary"], indent=4)


        combined_result["summary"] = self.json_to_markdown(json.loads(combined_result["summary"]))

        return [combined_result]
        
    
    def run(
        self, 
        chunks: List[TaskObject],
        sql_metadata: dict
    ) : 
        try : 
            if not chunks : 
                return []
            
            logging.info(f"Running DBSummarizer with {len(chunks)} chunks")
            cumulative_summary = ""
            all_results = []
            full_content = ""
            count = 0
            logging.info(f"Got metadata {sql_metadata}")
            first_chunk = chunks[0] # First Chunk will contain Table Name
            first_chunk: TaskObject

            try : 
                table_info = first_chunk.file_content
                match = re.search(
                    r'CREATE TABLE\s+\`([^\`]+)\`', table_info, re.IGNORECASE
                    )
                table_info = match.group(1)

            except AttributeError : 
                table_info = ""
                pass # Continue without error if table_info is not found

            logging.info(f"Inside DB Summarizer")
            logging.info(f"Table Name: {table_info}")

            count = -1

            all_content = "".join([task.file_content for task in chunks if task.file_content])
            full_content = all_content.strip()
            
            # Iterate through each chunk
            for task in chunks :
                count += 1

                logging.info(f"From DBSummarizer : chunk number: {count}")
                
                self.chunk_start_time = time.time()
                task: TaskObject

                logging.info(f"Task Object : \n{task}\n")
                logging.info(f"Task ID type: {type(task.task_id)}, value: {task.task_id}\n")
                logging.info(f"GitHub URL type: {type(task.github_url)}, value: {task.github_url}\n")
                logging.info(f"File Path type: {type(task.file_path)}, value: {task.file_path}\n")
                logging.info(f"File Content type: {type(task.file_content)}, value: {task.file_content}\n")
                logging.info(f"File ID type: {type(task.file_id)}, value: {task.file_id}\n")
                logging.info(f"Chunk ID type: {type(task.chunk_id)}, value: {task.chunk_id}\n")
                logging.info(f"File Status type: {type(task.file_status)}, value: {task.file_status}\n")
                logging.info(f"Is Chunked type: {type(task.is_chunked)}, value: {task.is_chunked}\n")
                logging.info(f"Chunk Number type: {type(task.chunk_number)}, value: {task.chunk_number}\n")

                #init the ChunkOutput object
                chunk_summary_results = ChunkOutput(
                    task_id = task.task_id, 
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

                # full_content += task.file_content
                chunk_cost=0.0 # initialize chunk costs

                # If chunk is end chunk, append the result to all_results
                if task.file_status == FileStatus.end.value  : 
                    logging.info(f"========================== INSIDE END CHUNK IF STATEMENT ==========================")
                    all_results.append(ChunkOutput(
                        task_id=task.task_id,
                        git_url='',
                        file=FileStatus.end.value,
                        file_id=FileStatus.end.value,
                        chunk_id=FileStatus.end.value,
                        update_type=FileStatus.end.value,
                        summary='',
                        content=FileStatus.end.value,
                        chunk_details={
                            "column_names": {},
                            "services": {
                                "external_services": [],
                                "internal_services": []
                            }
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
                
                # logging.info(f"Columns sent to LLM : \n\n {task.file_content} \n\n")
                
                # generate descriptions for current chunk
                chunk_summary_json, cost = self._generate_summary(
                    filename=task.file_path,
                    table_info=table_info,
                    previous_chunk_summary="",
                    column_chunk_info=task.file_content,
                    metadata=sql_metadata, 
                    type="column"
                )

                logging.info(f"LLM Response for chunk summary : {chunk_summary_json}")
                
                # APPEND CURRENT CHUNK SUMMARY 
                try:
                    chunk_summary_dict: dict = json.loads(chunk_summary_json)
                    # chunk_summary_list: list[dict] = chunk_summary_json.get("columns", "") 
                    chunk_cost += cost
                    chunk_summary_results.cost = chunk_cost
                    chunk_summary_results.summary = json.dumps(chunk_summary_dict)
                    
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
                    other_key_descriptions = []
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
                        logging.info(f"Col descriptions: {col_description}")
                        logging.info(f"Col data: {col_data}")
                        # if str(col_description.get("is_primary_key", "")) == "True":
                        #     primary_key_descriptions.append(col_data)
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

                    if count == len(chunks)-1:  # Last chunk
                        logging.info(f"Last chunk of file {task.file_path} reached. Generating table summary...")
                        # Generate Table Descriptions here for last chunk:
                        table_summary, cost = self._generate_summary(
                            previous_chunk_summary=f"Cumulative Summary : {cumulative_summary}\nFull Content : {full_content}",
                            metadata=sql_metadata,
                            type="table"
                        )

                        logging.info(f"Table Summary : {table_summary}")
                        table_summary = json.loads(table_summary)
                        table_description = table_summary.get("table_description", "")
                        # for key in ["table_description", "table_name", "foreign_keys", "primary_keys"]:
                        for key in table_summary.keys():
                            try:
                                chunk_summary_dict[key] = table_summary[key]
                            except KeyError:
                                logging.info(f"{key} not found. Proceeding without it.")
                        
                        logging.info(f"Final chunk summary dict: \n{chunk_summary_dict}")

                    for col_description in chunk_summary_dict.get("columns", []):
                        cumulative_summary += str(col_description["column_description"]).strip() + " \n"
                    logging.info(f"Cumulative Summary : {cumulative_summary}")

                
                except json.decoder.JSONDecodeError as e:
                    try:
                        logging.warning(f"Json parsing failed for the generated summary: trying ast.literal_val")
                        chunk_summary_dict: dict = ast.literal_eval(chunk_summary_dict)
                    except Exception as e:
                        logging.warning(f"Json parsing failed for the generated summary: {chunk_summary_dict}")
                        logging.info(f"Column Descriptions from inside 497 in dbFileSummaryGeneration.py \n\n{column_descriptions}\n\n")
                        all_results.append(
                            ChunkOutput(
                                task_id=task.task_id,
                                git_url='',
                                file=task.file_path,
                                file_id=task.file_id,
                                chunk_id=task.chunk_id,
                                update_type=FileStatus.new.value,
                                summary=f"{json.dumps(chunk_summary_dict)}",
                                content=task.file_content,
                                chunk_details={
                                    "columns": column_descriptions,
                                    # "column_names": {
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
                                cost=chunk_cost,
                                time_taken=time.time() - self.chunk_start_time,
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
                                chunk_type="chunk"
                            ).model_dump()
                        )
                        continue
               

                chunk_summary_results.summary = json.dumps(chunk_summary_dict)
                logging.info(f"Column Descriptions from inside 536 in dbFileSummaryGeneration.py \n\n{column_descriptions}\n\n")
                chunk_summary_results.chunk_details = {
                    "columns": column_descriptions,
                    # "column_names": {
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
                    "services" : {
                        "external_services": [],
                        "internal_services": []
                    }
                }

                d = chunk_summary_dict.get
                ga = d("column_names", {})
                gv = ga.get("global_variables", {})
                chunk_summary_results.file_stats = {
                    "column_count": len(column_descriptions),
                    # "functions_count": len(ga.get("functions") or []),
                    # "primary_key_count" : len(primary_key_descriptions),
                    # "foreign_key_count" : len(foreign_key_descriptions),
                    # "composite_key_count" : len(composite_key_descriptions),
                    # "indexed_column_count" : len(indexed_column_descriptions),
                    # "not_null_constraint_count" : len(not_null_constraint_descriptions),
                    # "check_constraint_count" : len(check_constraint_descriptions),
                    # "unique_key_constraint_count" : len(unique_key_constraint_descriptions),
                    # "default_key_constarint_count" : len(default_key_constarint_descriptions),
                    # "other_columns_count" : len(other_key_descriptions),
                    # "classes_count": len(ga.get("classes") or []),
                    # "markup_tags_count": len(ga.get("markup_tags") or []),
                    # "global_variables_used_count": len(gv.get("used") or []),
                    # "global_variables_not_used_count": len(gv.get("not_used") or []),
                    # "code_lines_count": len(task.file_content.splitlines()),
                    # "total_chunks_count": len(all_results),
                    # "tables_count": len(d("tables") or [])
                }
                all_results.append(chunk_summary_results.model_dump())
                logging.info("Appended chunk summary results to all_results")

                logging.info(f"Chunk Summary ChunkOutput : {chunk_summary_results.model_dump()}")

                # END OF FOR LOOP ITERATION == 
            if task.is_chunked : 
                
                if not chunk_summary_dict:
                    logging.info(f"No summary generated for chunk {task.chunk_number} of file {task.file_path}. Exiting...")
                    exit(1)
                

            logging.info(f"All Results : {all_results}")
            # all_results = self._combine_all_summaries(all_results)
            for result in all_results:
                result["summary"] = self.json_to_markdown(table_description)
                

            return all_results
                

        except Exception as e: 
            logging.error(f"Error generating summary from DBSummarizer: {e}")
            logging.error("".join(traceback.format_exception(type(e), e, e.__traceback__)))
            return []

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
        
