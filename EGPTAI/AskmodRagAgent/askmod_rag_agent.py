from typing import Optional, Annotated, Literal, List, Dict
from pydantic import BaseModel, Field
import json
from enum import Enum
import logging
from thefuzz.fuzz import partial_ratio
import ast
import re
import requests
import datetime
from agents.AgentBase import AgentBase, AgentSetupBase, AgentsTraceBase
from tools.doc_retrieval_tool import DocRetrievalTool
from tools.context_aware_response_generator import ContextAwareResponseTool
from tools.query_alteration_tool import QueryAlterationTool
from utils.utils import update_dict
from utils.utils import get_setup_details, accumulator, process_config, citation_generator, enum_to_list
from utils.utils_traceability import AITrace
from utils.agent_enums import DisplayFormat
from sklearn.metrics.pairwise import cosine_similarity
import time

class CitationTypes(str, Enum):
    normal = "Normal"
    hc_book_order = "HC Book Order Format"
    hc_book = "HC Book Format"
    

class AskModRAGAgentSetup(AgentSetupBase):
    """
        RAG based Agent designed to understand the user query, retrieve appropriate documents and generate a response based on the documents.
    """ 
    fallback_response: Optional[str] = Field("There was an issue processing the request.",
                                            description="Default response when no suitable answer can be generated.")
    show_headers: Optional[bool] = Field(False,
                                        description="Whether to show thinking steps for this agent.")
    generate_response_from_docs: Optional[bool] = Field(True,
                                                        description="This describes whether we need to generate response or just return the documents retrieved.", 
                                                        title="Generate Response From Retrieved Documents")
    citation_format: Literal[tuple(enum_to_list(CitationTypes))] = Field(CitationTypes.normal.value, 
                                                                        description="The format of the citations")
    filter_citations: Optional[bool] = Field(False,
                                            description="Whether to filter the citations based on the generated response. (Works only when response is generated from the documents)")
    filter_citation_threshold: Optional[int] = Field(90, 
                                                     description="The threshold for fuzzy match for the citations", 
                                                     le=100, ge=0)
    executive_summary_endpoint: Optional[str] = Field("https://dev-appmod.techo.camp/analyzer/get_executive_summary_prompt",
                                            description="Endpoint URL for retrieving executive summary.",
                                               title="Executive Summary Endpoint")
    alter_query: Optional[bool] = Field(False,
                                        description="Should the query be altered to include more context from the chat history.", title="Alter Query")
    smart_user_prompt_adjustment: Optional[bool] = Field(True, description="Toggle to enable/disable the root orchestrator to modify the input query.", title="Smart User Prompt Adjustment")

    date_for_updated_data: Optional[str] = Field(default="2025-06-02 00:00:00", description="Whether to use the date for updated data in the response.", title="Prod Release Date")

    link_for_old_appmod: Optional[str] = Field(default="https://dev-appmod.techo.camp",
                                                description="Link for the old appmod.", title="Link for Old Appmod")

    reanalysis_required_message: Optional[str] = Field(default="\n New features just dropped! Re-analyze your selected codebases to see them in action. \n",
                                                  description="Message to the user for reanalysis", title="Reanalysis Required Message")

    project_extraction_limit : Optional[int] = Field(3, description="The limit for the number of relevant projects to be extracted.", title="Project Extraction Limit")
    project_information_endpoint: Optional[str] = Field("https://dev-appmod.techo.camp/api/error-check", description="Endpoint URL for retrieving project information.", title="Project Information Endpoint")
    project_extraction_threshold: Optional[float] = Field(0.4, description="The threshold for project information and query matching.", title="Project Extraction Threshold")



class AskModRAGAgentTrace(AgentsTraceBase):
    user_query: str = Field(..., description="Input query for the rag agent", 
                            title="Input Query", 
                            displayOnCard=True)
    fallback_response: str = Field(..., description="Default response when no suitable answer can be generated.", 
                                   title="Fallback Response")
    alter_query: Optional[bool] = Field(...,
                                        description="Should the query be altered to include more context from the chat history.", 
                                        title="Alter Query", 
                                        displayOnCard=True)
    generate_response_from_docs: Optional[bool] = Field(...,
                                                        description="This describes whether we need to generate response or just return the documents retrieved.", 
                                                        title="Generate Response From Retrieved Documents", 
                                                        displayOnCard=True)
    citation_format: Literal[tuple(enum_to_list(CitationTypes))] = Field(..., 
                                                                        description="The format of the citations")
    filter_citations: Optional[bool] = Field(...,
                                            description="Whether to filter the citations based on the generated response. (Works only when response is generated from the documents)",
                                            displayOnCard=True)
    filter_citation_threshold: Optional[int] = Field(..., 
                                                     description="The threshold for fuzzy match for the citations", 
                                                     le=100, ge=0)
    show_headers: Optional[bool] = Field(..., description="Whether to show thinking steps for this agent.", 
                                        title="Show Headers", 
                                        displayOnCard=True)
    response: str = Field(..., description="A concise and accurate answer addressing the user's query effectively.", 
                          title="Output Response", 
                          displayOnCard=True)
    citations: List[Dict] = Field(..., description="List of citations for the documents retrieved.",
                                  title="Citations")
    smart_user_prompt_adjustment: Optional[bool] = Field(...,
                                                        description="Toggle to enable/disable the root orchestrator to modify the input query.",
                                                        title="Smart User Prompt Adjustment")


class AskModRAGAgent(AgentBase):
    """
        RAG based Agent designed to understand the user query, retrieve appropriate documents and generate a response based on the documents.
    """

    def setup(self, concierge_id: Optional[str] = None, agent_id: Optional[str] = None,
              config: Optional[dict] = None, data: Optional[dict] = None) -> dict:
        
        if config is None:
            assert agent_id is not None, "If config is not provided agent_id is a required parameter"
            assert concierge_id is not None, "If config is not provided concierge_id is a required parameter"
            config = get_setup_details(concierge_id, agent_id)
        
        super().setup(config=config)
        self.generate_response_from_docs = config.get("generate_response_from_docs",AskModRAGAgentSetup.get_default_value("generate_response_from_docs"))
        self.fallback_response = config["fallback_response"]
        self.show_headers = config.get("show_headers", AskModRAGAgentSetup.get_default_value("show_headers"))
        self.alter_query = config.get("alter_query", AskModRAGAgentSetup.get_default_value("alter_query"))
        self.citation_format = config.get("citation_format", AskModRAGAgentSetup.get_default_value("citation_format"))
        self.filter_citations = config.get("filter_citations", AskModRAGAgentSetup.get_default_value("filter_citations"))
        self.filter_citation_threshold = config.get("filter_citation_threshold", AskModRAGAgentSetup.get_default_value("filter_citation_threshold"))
        self.smart_user_prompt_adjustment = config.get("smart_user_prompt_adjustment", AskModRAGAgentSetup.get_default_value("smart_user_prompt_adjustment"))
        self.input_user_question = data.get("question", None)
        self.executive_summary_endpoint = config.get("executive_summary_endpoint", AskModRAGAgentSetup.get_default_value("executive_summary_endpoint"))
        self.date_for_updated_data = config.get("date_for_updated_data", AskModRAGAgentSetup.get_default_value("date_for_updated_data"))
        self.reanalysis_required_message = config.get("reanalysis_required_message", AskModRAGAgentSetup.get_default_value("reanalysis_required_message"))
        self.link_for_old_appmod = config.get("link_for_old_appmod", AskModRAGAgentSetup.get_default_value("link_for_old_appmod"))

        self.project_information_endpoint = config.get("project_information_endpoint", AskModRAGAgentSetup.get_default_value("project_information_endpoint"))
        self.project_extraction_threshold = float(config.get("project_extraction_threshold", AskModRAGAgentSetup.get_default_value("project_extraction_threshold")))
        self.project_extraction_limit = int(config.get("project_extraction_limit", AskModRAGAgentSetup.get_default_value("project_extraction_limit")))
        
        try : 
            self.metadata = json.loads(data.get("metadata", {}))
        except : 
            self.metadata = ast.literal_eval(data.get("metadata", {}))
        # self.metadata = (data.get("metadata", {}))


        self.throw_error_response = False
        self.is_cw_flow = False
        self.multiple_projects = False
        self.reanalysis_required = []
        if self.metadata.get("is_cw_flow", False):
            self.is_cw_flow = True
            project_details = self.metadata.get("selectedProjectDetails", {})
            userSelectedProjects = self.metadata.get("userSelectedProjects", [])
            self.current_user_id = project_details.get("current_user_id", None)
            self.updated_at = project_details.get("updated_at", None) 
            database = project_details.get("assistantName", None)
            if database:
                if(self.updated_at and self.updated_at < self.date_for_updated_data):
                    self.reanalysis_required.append({
                        "is_needed": False,
                        "task_id": "",
                        "project_name": "",
                        "last_updated_at": ""
                    })
                    self.reanalysis_required[0]["is_needed"] = True
                    self.reanalysis_required[0]["task_id"] = project_details.get("task_id", "")
                    self.reanalysis_required[0]["project_name"] = project_details.get("project_name", "")
                    self.reanalysis_required[0]["last_updated_at"] = project_details.get("updated_at", "")
                self.metadata["database_index"] = "84lumber" + "-" + database

            else :
                self.throw_error_response = True
                if(userSelectedProjects): 
                    self.multiple_dbs = [
                                    f"84lumber-{project['databaseIndex'].strip()}" 
                                    for project in userSelectedProjects 
                                    if project.get('databaseIndex') and project['databaseIndex'].strip()
                    ]
                    if self.date_for_updated_data: 
                        for date in userSelectedProjects:
                            if date["updated_at"] and date["updated_at"] < self.date_for_updated_data:
                                reanalysis_data = {}
                                reanalysis_data["is_needed"] = True
                                reanalysis_data["task_id"] = date.get("taskId", "")
                                reanalysis_data["project_name"] = date.get("project_name", "")
                                reanalysis_data["last_updated_at"] = date.get("updated_at", "")
                                self.reanalysis_required.append(reanalysis_data)
                                
                    self.multiple_projects = True
            
        logging.info(f"Metadata: {self.metadata}")
            

        data["hide_headers"] = not self.show_headers

        retriever_config = config["tools"][DocRetrievalTool.__name__]
        retriever_config = process_config(config=retriever_config, sub_level="integrations")
        retriever_config["database_index"] = self.metadata.get("database_index", retriever_config["database_index"])

        response_tool_config = config["tools"][ContextAwareResponseTool.__name__]
        response_tool_config = process_config(config=response_tool_config, sub_level="integrations")

        if self.alter_query:
            query_alteration_tool_config = config["tools"][QueryAlterationTool.__name__]
            query_alteration_tool_config = process_config(config=query_alteration_tool_config, sub_level="integrations")
            self.query_alteration_tool = QueryAlterationTool()
            self.query_alteration_tool.setup(config=query_alteration_tool_config, data=data)

        self.retriever_tool = DocRetrievalTool()
        retriever_config['agent_id'] = str(agent_id)
        guide_data = data.get("agent_settings", {})
        retriever_config[DisplayFormat.GUIDE_EMBEDDING_COL_NAME.value] = guide_data.get(DisplayFormat.GUIDE_EMBEDDING_COL_NAME.value, None)
        retriever_config[DisplayFormat.GUIDE_EMBEDDING_MODEL_NAME.value] = guide_data.get(DisplayFormat.GUIDE_EMBEDDING_MODEL_NAME.value, None)
        self.retriever_tool.setup(config=retriever_config, data=data)

        self.response_tool = ContextAwareResponseTool()
        self.response_tool.setup(config=response_tool_config, data=data)
    
    @AITrace(AskModRAGAgentTrace)
    def run(
            self,
            user_query: Annotated[
                str, "User query/request for which appropriate documents have to be retrieved, and a response needs to be generated."],
            knowledge_base: Annotated[Optional[str], "Unique identifier for the Knowledge base to be used for the agent mentioned by user"] = None,
            next_trace=None,
            trace=None
    ) -> str:
        """
        This tool retrieves appropriate documents related to the user query from a particular database, and generates a response based on the retrieved documents.      
        """

        logging.info("Running PA Agent")
        logging.info(f"User Query: {user_query}")
        logging.info(f"Knowledge Base: {knowledge_base}")

        if self.is_cw_flow:
            knowledge_base = None
        #     if self.metadata.get("github_token", None) is None:
        #         return "Authenticate with Github before proceeding with Appmod Mode.", [], []
            
            
        if self.throw_error_response and not self.multiple_projects:
            return "No project was selected, can you select a codebase to proceed.", [], []
            
            # url = f"{self.project_information_endpoint}?user_id={self.current_user_id}"
            # response = requests.get(url)
            # if response.status_code == 200:
            #     logging.info("Trying to get project information")
            #     data = response.json()["data"]
            #     query_embedding = self.embedding_model.encode(user_query, convert_to_tensor=True)

            #     results= []

            #     for project_data in data:
            #         # Concatenate relevant fields
            #         combined_text = f"{project_data['updated_executive_summary']} {project_data['feature_summary']}"
            #         doc_embedding = self.embedding_model.encode(combined_text, convert_to_tensor=True)

            #         # Compute cosine similarity
            #         similarity_score = cosine_similarity(query_embedding.reshape(1, -1), doc_embedding.reshape(1, -1))[0][0]
            #         results.append((project_data["name"], similarity_score, project_data["task_id"]))

            #     # top_projects = sorted([r for r in results if r[1] >= self.project_extraction_threshold], key=lambda x: x[1], reverse=True)[:self.project_extraction_limit]
            #     top_projects = sorted(results, key=lambda x: x[1], reverse=True)[:self.project_extraction_limit]

            #     if len(top_projects) > 0:
            #         result_response = "You haven't selected a project, but your query can be catered by one of the following projects: \n"

            #         result_response += "<codeprojectlist>"

            #         for i in range(len(top_projects)):
            #             result_response += f"<code_project><project_name>{i+1}. {top_projects[i][0]}</project_name><task_id>{top_projects[i][2]}</task_id></code_project>"

            #         result_response += "</codeprojectlist>"
            #         result_response += "Please select one of the projects to proceed."
            #     else:
            #         result_response = "No projects found that match your query. Please select a project to proceed."

            #     result_documents = []
            #     result_citations = []
            #     logging.info(f"Returning values:, {result_response}")

            #     return result_response, result_documents, result_citations
            # else:
            #     return "No project was selected, can you select a codebase to proceed.", [], []


        self.user_query = user_query if self.smart_user_prompt_adjustment else self.input_user_question
        logging.info(f"User query: {self.user_query} \t Smart User Prompt Adjustment: {self.smart_user_prompt_adjustment}")
        if self.alter_query:
            self.user_query = self.query_alteration_tool.run(query=user_query, trace=trace, next_trace=next_trace)

        if knowledge_base : 
            original_database_index = self.retriever_tool.database_index
            self.retriever_tool.database_index = knowledge_base
            
            # Also update the doc_feedback_database_index
            original_doc_feedback_database_index = self.retriever_tool.doc_feedback_database_index
            self.retriever_tool.doc_feedback_database_index = f"{knowledge_base}-agentGuide"
            
            # Update the query templates with the new database_index
            self.retriever_tool.alloydb_query_template = self.retriever_tool.alloydb_query_template.replace(
                f'"{original_database_index}"', f'"{knowledge_base}"')
            
            self.retriever_tool.alloydb_documents_feedback_table_existance_query = self.retriever_tool.alloydb_documents_feedback_table_existance_query.replace(
                f"'{original_doc_feedback_database_index}'", f"'{self.retriever_tool.doc_feedback_database_index}'")
                
            self.retriever_tool.alloydb_document_feedback_query_template = self.retriever_tool.alloydb_document_feedback_query_template.replace(
                f'"{original_doc_feedback_database_index}"', f'"{self.retriever_tool.doc_feedback_database_index}"')
                
            self.retriever_tool.alloydb_associated_docs_retrieval_query_template = self.retriever_tool.alloydb_associated_docs_retrieval_query_template.replace(
                f'"{original_database_index}"', f'"{knowledge_base}"')
                
            self.retriever_tool.alloydb_remaining_retrieval_after_dissociation = self.retriever_tool.alloydb_remaining_retrieval_after_dissociation.replace(
                f'"{original_database_index}"', f'"{knowledge_base}"')

        if self.multiple_projects:
            self.retriever_tool.table_names = self.multiple_dbs
            results: list[dict] = self.retriever_tool.run(query=self.user_query, next_trace=next_trace, trace=trace, mutiple_search=self.multiple_projects)
        else:
            results: list[dict] = self.retriever_tool.run(query=self.user_query, next_trace=next_trace, trace=trace)

 
        if self.executive_summary_endpoint and (self.retriever_tool.database_index or (self.multiple_projects and self.retriever_tool.table_names)):
            try:
                # Call the executive summary API with database_index as a parameter
                logging.info(f"Calling executive summary API with database_index: {knowledge_base}")
                logging.info(f"Executive Summary Endpoint: {self.executive_summary_endpoint}")
                searched_database_index = [self.retriever_tool.database_index] if not self.multiple_projects else self.retriever_tool.table_names
                print(searched_database_index)
                summary = ""
                for db_index in searched_database_index:
                    response = requests.get(f"{self.executive_summary_endpoint}", params={"database_index": db_index}, timeout=15)
                    if response.status_code == 200:
                        summary += response.json().get("summary", "") + '\n'
                    else:
                        logging.info(f"Failed to get executive summary. Status code: {response.status_code}")

                print(f"Executive Summary: {summary}")

                if summary:
                    executive_summary = self.response_tool.context.replace('<summary_place_holder>', summary)
                    current_date = datetime.datetime.now()
                    date_prompt = f"## date_today: {current_date.strftime('%Y-%m-%d')}"
                    self.response_tool.context = date_prompt + executive_summary
                    if self.response_tool.add_citations_in_response:
                        self.response_tool.context+=self.response_tool.add_citation_prompt
                    
                    if self.response_tool.fallback_response:
                        # Adding fallback response in system prompt
                        self.response_tool.context += f"\n\n## Fallback Response\nIn case the query is outside of your current capabilities or outisde of the provided context answer with - '{self.fallback_response}'"

                    if self.response_tool.contact_options:
                        # Adding contact options in system prompt
                        self.response_tool.context += f"\n\n## Contact Options\nYou must use these contact options whenever asked for - '{self.response_tool.contact_options}'"

                    if self.response_tool.boundaries:
                        # Adding Boundaries in system prompt
                        self.response_tool.context += f"\n\n## Boundaries\nYou must always follow these proivded bounderies and answer accordingly:\n'{self.response_tool.boundaries}'"

                    if self.response_tool.addtional_info:
                        # Adding Additional Information in system prompt
                        self.response_tool.context += f"\n\n## Additional Information\n'{self.response_tool.addtional_info}'"

                    if hasattr(self.response_tool, 'llm_integration'):
                        self.response_tool.llm_integration.context = self.response_tool.context
                    logging.info(f"Executive Summary: {executive_summary}")
            
            except requests.exceptions.Timeout:
                logging.info("Executive summary API request timed out after 15 seconds.")
            except Exception as e:
                logging.info(f"Error calling executive summary API: {str(e)}")


        if self.citation_format==CitationTypes.hc_book_order:
            citations = [
                citation_generator(
                    agent_name="RAGAgent",
                    title=item.get("book_title","book_title"),
                    description=item.get("book_description","book_description"),
                    url=item.get("book_url","book_url"),
                    displayFormat="BookPurchaseCard",
                    filter=item.get("filter"),
                    customMetaData=item
                ) 
                for item in results
            ]
        elif self.citation_format==CitationTypes.hc_book:
            citations = [
                citation_generator(
                    agent_name="RAGAgent",
                    title=item.get("displayName","book_title"),
                    description=item.get("text","text"),
                    url=item.get("webViewLink","url"),
                    displayFormat="BookCitationCard",
                    filter=item.get("filter"),
                    customMetaData=item
                ) 
                for item in results
            ]
        else:
            citations = [
                citation_generator(
                    agent_name="RAGAgent",
                    title=f"{item.get('filename', item.get('docName', 'filename'))}",
                    description=item.get("text","text"),
                    url=item.get("webViewLink", item.get('webUrl', 'www.techolution.com')),
                    filter=item.get("filter"),
                    customMetaData=update_dict(item, self.metadata)
                ) 
                for item in results
            ]

        self.citations = citations
        filtered_results = []
        
        filtered_items = []
        for item in results:
            filtered_items = item.copy()
            # filter out the keys that are not needed
            filter_keys = set([
                "similarity_score",
                "pinecone_embeddingId",
                "webUrl",
                "rlef_resourceId",
                "isSyncEnabled",
                "modifiedTime",
                "conditions_whentoapply",
                "conditions_whennottoapply"
            ])

            if filtered_items.get('displayFormat') == "ecomm_nutrition_card":
                filter_keys.update([
                    "product_ingredients", 
                    "product_how_to_use", 
                    "product_details", 
                    "product_name"
                ])
                temp_flavours = []
                for flavours in filtered_items.get('product_flavours', []):
                    if flavours.get("detail", {}).get("Name", "") != filtered_items.get('filename', ""):
                        temp_flavours.append({key: value for key, value in flavours.items() if key != "detail"})
                
                filtered_items['product_flavours'] = temp_flavours

            elif filtered_items.get('displayFormat') == "apria_card":
                filter_keys = ["text"]

            elif item.get('paragraphChunks'):
                filter_keys.update([
                    "paragraphChunks"
                ])

            filtered_results.append(
                {
                    key: value for key, value in filtered_items.items() 
                    if key not in filter_keys
                }
            )
        
        if not self.generate_response_from_docs:
            retrieved_documents = "\n\n".join(
                    [f"^^CONTEXT STARTS^^\n{doc}\n^^CONTEXT ENDS^^" for doc in filtered_results])
                
            return f"\n\n## CONTEXT:\n{retrieved_documents}", results, citations
        
        self.filtered_results = filtered_results
        
        try:
            logging.info(f"Documents already present in the ContextAwareResponseTool:\n{self.response_tool.retrieved_documents}")
            self.response_tool.retrieved_documents.extend(filtered_results)
            response = self.response_tool.run(query=self.user_query, next_trace=next_trace, trace=trace)
            logging.info(f"RAG agent response: {response}")
            self.response = response
            filtered_citations = (tuple(filter(
                                    lambda x: (citation_url := x.get("filter") or x.get("url")) and 
                                            (partial_ratio(citation_url, response) > self.filter_citation_threshold), 
                                    citations))
                                ) if self.filter_citations else citations
            
            self.response = self.add_backticks_preprocessing(self.response)
            self.response = self.process_citations(self.response)
            self.response = self.remove_code_block_backticks(self.response)

            if self.is_cw_flow and self.reanalysis_required:
                response_to_user = self.reanalysis_required_message
                for reanalysis in self.reanalysis_required:
                    if reanalysis["is_needed"]:
                        response_to_user += f"\n Project: **{reanalysis['project_name']}** \n <reanalysislink>{self.link_for_old_appmod}/user-mode/project-analysis/{reanalysis['task_id']}/update?task_id={reanalysis['task_id']}&project_name={reanalysis['project_name']}</reanalysislink>"
                self.response = self.response + "\n\n" + response_to_user  

            return self.response, results, filtered_citations
        
        except Exception as e:
            self.error = f"Error while calling ContextAwareResponseTool: {str(e)}"
            logging.error(self.error)
            raise Exception(self.fallback_response)
        
    def remove_code_block_backticks(self, text):
        # Pattern to match ```language content```
        pattern = r'^```\w*\s*(.*?)```$'
        
        # Use DOTALL flag to match newlines with .*
        match = re.match(pattern, text.strip(), re.DOTALL)
        
        if match:
            return match.group(1).strip()
        else:
            return text

    def get_file_content(self, file_path):
        final_result = ""
        for result in self.filtered_results:
            if result.get("filename") == file_path:
                final_result += result.get("text")
        return final_result
            
    def validate_citation(self, file_content, file_path, function_name):
        file_name = file_path.split("/")[-1]

        if file_name == function_name:
            return True
        
        if function_name in file_content:
            return True
        
        return False
    
    def add_backticks_preprocessing(self, text):
        # Pattern to match citations WITHOUT backticks: [functionName()](filePath?params)
        no_backticks_pattern = r'\[([^`\]]+)\]\(([^)]+)\)'
        
        def add_backticks_replacer(match):
            function_name = match.group(1)
            url_part = match.group(2)
            return f'[`{function_name}()`]({url_part})'
        
        return re.sub(no_backticks_pattern, add_backticks_replacer, text)
        
    def process_citations(self, text_chunk):
        
        # Regex pattern to match [`functionName()`](filePath?startLine=X&endLine=Y)
        citation_pattern = r'\[`([^`]+)`\]\(([^?)]+)(?:\?(?:.*?startLine=(\d+))?(?:.*?endLine=(\d+))?.*?)?\)'
        
        def replace_citation(match):
            function_name = match.group(1)
            file_path = match.group(2)
            start_line = match.group(3)
            end_line = match.group(4)
            
            try:
                # Get file content
                file_content = self.get_file_content(file_path)
                
                # Validate citation
                is_valid = self.validate_citation(file_content, file_path, function_name)
                
                validation_param = f"&validation={str(is_valid)}"
                if start_line and end_line:
                    updated_citation = f"[`{function_name}`]({file_path}?startLine={start_line}&endLine={end_line}{validation_param})"
                else:
                    updated_citation = f"[`{function_name}`]({file_path}?validation={str(is_valid).lower()})"

                return updated_citation
                
            except Exception as e:
                # If there's an error, add validation=False
                validation_param = "&validation=False"
                updated_citation = f"[`{function_name}()`]({file_path}?startLine={start_line}&endLine={end_line}{validation_param})"
                return updated_citation
        
        # Replace all citations in the text
        updated_text = re.sub(citation_pattern, replace_citation, text_chunk)
    
        return updated_text
    
    @classmethod
    def get_setup_config(cls) -> dict:
        return accumulator(
            AskModRAGAgentSetup,
            cls.__name__,
            [
                DocRetrievalTool,
                ContextAwareResponseTool,
                QueryAlterationTool
            ],
            "tools"
        )




