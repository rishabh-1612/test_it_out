import ast
import json
import Config
import logging
import traceback
import tiktoken
import sys
import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Uncomment to run locally
from rapidfuzz import fuzz
from typing import List, Dict, Any
from data_extraction.chunkerBase import ChunkerBase

from data_extraction.utils.github_repo_prompts import Prompts
from helperClasses.GoogleAI import GoogleAI
from data_extraction.Enums.github_repo_enums import TaskObject
from Enums.model_type import google_model_name, google_region, vertex_ai_supported_models_for_token_counting, openai_model_name
from data_extraction.utils.token_utils import TokenUtils
class CodeChunker(ChunkerBase):
    def __init__(
            self,
            maxChunkLength: int = Config.PA_FILE_SUMMARY_MAX_CHUNK_SIZE,
            modelName: str = google_model_name.RESPONSE_GEMINI_2_FLASH.value,
            region: str = google_region.US_CENTRAL1.value
    ) -> None:
        self.maxChunkLength = maxChunkLength
        self.modelName = modelName
        
        if not vertex_ai_supported_models_for_token_counting.has_value(self.modelName):
            self.modelName = google_model_name.RESPONSE_GEMINI_2_FLASH.value
        
        self.region = region
        self.googleai = GoogleAI()
    
    @staticmethod
    def _get_lined_code(code: str) -> str:
        """
        Function to add line numbers in the code string

        Args:
            code: code file content
        
        Returns:
            code file content with line numbers
        """

        line_split_code = code.splitlines(keepends=True)
        line_split_code = [f"{line_number} {code_line}" for line_number, code_line in enumerate(line_split_code, start=1)]
        return ''.join(line_split_code)
    
    @staticmethod
    def _insert_to_chunk_list(chunk_list: list, chunk: str):
        
        # Insert the chunk at the beginning of the list
        if isinstance(chunk, str):
            chunk_list.append(chunk)
        elif isinstance(chunk, list):
            chunk_list.extend(chunk)
        else:
            raise ValueError(f"chunk_list must be a string or a list not type {type(chunk)}")
        return chunk_list
    
    
    
    @staticmethod
    def compute_tokens_using_openai(text):
        try:
            enc = tiktoken.encoding_for_model(openai_model_name.RESPONSE_35_16K.value)
            tokens = enc.encode(text)
            # logging.info(f"Token count using OpenAI: {token}")
            return tokens
        except Exception as e:
            logging.error(f"Error counting tokens: {e}")
            return None

    @staticmethod
    def decode_tokens_using_openai(tokens):
        try:
            enc = tiktoken.encoding_for_model(openai_model_name.RESPONSE_35_16K.value)
            text = enc.decode(tokens)
            # logging.info(f"Token count using OpenAI: {token}")
            return text
        except Exception as e:
            logging.error(f"Error counting tokens: {e}")
            return None


    def getLastCodeDefinition(
        self,
        codeWindow: str, 
        level = 'major',
        chat_history=[]
    ) -> int:
        try:
            response = self.googleai.generate_gemini_tool_response(
                data=None,
                context=Prompts.last_logical_major_code_name_prompt if level == 'major' else Prompts.last_logical_minor_code_name_prompt,
                query=codeWindow,
                chat_history=chat_history,
                model_name=self.modelName,
                stream=False,
                tools=[],
                parameters={
                    "seed": 42,
                    "response_mime_type": "application/json",
                    "response_schema": {"type":"OBJECT","properties":{"code_block_exact_definition_line":{"type":"STRING","description":"The exact code of the last major function definition name at the end of the code snippet. Just the function name without any other text. This should be maximum 10 words only."}}},
                    "top_p": 0.95,
                    "temperature": 0,
                    "response_modalities": ["TEXT"],
                    "max_output_tokens": 8192,
                },
                region=self.region,
            )
            if "error" in response:
                logging.error(f"Error identifying logical breaks: {response['error']}")
                raise Exception(f"Error identifying logical breaks: {response['error']}")
            
            cost: float = response.get("total_tokens_cost", 0.0)
            
            last_definition: str = response.get("content", '')
            try:
                last_definition_out: dict = json.loads(last_definition)
            except json.decoder.JSONDecodeError as e:
                try:
                    logging.warning(f"Json parsing failed for the generated summary: trying ast.literal_val")
                    last_definition_out: dict = ast.literal_eval(last_definition)
                except Exception as e:
                    logging.warning(f"Json parsing failed for the generated summary: {last_definition}")
                    last_definition_out: dict = {
                        "code_block_exact_definition_line": last_definition
                    }
            
            last_definition_out: str = last_definition_out.get("code_block_exact_definition_line", '')

            if not last_definition:
                logging.warning("No logical break found")
                raise ValueError("No logical break found")

            logging.debug(f"Logical break identified: {last_definition}")
            return last_definition_out, cost, last_definition
        except Exception as e:
            logging.error(f"Error identifying logical break: {traceback.format_exc()}")
            return '', 0.0, ''

    @staticmethod
    def fuzzyRFind(text, pattern, threshold=90, max_drops_allowed=2):
        """
        Efficient fuzzy reverse-find using RapidFuzz, with tolerance for slight drop in match quality.

        Args:
            text (str): The string to search within.
            pattern (str): The pattern to search for.
            threshold (int): Minimum similarity score (0–100) to accept a match.
            max_drops_allowed (int): How many declining matches to allow after a best score before breaking.

        Returns:
            int: Index of the best fuzzy match, or -1 if none found.
        """
        best_index = -1
        best_score = -1
        drop_count = 0

        for i in range(len(text) - len(pattern), -1, -1):
            substring = text[i:i + len(pattern)]
            similarity = fuzz.ratio(pattern, substring)  # Change to partial_ratio if needed

            if similarity >= threshold:
                if similarity >= best_score:
                    best_score = similarity
                    best_index = i
                    drop_count = 0  # Reset drop count if we get a better match
                else:
                    drop_count += 1
                    if drop_count > max_drops_allowed:
                        break
            elif best_score >= threshold:
                drop_count += 1
                if drop_count > max_drops_allowed:
                    break

        return best_index

    def validateForLastCodeIndex(
        self, 
        lastCodeBlockIndex: int,
        codeWindow: str,
        lastCodeBlockDefinition: str,
        lastResponse: str
    ):
        # If the code block is not found in code window
        # make another llm call to check for grammar or any other mistake in the generation
        if lastCodeBlockIndex == -1:
            logging.error("Error in getting last code block index, skipping chunk, fixing the text and trying again")
            chat_history = [
                {
                    "role": "user",
                    "content": codeWindow
                },
                {
                    "role": "assistant",
                    "content": lastResponse
                },
            ]
            lastCodeBlockDefinition, cost, _ = self.getLastCodeDefinition(
                codeWindow="The provided code_block_exact_definition_line seems to have some grammatical mistake or is incorrect and not mentioned in the code. Provide the correct response.", 
                level='major', 
                chat_history=chat_history
            )
            self.totalCost += cost
            self.totalLLMCalls += 1

            lastCodeBlockIndex = codeWindow.rfind(lastCodeBlockDefinition)

            # if still not found then set it to 0 to start check for fuzzy match
            if lastCodeBlockIndex == -1:
                lastCodeBlockIndex = 0
        
        # if the last code block index is 0 that means there is no major logical break at the end
        # then check for the minor logical break in the code.
        if lastCodeBlockIndex == 0 or lastCodeBlockIndex == 1:
            logging.error("Error in getting last code block index, doing fuzzy match.")
            
            lastCodeBlockIndex = self.fuzzyRFind(
                text=codeWindow,
                pattern=lastCodeBlockDefinition
            )

            # If even fuzzy match is not found then use the whole chunk itself.
            if lastCodeBlockIndex == -1 or lastCodeBlockIndex == 0 or lastCodeBlockIndex == 1:
                logging.error("Error in getting last code block index, skipping chunk.")
                lastCodeBlockIndex = len(codeWindow)-1
        
        return lastCodeBlockIndex

    def chunkData(
        self,
        codeFileString: str,
        codeFileName: str,
    ) -> List[TaskObject]:
        
        self.totalCost = 0.0
        self.totalLLMCalls = 0
        try:
            logging.debug(f"Chunking file: {codeFileName}")
            logging.debug(f"Model name: {self.modelName}")
            
            # Add line number in the code snippet.
            codeFileString = self._get_lined_code(codeFileString)
            
            # totalTokens, totalTokenIds = self.googleai.gemini_compute_tokens(
            #     context=codeFileString,
            #     model_name=self.modelName, 
            #     region=self.region
            # )
            totalTokenIds = self.compute_tokens_using_openai(codeFileString)
            
            # If no tokens are found
            if not totalTokenIds:
                logging.warning(f"No tokens found in the file {codeFileName}")
                return [codeFileString], self.totalCost, self.totalLLMCalls
            
            tokenCount = len(totalTokenIds)

            # return if file is smaller than or equal to the max chunk length
            if tokenCount <= self.maxChunkLength:
                return [codeFileString], self.totalCost, self.totalLLMCalls

            logging.debug(f"Total tokens in file {codeFileName}: {tokenCount}")
            logging.debug(f"Max chunk size: {self.maxChunkLength}")
            
            lastCodeBlockIndex = 0
            startTokenIndex = 0
            codeChunks = []

            # Try for only 50 + possible ideal scenario of chunk count
            maxLoopCount = (tokenCount // self.maxChunkLength) + 50
            logging.debug(f"Looping for max {maxLoopCount} chunks")

            for _ in range(maxLoopCount):
                endTokenIndex = startTokenIndex + self.maxChunkLength if startTokenIndex + self.maxChunkLength <= tokenCount else tokenCount

                # Get the code token window to process
                codeWindowTokens = totalTokenIds[startTokenIndex : endTokenIndex]
                codeWindow = self.decode_tokens_using_openai(codeWindowTokens)
                # try:
                #     codeWindow = b''.join(codeWindowTokens).decode('utf-8')
                # except UnicodeDecodeError as e:
                #     logging.warning(f"Error reading the file: {traceback.format_exc()}")
                #     codeWindow = b''.join(codeWindowTokens).decode('utf-8f')
                # except Exception as e:
                #     logging.error(f"Something failed while reading the file: {traceback.format_exc()}")
                #     raise e

                # check if reached the end of file content
                if endTokenIndex == tokenCount:
                    logging.info(f"Reached the last chunk, inserting to the code chunks.")
                    codeChunks = self._insert_to_chunk_list(codeChunks, [codeWindow])
                    logging.debug(f"Inserted chunk {len(codeChunks)}")
                    break
                
                # Identify the last logical break in the chunk
                lastCodeBlockDefinition, cost, response = self.getLastCodeDefinition(
                    codeWindow=codeWindow, 
                    level='major'
                )
                self.totalCost += cost
                self.totalLLMCalls += 1
                lastCodeBlockIndex = codeWindow.rfind(lastCodeBlockDefinition)

                lastCodeBlockIndex = self.validateForLastCodeIndex(
                    lastCodeBlockIndex=lastCodeBlockIndex,
                    codeWindow=codeWindow,
                    lastCodeBlockDefinition=lastCodeBlockDefinition,
                    lastResponse=response
                )
                
                logging.debug(f"Start Index: {startTokenIndex}, End Index: {endTokenIndex}, Last Code block index: {lastCodeBlockIndex}")

                # Update the start token index
               
                extractedTokenCount = TokenUtils.count_tokens_using_openai(codeWindow[lastCodeBlockIndex:])
                startTokenIndex = (startTokenIndex + self.maxChunkLength) - extractedTokenCount
                logging.debug(f"New start token index: {startTokenIndex}")

                codeChunks = self._insert_to_chunk_list(
                    chunk_list=codeChunks, 
                    chunk=codeWindow[:lastCodeBlockIndex]
                )
                logging.debug(f"Inserted chunk {len(codeChunks)}")
            
            logging.info(f"Chunked the code file: {codeFileName} into {len(codeChunks)} chunks")
            return codeChunks, self.totalCost, self.totalLLMCalls

        except Exception as e:
            logging.error(f"Error chunking code: {e} Traceback: {traceback.format_exc()}")
            raise e
