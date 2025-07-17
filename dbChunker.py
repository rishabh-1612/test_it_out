import os
import sys
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Uncomment to run locally
import json
import Config
import logging
import traceback
import tiktoken

from typing import List, Dict, Any
from data_extraction.chunkerBase import ChunkerBase

from data_extraction.Enums.github_repo_enums import TaskObject
from Enums.model_type import openai_model_name
from data_extraction.utils.token_utils import TokenUtils


class DBChunker(ChunkerBase):
    def __init__(
            self,
            maxChunkLength: int = Config.PA_DB_SUMMARY_MAX_CHUNK_SIZE,
    ) -> None:
        self.maxChunkLength = maxChunkLength
        self.totalLLMCalls = 0 
        self.totalCost = 0.0

    
    def _preprocess(self, DbFileString):
        DbFileString = DbFileString.strip()

        return json.loads(DbFileString)
    

    def chunkData(
        self,
        DbFileString: str, 
        DbFileName: str
    ) -> List[TaskObject]:

        if not isinstance(DbFileString, str) :
            raise ValueError("DbFileList must be a list of strings.")

        try :
            dbChunks = []
            currentChunk = []
            currentTokenCount = 0

            DbFile = self._preprocess(DbFileString)

            ddl_info = DbFile["ddl"]
            logging.info(f"Chunk size got {self.maxChunkLength} tokens")
            if "\n" in ddl_info:
                db_columns = ddl_info.split("\n")            
            else:
                db_columns = ddl_info.split(",")

            # Store original line numbers for each column
            column_line_mapping = []
            
            for line_num, col in enumerate(db_columns, 1):
                col = col.strip()
                if col:  # Skip empty lines
                    column_line_mapping.append((line_num, col))

            for line_num, col in column_line_mapping:
                col_tokens = TokenUtils.count_tokens_using_openai(col)

                if col_tokens > self.maxChunkLength:
                    if currentChunk:
                        dbChunks.append(currentChunk)
                        currentChunk = []
                        currentTokenCount = 0
                    dbChunks.append([(line_num, col)])
                    continue

                if currentTokenCount + col_tokens <= self.maxChunkLength:
                    currentChunk.append((line_num, col))
                    currentTokenCount += col_tokens
                else:
                    dbChunks.append(currentChunk)
                    currentChunk = [(line_num, col)]
                    currentTokenCount = col_tokens

            if currentChunk:
                dbChunks.append(currentChunk)

            # Convert chunks to strings with line numbers
            for i, chunk in enumerate(dbChunks):
                chunk_with_line_numbers = []
                for line_num, col_content in chunk:
                    chunk_with_line_numbers.append(f"{line_num} {col_content}")
                dbChunks[i] = "\n".join(chunk_with_line_numbers)

            # dbChunks is a list of Strings -> each string under 500 tokens with line numbers.

            return dbChunks, self.totalCost, self.totalLLMCalls

        except Exception as e:
            logging.error(f"Error chunking code: {e} Traceback: {traceback.format_exc()}.\nContinuing Execution without Chunking ...")
            return [DbFileString], self.totalCost, self.totalLLMCalls

