import time
import traceback
import uuid
import os
import vertexai
from pymongo.synchronous.collection import Collection
from vertexai.language_models import TextEmbeddingModel
from langchain_community.utils.math import cosine_similarity
from langchain_openai import AzureOpenAIEmbeddings
import openai

import config
from logger import error_logger, info_logger
from models import ecg_db

AZURE_API_TYPE=config.AZURE_API_TYPE
AZURE_API_BASE=config.AZURE_API_BASE
AZURE_API_VERSION=config.AZURE_API_VERSION
AZURE_OPENAI_API_KEY=config.AZURE_OPENAI_API_KEY

class Embeddings:
    def __init__(self):
        print("Initializing EmbeddingManager...")
        vertexai.init(project=config.LLM_PROJECT_ID, location="us-central1")
        try:
            self.google_embed_model = TextEmbeddingModel.from_pretrained("text-embedding-005")
        except Exception as e:
            error_logger.error(f"Failed to load embedding model: {e}")
            raise e

        try:
            # Validate Azure OpenAI credentials before initializing
            if not AZURE_OPENAI_API_KEY:
                raise ValueError("AZURE_OPENAI_API_KEY is not set in config")
            if not AZURE_API_BASE:
                raise ValueError("AZURE_API_BASE is not set in config")

            self.embeddings = AzureOpenAIEmbeddings(
                model="text-embedding-ada-002",
                azure_endpoint=AZURE_API_BASE,
                azure_deployment="Embedding",
                chunk_size=1,
                openai_api_version=AZURE_API_VERSION,
                api_key=AZURE_OPENAI_API_KEY,
            )
        except Exception as e:
            traceback.print_exc()
            error_logger.error(f"Failed to initialize Azure OpenAI embeddings: {e}")
            raise e

    def generate_google_embeddings(self, items, fields_for_embedding):
        print("Generating embeddings...")
        embeddings = []
        max_retries = 3
        while max_retries > 0:
            try:
                for item in items:
                    text = "\n".join([f"{field}: {item.get(field, '')}" for field in fields_for_embedding])
                    embedding = self.google_embed_model.get_embeddings([text])[0].values
                    embeddings.append(embedding)
                break  # Exit the loop if successful
            except Exception as e:
                traceback.print_exc()
                error_logger.error(f"Error while generating embeddings: {e}")
                max_retries -= 1
                if max_retries > 0:
                    time.sleep(10)  # Wait before retrying
                else:
                    raise e  # Re-raise the exception if retries are exhausted

        return embeddings

    def azure_Embedding(self,query):
        query = str(query)
        query_embedding = self.embeddings.embed_query(query)
        return query_embedding
