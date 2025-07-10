import io
import json
import os
import pandas as pd
import requests
import ai_suggestions_promptexamples
import config
from crashanalytics import log_into_bigquery
from helper_func import project_exec_call
from models import storage_client, project_summary_db
from logger import info_logger, error_logger
import traceback
from tenacity import retry, stop_after_attempt, wait_exponential
from analyser_repo_pipeline.alloydb_helpers import AlloyDBConnector

from repo_services_2 import extract_response_from_delimeters
from analysis_status_update import update_step_status
from utils.analysis_status_enums import StepStatus, PossibleStatuses


class AlloyDBTableManager:
    def __init__(self, table_name):
        self.table_name = table_name

    def create_table(self):
        try:
            alloydb_connector = AlloyDBConnector(
                database=config.EGPT_ALLOY_DB_NAME,
                username=config.EGPT_ALLOY_DB_USERNAME,
                password=config.EGPT_ALLOY_DB_PASSWORD,
                host=config.EGPT_ALLOY_DB_HOST,
                port=config.EGPT_ALLOY_DB_PORT
            )
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS "{self.table_name}" (
                    id UUID PRIMARY KEY,
                    openai_embedding    VECTOR(1536),
                    st_embedding        VECTOR(768),
                    googleai_embedding  VECTOR(768),
                    metadata JSONB,
                    row_summary TEXT,
                    file_path TEXT
                );
            """
            alloydb_connector.run(query=create_table_query, query_type="ddl")
            info_logger.info(f"Table {self.table_name} created successfully.")
        except Exception as e:
            error_logger.error(f"Error creating table {self.table_name}: {traceback.format_exc()}")
            raise e

    def save_mapping(self, mapping, task_id, single_row=False):
        try:
            alloydb_connector = AlloyDBConnector(
                database=config.EGPT_ALLOY_DB_NAME,
                username=config.EGPT_ALLOY_DB_USERNAME,
                password=config.EGPT_ALLOY_DB_PASSWORD,
                host=config.EGPT_ALLOY_DB_HOST,
                port=config.EGPT_ALLOY_DB_PORT
            )

            if single_row:
                # For single row, use regular parameterized query
                upsert_query = f"""
                    INSERT INTO "{self.table_name}" (id, openai_embedding, st_embedding, googleai_embedding, metadata, row_summary, file_path)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        openai_embedding = EXCLUDED.openai_embedding,
                        st_embedding = EXCLUDED.st_embedding,
                        googleai_embedding = EXCLUDED.googleai_embedding,
                        metadata = EXCLUDED.metadata,
                        row_summary = EXCLUDED.row_summary,
                        file_path = EXCLUDED.file_path
                """
                info_logger.info(f"Upserting single row into table {self.table_name}")
                alloydb_connector.bulk_insert = False
                alloydb_connector.run(
                    query=upsert_query,
                    insertion_data=mapping,
                    query_type="insert"
                )
            else:
                # For bulk insert, use execute_values compatible query
                upsert_query_template = f"""
                    INSERT INTO "{self.table_name}" (id, openai_embedding, st_embedding, googleai_embedding, metadata, row_summary, file_path)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        openai_embedding = EXCLUDED.openai_embedding,
                        st_embedding = EXCLUDED.st_embedding,
                        googleai_embedding = EXCLUDED.googleai_embedding,
                        metadata = EXCLUDED.metadata,
                        row_summary = EXCLUDED.row_summary,
                        file_path = EXCLUDED.file_path
                """
                info_logger.info(f"Upserting multiple rows into table {self.table_name}")
                alloydb_connector.bulk_insert = True
                alloydb_connector.run(
                    query=upsert_query_template,
                    insertion_data=mapping,
                    query_type="insert"
                )

            info_logger.info(f"Upsertion completed in table {self.table_name}")
            return len(mapping) if not single_row else 1
        except Exception as e:
            error_logger.error(
                f"Error upserting data to the table {self.table_name} with error: {traceback.format_exc()}")
            return 0

    def alter_existing_table_to_uuid(self):
        """
        Helper method to alter existing table to use UUID instead of SERIAL
        WARNING: This will drop the existing table and recreate it!
        """
        try:
            alloydb_connector = AlloyDBConnector(
                database=config.EGPT_ALLOY_DB_NAME,
                username=config.EGPT_ALLOY_DB_USERNAME,
                password=config.EGPT_ALLOY_DB_PASSWORD,
                host=config.EGPT_ALLOY_DB_HOST,
                port=config.EGPT_ALLOY_DB_PORT
            )

            # Drop existing table and recreate with UUID
            alter_query = f"""
                DROP TABLE IF EXISTS "{self.table_name}";
                CREATE TABLE "{self.table_name}" (
                    id UUID PRIMARY KEY,
                    openai_embedding    VECTOR(1536),
                    st_embedding        VECTOR(768),
                    googleai_embedding  VECTOR(768),
                    metadata JSONB,
                    row_summary TEXT,
                    file_path TEXT
                );
            """
            alloydb_connector.run(query=alter_query, query_type="ddl")
            info_logger.info(f"Table {self.table_name} altered to use UUID primary key.")
        except Exception as e:
            error_logger.error(f"Error altering table {self.table_name}: {traceback.format_exc()}")
            raise e

    def update_deleted_file_on_alloydb(self,deleted_file_paths):
        try:
            alloydb_connector = AlloyDBConnector(
                database=config.EGPT_ALLOY_DB_NAME,
                username=config.EGPT_ALLOY_DB_USERNAME,
                password=config.EGPT_ALLOY_DB_PASSWORD,
                host=config.EGPT_ALLOY_DB_HOST,
                port=config.EGPT_ALLOY_DB_PORT
            )

            # Handle single ID to ensure SQL syntax correctness
            if len(deleted_file_paths) == 1:
                id_tuple = f"('{deleted_file_paths[0]}')"
            else:
                id_tuple = tuple(deleted_file_paths)

            delete_query = f"""
                DELETE FROM "{self.table_name}"
                WHERE file_path IN {id_tuple};
            """
            info_logger.info(f"Deleting rows with embed_ids {deleted_file_paths} from the table {self.table_name}")

            alloydb_connector.run(delete_query, query_type="delete")  # Using run from alloydb_helpers

            info_logger.info("Deletion Completed")
            return len(deleted_file_paths)

        except Exception as e:
            error_logger.error(f"Error deleting data from the table: {traceback.format_exc()}")
            return 0