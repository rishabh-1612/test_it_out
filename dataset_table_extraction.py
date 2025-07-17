import logging
import traceback
from utils.utils import exception_handling


# Fetch table relations based on pre-fetched column names and datatypes
def get_table_relations(client, project_id: str, dataset_id: str, table_name: str) -> list:
    """
    Identify potential relationships (based on column name and datatype) between
    the given table and other tables in the dataset.

    Args:
        client: BigQuery client for executing queries.
        project_id: The project ID in GCP.
        dataset_id: The dataset to search for relationships.
        table_name: The table name to check for relationships.

    Returns:
        list: List of tables with potential foreign key relationships.
    """
    relations = []

    try:
        # Query to get foreign key constraints for the table
        constraints_query = f"""
        SELECT
            constraint_name, table_name, constraint_type
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS`
        WHERE table_name = '{table_name}' AND constraint_type = 'FOREIGN KEY';
        """

        logging.info(f"Fetching foreign key constraints for table {table_name} with query: {constraints_query}")
        constraints_result = client.query(constraints_query).result()

        for constraint in constraints_result:
            constraint_name = constraint.constraint_name

            # Now fetch the columns involved in this foreign key constraint from KEY_COLUMN_USAGE
            key_column_query = f"""
            SELECT
                table_name, column_name, referenced_table_name, referenced_column_name
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
            WHERE constraint_name = '{constraint_name}';
            """

            logging.info(f"Fetching key column usage for constraint {constraint_name} with query: {key_column_query}")
            key_column_result = client.query(key_column_query).result()

            for row in key_column_result:
                referenced_table = row.referenced_table_name
                referenced_column = row.referenced_column_name
                current_column = row.column_name

                if referenced_table not in relations:
                    relations[referenced_table] = {"columns": []}

                # Append the column relationship (current_table.current_column -> referenced_table.referenced_column)
                relations[referenced_table]["columns"].append({
                    f"{table_name}.{current_column}": f"{referenced_table}.{referenced_column}"
                })

    except Exception as e:
        exception_handling(
            "Exception while fetching table relations",
            {"dataset_id": dataset_id, "table_name": table_name},
            e, traceback.format_exc()
        )
        return relations

    return relations


# Fetch Table Name from the google dataset if Exists
def table_exists(client, project_id: str, dataset_id: str, table_name: str) -> any:
    """ Return True if the table name exists in the google dataset"""
    try:
        table_check_query = f"""
        SELECT table_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = '{table_name}';
        """

        results = client.query(table_check_query).result()
    except Exception as e:
        exception_handling("Exception while fetching table name for google bigquery",
                           {"dataset_id": dataset_id, "table_name": table_name}, e,
                           traceback.print_exc())

    return any(row.table_name == table_name for row in results)


# Function to get metadata of a specific table along with column names and datatypes
def get_table_metadata(client, project_id: str, dataset_id: str, table_name: str, number_of_rows: int = 2) -> dict:
    """Get the metadata from google bigquery of a specific table 
    along with column names and datatypes and custom number of rows

    Args:
        client: BigQuery client for executing queries.
        project_id: The project ID in GCP.
        dataset_id: dataset to fetch metadata from.
        table_name: table name to fetch metadata
        number_of_rows: number of rows to fetch metadata from a table.

    Result:
        metadata: given table metadata with custom number of rows and its datatypes and relations"""

    metadata = {}

    # Query to get column names and data types from the specific table
    column_query = f"""
    SELECT column_name, data_type
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}';
    """
    logging.info("query built for given table metadata request" + column_query)

    try:

        columns_result = client.query(column_query).result()
        columns = [{"name": col.column_name, "type": col.data_type} for col in columns_result]

        # Query to get the first 4 records from the table
        records_query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}` LIMIT {number_of_rows}"
        records = client.query(records_query).result()
        rows = [dict(record.items()) for record in records]

        if not rows:
            logging.info(f"No records found for given table: {table_name} metadata request")

            return None

        # Fetch potential relationships using pre-fetched columns
        relations = get_table_relations(client, project_id, dataset_id, table_name)
        # Check if no relations are found
        if not relations:
            logging.info(f"No relationships found for table {table_name}.")
            relations = {}  # Return an empty dict for relations if none found

    except Exception as e:
        exception_handling("Exception while querying table metadata",
                           {"dataset_id": dataset_id, "table_name": table_name}, e,
                           traceback.print_exc())

    # Build metadata structure
    metadata = {
        "content_type": {
            "table_name": table_name,
            "type": "table",
            "dataset": dataset_id,
            "columns": columns,  # Include column names and data types
            "records": rows,  # Include the first 4 records
            "relations": relations  # Include the related tables
        }
    }

    return metadata


# Function to get all data from the Tables of given BigQuery Dataset
def get_bigquery_tables_data(client, dataset_id, fetch_only_table_names=False):
    """
    Retrieves data from all tables in a specified BigQuery dataset.
    Iterates over each table in the dataset, fetches all rows and columns using
    a BigQuery SQL query, and returns the results in a structured dictionary format.
    To handle large datasets, the function retrieves rows in pages to reduce memory usage.
    """
    dataset_ref = client.dataset(dataset_id)
    tables = client.list_tables(dataset_ref)

    if fetch_only_table_names:
        try:
            table_names = [f"{table.table_id}" for table in tables]
        except Exception as e:
            table_names = []
            exception_handling("Exception in Big Query Extraction to get table names:", {"dataset_ref": dataset_ref}, e,
                               traceback.format_exc())
        return table_names

    tables_data_dict = {}

    for table in tables:
        try:
            table_ref = f"{dataset_id}.{table.table_id}"
            table_info = client.get_table(table_ref)
            columns = [schema_field.name for schema_field in table_info.schema]

            tables_data_dict[table.table_id] = {column: [] for column in columns}

            query = f"SELECT * FROM `{table_ref}`"
            query_job = client.query(query)

            rows_iter = query_job.result(page_size=1000)  # Adjust page_size as needed
            for row in rows_iter:
                for column in columns:
                    if column not in tables_data_dict[table.table_id]:
                        tables_data_dict[table.table_id][column] = []
                    tables_data_dict[table.table_id][column].append(row[column])

        except Exception as e:
            exception_handling("Exception in Big Query Extraction:", {"table_ref": table_ref}, e,
                               traceback.format_exc())
            continue

    return tables_data_dict
