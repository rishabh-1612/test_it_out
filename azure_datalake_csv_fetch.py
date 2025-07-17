from azure.storage.filedatalake import DataLakeServiceClient
import csv
import time
import Config
import logging


def get_data_lake_service_client(account_name, account_key):
    try:
        service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net/",
                                               credential=account_key)
        return service_client
    except Exception as e:
        logging.info(f"Error creating Data Lake Service Client: {e}")
        return None


def extract_names_from_filename(file_name):
    base_name = file_name.split('.')
    if len(base_name) > 1:
        base_name = base_name[0]
    if base_name:
        return base_name
    else:
        return "Unknown Spreadsheet"


service_client = get_data_lake_service_client(Config.DATA_LAKE_ACCOUNT_NAME, Config.DATA_LAKE_ACCOUNT_KEY)


def csv_content_to_json(csv_content, spreadsheet_name):
    start_time = time.time()
    csv_reader = csv.DictReader(csv_content.splitlines())
    json_list = []

    for row in csv_reader:
        json_obj = {}
        for header, value in row.items():
            keys = header.split('.')
            current_level = json_obj
            for key in keys[:-1]:
                if key not in current_level:
                    current_level[key] = {}
                current_level = current_level[key]
            current_level[keys[-1]] = value
        json_list.append(json_obj)

    response_data = {
        "data": [
            {
                "spreadsheet_name": spreadsheet_name,
                "spreadsheet_data": {
                    "0": {
                        "subsheet_data": {
                            "0": json_list
                        }
                    }
                }
            }
        ],
        "time_taken": time.time() - start_time
    }

    return response_data
