import os
from dotenv import load_dotenv
import logging

if "APPMOD_MONGODB" not in os.environ and "APPMOD_MONGODB_CLIENT" not in os.environ:
    logging.info("Using data extraction specific local ENVs.")
    load_dotenv("data_extraction/data_extraction.env", override=False)

APPMOD_MONGODB = os.environ.get("APPMOD_MONGODB")
APPMOD_MONGODB_CLIENT = os.environ.get("APPMOD_MONGODB_CLIENT")

WORKER_COUNT = int(os.environ.get("WORKER_COUNT", 3))
WORKER_THREADS_COUNT = int(os.environ.get("WORKER_THREADS_COUNT", 1))
WORKER_TIMEOUT = int(os.environ.get("WORKER_TIMEOUT", 10800))
WORKER_GRACEFUL_TIMEOUT = int(os.environ.get("WORKER_GRACEFUL_TIMEOUT", 3600))