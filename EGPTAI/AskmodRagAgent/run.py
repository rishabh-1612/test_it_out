import os
import json
import threading
from flask import Flask, jsonify, request
from google.cloud import pubsub_v1
import logging
import Config
from agents.askmod_rag_agent.askmod_rag_agent import AskModRAGAgent
from utils import utils_agent_pubsub
from utils.utils import process_config, get_setup_details
from flask_cors import cross_origin
import google.cloud.logging
import traceback


logging.getLogger().handlers = []  # FIXME: Comment this before running locally
client = google.cloud.logging.Client()
client.setup_logging()

app = Flask(__name__)
app.json.sort_keys = False

PREFIX = "/utility/askmod-rag-agent"  # NOTE: /utility/<agent-pod-name> should be kept here
AGENT_NAME = AskModRAGAgent


@app.route(f'{PREFIX}/')
@cross_origin(supports_credentials=True)
def home():
    return f"{AGENT_NAME.__name__} is running!", 200  # NOTE: health check


def prediction(data, trace):
    rag_agent = AGENT_NAME()
    logging.info("######################### INPUT DATA #########################")
    logging.info(f"Input data: {data}")
    if data.get('agent_config', None):
        logging.info("Got the setup config from the input data payload")
        config = data.get('agent_config')
    else:
        logging.info("Calling MongoDB to fetch the setup config")
        config = get_setup_details(data['concierge_id'], data['agent_id'])
        
    if config is None:
        raise Exception("Could not fetch setup config from MongoDB")
    
    agent_config = process_config(config=config, sub_level="tools")

    rag_agent.setup(config=agent_config, data=data, agent_id=data['agent_id'])

    logging.info(data['agent_arguments'])

    next_trace = {}
    logging.info("Setup completed")
    response, retrieved_documents, citations = rag_agent.run(next_trace=next_trace, trace=trace, **data['agent_arguments'])
    return response, retrieved_documents, citations


@app.route(f'{PREFIX}/prediction', methods=['POST'])
@cross_origin(supports_credentials=True)
def predict_api():
    trace = {}
    try:
        data = request.get_json()
        response, retrieved_documents, citations = prediction(data, trace)
        # pubsub_publish(response) 
        agent_guide = None
        if retrieved_documents:
            agent_guide = {
                "retrieved_documents": retrieved_documents,
            }

        return jsonify({"response": response, "agent_guide":agent_guide, "sources":citations, "trace": trace, "trace_root": trace.pop('root', None), "bypass_orchestrator_response": True}), 200
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error in prediction: {e}")
        return jsonify({"error": str(e), "trace": trace, "trace_root": trace.pop('root', None)}), 500


@app.route(f'{PREFIX}/get_config', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_config_api():
    try:
        response = AGENT_NAME.get_setup_config()
        if response:
            return jsonify(response), 200
        else:
            return jsonify({"message": "No configuration found"}), 204
    except Exception as e:
        logging.info(f"Error in get_config_api: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route(f'{PREFIX}/get_llm_config', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_llm_config_api():
    try:
        response = AGENT_NAME().get_llm_config()
        if response:
            return jsonify(response), 200
        else:
            return jsonify({"message": "No LLM configuration found"}), 204
    except Exception as e:
        logging.info(f"Error in get_llm_config_api: {str(e)}")
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5002) # FIXME: Uncomment this before running locally

    # gunicorn_command = (
    #     f"gunicorn --workers {Config.WORKER_COUNT} --worker-class gthread --bind 0.0.0.0:5002 "
    #     f"--timeout {Config.WORKER_TIMEOUT} --keep-alive 120 --max-requests {Config.MAX_REQUEST_TO_WORKER_RESTART} --max-requests-jitter 50 "
    #     f"--log-level info --threads {Config.WORKER_THREADS_COUNT} --access-logfile - --error-logfile - "
    #     f"--graceful-timeout {Config.WORKER_GRACEFUL_TIMEOUT} --limit-request-line 8190 run:app"
    # )
    
    # os.system(gunicorn_command)  # FIXME: Comment this before running locally



    


    
