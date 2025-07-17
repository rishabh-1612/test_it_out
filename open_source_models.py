import google.oauth2.id_token
import google.auth.transport.requests
import Config
import requests
import json
import logging
import tiktoken
import time


def count_tokens(text):
  encoding = tiktoken.get_encoding("cl100k_base")
  return len(encoding.encode(text))


SystemPrompt = """
    You are Qwen, created by Alibaba Cloud. You are a helpful assistant.

    Here is the code file for which you need to perform two tasks: {file_content}
    Provide the detailed and lengthy executive summary of the code file. You should cover the entire context of the file and not just a particular setting.
    Provide external, third-party, and internal services present in the code file.
    Give the output in a JSON format.


    ### Role:
    You are an expert code analyzer specializing in summarizing complex codebases.

    ### Tasks:
    1. **Generate a Summary:**
      - Provide a detailed and lengthy executive summary of 10 bullet points, each separated by a newline character ("\\n"). The summary should cover the entire file comprehensively, not just a few functions. Ensure the summary encapsulates all major aspects and functionality of the code.

    2. **Identify Services:**
      - Identify and categorize all services used in the code into three types:

        **External Services:**
        - Definition: Services provided by external companies or cloud providers, essential for functionalities like hosting, data storage, or external computations.
        - Examples: AWS, Google Cloud Platform (GCP), Docker, MongoDB, etc.

        **Third-Party Services:**
        - Definition: Frameworks, packages, or libraries, used to provide specific functionalities.
        - Examples: Flask, Django, jslint, axios, etc.

        **Internal Services:**
        - Definition: Internal project components, self-made packages, or in-house tools developed specifically for the project, encapsulating unique business logic or proprietary algorithms.
        - Examples: Custom logging modules, proprietary API wrappers.

      - Identify every instance of a service being used, either in a subprocess or a command. Use precise and technical names for each service.

    ### Remember:
    - If the file content has only comments and no code, give the summary as empty string and services as an empty list.


    ### Output Structure:
    {{
      "summary": "the detailed lengthy executive summary of 10 bullet points separated by newline characters (\"\\n\") for the given code file",
      "services": [service_1, service_2, service_3, ...] //list of 15 technical service names
    }}

    ### Strict Constraints:
    - You will be fined 100000$ if you miss any services or provide any wrong service name.
    - The JSON structure has to be strict. The summary is a string. The services are a list of services.
"""


def generate_fresh_token(target_audience):
    """
    Generates a fresh identity token for the target audience.

    Parameters:
        target_audience (str): The URL of the service for which the token is being generated.

    Returns:
        str: A fresh identity token.
    """
    try:
        request = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(request, target_audience)
        return id_token
    except Exception as e:
        raise RuntimeError(f"Failed to generate token: {e}")


def generate_summary_with_qwen(QWEN_DEPLOYMENT_URL, file_content):
    max_retries = 3
    token = generate_fresh_token(QWEN_DEPLOYMENT_URL)
    prompt = SystemPrompt.format(file_content=file_content)
    num_ctx = count_tokens(prompt) + 1000
    logging.info(f"Num_ctx: {num_ctx}")
    if num_ctx >= 13000:
        raise RuntimeError(f"Token count exceeds the limit (num_ctx: {num_ctx}). Triggering fallback.")
    for attempt in range(max_retries):
        try:
            payload = {
                "prompt": prompt,
                "model": "qwen2.5-coder:14b",
                "format": "json",
                "stream": False,
                "options": {
                    "num_ctx": num_ctx
                }
            }
            headers = {
                "Authorization": f"bearer {token}",
                "Content-Type": "application/json",
            }

            response = requests.post(
                f"{QWEN_DEPLOYMENT_URL}/api/generate",
                headers=headers,
                data=json.dumps(payload)
            )

            response_data = json.loads(response.json().get('response', '{}'))
            summary = response_data.get("summary", "").split("\n")
            services = response_data.get("services", [])

            if not summary or all(not line.strip() for line in summary) or summary == [""] or response.json()['eval_count'] <200:  # Retry if summary is empty
                logging.warning(f"Attempt {attempt + 1} failed: Empty summary, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
                continue

            formatted_summary = "```summary\n" + "\n".join(f"- {line}" for line in summary if line.strip()) + "\n```"
            formatted_services = "```service_names\n" + "\n".join(f"- {service}" for service in services) + "\n```"

            logging.info(f"Summary Generated by Qwen 2.5 Coder 14B model")
            return formatted_summary + "\n\n" + formatted_services

        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing error on attempt {attempt + 1}: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error on attempt {attempt + 1}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error on attempt {attempt + 1}: {e}")

        if attempt == max_retries - 1:
            raise RuntimeError(f"Failed to generate summary by Qwen after {max_retries} attempts")