import json
import threading
import time
import os
import uuid
import traceback

import requests
import config
from google.cloud import pubsub_v1
from sync_project_summary.local_sync.embed_helper import Embeddings
from sync_project_summary.local_sync.alloydb_ingestion import AlloyDBTableManager

PROJECT_ID = config.LLM_PROJECT_ID
TOPIC_ID = config.PUBSUB_GITHUB_SYNC_TOPIC_ID
DATA_EXTRACTION_API_URL = f"{config.EGPT_DOMAIN}/data_extraction/local_sync_summarizer"

class PubSubHelper:
    def __init__(self, sync_id: str, task_id: str, payload: dict):
        self.sync_id = sync_id
        self.task_id = task_id
        self.payload = payload
        self.project_id = PROJECT_ID
        self.topic_id = TOPIC_ID
        self.api_url = DATA_EXTRACTION_API_URL
        self.embedding_map = []  # optional, can be used later

    def call_sync_summarizer_api(self, payload: dict) -> str:
        res = requests.post(self.api_url, json=payload)
        res.raise_for_status()
        response_data = res.json()
        # with open("sync_summarizer_response.json", "w") as f:
        #     json.dump(response_data, f, indent=4)
        task_id = response_data["task_id"]
        # print(f"Task ID received: {task_id}")
        return task_id

    def create_subscription(self, task_id: str) -> str:
        subscription_id = f"local-sync-sub-{uuid.uuid4()}"
        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()
        topic_path = publisher.topic_path(self.project_id, self.topic_id)
        subscription_path = subscriber.subscription_path(self.project_id, subscription_id)

        # Avoid closing client early – no 'with' here
        subscription = subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "filter": f'attributes.task_id="{task_id}"'
            }
        )
        print(f"Created subscription with filtering: {subscription.name}")
        return subscription_path

    def listen_to_subscription(self, subscription_path: str):
        batch_data = []
        should_end = threading.Event()
        embedding_obj = Embeddings()

        table_name = f"{config.ORGANIZATION_NAME}-local-sync-{self.sync_id}"
        alloydbmanager = AlloyDBTableManager(table_name)
        alloydbmanager.create_table()  # Move outside callback

        def callback(message):
            try:
                data = json.loads(message.data.decode("utf-8"))
                print("Received message (truncated):", str(data)[:100])

                for file_data in data.get("data", [])[::-1]:
                    if file_data.get("file") == "end":
                        print("End message received.")
                        should_end.set()
                        batch_data.append(file_data)
                        break

                    summary = file_data.get("summary", "")
                    if not summary:
                        print(f"Skipping file with no summary: {file_data.get('file')}")
                        continue

                    print("Processing file data:", file_data.get("file", ""))
                    embedding = embedding_obj.azure_Embedding(summary)

                    # ✅ Sanitize embedding values
                    openai_embedding = embedding if isinstance(embedding, list) and embedding else None
                    st_embedding = None
                    googleai_embedding = None

                    row_id = str(uuid.uuid4())
                    # file_data["openai_embedding"] = openai_embedding
                    # file_data["row_id"] = row_id

                    clean_summary = summary.replace("```", "").strip()
                    clean_metadata = json.dumps(file_data).replace("```", "")

                    self.embedding_map.append(
                        (
                            row_id,
                            openai_embedding,
                            st_embedding,
                            googleai_embedding,
                            clean_metadata,
                            clean_summary,
                            file_data.get("file", "")
                        )
                    )

                    batch_data.append(file_data)

                message.ack()

            except Exception as e:
                traceback.print_exc()
                print(f"Error processing message: {e}")
                message.nack()

            try:
                if self.embedding_map:
                    assert all(len(row) == 7 for row in self.embedding_map), "Invalid tuple length in embedding_map"
                    alloydbmanager.save_mapping(mapping=self.embedding_map,
                                                task_id=self.task_id,
                                                single_row=False)
                    self.embedding_map = []  # ✅ Clear after saving
            except Exception as e:
                traceback.print_exc()
                print(f"Error saving mapping: {e}")

        subscriber = pubsub_v1.SubscriberClient()
        future = subscriber.subscribe(subscription_path, callback=callback)
        print(f"Listening to subscription: {subscription_path}")

        try:
            while not should_end.is_set():
                time.sleep(1)

            print("End signal received. Cancelling subscription listener...")
            future.cancel()
            subscriber.delete_subscription(request={"subscription": subscription_path})
            print("Subscription deleted.")

        except KeyboardInterrupt:
            print("Interrupted manually. Cleaning up...")
            future.cancel()

        return batch_data

    def run(self):
        task_id = self.call_sync_summarizer_api(self.payload)
        print(f"Task ID received: {task_id}")
        subscription_path = self.create_subscription(task_id)
        print(f"Subscription created: {subscription_path}")
        changed_file_data = self.listen_to_subscription(subscription_path)
        print(f"Processed {len(changed_file_data)} files.")
        return changed_file_data


# # Example usage
# if __name__ == "__main__":
#     with open("/Users/yashmangalik/Desktop/projects/Appmod-Backend/backend/project_summary_extraction/sync_project_summary/local_sync/sample_payload.json") as f:
#         sample_payload = json.load(f)
#
#     pubsub_helper = PubSubHelper(user_id="temp_user", task_id="temp_task", payload=sample_payload)
#     result = pubsub_helper.run()
#     print(result)
