from typing import List
import logging
import azure.functions as func
import json
import os
import uuid
from azure.storage.blob import BlobServiceClient

CONTAINER_NAME = "lighttemp-data"

def get_or_create_container():
    connection_str = os.environ["STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_str)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    try:
        container_client.create_container()
        logging.info(f"Container criado: {CONTAINER_NAME}")
    except Exception:
        # já existe
        pass

    return container_client

def main(events: List[func.EventHubEvent]):
    container_client = get_or_create_container()

    for event in events:
        raw = event.get_body().decode("utf-8")
        logging.info(f"RAW={raw}")

        body = json.loads(raw)
        if isinstance(body, str):
            body = json.loads(body)

        device_id = event.iothub_metadata.get("connection-device-id", "unknown")
        ts = event.iothub_metadata.get("time")

      
        valorlight = body.get("light")
        valortemp  = body.get("temp")

       
        if valorlight is None and "dados" in body:
            valorlight = body["dados"].get("light")
            valortemp  = body["dados"].get("temp")

        blob_name = f"{device_id}/{uuid.uuid4()}.json"
        blob_client = container_client.get_blob_client(blob_name)

        blob_body = {
            "device_id": device_id,
            "timestamp": ts,
            "dados": {
                "light": valorlight,
                "temp": valortemp
            }
        }

        logging.info(f"Writing blob to {blob_name} - {blob_body}")
        blob_client.upload_blob(json.dumps(blob_body), overwrite=True)

