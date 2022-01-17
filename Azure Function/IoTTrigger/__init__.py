# Import Modules
from typing import List
import logging
import gzip
from io import BytesIO
import azure.functions as func
from os import listdir
import os
import json

# Define Event Hub Connection Details
logEventHubString = os.environ["logEventHubString"]
logEventHubName = os.environ["logEventHubName"]
alarmEventHubString = os.environ["alarmEventHubString"]
alarmEventHubName = os.environ["alarmEventHubName"]
realtimeEventHubString = os.environ["realtimeEventHubString"]
realtimeEventHubName = os.environ["realtimeEventHubName"]

# Get Message from IoT Hub
def main(event: List[func.EventHubEvent]) -> str:

    # Log Event Details
    logging.info(f'  EnqueuedTimeUtc = {event.enqueued_time}')
    
    # Unzip and Decode GZIP File
    buf = BytesIO(event.get_body())

    # Open GPZIP File
    with gzip.open(buf, 'rb') as f_in:
        file_content = f_in.read()
        file_content = file_content.decode("utf-8")
        result = json.loads(file_content)

        try:
            # Process Realtime File
            if 'tagId' in result[0]:

                data = json.dumps(result)
                logging.info("File Processed")

                return data

            else:
                logging.info("Not Realtime File")
                return

        except IndexError:
            logging.info("Empty File Detected")
            return
