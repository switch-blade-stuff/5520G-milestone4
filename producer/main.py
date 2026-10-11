import glob
import os
from google.cloud import pubsub_v1
import time
import json
import random
import threading
import uuid
import csv

files = glob.glob("*.json")
if len(files) > 0:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

project_id = "project-2edd459c-6e8e-42b5-9dd"
topic_name = "smartMeterReadings"
pub = pubsub_v1.PublisherClient()
topic_path = pub.topic_path(project_id, topic_name)


def msg_from_row(row):
    temp = float(row["temperature"]) if row["temperature"].strip() else None
    hum = float(row["humidity"]) if row["humidity"].strip() else None
    pres = float(row["pressure"]) if row["pressure"].strip() else None
    return {
        "time": int(float(row["time"])),
        "profile_name": row["profileName"],
        "temperature": temp,
        "humidity": hum,
        "pressure": pres,
    }


with open("Labels.csv", "r") as f:
    data = csv.DictReader(f)
    for row in data:
        message = json.dumps(msg_from_row(row)).encode("utf-8")
        future = pub.publish(topic_path, message, function="filter")
        future.result()
        print(f"Published message {row}")
