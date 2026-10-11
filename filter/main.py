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
sub_id = topic_name + "-filter-sub"
sub_filter = 'attributes.function="filter"'

sub = pubsub_v1.SubscriberClient()
sub_path = sub.subscription_path(project_id, sub_id)
print(f"Listening for messages on {sub_path}..\n")

pub = pubsub_v1.PublisherClient()
topic_path = pub.topic_path(project_id, topic_name)


def filter_row(data):
    return not (
        (data["temperature"] is None)
        | (data["humidity"] is None)
        | (data["pressure"] is None)
    )


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    data = json.loads(message.data.decode("utf-8"))
    message.ack()
    if filter_row(data):
        message = json.dumps(data).encode("utf-8")
        future = pub.publish(topic_path, message, function="result")
        future.result()
        print(f"Published filtered message {data}")
    else:
        print(f"Rejected filtered message {data}")


with sub:
    try:
        sub.create_subscription(
            request={
                "name": sub_path,
                "topic": topic_path,
                "filter": sub_filter,
            }
        )
    except:
        pass

    streaming_pull_future = sub.subscribe(sub_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
