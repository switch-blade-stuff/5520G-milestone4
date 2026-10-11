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
sub_id = topic_name + "-converter-sub"
sub_filter = 'attributes.function="convert"'

sub = pubsub_v1.SubscriberClient()
sub_path = sub.subscription_path(project_id, sub_id)
print(f"Listening for messages on {sub_path}..\n")

pub = pubsub_v1.PublisherClient()
topic_path = pub.topic_path(project_id, topic_name)


def convert_row(data):
    data["temperature"] = data["temperature"] * 1.8 + 32
    data["pressure"] = data["pressure"] / 6.895
    return data


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    data = convert_row(json.loads(message.data.decode("utf-8")))
    message.ack()
    message = json.dumps(data).encode("utf-8")
    future = pub.publish(topic_path, message, function="result")
    future.result()
    print(f"Published converted message {data}")


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
