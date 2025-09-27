import argparse
import json

import uuid
import random
import time
from datetime import datetime
from kafka import KafkaProducer, errors

import requests

# from kafka_producer import init_producer

RETRY_COUNT = 3


def get_data():
    # URL = "https://en.wikipedia.org/w/api.php?&action=query&format=json&list=recentchanges&rcprop=title|ids|sizes|flags|user|comment|timestamp|parsedcomment&rclimit=10"
    URL = "https://en.wikipedia.org/w/api.php"
    header = {"User-Agent": "DemoBot/1.0", "Accept-Encoding": "gzip"}
    params = {
        "action": "query",
        "format": "json",
        "list": "recentchanges",
        "formatversion": "2",
        "rcprop": "title|ids|sizes|flags|user|comment|timestamp",
        "rclimit": "500",
    }
    r = requests.get(URL, headers=header, params=params)

    data = r.json()
    with open("raw_data.json", "w+") as f:
        json.dump(data, f, indent=4)

    # print(data)
    # print(data_dict)

    res = data["query"]["recentchanges"]

    return json.dumps(res)


def get_data_dummy() -> str:

    dt = datetime.now()

    k_message = {
        "user-id": str(random.randint(0, 999999)).zfill(6),
        "action": random.choice(
            ["click", "like", "unlike", "repost", "subscribe", "unsubscribe"]
        ),
        "platform": random.choice(
            ["ios", "android", "macos", "linux", "windows", "other"]
        ),
        "timestamp": dt.isoformat(),
    }

    k_message_ser = json.dumps(k_message)
    print("got message: ", k_message_ser)
    return k_message_ser


def send_message(producer, topic: str, message: str):
    # Produce a message
    try:
        producer.send(topic, message.encode("utf-8"))
        producer.flush()
        # print("Message produced without Avro schema!")
    except Exception as e:
        print(f"Error producing message: {e}")
    # finally:
    #     producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--topic",
        help="kafka topic to send message",
        required=True,
        type=str,
    )
    args = parser.parse_args()
    topic = args.topic

    print("starting script...")
    print(f"Producing Topic: '{topic}'")
    # print(f"retry count: {os.getenv('RETRY_COUNT')}")

    # producer = init_producer(topic)
    retry = 0
    producer = None

    for _ in range(RETRY_COUNT):
        try:
            # Create a standard Kafka Producer
            producer = KafkaProducer(
                bootstrap_servers="broker:19092",
                # bootstrap_servers="localhost:9092",
                # sasl_mechanism="SCRAM-SHA-256",
                # security_protocol="SASL_SSL",
                # sasl_plain_username=os.getenv("SASL_PLAIN_USERNAME"),
                # sasl_plain_password=os.getenv("SASL_PLAIN_PASSWORD"),
            )
            break

        except errors.NoBrokersAvailable:
            retry += 1
            if retry < RETRY_COUNT:
                print(f"retrying..{retry}[{RETRY_COUNT}]")
                continue
            print(f"retry limit reached (limit: {RETRY_COUNT})")

        # except errors.RequestTimedOutError:
        #     print("request-time-out")

        except Exception:
            raise

    if producer is not None:
        try:
            # while True:
            print("sending message")
            # producer.send(topic, b"raw_bytes")
            # message = get_data()
            # for i in range(10):
            key = 0
            while True:
                message = get_data_dummy()
                # send_message(producer, topic, message)
                producer.send(
                    topic, message.encode("utf-8"), key=str(key).encode("utf-8")
                )
                time.sleep(1)
                # print(f"Message count: {i+1}")
                key += 1
            producer.commit_transaction()
        finally:
            producer.close()
            print("producer closed")
