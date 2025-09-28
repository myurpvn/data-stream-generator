import argparse
import json
import random
import time
from datetime import datetime

import requests
from kafka import KafkaProducer, errors
from structlog import get_logger

logger = get_logger()
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
    with open("raw_data.json", "w") as f:
        json.dump(data, f, indent=4)

    res = data["query"]["recentchanges"]

    return json.dumps(res)


def get_dummy_data() -> str:

    dt = datetime.now()

    data = {
        "user-id": str(random.randint(0, 999999)).zfill(6),
        "action": random.choice(
            ["click", "like", "unlike", "repost", "subscribe", "unsubscribe"]
        ),
        "platform": random.choice(
            ["ios", "android", "macos", "linux", "windows", "other"]
        ),
        "timestamp": dt.isoformat(),
    }

    data_str = json.dumps(data)
    logger.info("got message: ", msg=data_str)
    return data_str


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

    logger.info("starting script...")
    logger.info(f"using to topic: '{topic}'")

    retry = 0
    producer = None

    for _ in range(RETRY_COUNT):
        try:
            producer = KafkaProducer(
                bootstrap_servers="kafka:19092",
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
                logger.info(f"retrying..{retry}[{RETRY_COUNT}]")
                continue
            logger.info(f"retry limit reached (limit: {RETRY_COUNT})")

        # except errors.RequestTimedOutError:
        #     logger.error("request-time-out")

        except Exception:
            raise

    if producer is None:
        logger.error("No producers available")
        raise Exception("No producers available")

    try:
        logger.info("sending message")
        key = 0
        while True:

            message = get_dummy_data()

            producer.send(
                topic,
                message.encode("utf-8"),
                key=str(key).encode("utf-8"),
            )
            time.sleep(1)
            key += 1
    finally:
        producer.close()
        logger.info("producer closed")
