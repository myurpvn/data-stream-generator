import os
from kafka import errors
from kafka import KafkaProducer

RETRY_COUNT = 3


def init_producer(topic: str, retry: int = 0) -> KafkaProducer | None:
    producer = None
    try:
        # Create a standard Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers="broker:9092",
            # sasl_mechanism="SCRAM-SHA-256",
            # security_protocol="SASL_SSL",
            # sasl_plain_username=os.getenv("SASL_PLAIN_USERNAME"),
            # sasl_plain_password=os.getenv("SASL_PLAIN_PASSWORD"),
        )

    except errors.NoBrokersAvailable:
        retry += 1
        if retry < RETRY_COUNT:
            print(f"retrying..{retry}[{RETRY_COUNT}]")
            return init_producer(topic, retry)
        print(f"retry limit reached (limit: {RETRY_COUNT})")

    except errors.RequestTimedOutError:
        print("request-time-out")

    except Exception:
        raise

    else:
        return producer
