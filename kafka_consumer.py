from kafka import KafkaConsumer
from kafka import errors
import os


# TODO: "Create a class"

# class kafka_consumer():

#     def __init__(self) -> None:
#         pass
RETRY_COUNT = 3


def init_consumer(topic: str, retry: int = 0) -> KafkaConsumer | None:
    # Kafka Consumer configuration
    consumer = None
    try:
        consumer = KafkaConsumer(
            bootstrap_servers="broker:9092",
            # sasl_mechanism="SCRAM-SHA-256",
            # security_protocol="SASL_SSL",
            # sasl_plain_username=os.getenv("SASL_PLAIN_USERNAME"),
            # sasl_plain_password=os.getenv("SASL_PLAIN_PASSWORD"),
            # auto_offset_reset="earliest",
            # group_id="$GROUP_ID",
        )

        return consumer

    except errors.NoBrokersAvailable:
        retry += 1
        if retry < RETRY_COUNT:
            print(f"retrying..{retry}[{RETRY_COUNT}]")
            return init_consumer(topic, retry)
        print(f"retry limit reached (limit: {RETRY_COUNT})")

    except errors.RequestTimedOutError:
        print("request-time-out")

    except Exception:
        raise

    else:
        return consumer
