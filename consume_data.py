import argparse
from kafka import KafkaConsumer, errors

BUFFER_SIZE = 10
RETRY_COUNT = 3

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--topic",
        help="kafka topic to consume",
        required=True,
        type=str,
    )
    args = parser.parse_args()
    topic = args.topic

    print("starting script...")
    print(f"Consuming Topic: '{topic}'")
    print(f"retry count: {RETRY_COUNT}")
    print(f"buffer size: {BUFFER_SIZE}")
    # consumer = init_consumer(topic)

    consumer = None
    retry = 0

    for _ in range(RETRY_COUNT):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers="broker:19092",
                # bootstrap_servers="localhost:9092",
                # sasl_mechanism="SCRAM-SHA-256",
                # security_protocol="SASL_SSL",
                # sasl_plain_username=os.getenv("SASL_PLAIN_USERNAME"),
                # sasl_plain_password=os.getenv("SASL_PLAIN_PASSWORD"),
                # auto_offset_reset="earliest",
                # group_id="$GROUP_ID",
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

    if consumer is not None:
        # basic_consume_loop(consumer, [topic])
        try:
            consumer.subscribe([topic])
            for msg in consumer:
                print(
                    "%s:%d:%d: key=%s value=%s"
                    % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
                )
        finally:
            # Close down consumer to commit final offsets.
            consumer.close(autocommit=True)
            print("consumer closed")
