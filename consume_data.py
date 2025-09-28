import argparse
from kafka import KafkaConsumer, errors
from structlog import get_logger


logger = get_logger()
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

    logger.info("starting script...")
    logger.info(f"Consuming Topic: '{topic}'")
    # logger.info(f"retry count: {RETRY_COUNT}")
    # logger.info(f"buffer size: {BUFFER_SIZE}")

    consumer = None
    retry = 0

    for _ in range(RETRY_COUNT):
        try:
            consumer = KafkaConsumer(
                # bootstrap_servers="broker:19092",
                bootstrap_servers="localhost:9092",
                # sasl_mechanism="SCRAM-SHA-256",
                # security_protocol="SASL_SSL",
                # sasl_plain_username=os.getenv("SASL_PLAIN_USERNAME"),
                # sasl_plain_password=os.getenv("SASL_PLAIN_PASSWORD"),
                # auto_offset_reset="earliest",
                group_id="gid1",
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

    if consumer is None:
        logger.error("No consumers available")
        raise Exception("No consumers available")

    try:
        consumer.subscribe([topic])
        for msg in consumer:
            logger.info(
                "%s:%d:%d: key=%s value=%s"
                % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
            )
            consumer.commit()
    finally:
        consumer.close(autocommit=True)
        logger.info("consumer closed")
