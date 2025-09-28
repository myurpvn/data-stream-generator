# A Kafka stream generator

## Requirements

1. Docker Desktop

## Quick start

1. `make generator-up` / `docker compose up` to start generator in Docker
2. Access data stream via `localhost` port `9092`

### Sample consumer code

```python
consumer = KafkaConsumer(bootstrap_servers="localhost:9092")

consumer.subscribe([topic])
for msg in consumer:
    print(
        "%s:%d:%d: key=%s value=%s"
        % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
    )
```
