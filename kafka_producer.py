import json
import time
import os
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="project-kafka-26-project-193d.c.aivencloud.com:17874",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "reddit_posts"

source_file = "reddit_raw_data.jsonl"
state_file = "kafka_producer_state.txt"


if os.path.exists(state_file):
    with open(state_file, "r") as file:
        last_sent = int(file.read().strip())
else:
    last_sent = 0

print(f"Last sent record: {last_sent}")


count = 0
total_records = 0

with open(source_file, "r", encoding="utf-8") as file:

    for line in file:

        line = line.strip()

        if not line:
            continue

        current_position = total_records
        total_records += 1

        if current_position < last_sent:
            continue

        record = json.loads(line)

        producer.send(topic, value=record)

        count += 1

        print(f"Sent record {current_position + 1}")

        time.sleep(0.2)


producer.flush()
producer.close()


with open(state_file, "w") as file:
    file.write(str(total_records))


print("Kafka ingestion completed")