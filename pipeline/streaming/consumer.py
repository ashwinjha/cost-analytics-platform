from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "cost-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Listening for events...\n")

for message in consumer:
    print("Received:", message.value)

