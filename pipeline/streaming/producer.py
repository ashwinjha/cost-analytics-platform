import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "cost-events"

events = [
    {"account_id": "A1", "service": "EC2", "cost": 18.5},
    {"account_id": "A2", "service": "S3", "cost": 5.2},
    {"account_id": "A1", "service": "Lambda", "cost": 3.1}
]

for event in events:
    producer.send(topic, event)
    print("Sent:", event)
    time.sleep(1)

producer.flush()

