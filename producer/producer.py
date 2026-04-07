import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 1),
    request_timeout_ms=30000
)

with open("transactions_from_kaggle.jsonl") as f:
    for line in f:
        data = json.loads(line)

        producer.send("transactions", value=data)
        print("Sent:", data)

        time.sleep(1)
