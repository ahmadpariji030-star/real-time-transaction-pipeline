from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = psycopg2.connect(
    dbname="transactions_db",
    user="postgres",
    password="123456",
    host="127.0.0.1",
    port="5432"
)

cursor = conn.cursor()

def is_valid(data):
    try:
        if not all(k in data for k in ["user_id", "amount", "timestamp", "source"]):
            return False, "missing_field"

        if not isinstance(data["amount"], (int, float)):
            return False, "invalid_type"

        if data["amount"] <= 0:
            return False, "invalid_amount"

        if data["source"] not in ["mobile", "web"]:
            return False, "invalid_source"

        return True, None
    except:
        return False, "error"

for msg in consumer:
    data = msg.value
    valid, error = is_valid(data)

    if valid:
        cursor.execute(
            "INSERT INTO transactions_valid VALUES (%s, %s, %s, %s)",
            (
                data["user_id"],
                data["amount"],
                datetime.fromisoformat(data["timestamp"]),
                data["source"]
            )
        )
        print("VALID:", data)
    else:
        cursor.execute(
            "INSERT INTO transactions_invalid VALUES (%s, %s)",
            (error, json.dumps(data))
        )
        print("INVALID:", data, "| Error:", error)

    conn.commit()
