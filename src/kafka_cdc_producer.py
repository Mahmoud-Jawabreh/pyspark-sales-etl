import json
import time
from datetime import datetime

from confluent_kafka import Producer


producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

topic = "sales-cdc-events"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
        return

    print(
        f"Delivered | "
        f"key={msg.key().decode('utf-8')} | "
        f"partition={msg.partition()} | "
        f"offset={msg.offset()}"
    )


events = [
    {
        "event_id": "E001",
        "sequence_number": 1,
        "operation": "I",
        "sale_id": 301,
        "customer_id": 3001,
        "product": "SSD",
        "quantity": 1,
        "price": 300.0,
    },

    {
        "event_id": "E002",
        "sequence_number": 1,
        "operation": "I",
        "sale_id": 302,
        "customer_id": 3002,
        "product": "Mouse",
        "quantity": 2,
        "price": 120.0,
    },

    {
        "event_id": "E003",
        "sequence_number": 2,
        "operation": "U",
        "sale_id": 301,
        "customer_id": 3001,
        "product": "SSD",
        "quantity": 3,
        "price": 300.0,
    },

    {
        "event_id": "E004",
        "sequence_number": 2,
        "operation": "D",
        "sale_id": 302,
        "customer_id": None,
        "product": None,
        "quantity": None,
        "price": None,
    },

    {
        "event_id": "E005",
        "sequence_number": 1,
        "operation": "I",
        "sale_id": 303,
        "customer_id": 3003,
        "product": "Keyboard",
        "quantity": 1,
        "price": 250.0,
    },
]


for event in events:

    event["event_time"] = (
        datetime.now()
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    producer.produce(
        topic=topic,
        key=str(event["sale_id"]),
        value=json.dumps(event),
        on_delivery=delivery_report,
    )

    producer.poll(0)

    # Helps us visually see CDC events arriving over time
    time.sleep(2)


producer.flush()

print("\nAll CDC events sent.")