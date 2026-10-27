import json
from datetime import datetime

from confluent_kafka import Producer


producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

topic = "sales-keyed-events"


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


sales = [
    {
        "sale_id": 101,
        "customer_id": 1001,
        "product": "Laptop",
        "quantity": 1,
        "price": 3500.0,
    },
    {
        "sale_id": 102,
        "customer_id": 1002,
        "product": "Mouse",
        "quantity": 2,
        "price": 120.0,
    },
    {
        "sale_id": 103,
        "customer_id": 1001,
        "product": "Monitor",
        "quantity": 1,
        "price": 900.0,
    },
    {
        "sale_id": 104,
        "customer_id": 1003,
        "product": "Keyboard",
        "quantity": 1,
        "price": 250.0,
    },
    {
        "sale_id": 105,
        "customer_id": 1002,
        "product": "Headset",
        "quantity": 2,
        "price": 300.0,
    },
    {
        "sale_id": 106,
        "customer_id": 1001,
        "product": "USB Hub",
        "quantity": 1,
        "price": 100.0,
    },
]


for sale in sales:
    sale["event_time"] = (
        datetime.now()
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    key = str(sale["customer_id"])
    value = json.dumps(sale)

    producer.produce(
        topic=topic,
        key=key,
        value=value,
        on_delivery=delivery_report,
    )

    producer.poll(0)


producer.flush()

print("\nAll sales sent.")