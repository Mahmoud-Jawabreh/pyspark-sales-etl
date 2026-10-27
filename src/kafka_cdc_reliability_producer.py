import json
import time

from datetime import datetime
from confluent_kafka import Producer


producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

topic = "sales-cdc-events"


def send(event):

    event["event_time"] = (
        datetime.now()
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    producer.produce(
        topic=topic,
        key=str(event["sale_id"]),
        value=json.dumps(event),
    )

    producer.flush()

    print(
        f"Sent | "
        f"{event['operation']} | "
        f"sale={event['sale_id']} | "
        f"sequence={event['sequence_number']}"
    )

    time.sleep(2)


# Normal INSERT
send({
    "event_id": "E101",
    "sequence_number": 1,
    "operation": "I",
    "sale_id": 401,
    "customer_id": 4001,
    "product": "Monitor",
    "quantity": 1,
    "price": 900.0,
})


# Legitimate UPDATE
send({
    "event_id": "E102",
    "sequence_number": 2,
    "operation": "U",
    "sale_id": 401,
    "customer_id": 4001,
    "product": "Monitor",
    "quantity": 2,
    "price": 900.0,
})


# DELETE
send({
    "event_id": "E103",
    "sequence_number": 3,
    "operation": "D",
    "sale_id": 401,
    "customer_id": None,
    "product": None,
    "quantity": None,
    "price": None,
})


# OLD UPDATE ARRIVES LATE
send({
    "event_id": "E104",
    "sequence_number": 2,
    "operation": "U",
    "sale_id": 401,
    "customer_id": 4001,
    "product": "Monitor",
    "quantity": 99,
    "price": 900.0,
})


# DUPLICATE DELETE
send({
    "event_id": "E103",
    "sequence_number": 3,
    "operation": "D",
    "sale_id": 401,
    "customer_id": None,
    "product": None,
    "quantity": None,
    "price": None,
})


print("\nReliability test complete.")