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

    key = (
        msg.key().decode("utf-8")
        if msg.key()
        else "NULL"
    )

    print(
        f"Delivered | "
        f"key={key} | "
        f"partition={msg.partition()} | "
        f"offset={msg.offset()}"
    )


event_time = datetime.now().strftime(
    "%Y-%m-%d %H:%M:%S"
)


# --------------------------------------------------
# 1. VALID EVENT
# --------------------------------------------------

valid_sale = {
    "sale_id": 201,
    "customer_id": 2001,
    "product": "SSD",
    "quantity": 2,
    "price": 300.0,
    "event_time": event_time,
}


# --------------------------------------------------
# 2. DUPLICATE EVENT
#
# Same sale_id = 201
# Silver should keep only one.
# --------------------------------------------------

duplicate_sale = {
    "sale_id": 201,
    "customer_id": 2001,
    "product": "SSD",
    "quantity": 2,
    "price": 300.0,
    "event_time": event_time,
}


# --------------------------------------------------
# 3. INVALID QUANTITY
# --------------------------------------------------

bad_quantity = {
    "sale_id": 202,
    "customer_id": 2002,
    "product": "RAM",
    "quantity": -3,
    "price": 200.0,
    "event_time": event_time,
}


# --------------------------------------------------
# 4. INVALID PRODUCT
# --------------------------------------------------

bad_product = {
    "sale_id": 203,
    "customer_id": 2003,
    "product": "",
    "quantity": 1,
    "price": 150.0,
    "event_time": event_time,
}


# --------------------------------------------------
# 5. MISSING CUSTOMER_ID
# --------------------------------------------------

missing_customer = {
    "sale_id": 204,
    "product": "Webcam",
    "quantity": 1,
    "price": 400.0,
    "event_time": event_time,
}


events = [
    ("2001", valid_sale),
    ("2001", duplicate_sale),
    ("2002", bad_quantity),
    ("2003", bad_product),
    ("unknown", missing_customer),
]


for key, event in events:

    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(event),
        on_delivery=delivery_report,
    )

    producer.poll(0)


# --------------------------------------------------
# 6. MALFORMED JSON
# --------------------------------------------------

producer.produce(
    topic=topic,
    key="malformed",
    value='{"sale_id": 205, "customer_id": ',
    on_delivery=delivery_report,
)


producer.flush()

print("\nAll test events sent.")