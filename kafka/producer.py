"""
    Simple kafka producer with a send_message method that can be called
"""

from confluent_kafka import Producer
import atexit

producer_config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f'Delivery failed: {err}')
    else:
        print(f"Delivered {msg.value().decode('utf-8')}")
        print(msg.offset())

def send_message(topic: str, value: bytes):
    producer.produce(
        topic=topic,
        value=value,
        callback=delivery_report
    )
    producer.poll(0)

# Ensure all messages are delivered on shutdown
atexit.register(producer.flush)
