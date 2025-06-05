import json
import time
from kafka import KafkaProducer

# Load messages from messages.json
with open("/home/naman/Downloads/capstone/bootcamp-project/data/messages.json", "r") as file:
    messages = json.load(file)

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "capstone"

# Continuously send messages
while True:
    for msg in messages:
        msg['timestamp'] = int(time.time())  # Update timestamp to current time
        print(f"Sending: {msg}")
        producer.send(topic, msg)
        time.sleep(1)  # Send a message every second
