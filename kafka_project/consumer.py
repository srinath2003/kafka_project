from kafka import KafkaConsumer
import json

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'product_interactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Function to process interaction messages
def process_interaction(event):
    try:
        user_id = event['user_id']
        product_id = event['product_id']
        action = event['action']
        timestamp = event['timestamp']  # Get the timestamp
        print(f"User {user_id} performed {action} on product {product_id} at {timestamp}")
    except KeyError as e:
        print(f"KeyError: {e} not found in the event message.")
        # Handle missing field, e.g., log or set a default value
        print("Missing field in event message.")

# Consuming messages from Kafka topic
for message in consumer:
    event = message.value
    process_interaction(event)