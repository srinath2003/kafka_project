from flask import Flask, request, jsonify, render_template
from kafka import KafkaProducer
import json
from uuid import uuid4
from datetime import datetime

app = Flask(__name__)

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to send data to Kafka topic
def send_to_kafka(topic, message):
    producer.send(topic, message)
    producer.flush()

@app.route('/')
def home():
    products = [
        {"product_id": str(uuid4()), "name": "Laptop", "price": 1000},
        {"product_id": str(uuid4()), "name": "Headphones", "price": 200},
        {"product_id": str(uuid4()), "name": "Smartphone", "price": 800},
    ]
    return render_template('index.html', products=products)

@app.route('/click/<product_id>', methods=['POST'])
def click_product(product_id):
    user_id = request.json.get('user_id', str(uuid4()))
    timestamp = datetime.now().isoformat()  # Get the current timestamp
    send_to_kafka('product_interactions', {
        "user_id": user_id,
        "product_id": product_id,
        "action": "click",
        "timestamp": timestamp  # Include the timestamp
    })
    return jsonify({"message": "Click recorded."})

@app.route('/add_to_cart/<product_id>', methods=['POST'])
def add_to_cart(product_id):
    user_id = request.json.get('user_id', str(uuid4()))
    timestamp = datetime.now().isoformat()  # Get the current timestamp
    send_to_kafka('product_interactions', {
        "user_id": user_id,
        "product_id": product_id,
        "action": "add_to_cart",
        "timestamp": timestamp  # Include the timestamp
    })
    return jsonify({"message": "Added to cart."})

@app.route('/buy/<product_id>', methods=['POST'])
def buy_product(product_id):
    user_id = request.json.get('user_id', str(uuid4()))
    timestamp = datetime.now().isoformat()  # Get the current timestamp
    send_to_kafka('product_interactions', {
        "user_id": user_id,
        "product_id": product_id,
        "action": "buy",
        "timestamp": timestamp  # Include the timestamp
    })
    return jsonify({"message": "Purchase recorded."})

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)