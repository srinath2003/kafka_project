from flask import Flask, request, jsonify, render_template
from kafka import KafkaProducer
import json
import uuid
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.util import datetime_from_timestamp

app = Flask(__name__)

# Initialize Cassandra connection
try:
    cluster = Cluster(['localhost'])
    session = cluster.connect('ecommerce')  # Use the existing keyspace
except Exception as e:
    print("Cassandra connection error:", e)
    session = None

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print("Kafka producer error:", e)
    producer = None

def save_to_cassandra(user_id, product_id, action, timestamp):
    """
    Save interaction to Cassandra with corrected schema
    
    Args:
        user_id (str): User ID (now stored as text)
        product_id (str): Product ID
        action (str): User action
        timestamp (datetime): Timestamp of action
    """
    interaction_id = str(uuid.uuid4())  # Generate a unique ID for the interaction
    
    query = """
    INSERT INTO product_interactions (
        interaction_id, 
        user_id, 
        product_id, 
        action, 
        timestamp
    ) VALUES (%s, %s, %s, %s, %s)
    """
    
    try:
        session.execute(query, (
            uuid.UUID(interaction_id), 
            str(user_id),  # Explicitly convert to string 
            uuid.UUID(product_id), 
            action, 
            datetime_from_timestamp(datetime.now().timestamp())
        ))
    except Exception as e:
        print(f"Error saving to Cassandra: {e}")
        raise

# Function to send data to Kafka topic
def send_to_kafka(topic, message):
    if producer:
        producer.send(topic, message)
        producer.flush()

@app.route('/')
def home():
    products = [
        {"product_id": str(uuid.uuid4()), "name": "Laptop", "price": 1000},
        {"product_id": str(uuid.uuid4()), "name": "Headphones", "price": 200},
        {"product_id": str(uuid.uuid4()), "name": "Smartphone", "price": 800},
    ]
    return render_template('index.html', products=products)

@app.route('/click/<product_id>', methods=['POST'])
def click_product(product_id):
    # Use the provided user_id or generate a new one
    user_id = request.json.get('user_id', str(uuid.uuid4()))
    timestamp = datetime.now()
    
    save_to_cassandra(user_id, product_id, 'click', timestamp)
    send_to_kafka('product_interactions', {
        "user_id": str(user_id),
        "product_id": str(product_id),
        "action": "click",
        "timestamp": timestamp.isoformat()
    })
    return jsonify({"message": "Click recorded."})

@app.route('/add_to_cart/<product_id>', methods=['POST'])
def add_to_cart(product_id):
    user_id = request.json.get('user_id', str(uuid.uuid4()))
    timestamp = datetime.now()
    
    save_to_cassandra(user_id, product_id, 'add_to_cart', timestamp)
    send_to_kafka('product_interactions', {
        "user_id": str(user_id),
        "product_id": str(product_id),
        "action": "add_to_cart",
        "timestamp": timestamp.isoformat()
    })
    return jsonify({"message": "Added to cart."})

@app.route('/buy/<product_id>', methods=['POST'])
def buy_product(product_id):
    user_id = request.json.get('user_id', str(uuid.uuid4()))
    timestamp = datetime.now()
    
    save_to_cassandra(user_id, product_id, 'buy', timestamp)
    send_to_kafka('product_interactions', {
        "user_id": str(user_id),
        "product_id": str(product_id),
        "action": "buy",
        "timestamp": timestamp.isoformat()
    })
    return jsonify({"message": "Purchase recorded."})

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)