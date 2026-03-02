import json
import time
import random
import logging
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

# Configure professional logging for monitoring the engine status
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()

# Kafka configuration aligning with the docker-compose setup
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'aml_transactions'

def delivery_report(err, msg):
    """
    Callback function triggered by Kafka to confirm message delivery.
    Logs errors if the delivery fails.
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")

def generate_transaction():
    """
    Generates a synthetic financial transaction.
    Injects occasional high-value transactions to simulate potential money laundering patterns.
    """
    # Simulate an account pool using random integers to mimic hashed account IDs
    sender = f"ACC_{random.randint(1000, 9999)}"
    receiver = f"ACC_{random.randint(1000, 9999)}"
    
    # Ensure sender and receiver are not the same account
    while sender == receiver:
        receiver = f"ACC_{random.randint(1000, 9999)}"

    # Inject 1% probability of a high-value transaction (Anomaly/Alert trigger)
    is_suspicious = random.random() < 0.01
    
    if is_suspicious:
        amount = round(random.uniform(100000.0, 500000.0), 2)
    else:
        amount = round(random.uniform(50.0, 5000.0), 2)

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "sender_account": sender,
        "receiver_account": receiver,
        "amount": amount,
        "currency": "THB",
        # Fix Python 3.12+ deprecation warning by using timezone-aware UTC datetime
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "channel": random.choice(["MOBILE_APP", "WEB", "ATM"])
    }
    
    return transaction

def start_engine():
    """
    Initializes the Kafka producer and runs the continuous data generation loop.
    """
    conf = {'bootstrap.servers': KAFKA_BROKER}
    
    try:
        producer = Producer(conf)
        logger.info(f"Starting PaySim Engine. Producing messages to Kafka topic: {TOPIC_NAME}")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        return

    transaction_count = 0

    try:
        while True:
            txn_data = generate_transaction()
            json_data = json.dumps(txn_data)
            
            # Asynchronously produce the message to the Kafka topic
            producer.produce(
                TOPIC_NAME, 
                json_data.encode('utf-8'), 
                callback=delivery_report
            )
            
            # Poll for delivery report callbacks to prevent memory leaks
            producer.poll(0)
            
            transaction_count += 1
            
            # Log progress periodically to avoid console spam but provide visibility
            if transaction_count % 100 == 0:
                logger.info(f"Successfully generated and pushed {transaction_count} transactions to Kafka.")
            
            # Control throughput rate (e.g., 10 transactions per second)
            time.sleep(0.1) 
            
    except KeyboardInterrupt:
        logger.info("Engine stopped manually by the user.")
    finally:
        # Ensure all queued messages are delivered before shutting down
        logger.info("Flushing pending messages to Kafka broker...")
        producer.flush()

if __name__ == "__main__":
    start_engine()