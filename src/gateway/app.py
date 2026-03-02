import json
import logging
from fastapi import FastAPI, HTTPException, status
from confluent_kafka import Producer
from src.gateway.validators import EMVCoPayload

# Configure professional logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="AML Gateway API", description="Inbound gateway for financial transactions")

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'aml_transactions'

# Initialize Kafka Producer singleton for the API lifecycle
try:
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    logger.info("Gateway successfully connected to Kafka broker.")
except Exception as e:
    logger.error(f"Gateway failed to connect to Kafka: {e}")
    producer = None

def delivery_report(err, msg):
    """
    Callback function triggered by Kafka to confirm message delivery.
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")

@app.post("/api/v1/transactions/qr", status_code=status.HTTP_202_ACCEPTED)
async def process_qr_transaction(payload: EMVCoPayload):
    """
    Endpoint to receive and validate EMVCo QR transactions.
    Valid payloads are pushed to Kafka for downstream AML processing.
    """
    if not producer:
        raise HTTPException(status_code=503, detail="Message broker unavailable")

    try:
        # Construct the transaction payload for the pipeline
        transaction_data = {
            "source": "QR_GATEWAY",
            "raw_payload": payload.qr_payload,
            "status": "VALIDATED"
        }
        
        json_data = json.dumps(transaction_data)
        
        # Asynchronously produce the message to the Kafka topic
        producer.produce(
            TOPIC_NAME,
            json_data.encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)
        
        return {"message": "Transaction accepted for processing"}
    
    except Exception as e:
        logger.error(f"Error processing transaction: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")