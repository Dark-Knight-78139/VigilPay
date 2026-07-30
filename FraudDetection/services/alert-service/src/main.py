import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from loguru import logger
from pydantic_settings import BaseSettings
from prometheus_fastapi_instrumentator import Instrumentator

from src.domain.events import (
    AlertSeverity,
    EventProducer,
    FraudAlertEvent,
    FraudAlertPayload,
    FraudPredictionEvent,
)
from src.infrastructure.kafka_client import KafkaConsumerClient, KafkaProducerClient

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_predictions_topic: str = "bank.transactions.predictions"
    kafka_alerts_topic: str = "bank.fraud.alerts"
    fraud_threshold: float = 0.5

settings = Settings()

# Dependencies
kafka_producer = KafkaProducerClient(bootstrap_servers=settings.kafka_bootstrap_servers)
kafka_consumer = KafkaConsumerClient(
    bootstrap_servers=settings.kafka_bootstrap_servers,
    group_id="alert-service-group",
    topics=[settings.kafka_predictions_topic]
)

consume_task = None

async def handle_message(raw_value: str):
    try:
        prediction_event = FraudPredictionEvent.model_validate_json(raw_value)
        payload = prediction_event.payload
        
        # Check if the prediction probability exceeds the alert threshold
        if payload.probability > settings.fraud_threshold:
            severity = AlertSeverity.HIGH if payload.probability > 0.8 else AlertSeverity.MEDIUM
            
            alert_payload = FraudAlertPayload(
                transaction_id=payload.transaction_id,
                customer_id=payload.customer_id,
                severity=severity,
                probability=payload.probability,
                explanation=payload.explanation,
            )
            
            alert_event = FraudAlertEvent(
                correlation_id=prediction_event.correlation_id,
                producer=EventProducer.ALERT_SERVICE,
                payload=alert_payload
            )
            
            kafka_producer.publish(
                topic=settings.kafka_alerts_topic,
                key=payload.customer_id,
                event=alert_event
            )
            logger.info(f"Published Fraud Alert for tx {payload.transaction_id} (prob: {payload.probability})")
        else:
            logger.debug(f"Transaction {payload.transaction_id} is benign (prob: {payload.probability})")

    except Exception as e:
        logger.error(f"Error handling message: {e}")
        raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    kafka_producer.start()
    kafka_consumer.start()
    
    global consume_task
    consume_task = asyncio.create_task(kafka_consumer.consume_loop(handle_message))
    
    yield
    
    kafka_consumer.stop()
    if consume_task:
        await consume_task
    kafka_producer.stop()

app = FastAPI(
    title="Alert Service",
    description="Generates business alerts for fraudulent transactions.",
    lifespan=lifespan
)

Instrumentator().instrument(app).expose(app)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "consumer_running": kafka_consumer.running,
        "producer_ready": kafka_producer.producer is not None,
    }
