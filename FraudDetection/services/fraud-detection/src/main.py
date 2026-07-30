import asyncio
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI
from loguru import logger
from pydantic_settings import BaseSettings

from src.domain.events import (
    EnrichedTransactionEvent,
    EventProducer,
    FraudPredictionEvent,
    PredictionResult
)
from src.infrastructure.kafka_client import KafkaConsumerClient, KafkaProducerClient
from src.ml.registry import ModelRegistry

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_enriched_topic: str = "bank.transactions.enriched"
    kafka_predictions_topic: str = "bank.transactions.predictions"

settings = Settings()

# Dependencies
registry = ModelRegistry()
kafka_producer = KafkaProducerClient(bootstrap_servers=settings.kafka_bootstrap_servers)
kafka_consumer = KafkaConsumerClient(
    bootstrap_servers=settings.kafka_bootstrap_servers,
    group_id="fraud-detection-group",
    topics=[settings.kafka_enriched_topic]
)

consume_task = None

async def handle_message(raw_value: str):
    try:
        start_time = time.time()
        
        # Parse incoming enriched event
        enriched_event = EnrichedTransactionEvent.model_validate_json(raw_value)
        payload = enriched_event.payload
        
        # Get active model and run inference
        model = registry.get_model()
        probability, explanation = model.predict(payload)
        
        latency_ms = (time.time() - start_time) * 1000.0
        
        # Build prediction result
        prediction = PredictionResult(
            transaction_id=payload.transaction.transaction_id,
            customer_id=payload.transaction.customer_id,
            model_version=model.version,
            probability=probability,
            is_fraud=probability > 0.5,
            explanation=explanation,
            latency_ms=latency_ms
        )
        
        # Build and publish outgoing event
        prediction_event = FraudPredictionEvent(
            correlation_id=enriched_event.correlation_id,
            producer=EventProducer.FRAUD_DETECTION,
            payload=prediction
        )
        
        kafka_producer.publish(
            topic=settings.kafka_predictions_topic,
            key=payload.transaction.customer_id,
            event=prediction_event
        )
        logger.debug(f"Predicted fraud={prediction.is_fraud} for tx {prediction.transaction_id} using {model.version} in {latency_ms:.2f}ms")
    except Exception as e:
        logger.error(f"Error handling message: {e}")
        raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    registry.load_default()
    kafka_producer.start()
    kafka_consumer.start()
    
    global consume_task
    consume_task = asyncio.create_task(kafka_consumer.consume_loop(handle_message))
    
    yield
    
    # Shutdown
    kafka_consumer.stop()
    if consume_task:
        await consume_task
    kafka_producer.stop()

app = FastAPI(
    title="Fraud Detection Service",
    description="ML Inference service for real-time fraud detection.",
    lifespan=lifespan
)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "consumer_running": kafka_consumer.running,
        "producer_ready": kafka_producer.producer is not None,
        "active_model_version": registry.get_model().version
    }

@app.post("/reload")
async def reload_model():
    """Dynamically reload the ML model without restarting the service."""
    new_version = registry.reload_model()
    return {"status": "success", "new_model_version": new_version}
