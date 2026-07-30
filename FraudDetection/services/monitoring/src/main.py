import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from loguru import logger
from pydantic_settings import BaseSettings
from prometheus_client import make_asgi_app

from src.domain.events import FraudPredictionEvent
from src.infrastructure.kafka_client import KafkaConsumerClient
from src.metrics import drift_monitor

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_predictions_topic: str = "bank.transactions.predictions"

settings = Settings()

# Dependencies
kafka_consumer = KafkaConsumerClient(
    bootstrap_servers=settings.kafka_bootstrap_servers,
    group_id="monitoring-service-group",
    topics=[settings.kafka_predictions_topic]
)

consume_task = None

async def handle_message(raw_value: str):
    try:
        prediction_event = FraudPredictionEvent.model_validate_json(raw_value)
        payload = prediction_event.payload
        
        # Record the prediction in our Prometheus metrics
        drift_monitor.record_prediction(
            model_version=payload.model_version,
            probability=payload.probability,
            is_fraud=payload.is_fraud,
            latency_ms=payload.latency_ms
        )
        
    except Exception as e:
        logger.error(f"Error handling message: {e}")
        raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    kafka_consumer.start()
    
    global consume_task
    consume_task = asyncio.create_task(kafka_consumer.consume_loop(handle_message))
    
    yield
    
    kafka_consumer.stop()
    if consume_task:
        await consume_task

app = FastAPI(
    title="Monitoring Service",
    description="MLOps observability service for model drift detection.",
    lifespan=lifespan
)

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "consumer_running": kafka_consumer.running,
    }
