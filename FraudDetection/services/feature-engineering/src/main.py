import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from loguru import logger
from pydantic_settings import BaseSettings

from src.domain.events import EventProducer, RawTransactionEvent, EnrichedTransactionEvent, EnrichedTransactionPayload
from src.infrastructure.kafka_client import KafkaConsumerClient, KafkaProducerClient
from src.infrastructure.redis_client import RedisStateStore
from src.features.compute import FeatureComputer

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_raw_topic: str = "bank.transactions.raw"
    kafka_enriched_topic: str = "bank.transactions.enriched"
    redis_url: str = "redis://localhost:6379/0"

settings = Settings()

# Dependencies
redis_store = RedisStateStore(redis_url=settings.redis_url)
feature_computer = FeatureComputer(store=redis_store)
kafka_producer = KafkaProducerClient(bootstrap_servers=settings.kafka_bootstrap_servers)
kafka_consumer = KafkaConsumerClient(
    bootstrap_servers=settings.kafka_bootstrap_servers,
    group_id="feature-engineering-group",
    topics=[settings.kafka_raw_topic]
)

consume_task = None

async def handle_message(raw_value: str):
    """Processes a raw transaction and produces an enriched transaction."""
    try:
        # Parse incoming event
        raw_event = RawTransactionEvent.model_validate_json(raw_value)
        
        # Compute features
        features = await feature_computer.compute_features(raw_event.payload)
        
        # Create enriched event
        enriched_payload = EnrichedTransactionPayload(
            transaction=raw_event.payload,
            features=features
        )
        enriched_event = EnrichedTransactionEvent(
            correlation_id=raw_event.correlation_id,
            producer=EventProducer.FEATURE_ENGINEERING,
            payload=enriched_payload
        )
        
        # Publish
        kafka_producer.publish(
            topic=settings.kafka_enriched_topic,
            key=raw_event.payload.customer_id,
            event=enriched_event
        )
        logger.debug(f"Processed transaction {raw_event.payload.transaction_id}")
    except Exception as e:
        logger.error(f"Error handling message: {e}")
        raise # Reraise to prevent offset commit

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await redis_store.connect()
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
    await redis_store.disconnect()

app = FastAPI(
    title="Feature Engineering Service",
    description="Stateful real-time feature computation.",
    lifespan=lifespan
)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "redis_connected": redis_store.client is not None,
        "consumer_running": kafka_consumer.running,
        "producer_ready": kafka_producer.producer is not None
    }
