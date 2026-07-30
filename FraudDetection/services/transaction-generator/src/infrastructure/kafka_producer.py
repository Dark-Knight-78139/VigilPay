import json
import logging
from typing import Optional
from confluent_kafka import Producer
from loguru import logger
from pydantic import BaseModel

class KafkaProducerClient:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[Producer] = None

    def start(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'transaction-generator',
            'enable.idempotence': True,
            'acks': 'all',
            'retries': 5,
            'max.in.flight.requests.per.connection': 5,
            'compression.type': 'lz4',
            'linger.ms': 10
        }
        self.producer = Producer(conf)
        logger.info(f"Kafka Producer started, connected to {self.bootstrap_servers}")

    def stop(self):
        if self.producer:
            logger.info("Flushing Kafka producer...")
            self.producer.flush()
            logger.info("Kafka producer flushed and stopped.")
            self.producer = None

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def publish(self, topic: str, key: str, event: BaseModel):
        if not self.producer:
            raise RuntimeError("Kafka producer is not started.")

        try:
            # Serialize the Pydantic event model to JSON string
            value_json = event.model_dump_json()
            
            # Produce the message
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8') if key else None,
                value=value_json.encode('utf-8'),
                callback=self.delivery_report
            )
            
            # Trigger any available delivery report callbacks
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to publish message to topic {topic}: {e}")
            raise
