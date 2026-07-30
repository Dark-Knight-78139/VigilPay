import asyncio
from typing import Callable, Optional
from confluent_kafka import Consumer, KafkaError
from loguru import logger

class KafkaConsumerClient:
    def __init__(self, bootstrap_servers: str, group_id: str, topics: list[str]):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics
        self.consumer: Optional[Consumer] = None
        self.running = False

    def start(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False, 
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe(self.topics)
        self.running = True
        logger.info(f"Kafka Consumer started for topics {self.topics}")

    def stop(self):
        self.running = False
        if self.consumer:
            self.consumer.close()
            self.consumer = None
            logger.info("Kafka Consumer stopped.")

    async def consume_loop(self, message_handler: Callable):
        if not self.consumer:
            raise RuntimeError("Consumer not started")

        while self.running:
            msg = await asyncio.to_thread(self.consumer.poll, 1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

            try:
                raw_value = msg.value().decode('utf-8')
                await message_handler(raw_value)
                await asyncio.to_thread(self.consumer.commit, msg)
            except Exception as e:
                logger.error(f"Failed to process message: {e}")
