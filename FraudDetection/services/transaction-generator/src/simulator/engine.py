import asyncio
import random
import uuid
from datetime import datetime
from typing import List

from loguru import logger

from src.domain.entities import Customer, Merchant
from src.domain.events import EventProducer, RawTransactionEvent, TransactionPayload
from src.infrastructure.kafka_producer import KafkaProducerClient


class SimulationEngine:
    def __init__(self, kafka_client: KafkaProducerClient, topic: str):
        self.kafka_client = kafka_client
        self.topic = topic
        self.running = False
        self.customers: List[Customer] = []
        self.merchants: List[Merchant] = []

    def initialize_data(self, num_customers: int = 100, num_merchants: int = 20):
        """Generate some baseline realistic data."""
        logger.info(f"Initializing simulator with {num_customers} customers and {num_merchants} merchants.")
        categories = ["Groceries", "Electronics", "Travel", "Dining", "Retail"]
        locations = ["US-NY", "US-CA", "US-TX", "UK-LON", "FR-PAR"]

        self.merchants = [
            Merchant(
                merchant_id=f"M_{uuid.uuid4().hex[:8]}",
                name=f"Merchant {i}",
                category=random.choice(categories),
                location=random.choice(locations),
                risk_score=random.uniform(0.01, 0.1),
                average_transaction_amount=random.uniform(10.0, 500.0)
            ) for i in range(num_merchants)
        ]

        self.customers = [
            Customer(
                customer_id=f"C_{uuid.uuid4().hex[:8]}",
                name=f"Customer {i}",
                salary=random.uniform(40000, 150000),
                risk_profile=random.choice(["low", "medium", "high"]),
                preferred_merchants=random.sample(self.merchants, k=random.randint(1, 5)),
                home_location=random.choice(locations)
            ) for i in range(num_customers)
        ]

    async def _generate_transaction(self, customer: Customer) -> RawTransactionEvent:
        """Generate a single realistic transaction for a customer."""
        merchant = random.choice(customer.preferred_merchants)
        
        # Add some random variance to the amount
        amount_variance = random.uniform(0.5, 1.5)
        amount = round(merchant.average_transaction_amount * amount_variance, 2)
        
        device_id = random.choice(customer.devices)

        payload = TransactionPayload(
            transaction_id=f"T_{uuid.uuid4().hex}",
            customer_id=customer.customer_id,
            merchant_id=merchant.merchant_id,
            amount=amount,
            currency="USD",
            timestamp=datetime.utcnow(),
            device_id=device_id,
            location=customer.home_location
        )

        event = RawTransactionEvent(
            correlation_id=str(uuid.uuid4()),
            producer=EventProducer.TRANSACTION_GENERATOR,
            payload=payload
        )
        return event

    async def run(self, events_per_second: int = 10):
        """Main simulation loop."""
        if not self.customers or not self.merchants:
            self.initialize_data()

        self.running = True
        logger.info(f"Simulation engine started. Target EPS: {events_per_second}")
        
        sleep_interval = 1.0 / events_per_second if events_per_second > 0 else 1.0

        while self.running:
            try:
                # Pick a random customer
                customer = random.choice(self.customers)
                
                # Generate transaction event
                event = await self._generate_transaction(customer)
                
                # Publish to Kafka
                self.kafka_client.publish(
                    topic=self.topic,
                    key=customer.customer_id, # Partition by customer_id to maintain ordering
                    event=event
                )
                
                await asyncio.sleep(sleep_interval)
            except Exception as e:
                logger.error(f"Error in simulation loop: {e}")
                await asyncio.sleep(1) # Backoff on error

    def stop(self):
        logger.info("Stopping simulation engine...")
        self.running = False
