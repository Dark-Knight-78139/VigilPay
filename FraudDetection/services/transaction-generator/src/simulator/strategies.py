import random
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List

from src.domain.entities import Customer, Merchant
from src.domain.events import EventProducer, RawTransactionEvent, TransactionPayload

class FraudStrategy(ABC):
    @abstractmethod
    def generate_fraud(self, customer: Customer, available_merchants: List[Merchant]) -> List[RawTransactionEvent]:
        """
        Generates a sequence of fraudulent transactions for a customer.
        Returns a list of events to be published in order.
        """
        pass

def _create_event(customer: Customer, merchant: Merchant, amount: float, location: str, device_id: str) -> RawTransactionEvent:
    payload = TransactionPayload(
        transaction_id=f"T_{uuid.uuid4().hex}",
        customer_id=customer.customer_id,
        merchant_id=merchant.merchant_id,
        amount=round(amount, 2),
        currency="USD",
        timestamp=datetime.utcnow(),
        device_id=device_id,
        location=location
    )
    return RawTransactionEvent(
        correlation_id=str(uuid.uuid4()),
        producer=EventProducer.TRANSACTION_GENERATOR,
        payload=payload
    )

class ImpossibleTravelStrategy(FraudStrategy):
    def generate_fraud(self, customer: Customer, available_merchants: List[Merchant]) -> List[RawTransactionEvent]:
        # Transaction 1: Home location
        m1 = random.choice(customer.preferred_merchants) if customer.preferred_merchants else random.choice(available_merchants)
        e1 = _create_event(customer, m1, m1.average_transaction_amount, customer.home_location, customer.devices[0])
        
        # Transaction 2: Distant location immediately after
        m2 = random.choice(available_merchants)
        distant_location = "RU-MOW" if customer.home_location != "RU-MOW" else "CN-SHA"
        e2 = _create_event(customer, m2, m2.average_transaction_amount * 2, distant_location, "UNKNOWN_DEVICE")
        
        return [e1, e2]

class CardTestingStrategy(FraudStrategy):
    def generate_fraud(self, customer: Customer, available_merchants: List[Merchant]) -> List[RawTransactionEvent]:
        events = []
        device_id = "UNKNOWN_DEVICE"
        location = customer.home_location
        
        # 3 small testing transactions
        for _ in range(3):
            m = random.choice(available_merchants)
            events.append(_create_event(customer, m, random.uniform(0.5, 2.0), location, device_id))
            
        # 1 massive transaction
        m_large = random.choice(available_merchants)
        events.append(_create_event(customer, m_large, random.uniform(1000, 5000), location, device_id))
        
        return events

class HighVelocityStrategy(FraudStrategy):
    def generate_fraud(self, customer: Customer, available_merchants: List[Merchant]) -> List[RawTransactionEvent]:
        events = []
        device_id = customer.devices[0] if customer.devices else "UNKNOWN_DEVICE"
        
        # 15 transactions back to back
        for _ in range(15):
            m = random.choice(available_merchants)
            amount = m.average_transaction_amount * random.uniform(0.8, 1.2)
            events.append(_create_event(customer, m, amount, customer.home_location, device_id))
            
        return events
