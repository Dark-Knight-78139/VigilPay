import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generic, TypeVar
from pydantic import BaseModel, Field

PayloadT = TypeVar("PayloadT", bound=BaseModel)

class EventProducer(str, Enum):
    TRANSACTION_GENERATOR = "transaction-generator"
    FEATURE_ENGINEERING = "feature-engineering"
    FRAUD_DETECTION = "fraud-detection"

class BaseEvent(BaseModel, Generic[PayloadT]):
    """
    Standard event envelope for all Kafka messages.
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_version: str = "1.0"
    event_timestamp: datetime = Field(default_factory=datetime.utcnow)
    correlation_id: str
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    producer: EventProducer
    payload: PayloadT

class TransactionPayload(BaseModel):
    """
    Payload for a raw banking transaction.
    """
    transaction_id: str
    customer_id: str
    merchant_id: str
    amount: float
    currency: str = "USD"
    timestamp: datetime
    device_id: str | None = None
    ip_address: str | None = None
    location: str | None = None

class RawTransactionEvent(BaseEvent[TransactionPayload]):
    """
    Specific event type for bank.transactions.raw
    """
    pass
