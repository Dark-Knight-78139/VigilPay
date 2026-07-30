import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar
from pydantic import BaseModel, Field

PayloadT = TypeVar("PayloadT", bound=BaseModel)

class EventProducer(str, Enum):
    TRANSACTION_GENERATOR = "transaction-generator"
    FEATURE_ENGINEERING = "feature-engineering"
    FRAUD_DETECTION = "fraud-detection"

class BaseEvent(BaseModel, Generic[PayloadT]):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_version: str = "1.0"
    event_timestamp: datetime = Field(default_factory=datetime.utcnow)
    correlation_id: str
    trace_id: str
    producer: EventProducer
    payload: PayloadT

class TransactionPayload(BaseModel):
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
    pass

class FeaturesPayload(BaseModel):
    velocity_1h: int
    average_amount_24h: float
    country_change: bool

class EnrichedTransactionPayload(BaseModel):
    transaction: TransactionPayload
    features: FeaturesPayload

class EnrichedTransactionEvent(BaseEvent[EnrichedTransactionPayload]):
    pass
