import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar
from pydantic import BaseModel, Field

PayloadT = TypeVar("PayloadT", bound=BaseModel)

class EventProducer(str, Enum):
    FRAUD_DETECTION = "fraud-detection"

class BaseEvent(BaseModel, Generic[PayloadT]):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_version: str = "1.0"
    event_timestamp: datetime = Field(default_factory=datetime.utcnow)
    correlation_id: str
    trace_id: str
    producer: EventProducer
    payload: PayloadT

class PredictionResult(BaseModel):
    transaction_id: str
    customer_id: str
    model_version: str
    probability: float
    is_fraud: bool
    explanation: str
    latency_ms: float
    timestamp: datetime

class FraudPredictionEvent(BaseEvent[PredictionResult]):
    pass
