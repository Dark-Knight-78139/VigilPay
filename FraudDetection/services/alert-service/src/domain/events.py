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
    ALERT_SERVICE = "alert-service"

class BaseEvent(BaseModel, Generic[PayloadT]):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_version: str = "1.0"
    event_timestamp: datetime = Field(default_factory=datetime.utcnow)
    correlation_id: str
    trace_id: str
    producer: EventProducer
    payload: PayloadT

# --- Consumed Event Structures ---

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

# --- Produced Event Structures ---

class AlertSeverity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class FraudAlertPayload(BaseModel):
    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    transaction_id: str
    customer_id: str
    severity: AlertSeverity
    probability: float
    explanation: str
    action_required: str = "REVIEW_TRANSACTION"
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class FraudAlertEvent(BaseEvent[FraudAlertPayload]):
    pass
