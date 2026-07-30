import pytest
from datetime import datetime
from src.domain.events import (
    TransactionPayload, 
    FeaturesPayload, 
    EnrichedTransactionPayload, 
    EnrichedTransactionEvent, 
    EventProducer
)

def test_enriched_event_serialization():
    tx_payload = TransactionPayload(
        transaction_id="T_123",
        customer_id="C_123",
        merchant_id="M_123",
        amount=55.50,
        currency="USD",
        timestamp=datetime.utcnow(),
        device_id="device_1",
        location="US-NY"
    )
    
    features = FeaturesPayload(
        velocity_1h=5,
        average_amount_24h=120.5,
        country_change=False
    )
    
    enriched_payload = EnrichedTransactionPayload(
        transaction=tx_payload,
        features=features
    )
    
    event = EnrichedTransactionEvent(
        correlation_id="corr_123",
        producer=EventProducer.FEATURE_ENGINEERING,
        payload=enriched_payload
    )
    
    json_str = event.model_dump_json()
    assert "T_123" in json_str
    assert "velocity_1h" in json_str
    assert "feature-engineering" in json_str
