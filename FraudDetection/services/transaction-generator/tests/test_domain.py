import pytest
from datetime import datetime
from src.domain.entities import Customer, Merchant
from src.domain.events import EventProducer, RawTransactionEvent, TransactionPayload

def test_merchant_creation():
    merchant = Merchant(
        merchant_id="M_123",
        name="Test Merchant",
        category="Groceries",
        location="US-NY",
        risk_score=0.05,
        average_transaction_amount=50.0
    )
    assert merchant.merchant_id == "M_123"

def test_customer_creation():
    customer = Customer(
        customer_id="C_123",
        name="Test Customer",
        salary=100000,
        risk_profile="low"
    )
    assert customer.customer_id == "C_123"
    assert len(customer.devices) > 0 # Devices should be auto-generated

def test_raw_transaction_event_serialization():
    payload = TransactionPayload(
        transaction_id="T_123",
        customer_id="C_123",
        merchant_id="M_123",
        amount=55.50,
        currency="USD",
        timestamp=datetime.utcnow(),
        device_id="device_1",
        location="US-NY"
    )
    
    event = RawTransactionEvent(
        correlation_id="corr_123",
        producer=EventProducer.TRANSACTION_GENERATOR,
        payload=payload
    )
    
    json_str = event.model_dump_json()
    assert "T_123" in json_str
    assert "corr_123" in json_str
    assert "transaction-generator" in json_str
