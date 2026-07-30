import pytest
from datetime import datetime
from src.ml.baseline_model import BaselineHeuristicModel
from src.domain.events import EnrichedTransactionPayload, TransactionPayload, FeaturesPayload

def create_payload(velocity: int, avg_amount: float, country_change: bool, current_amount: float) -> EnrichedTransactionPayload:
    tx = TransactionPayload(
        transaction_id="T_1",
        customer_id="C_1",
        merchant_id="M_1",
        amount=current_amount,
        timestamp=datetime.utcnow(),
        location="US-NY"
    )
    features = FeaturesPayload(
        velocity_1h=velocity,
        average_amount_24h=avg_amount,
        country_change=country_change
    )
    return EnrichedTransactionPayload(transaction=tx, features=features)

def test_baseline_model_normal():
    model = BaselineHeuristicModel()
    payload = create_payload(velocity=2, avg_amount=100.0, country_change=False, current_amount=150.0)
    
    prob, explanation = model.predict(payload)
    
    assert prob == 0.01
    assert explanation == "normal_behavior"

def test_baseline_model_high_velocity():
    model = BaselineHeuristicModel()
    payload = create_payload(velocity=15, avg_amount=100.0, country_change=False, current_amount=150.0)
    
    prob, explanation = model.predict(payload)
    
    assert prob == 0.41 # 0.01 + 0.4
    assert "high_velocity" in explanation

def test_baseline_model_amount_anomaly():
    model = BaselineHeuristicModel()
    payload = create_payload(velocity=2, avg_amount=100.0, country_change=False, current_amount=1000.0)
    
    prob, explanation = model.predict(payload)
    
    assert prob == 0.31 # 0.01 + 0.3
    assert "amount_anomaly" in explanation

def test_baseline_model_all_flags():
    model = BaselineHeuristicModel()
    payload = create_payload(velocity=15, avg_amount=100.0, country_change=True, current_amount=1000.0)
    
    prob, explanation = model.predict(payload)
    
    assert prob == 0.91 # 0.01 + 0.4 + 0.3 + 0.2
    assert "high_velocity" in explanation
    assert "amount_anomaly" in explanation
    assert "country_change" in explanation
