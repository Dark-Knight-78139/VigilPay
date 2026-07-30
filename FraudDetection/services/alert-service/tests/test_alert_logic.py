import pytest
from datetime import datetime
from src.domain.events import AlertSeverity, FraudAlertPayload, FraudPredictionEvent, PredictionResult

def test_alert_severity_high():
    prediction = PredictionResult(
        transaction_id="T_1",
        customer_id="C_1",
        model_version="heuristic-v1",
        probability=0.85,
        is_fraud=True,
        explanation="high_velocity",
        latency_ms=10.0,
        timestamp=datetime.utcnow()
    )
    
    # Simulate handle_message logic
    severity = AlertSeverity.HIGH if prediction.probability > 0.8 else AlertSeverity.MEDIUM
    
    alert = FraudAlertPayload(
        transaction_id=prediction.transaction_id,
        customer_id=prediction.customer_id,
        severity=severity,
        probability=prediction.probability,
        explanation=prediction.explanation,
    )
    
    assert alert.severity == AlertSeverity.HIGH
    assert alert.transaction_id == "T_1"

def test_alert_severity_medium():
    prediction = PredictionResult(
        transaction_id="T_2",
        customer_id="C_2",
        model_version="heuristic-v1",
        probability=0.6,
        is_fraud=True,
        explanation="amount_anomaly",
        latency_ms=12.0,
        timestamp=datetime.utcnow()
    )
    
    severity = AlertSeverity.HIGH if prediction.probability > 0.8 else AlertSeverity.MEDIUM
    assert severity == AlertSeverity.MEDIUM
