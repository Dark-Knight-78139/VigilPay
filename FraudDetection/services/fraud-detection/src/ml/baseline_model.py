from src.domain.events import EnrichedTransactionPayload
from src.ml.interfaces import FraudModel

class BaselineHeuristicModel(FraudModel):
    """
    A concrete implementation of FraudModel that uses simple heuristics 
    to calculate fraud probability. This acts as our baseline model.
    """
    def __init__(self, version: str = "heuristic-v1"):
        self._version = version

    @property
    def version(self) -> str:
        return self._version

    def predict(self, payload: EnrichedTransactionPayload) -> tuple[float, str]:
        features = payload.features
        transaction = payload.transaction
        
        probability = 0.01 # Base risk
        reasons = []

        # High velocity logic
        if features.velocity_1h > 10:
            probability += 0.4
            reasons.append("high_velocity")
            
        # Amount anomaly logic (transaction amount vs 24h average)
        if features.average_amount_24h > 0:
            ratio = transaction.amount / features.average_amount_24h
            if ratio > 5.0:
                probability += 0.3
                reasons.append("amount_anomaly")
            
        # Country change logic
        if features.country_change:
            probability += 0.2
            reasons.append("country_change")

        probability = min(probability, 0.99)
        explanation = ", ".join(reasons) if reasons else "normal_behavior"
        
        return probability, explanation
