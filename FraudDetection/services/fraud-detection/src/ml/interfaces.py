from abc import ABC, abstractmethod
from src.domain.events import EnrichedTransactionPayload

class FraudModel(ABC):
    @property
    @abstractmethod
    def version(self) -> str:
        """Returns the version identifier for the model."""
        pass

    @abstractmethod
    def predict(self, payload: EnrichedTransactionPayload) -> tuple[float, str]:
        """
        Runs inference on the payload.
        Returns a tuple of (probability, explanation).
        Probability is a float between 0.0 and 1.0.
        """
        pass
