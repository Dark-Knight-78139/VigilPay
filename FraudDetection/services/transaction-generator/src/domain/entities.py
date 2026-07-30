from dataclasses import dataclass, field
from typing import List
import random
import uuid

@dataclass
class Merchant:
    merchant_id: str
    name: str
    category: str
    location: str
    risk_score: float
    average_transaction_amount: float

@dataclass
class Customer:
    customer_id: str
    name: str
    salary: float
    risk_profile: str # e.g., 'low', 'medium', 'high'
    preferred_merchants: List[Merchant] = field(default_factory=list)
    devices: List[str] = field(default_factory=list)
    home_location: str = "US"

    def __post_init__(self):
        if not self.devices:
            self.devices = [str(uuid.uuid4()) for _ in range(random.randint(1, 3))]
