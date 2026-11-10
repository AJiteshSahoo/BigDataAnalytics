"""
Domain models and data schemas for Real-Time Payment Fraud Stream Processing.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional
import time
import uuid


class RiskStatus(str, Enum):
    APPROVED = "APPROVED"
    REVIEW = "REVIEW"
    DECLINED = "DECLINED"


class AnomalyType(str, Enum):
    VELOCITY_SPIKE = "VELOCITY_SPIKE"
    GEO_ANOMALY = "GEO_ANOMALY"
    HIGH_VALUE_DRAIN = "HIGH_VALUE_DRAIN"
    NONE = "NONE"


@dataclass
class Transaction:
    """Represents a single payment transaction event in the streaming pipeline."""
    transaction_id: str
    user_id: str
    amount: float
    timestamp: float  # Unix epoch seconds (event time)
    merchant_category: str
    location: str  # ISO country code (e.g. US, GB, IN, DE, SG, JP)
    ip_address: str
    
    # Ground-truth metadata injected by simulator for benchmark evaluation
    is_synthetic_fraud: bool = False
    injected_anomaly_type: AnomalyType = AnomalyType.NONE

    def to_dict(self) -> dict:
        return {
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "amount": round(self.amount, 2),
            "timestamp": self.timestamp,
            "merchant_category": self.merchant_category,
            "location": self.location,
            "ip_address": self.ip_address,
            "is_synthetic_fraud": self.is_synthetic_fraud,
            "injected_anomaly_type": self.injected_anomaly_type.value,
        }


@dataclass
class RuleEvaluation:
    """Outcome of a single fraud detection rule evaluation."""
    rule_name: str
    triggered: bool
    score_impact: float
    explanation: str


@dataclass
class RiskResult:
    """Composite scoring output produced by the Real-Time Risk Engine."""
    transaction: Transaction
    risk_score: float  # 0 to 100
    status: RiskStatus
    rules_triggered: List[RuleEvaluation] = field(default_factory=list)
    latency_ms: float = 0.0
    evaluated_at: float = field(default_factory=time.time)

    @property
    def primary_reason(self) -> str:
        triggered = [r.explanation for r in self.rules_triggered if r.triggered]
        if triggered:
            return " | ".join(triggered)
        return "Normal baseline transaction pattern"

    def to_dict(self) -> dict:
        return {
            **self.transaction.to_dict(),
            "risk_score": round(self.risk_score, 1),
            "status": self.status.value,
            "primary_reason": self.primary_reason,
            "rules_count": len([r for r in self.rules_triggered if r.triggered]),
            "latency_ms": round(self.latency_ms, 3),
            "evaluated_at": self.evaluated_at,
        }


@dataclass
class UserProfile:
    """Static and baseline profile configuration for synthetic users."""
    user_id: str
    home_country: str
    typical_avg_amount: float
    typical_std_amount: float
    favorite_categories: List[str]
    device_ip_prefix: str
