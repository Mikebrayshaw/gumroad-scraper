"""Canonical data models for scraped marketplace products.

These models provide a stable shape for products across scrapes and
platforms so downstream ingestion, diffing, and export steps can treat
products uniformly.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass(slots=True)
class ProductSnapshot:
    """Canonical snapshot of a marketplace product at a point in time."""

    platform: str
    product_id: str
    url: str
    title: str
    creator_name: str
    creator_url: Optional[str]
    category: Optional[str]
    price_amount: Optional[float]
    price_currency: Optional[str]
    price_is_pwyw: bool
    rating_avg: Optional[float]
    rating_count: Optional[int]
    sales_count: Optional[int]
    revenue_estimate: Optional[float]
    revenue_confidence: str
    subcategory: Optional[str] = None
    description: Optional[str] = None
    mixed_review_count: Optional[int] = None
    mixed_review_percent: Optional[float] = None
    tags: List[str] = field(default_factory=list)
    scraped_at: datetime = field(default_factory=datetime.utcnow)
    raw_source_hash: str = ""

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["scraped_at"] = self.scraped_at.isoformat()
        return payload

    @classmethod
    def with_hash(cls, **kwargs) -> "ProductSnapshot":
        snapshot = cls(**kwargs)
        snapshot.raw_source_hash = snapshot.compute_hash()
        return snapshot

    def compute_hash(self) -> str:
        """Generate a deterministic hash of the key facts for change tracking."""

        serializable = self.to_dict().copy()
        serializable.pop("raw_source_hash", None)
        return hashlib.sha256(json.dumps(serializable, sort_keys=True).encode("utf-8")).hexdigest()


def estimate_revenue(
    price_amount: Optional[float],
    sales_count: Optional[int],
    price_is_pwyw: bool,
    price_currency: Optional[str],
) -> tuple[Optional[float], str]:
    """Estimate revenue and return a (value, confidence) tuple.

    Assumptions:
    - Revenue is approximated as ``price_amount * sales_count`` when both are present.
    - Revenue is discounted by a conservative multiplier.
    - If sales are missing, revenue is ``None`` with ``low`` confidence.
    - Pay-what-you-want pricing and unknown currency each reduce confidence by one tier.
    - Confidence tiers: ``high`` -> ``med`` -> ``low``.
    """
    conservative_multiplier = 0.85

    def downgrade(confidence: str) -> str:
        if confidence == "high":
            return "med"
        if confidence == "med":
            return "low"
        return "low"

    if sales_count is None or price_amount is None:
        return None, "low"

    revenue = round(price_amount * sales_count * conservative_multiplier, 2)
    confidence = "high"

    if price_is_pwyw or not price_currency:
        confidence = downgrade(confidence)
    if price_currency not in {"USD", "$", "usd"}:
        confidence = downgrade(confidence)

    return revenue, confidence
