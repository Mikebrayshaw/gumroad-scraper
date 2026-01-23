"""
Opportunity Scoring Module for Gumroad Products

Computes an opportunity score (0-100) for products based on signals
like rating, reviews, price, and sales velocity.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from typing import Iterable, Optional


@dataclass
class ScoredProduct:
    """Product with opportunity score and supporting signals."""
    opportunity_score: float  # 0-100 scale
    score_notes: str  # Explanation of score factors
    rating_signal: float  # 0-1, higher is better
    review_health_signal: float  # 0-1, based on review count and mixed%
    price_signal: float  # 0-1, favoring $10-$79 range
    sales_velocity_signal: float  # 0-1, based on sales count
    revenue_signal: float  # 0-1, based on estimated revenue


@dataclass
class TrendScore:
    """Trending score and velocity metrics for a product."""
    trend_score: float  # 0-100 scale
    score_notes: str
    sales_count_delta: int
    revenue_delta: float
    rating_count_delta: int
    last_week_sales_delta: int
    previous_week_sales_delta: int


# Scoring weights - adjust to tune the algorithm
WEIGHTS = {
    'rating': 0.25,
    'review_health': 0.20,
    'price': 0.15,
    'sales_velocity': 0.25,
    'revenue': 0.15,
}


def compute_rating_signal(
    average_rating: Optional[float],
    total_reviews: int,
) -> tuple[float, str]:
    """
    Compute rating signal (0-1).
    Favors products with 4.3+ rating and penalizes low ratings.
    """
    if average_rating is None:
        return 0.3, "no rating"

    if total_reviews == 0:
        return 0.3, "no reviews"

    # Rating score: 4.3+ is excellent, 4.0+ is good, below 3.5 is poor
    if average_rating >= 4.7:
        score = 1.0
        note = "excellent rating (4.7+)"
    elif average_rating >= 4.3:
        score = 0.85
        note = "great rating (4.3+)"
    elif average_rating >= 4.0:
        score = 0.7
        note = "good rating (4.0+)"
    elif average_rating >= 3.5:
        score = 0.5
        note = "average rating"
    else:
        score = 0.2
        note = "low rating"

    return score, note


def compute_review_health_signal(
    total_reviews: int,
    mixed_review_percent: float,
) -> tuple[float, str]:
    """
    Compute review health signal (0-1).
    Favors products with 20+ reviews and low mixed review percentage.
    """
    notes = []

    # Review count factor
    if total_reviews >= 100:
        count_score = 1.0
        notes.append("100+ reviews")
    elif total_reviews >= 50:
        count_score = 0.85
        notes.append("50+ reviews")
    elif total_reviews >= 20:
        count_score = 0.7
        notes.append("20+ reviews")
    elif total_reviews >= 10:
        count_score = 0.5
        notes.append("10+ reviews")
    elif total_reviews >= 5:
        count_score = 0.35
        notes.append("few reviews")
    else:
        count_score = 0.2
        notes.append("minimal reviews")

    # Mixed review penalty (2-4 star reviews indicate quality issues)
    # Lower mixed% is better - means mostly 5-star or clear negative feedback
    if mixed_review_percent <= 15:
        mixed_score = 1.0
    elif mixed_review_percent <= 25:
        mixed_score = 0.8
    elif mixed_review_percent <= 40:
        mixed_score = 0.6
    else:
        mixed_score = 0.4
        notes.append("high mixed reviews")

    # Combine scores
    final_score = (count_score * 0.7) + (mixed_score * 0.3)
    return final_score, ", ".join(notes)


def compute_price_signal(price_usd: float) -> tuple[float, str]:
    """
    Compute price signal (0-1).
    Favors moderate prices ($10-$79) - the sweet spot for impulse buys
    with decent margins.
    """
    if price_usd == 0:
        return 0.3, "free product"

    # Sweet spot: $10-$79
    if 10 <= price_usd <= 79:
        if 15 <= price_usd <= 49:
            return 1.0, "ideal price range ($15-$49)"
        return 0.85, "good price range ($10-$79)"

    # Below sweet spot - potential volume play
    if price_usd < 10:
        if price_usd >= 5:
            return 0.6, "low price point"
        return 0.4, "very low price"

    # Above sweet spot - premium territory
    if price_usd <= 149:
        return 0.7, "premium price ($80-$149)"
    if price_usd <= 299:
        return 0.5, "high price ($150-$299)"

    return 0.35, "very high price ($300+)"


def compute_sales_velocity_signal(
    sales_count: Optional[int],
) -> tuple[float, str]:
    """
    Compute sales velocity signal (0-1).
    Higher sales indicate proven demand.
    """
    if sales_count is None:
        return 0.3, "no sales data"

    if sales_count >= 10000:
        return 1.0, "viral (10K+ sales)"
    if sales_count >= 5000:
        return 0.9, "bestseller (5K+ sales)"
    if sales_count >= 1000:
        return 0.8, "strong sales (1K+)"
    if sales_count >= 500:
        return 0.7, "good sales (500+)"
    if sales_count >= 100:
        return 0.55, "moderate sales (100+)"
    if sales_count >= 50:
        return 0.4, "some sales (50+)"
    if sales_count >= 10:
        return 0.3, "early traction"

    return 0.2, "minimal sales"


def compute_revenue_signal(
    estimated_revenue: Optional[float],
) -> tuple[float, str]:
    """
    Compute revenue signal (0-1).
    Higher revenue indicates successful monetization.
    """
    if estimated_revenue is None:
        return 0.3, "no revenue data"

    if estimated_revenue >= 100000:
        return 1.0, "top earner ($100K+)"
    if estimated_revenue >= 50000:
        return 0.9, "high earner ($50K+)"
    if estimated_revenue >= 20000:
        return 0.8, "strong revenue ($20K+)"
    if estimated_revenue >= 10000:
        return 0.7, "good revenue ($10K+)"
    if estimated_revenue >= 5000:
        return 0.6, "moderate revenue ($5K+)"
    if estimated_revenue >= 1000:
        return 0.45, "some revenue ($1K+)"

    return 0.3, "low revenue"


def score_product(
    product_name: str,
    price_usd: float,
    average_rating: Optional[float],
    total_reviews: int,
    mixed_review_percent: float,
    sales_count: Optional[int],
    estimated_revenue: Optional[float],
) -> ScoredProduct:
    """
    Compute opportunity score and signals for a product.

    Args:
        product_name: Name of the product (for notes)
        price_usd: Price in USD
        average_rating: Average star rating (1-5) or None
        total_reviews: Number of reviews
        mixed_review_percent: Percentage of 2-4 star reviews
        sales_count: Number of sales or None
        estimated_revenue: Estimated revenue in USD or None

    Returns:
        ScoredProduct with opportunity_score (0-100) and signals
    """
    notes = []

    # Compute individual signals
    rating_signal, rating_note = compute_rating_signal(average_rating, total_reviews)
    notes.append(f"Rating: {rating_note}")

    review_signal, review_note = compute_review_health_signal(total_reviews, mixed_review_percent)
    notes.append(f"Reviews: {review_note}")

    price_signal, price_note = compute_price_signal(price_usd)
    notes.append(f"Price: {price_note}")

    sales_signal, sales_note = compute_sales_velocity_signal(sales_count)
    notes.append(f"Sales: {sales_note}")

    revenue_signal, revenue_note = compute_revenue_signal(estimated_revenue)
    notes.append(f"Revenue: {revenue_note}")

    # Weighted combination
    raw_score = (
        rating_signal * WEIGHTS['rating'] +
        review_signal * WEIGHTS['review_health'] +
        price_signal * WEIGHTS['price'] +
        sales_signal * WEIGHTS['sales_velocity'] +
        revenue_signal * WEIGHTS['revenue']
    )

    # Scale to 0-100
    opportunity_score = round(raw_score * 100, 1)

    return ScoredProduct(
        opportunity_score=opportunity_score,
        score_notes="; ".join(notes),
        rating_signal=round(rating_signal, 2),
        review_health_signal=round(review_signal, 2),
        price_signal=round(price_signal, 2),
        sales_velocity_signal=round(sales_signal, 2),
        revenue_signal=round(revenue_signal, 2),
    )


def score_product_dict(product: dict) -> dict:
    """
    Score a product from a dictionary (e.g., from dataclass asdict()).
    Returns the original dict with score fields added.
    """
    scored = score_product(
        product_name=product.get('product_name', ''),
        price_usd=product.get('price_usd', 0),
        average_rating=product.get('average_rating'),
        total_reviews=product.get('total_reviews', 0),
        mixed_review_percent=product.get('mixed_review_percent', 0),
        sales_count=product.get('sales_count'),
        estimated_revenue=product.get('estimated_revenue'),
    )

    return {
        **product,
        'opportunity_score': scored.opportunity_score,
        'score_notes': scored.score_notes,
        'rating_signal': scored.rating_signal,
        'review_health_signal': scored.review_health_signal,
        'price_signal': scored.price_signal,
        'sales_velocity_signal': scored.sales_velocity_signal,
        'revenue_signal': scored.revenue_signal,
    }


def get_top_scored_products(
    products: list[dict],
    n: int = 10,
    min_score: float = 0,
    category: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_rating: Optional[float] = None,
    min_reviews: Optional[int] = None,
) -> list[dict]:
    """
    Get top N products by opportunity score with optional filters.

    Args:
        products: List of product dicts (will be scored if not already)
        n: Number of products to return
        min_score: Minimum opportunity score (0-100)
        category: Filter by category
        min_price: Minimum price in USD
        max_price: Maximum price in USD
        min_rating: Minimum average rating
        min_reviews: Minimum number of reviews

    Returns:
        Top N products sorted by opportunity_score descending
    """
    # Score products if needed
    scored = []
    for p in products:
        if 'opportunity_score' not in p:
            p = score_product_dict(p)
        scored.append(p)

    # Apply filters
    filtered = []
    for p in scored:
        if p['opportunity_score'] < min_score:
            continue
        if category and p.get('category', '').lower() != category.lower():
            continue
        if min_price is not None and p.get('price_usd', 0) < min_price:
            continue
        if max_price is not None and p.get('price_usd', 0) > max_price:
            continue
        if min_rating is not None:
            rating = p.get('average_rating')
            if rating is None or rating < min_rating:
                continue
        if min_reviews is not None and p.get('total_reviews', 0) < min_reviews:
            continue
        filtered.append(p)

    # Sort by score and return top N
    filtered.sort(key=lambda x: x['opportunity_score'], reverse=True)
    return filtered[:n]


def get_score_breakdown(scored_product: dict) -> str:
    """
    Generate a human-readable breakdown of a product's score.
    """
    lines = [
        f"Opportunity Score: {scored_product.get('opportunity_score', 'N/A')}/100",
        "",
        "Signal Breakdown:",
        f"  Rating:        {scored_product.get('rating_signal', 0):.0%} (weight: {WEIGHTS['rating']:.0%})",
        f"  Review Health: {scored_product.get('review_health_signal', 0):.0%} (weight: {WEIGHTS['review_health']:.0%})",
        f"  Price:         {scored_product.get('price_signal', 0):.0%} (weight: {WEIGHTS['price']:.0%})",
        f"  Sales:         {scored_product.get('sales_velocity_signal', 0):.0%} (weight: {WEIGHTS['sales_velocity']:.0%})",
        f"  Revenue:       {scored_product.get('revenue_signal', 0):.0%} (weight: {WEIGHTS['revenue']:.0%})",
        "",
        "Notes:",
        f"  {scored_product.get('score_notes', 'N/A')}",
    ]
    return "\n".join(lines)


def _coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _latest_snapshot_before(
    snapshots: Iterable[dict],
    cutoff: datetime,
) -> Optional[dict]:
    return max(
        (snap for snap in snapshots if _coerce_datetime(snap["scraped_at"]) <= cutoff),
        key=lambda snap: _coerce_datetime(snap["scraped_at"]),
        default=None,
    )


def _scaled_signal(value: float, max_value: float) -> float:
    if max_value <= 0:
        return 0.0
    return min(max(value / max_value, 0.0), 1.0)


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if percentile <= 0:
        return min(values)
    if percentile >= 1:
        return max(values)
    sorted_values = sorted(values)
    index = max(0, math.ceil(percentile * len(sorted_values)) - 1)
    return sorted_values[index]


def _adaptive_scale_from_snapshots(
    sorted_snapshots: list[dict],
    metric: str,
    default_max: float,
    *,
    percentile: float = 0.9,
    buffer: float = 1.1,
) -> float:
    deltas: list[float] = []
    for older, newer in zip(sorted_snapshots, sorted_snapshots[1:]):
        delta = (newer.get(metric) or 0) - (older.get(metric) or 0)
        if delta > 0:
            deltas.append(float(delta))

    if not deltas:
        return default_max

    scale = _percentile(deltas, percentile) * buffer
    return max(default_max, scale)


def score_trend_from_snapshots(
    snapshots: list[dict],
    *,
    now: Optional[datetime] = None,
    min_rating: float = 4.0,
    min_sales: int = 10,
) -> TrendScore:
    """
    Score product trend based on snapshot velocity and recent growth.

    snapshots must include: sales_count, revenue_estimate, rating_count,
    rating_avg, scraped_at.
    """
    if not snapshots:
        return TrendScore(
            trend_score=0.0,
            score_notes="no snapshots",
            sales_count_delta=0,
            revenue_delta=0.0,
            rating_count_delta=0,
            last_week_sales_delta=0,
            previous_week_sales_delta=0,
        )

    sorted_snaps = sorted(
        snapshots,
        key=lambda snap: _coerce_datetime(snap["scraped_at"]),
    )
    latest = sorted_snaps[-1]
    current_sales = latest.get("sales_count") or 0
    current_rating_avg = latest.get("rating_avg")

    if current_rating_avg is None or current_rating_avg < min_rating:
        return TrendScore(
            trend_score=0.0,
            score_notes=f"filtered: rating < {min_rating}",
            sales_count_delta=0,
            revenue_delta=0.0,
            rating_count_delta=0,
            last_week_sales_delta=0,
            previous_week_sales_delta=0,
        )
    if current_sales < min_sales:
        return TrendScore(
            trend_score=0.0,
            score_notes=f"filtered: sales < {min_sales}",
            sales_count_delta=0,
            revenue_delta=0.0,
            rating_count_delta=0,
            last_week_sales_delta=0,
            previous_week_sales_delta=0,
        )

    end_time = now or _coerce_datetime(latest["scraped_at"])
    last_week_start = end_time - timedelta(days=7)
    prev_week_start = end_time - timedelta(days=14)

    last_week_start_snap = _latest_snapshot_before(sorted_snaps, last_week_start)
    prev_week_start_snap = _latest_snapshot_before(sorted_snaps, prev_week_start)

    def _delta(metric: str, newer: dict, older: Optional[dict]) -> float:
        newer_value = newer.get(metric) or 0
        older_value = (older or {}).get(metric) or 0
        return newer_value - older_value

    sales_count_delta = int(_delta("sales_count", latest, last_week_start_snap))
    revenue_delta = float(_delta("revenue_estimate", latest, last_week_start_snap))
    rating_count_delta = int(_delta("rating_count", latest, last_week_start_snap))

    previous_week_sales_delta = int(
        _delta("sales_count", last_week_start_snap or latest, prev_week_start_snap)
    )

    sales_scale = _adaptive_scale_from_snapshots(sorted_snaps, "sales_count", 100.0)
    revenue_scale = _adaptive_scale_from_snapshots(sorted_snaps, "revenue_estimate", 2000.0)
    rating_scale = _adaptive_scale_from_snapshots(sorted_snaps, "rating_count", 25.0)

    sales_signal = _scaled_signal(sales_count_delta, sales_scale)
    revenue_signal = _scaled_signal(revenue_delta, revenue_scale)
    rating_signal = _scaled_signal(rating_count_delta, rating_scale)

    base_score = (
        sales_signal * 0.5 +
        revenue_signal * 0.3 +
        rating_signal * 0.2
    ) * 100

    growth_boost = 1.0
    if sales_count_delta > previous_week_sales_delta:
        growth_boost += 0.15
    if rating_count_delta > 0 and rating_count_delta > previous_week_sales_delta:
        growth_boost += 0.05

    thresholds = [10, 50, 100, 250, 500, 1000]
    previous_sales = (last_week_start_snap or {}).get("sales_count") or 0
    threshold_bonus = 0
    crossed = [
        t for t in thresholds
        if previous_sales < t <= current_sales
    ]
    if crossed:
        threshold_bonus = min(10, len(crossed) * 3)

    trend_score = min((base_score * growth_boost) + threshold_bonus, 100)

    notes = [
        f"sales delta 7d: {sales_count_delta}",
        f"revenue delta 7d: {revenue_delta:.2f}",
        f"rating delta 7d: {rating_count_delta}",
    ]
    if sales_count_delta > previous_week_sales_delta:
        notes.append("recent growth > prior week")
    if crossed:
        notes.append(f"crossed threshold: {', '.join(str(t) for t in crossed)}")

    return TrendScore(
        trend_score=round(trend_score, 1),
        score_notes="; ".join(notes),
        sales_count_delta=sales_count_delta,
        revenue_delta=round(revenue_delta, 2),
        rating_count_delta=rating_count_delta,
        last_week_sales_delta=sales_count_delta,
        previous_week_sales_delta=previous_week_sales_delta,
    )
