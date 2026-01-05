"""Opportunity Engine
=====================

Pure functions for turning historical ``product_snapshots`` into an
explainable, opinionated "opportunity" view. No network calls or LLMs –
just heuristics tuned for fast daily runs.

Key responsibilities
--------------------
* Compute per-product component scores (velocity, price-to-value,
  novelty, copyability, saturation penalty).
* Produce an explainable opportunity score and short reason summary.
* Generate human-readable briefs for the top opportunities.
* Detect alerts (velocity spikes, new entrants, pricing moves) between
  runs.

All functions accept plain Python data structures and return rich dicts
so the CLI can persist results or render exports without coupling the
core logic to storage.
"""

from __future__ import annotations

import json
import math
import statistics
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

DEFAULT_CONFIG_PATH = Path("opportunity_config.json")


def load_config(config_path: Optional[str | Path] = None) -> dict:
    """Load thresholds/weights, merging any user config onto defaults."""

    base = {
        "weights": {
            "velocity": 0.35,
            "copyability": 0.2,
            "novelty": 0.15,
            "price_to_value": 0.2,
            "saturation_penalty": 0.1,
        },
        "velocity": {
            "rating_per_hour_for_max": 5,
            "sales_per_hour_for_max": 20,
            "spike_rating_delta": 12,
            "spike_sales_delta": 50,
            "min_hours": 6,
        },
        "price_to_value": {
            "sweet_spot": [15, 79],
            "acceptable": [5, 149],
            "penalty_high": 40,
            "penalty_low": 20,
        },
        "novelty": {
            "history_runs": 3,
            "min_token_length": 4,
        },
        "copyability": {
            "format_keywords": ["template", "checklist", "playbook", "framework", "prompts", "swipe", "spreadsheet", "notion"],
            "audience_markers": ["for", "to"],
            "brand_blocks": [" by ", "with "],
            "creator_penalty": 20,
        },
        "saturation": {
            "similarity_threshold": 0.55,
            "penalty_per_neighbor": 12,
            "max_penalty": 60,
        },
        "confidence": {
            "reviews_high": 25,
            "reviews_med": 5,
            "sales_high": 150,
            "sales_med": 25,
        },
        "alerts": {
            "price_pct_move": 0.25,
            "min_price_change": 5,
        },
    }

    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if path.exists():
        user = json.loads(path.read_text())
        base = _deep_merge(base, user)
    return base


def _deep_merge(base: MutableMapping, override: Mapping) -> MutableMapping:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def hours_between_runs(current_started_at: Optional[str], previous_started_at: Optional[str]) -> float:
    """Coerce ISO timestamps into hours between runs with a sane minimum."""

    if not current_started_at or not previous_started_at:
        return 24.0
    from datetime import datetime

    current_dt = datetime.fromisoformat(current_started_at)
    previous_dt = datetime.fromisoformat(previous_started_at)
    hours = max((current_dt - previous_dt).total_seconds() / 3600, 0.01)
    return max(hours, 1.0)


def _tokenize(title: str) -> List[str]:
    keep = ["-", " "]
    cleaned = "".join(ch.lower() if ch.isalnum() or ch in keep else " " for ch in title)
    return [tok for tok in cleaned.split() if tok]


def compute_velocity_score(diff: Mapping[str, Optional[float]], hours_delta: float, cfg: dict) -> Tuple[float, List[str]]:
    hours = max(hours_delta, cfg["velocity"].get("min_hours", 6))
    rating_rate = (diff.get("rating_count_delta") or 0) / hours
    sales_rate = (diff.get("sales_count_delta") or 0) / hours

    rating_score = min(1.0, rating_rate / max(cfg["velocity"].get("rating_per_hour_for_max", 5), 1))
    sales_score = min(1.0, sales_rate / max(cfg["velocity"].get("sales_per_hour_for_max", 20), 1))
    score = round((rating_score * 0.5 + sales_score * 0.5) * 100, 2)

    notes = []
    if rating_rate:
        notes.append(f"ratings +{diff.get('rating_count_delta', 0)} over {hours:.1f}h")
    if sales_rate:
        notes.append(f"sales +{diff.get('sales_count_delta', 0)} over {hours:.1f}h")
    return score, notes


def compute_price_to_value_score(price: Optional[float], category: Optional[str], cfg: dict) -> Tuple[float, str]:
    if price is None:
        return 55.0, "no price"

    sweet_low, sweet_high = cfg["price_to_value"].get("sweet_spot", [15, 79])
    ok_low, ok_high = cfg["price_to_value"].get("acceptable", [5, 149])

    if sweet_low <= price <= sweet_high:
        return 95.0, "priced in sweet spot"
    if ok_low <= price <= ok_high:
        return 80.0, "priced within acceptable band"
    if price < ok_low:
        penalty = cfg["price_to_value"].get("penalty_low", 20)
        return max(40.0, 80.0 - penalty), "very low price"

    penalty = cfg["price_to_value"].get("penalty_high", 40)
    return max(35.0, 80.0 - penalty), "premium priced"


def compute_novelty_score(
    title: str, category: Optional[str], category_titles: Sequence[str], cfg: dict
) -> Tuple[float, str]:
    tokens = [t for t in _tokenize(title) if len(t) >= cfg["novelty"].get("min_token_length", 4)]
    if not tokens:
        return 50.0, "plain title"

    category_tokens = [_tokenize(t) for t in category_titles]
    df = Counter(tok for row in category_tokens for tok in row if len(tok) >= 4)
    total_docs = max(len(category_tokens), 1)
    idfs = []
    for token in set(tokens):
        freq = df.get(token, 0)
        idf = math.log((1 + total_docs) / (1 + freq)) + 1
        idfs.append(idf)

    avg_idf = statistics.mean(idfs)
    normalized = min(100.0, avg_idf / 3 * 100)
    return round(normalized, 2), "unique phrasing" if normalized > 70 else "common wording"


def compute_copyability_score(title: str, cfg: dict) -> Tuple[float, str]:
    tokens = _tokenize(title)
    lower_title = title.lower()
    keywords = cfg["copyability"].get("format_keywords", [])
    keyword_hits = [kw for kw in keywords if kw in lower_title]

    audience_present = any(tok == "for" for tok in tokens)
    brand_signals = any(marker in lower_title for marker in cfg["copyability"].get("brand_blocks", []))
    score = 60 + 10 * len(keyword_hits)
    if audience_present:
        score += 10
    if brand_signals:
        score -= cfg["copyability"].get("creator_penalty", 20)
    return max(10.0, min(100.0, float(score))), _copyability_reason(keyword_hits, audience_present, brand_signals)


def _copyability_reason(keyword_hits: List[str], has_audience: bool, brand_signals: bool) -> str:
    parts = []
    if keyword_hits:
        parts.append(f"clear format ({', '.join(keyword_hits)})")
    if has_audience:
        parts.append("targets a specific audience")
    if brand_signals:
        parts.append("personal brand heavy")
    return "; ".join(parts) or "generic positioning"


def compute_saturation_penalty(title: str, category: Optional[str], category_titles: Sequence[str], cfg: dict) -> Tuple[float, str, List[str]]:
    threshold = cfg["saturation"].get("similarity_threshold", 0.55)
    neighbors = []
    for other in category_titles:
        if other == title:
            continue
        sim = _title_similarity(title, other)
        if sim >= threshold:
            neighbors.append((other, sim))

    neighbors.sort(key=lambda x: x[1], reverse=True)
    penalty = min(
        cfg["saturation"].get("max_penalty", 60),
        len(neighbors) * cfg["saturation"].get("penalty_per_neighbor", 12),
    )
    return float(penalty), "crowded niche" if neighbors else "few close comps", [n[0] for n in neighbors[:5]]


def _title_similarity(a: str, b: str) -> float:
    ta, tb = set(_tokenize(a)), set(_tokenize(b))
    if not ta or not tb:
        return 0.0
    overlap = len(ta & tb)
    return overlap / len(ta | tb)


def infer_confidence(rating_count: Optional[int], sales_count: Optional[int], cfg: dict) -> str:
    if (rating_count or 0) >= cfg["confidence"].get("reviews_high", 25) or (sales_count or 0) >= cfg["confidence"].get("sales_high", 150):
        return "high"
    if (rating_count or 0) >= cfg["confidence"].get("reviews_med", 5) or (sales_count or 0) >= cfg["confidence"].get("sales_med", 25):
        return "med"
    return "low"


def assemble_opportunity(
    snapshot: Mapping[str, any],
    diff: Mapping[str, Optional[float]],
    category_titles: Sequence[str],
    hours_delta: float,
    cfg: dict,
) -> dict:
    velocity_score, velocity_notes = compute_velocity_score(diff, hours_delta, cfg)
    price_score, price_reason = compute_price_to_value_score(snapshot.get("price_amount"), snapshot.get("category"), cfg)
    novelty_score, novelty_reason = compute_novelty_score(snapshot.get("title", ""), snapshot.get("category"), category_titles, cfg)
    copyability_score, copy_reason = compute_copyability_score(snapshot.get("title", ""), cfg)
    saturation_penalty, saturation_reason, competitors = compute_saturation_penalty(
        snapshot.get("title", ""), snapshot.get("category"), category_titles, cfg
    )

    weights = cfg["weights"]
    weighted = (
        velocity_score * weights.get("velocity", 0)
        + copyability_score * weights.get("copyability", 0)
        + novelty_score * weights.get("novelty", 0)
        + price_score * weights.get("price_to_value", 0)
    )
    weighted -= saturation_penalty * weights.get("saturation_penalty", 0)
    opportunity_score = round(max(0.0, min(100.0, weighted)), 2)

    confidence = infer_confidence(snapshot.get("rating_count"), snapshot.get("sales_count"), cfg)
    reason_summary = _reason_string(
        snapshot,
        opportunity_score,
        velocity_notes,
        price_reason,
        novelty_reason,
        copy_reason,
        saturation_reason,
    )

    return {
        "run_id": snapshot.get("run_id"),
        "platform": snapshot.get("platform"),
        "product_id": snapshot.get("product_id"),
        "title": snapshot.get("title"),
        "url": snapshot.get("url"),
        "category": snapshot.get("category"),
        "creator_name": snapshot.get("creator_name"),
        "price_amount": snapshot.get("price_amount"),
        "price_currency": snapshot.get("price_currency"),
        "rating_avg": snapshot.get("rating_avg"),
        "rating_count": snapshot.get("rating_count"),
        "rating_count_delta": diff.get("rating_count_delta"),
        "sales_count": snapshot.get("sales_count"),
        "sales_count_delta": diff.get("sales_count_delta"),
        "opportunity_score": opportunity_score,
        "velocity_score": round(velocity_score, 2),
        "novelty_score": round(novelty_score, 2),
        "copyability_score": round(copyability_score, 2),
        "price_to_value_score": round(price_score, 2),
        "saturation_penalty": round(saturation_penalty, 2),
        "confidence": confidence,
        "reason_summary": reason_summary,
        "saturation_examples": competitors,
    }


def _reason_string(
    snapshot: Mapping[str, any],
    score: float,
    velocity_notes: Sequence[str],
    price_reason: str,
    novelty_reason: str,
    copy_reason: str,
    saturation_reason: str,
) -> str:
    parts = [f"Score {score:.0f}/100"]
    if velocity_notes:
        parts.append("; ".join(velocity_notes))
    parts.append(price_reason)
    parts.append(novelty_reason)
    parts.append(copy_reason)
    parts.append(saturation_reason)
    clean = [p for p in parts if p]
    return " | ".join(clean)[:280]


def generate_opportunities(
    snapshots: Sequence[Mapping[str, any]],
    diffs_by_product: Mapping[Tuple[str, str], Mapping[str, Optional[float]]],
    historical_titles_by_category: Mapping[Optional[str], Sequence[str]],
    hours_delta: float,
    cfg: dict,
) -> List[dict]:
    opportunities: List[dict] = []
    for snap in snapshots:
        key = (snap.get("platform"), snap.get("product_id"))
        diff = diffs_by_product.get(key, {})
        category_titles = historical_titles_by_category.get(snap.get("category")) or []
        opportunities.append(assemble_opportunity(snap, diff, category_titles, hours_delta, cfg))

    opportunities.sort(key=lambda x: x["opportunity_score"], reverse=True)
    return opportunities


def render_opportunity_briefs(opportunities: Sequence[Mapping[str, any]], top_k: int = 10) -> str:
    lines = ["# Opportunity Briefs\n"]
    for opp in opportunities[:top_k]:
        lines.extend(_brief_block(opp))
    return "\n".join(lines)


def _brief_block(opp: Mapping[str, any]) -> List[str]:
    audience = _infer_audience(opp.get("title", ""))
    competitors = opp.get("saturation_examples", [])
    lines = [f"## {opp.get('title', 'Untitled')} ({opp.get('category') or 'Uncategorised'})"]
    lines.append(f"URL: {opp.get('url')}")
    lines.append("")
    lines.append(f"**What it is:** {opp.get('reason_summary')}.")
    lines.append(
        f"**Why it's winning:** Velocity {opp.get('velocity_score')} with price signal {opp.get('price_to_value_score')}."
    )
    lines.append(f"**Who it's for:** {audience}.")
    lines.append(
        f"**Copyable?** Copyability {opp.get('copyability_score')} / 100. Saturation penalty {opp.get('saturation_penalty')}."
    )
    if competitors:
        lines.append(f"**Saturation snapshot:** {len(competitors)} close competitors, e.g. {', '.join(competitors[:3])}.")
    else:
        lines.append("**Saturation snapshot:** No close competitors detected.")
    lines.append("**Suggested angles:**")
    lines.extend(
        [
            f"- Position as the fast-track solution for {audience}",
            f"- Emphasise done-for-you assets (score {opp.get('copyability_score')})",
            f"- Benchmark against rising demand (velocity {opp.get('velocity_score')})",
        ]
    )
    lines.append("")
    return lines


def _infer_audience(title: str) -> str:
    parts = title.split(" for ", 1)
    if len(parts) == 2 and parts[1]:
        return parts[1].split()[0:5] and "for " + " ".join(parts[1].split()[0:5])
    return "broad creators/indie builders"


def detect_alerts(
    run_id: str,
    snapshots: Sequence[Mapping[str, any]],
    diffs_by_product: Mapping[Tuple[str, str], Mapping[str, Optional[float]]],
    previous_run_id: Optional[str],
    cfg: dict,
) -> List[dict]:
    alerts: List[dict] = []
    for snap in snapshots:
        key = (snap.get("platform"), snap.get("product_id"))
        diff = diffs_by_product.get(key, {})
        price_delta = diff.get("price_delta")
        rating_delta = diff.get("rating_count_delta") or 0
        sales_delta = diff.get("sales_count_delta") or 0

        if previous_run_id is None:
            alerts.append(
                {
                    "run_id": run_id,
                    "platform": snap.get("platform"),
                    "product_id": snap.get("product_id"),
                    "alert_type": "new_entrant",
                    "message": f"New product in category {snap.get('category')}: {snap.get('title')}",
                    "metadata": {"category": snap.get("category")},
                }
            )
            continue

        if rating_delta >= cfg["velocity"].get("spike_rating_delta", 12) or sales_delta >= cfg["velocity"].get("spike_sales_delta", 50):
            alerts.append(
                {
                    "run_id": run_id,
                    "platform": snap.get("platform"),
                    "product_id": snap.get("product_id"),
                    "alert_type": "velocity_spike",
                    "message": f"{snap.get('title')} showing spike (+{rating_delta} ratings, +{sales_delta} sales)",
                    "metadata": {"rating_delta": rating_delta, "sales_delta": sales_delta},
                }
            )

        if price_delta:
            pct = None
            try:
                pct = price_delta / ((snap.get("price_amount") or 0) - price_delta)
            except ZeroDivisionError:
                pct = None
            if abs(price_delta) >= cfg["alerts"].get("min_price_change", 5) or (pct and abs(pct) >= cfg["alerts"].get("price_pct_move", 0.25)):
                alerts.append(
                    {
                        "run_id": run_id,
                        "platform": snap.get("platform"),
                        "product_id": snap.get("product_id"),
                        "alert_type": "pricing_move",
                        "message": f"{snap.get('title')} price changed by {price_delta}",
                        "metadata": {"price_delta": price_delta, "pct": pct},
                    }
                )

        if diff.get("previous_run_id") is None:
            alerts.append(
                {
                    "run_id": run_id,
                    "platform": snap.get("platform"),
                    "product_id": snap.get("product_id"),
                    "alert_type": "new_entrant",
                    "message": f"New entrant vs last run: {snap.get('title')}",
                    "metadata": {"category": snap.get("category")},
                }
            )
    return alerts


def render_alerts_markdown(alerts: Sequence[Mapping[str, any]], run_id: str) -> str:
    lines = [f"# Alerts for run {run_id}\n"]
    if not alerts:
        lines.append("No notable changes detected.")
        return "\n".join(lines)

    for alert in alerts:
        lines.append(f"- **{alert.get('alert_type')}**: {alert.get('message')}")
    return "\n".join(lines)


__all__ = [
    "assemble_opportunity",
    "compute_copyability_score",
    "compute_novelty_score",
    "compute_price_to_value_score",
    "compute_saturation_penalty",
    "compute_velocity_score",
    "detect_alerts",
    "generate_opportunities",
    "infer_confidence",
    "load_config",
    "render_alerts_markdown",
    "render_opportunity_briefs",
    "hours_between_runs",
]
