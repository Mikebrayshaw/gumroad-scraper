import math
from opportunity_engine import (
    assemble_opportunity,
    compute_copyability_score,
    compute_novelty_score,
    compute_price_to_value_score,
    compute_saturation_penalty,
    compute_velocity_score,
    detect_alerts,
    generate_opportunities,
    hours_between_runs,
    load_config,
)


def test_hours_between_runs_defaults_when_missing():
    assert hours_between_runs(None, None) == 24.0


def test_velocity_score_uses_deltas():
    cfg = load_config()
    score, notes = compute_velocity_score({"rating_count_delta": 10, "sales_count_delta": 50}, 12, cfg)
    assert score > 10
    assert any("ratings" in n for n in notes)


def test_novelty_detects_unique_terms():
    cfg = load_config()
    score_common, _ = compute_novelty_score("Design template pack", "design", ["Design template pack", "Design kit"], cfg)
    score_unique, _ = compute_novelty_score("AI automation roadmap", "design", ["Design template pack", "Design kit"], cfg)
    assert score_unique > score_common


def test_copyability_penalizes_branding():
    cfg = load_config()
    score_brand, reason = compute_copyability_score("Notion template by Jane Doe", cfg)
    score_plain, _ = compute_copyability_score("Notion template for founders", cfg)
    assert score_plain > score_brand
    assert "brand" in reason


def test_saturation_penalty_counts_neighbors():
    cfg = load_config()
    penalty, reason, examples = compute_saturation_penalty(
        "Startup pitch deck template",
        "design",
        ["Pitch deck template", "Resume template"],
        cfg,
    )
    assert penalty > 0
    assert reason in {"crowded niche", "few close comps"}
    assert examples


def test_generate_opportunities_and_alerts_workflow():
    cfg = load_config()
    snapshots = [
        {
            "platform": "gumroad",
            "product_id": "1",
            "run_id": "run-a",
            "url": "http://example.com/a",
            "title": "AI prompts for designers",
            "creator_name": "Alice",
            "category": "design",
            "price_amount": 29,
            "price_currency": "USD",
            "rating_avg": 4.8,
            "rating_count": 30,
            "sales_count": 150,
        }
    ]
    diffs = {("gumroad", "1"): {"rating_count_delta": 15, "sales_count_delta": 60, "previous_run_id": "run-prev"}}
    historical = {"design": ["Design template", "UI kit"]}
    opportunities = generate_opportunities(snapshots, diffs, historical, hours_delta=24, cfg=cfg)
    assert opportunities[0]["opportunity_score"] > 0

    alerts = detect_alerts("run-a", snapshots, diffs, previous_run_id="run-prev", cfg=cfg)
    assert alerts  # velocity spike should trigger
