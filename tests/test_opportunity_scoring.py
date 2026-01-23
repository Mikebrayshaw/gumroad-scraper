from datetime import datetime, timedelta, timezone

from opportunity_scoring import score_trend_from_snapshots


BASE_TIME = datetime(2024, 1, 15, tzinfo=timezone.utc)


def _snapshot(
    *,
    days_ago: int,
    sales_count: int,
    revenue_estimate: float,
    rating_count: int,
    rating_avg: float = 4.8,
) -> dict:
    return {
        "sales_count": sales_count,
        "revenue_estimate": revenue_estimate,
        "rating_count": rating_count,
        "rating_avg": rating_avg,
        "scraped_at": BASE_TIME - timedelta(days=days_ago),
    }


def test_trend_score_no_snapshots():
    score = score_trend_from_snapshots([])
    assert score.trend_score == 0.0
    assert score.score_notes == "no snapshots"


def test_trend_score_single_snapshot_defaults_to_zero_delta():
    snapshots = [
        _snapshot(
            days_ago=0,
            sales_count=5,
            revenue_estimate=250.0,
            rating_count=2,
        )
    ]
    score = score_trend_from_snapshots(snapshots, min_sales=0)
    assert score.sales_count_delta == 5
    assert score.revenue_delta == 250.0
    assert score.rating_count_delta == 2
    assert score.previous_week_sales_delta == score.sales_count_delta


def test_trend_score_weekly_deltas():
    snapshots = [
        _snapshot(
            days_ago=14,
            sales_count=100,
            revenue_estimate=1000.0,
            rating_count=10,
        ),
        _snapshot(
            days_ago=7,
            sales_count=140,
            revenue_estimate=1300.0,
            rating_count=15,
        ),
        _snapshot(
            days_ago=0,
            sales_count=200,
            revenue_estimate=1900.0,
            rating_count=25,
        ),
    ]
    score = score_trend_from_snapshots(snapshots)
    assert score.sales_count_delta == 60
    assert score.previous_week_sales_delta == 40


def test_trend_score_growth_boost_notes():
    snapshots = [
        _snapshot(
            days_ago=14,
            sales_count=80,
            revenue_estimate=900.0,
            rating_count=8,
        ),
        _snapshot(
            days_ago=7,
            sales_count=100,
            revenue_estimate=1100.0,
            rating_count=12,
        ),
        _snapshot(
            days_ago=0,
            sales_count=140,
            revenue_estimate=1600.0,
            rating_count=20,
        ),
    ]
    score = score_trend_from_snapshots(snapshots)
    assert "recent growth > prior week" in score.score_notes


def test_trend_score_threshold_bonus_notes():
    snapshots = [
        _snapshot(
            days_ago=7,
            sales_count=40,
            revenue_estimate=800.0,
            rating_count=8,
        ),
        _snapshot(
            days_ago=0,
            sales_count=120,
            revenue_estimate=1800.0,
            rating_count=18,
        ),
    ]
    score = score_trend_from_snapshots(snapshots)
    assert "crossed threshold" in score.score_notes


def test_trend_score_large_deltas_do_not_saturate_immediately():
    snapshots = [
        _snapshot(
            days_ago=14,
            sales_count=0,
            revenue_estimate=0.0,
            rating_count=0,
        ),
        _snapshot(
            days_ago=7,
            sales_count=5000,
            revenue_estimate=100000.0,
            rating_count=500,
        ),
        _snapshot(
            days_ago=0,
            sales_count=10000,
            revenue_estimate=200000.0,
            rating_count=1000,
        ),
    ]
    score = score_trend_from_snapshots(snapshots)
    assert score.trend_score < 100.0
