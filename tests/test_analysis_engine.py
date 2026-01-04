import json
from dataclasses import dataclass

import pytest

from analysis_engine import AnalysisResult, CrewAnalyzer, dataset_cache_key


def test_analyzer_parses_json_payload():
    payload = {
        "summary": "Design and productivity dominate.",
        "trending_categories": ["Design", "Productivity"],
        "top_sellers": ["Ultimate UI Kit"],
        "pricing_patterns": ["$19-$39 sweet spot"],
        "sentiment_insights": ["Positive about templates"],
    }

    def fake_crew(_tasks):
        class Dummy:
            def kickoff(self):
                return json.dumps(payload)

        return Dummy()

    analyzer = CrewAnalyzer(crew_builder=fake_crew)
    result = analyzer.analyze([{"product_name": "UI Kit"}], dataset_id="run-1", source_label="Test")

    assert isinstance(result, AnalysisResult)
    assert result.summary == payload["summary"]
    assert result.trending_categories == payload["trending_categories"]
    assert result.top_sellers == payload["top_sellers"]
    assert result.pricing_patterns == payload["pricing_patterns"]
    assert result.sentiment_insights == payload["sentiment_insights"]


def test_dataset_cache_key_depends_on_rows():
    rows = [{"product_name": "A"}, {"product_name": "B", "price_usd": 10}]
    key1 = dataset_cache_key(rows, "alpha")
    key2 = dataset_cache_key(rows, "alpha")
    key3 = dataset_cache_key(rows + [{"product_name": "C"}], "alpha")

    assert key1 == key2
    assert key1 != key3


def test_analyzer_requires_products():
    analyzer = CrewAnalyzer(crew_builder=lambda _tasks: None)
    with pytest.raises(ValueError):
        analyzer.analyze([], dataset_id="empty", source_label="none")
