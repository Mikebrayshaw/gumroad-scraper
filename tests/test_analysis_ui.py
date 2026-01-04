import types
from dataclasses import dataclass

import analysis_ui
from analysis_engine import AnalysisResult


def test_to_rows_handles_dataclasses_and_dicts():
    @dataclass
    class Row:
        name: str
        price: int

    rows = analysis_ui._to_rows([Row("One", 10), {"name": "Two", "price": 20}])
    assert rows == [
        {"name": "One", "price": 10},
        {"name": "Two", "price": 20},
    ]


def test_render_result_uses_streamlit_primitives(monkeypatch):
    calls = []

    class FakeColumn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def markdown(self, text):
            calls.append(("markdown", text))

        def write(self, text):
            calls.append(("write", text))

        def caption(self, text):
            calls.append(("caption", text))

    fake_columns = [FakeColumn(), FakeColumn()]

    def columns(count):
        assert count == 2
        return fake_columns

    class FakeExpander:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def json(self, payload):
            calls.append(("json", payload))

    stub = types.SimpleNamespace(
        subheader=lambda text: calls.append(("subheader", text)),
        write=lambda text: calls.append(("write", text)),
        columns=columns,
        expander=lambda _label: FakeExpander(),
        markdown=lambda text: calls.append(("markdown", text)),
        caption=lambda text: calls.append(("caption", text)),
        json=lambda payload: calls.append(("json", payload)),
    )

    monkeypatch.setattr(analysis_ui, "st", stub)

    analysis_ui._render_result(
        AnalysisResult(
            dataset_id="demo",
            source_label="test",
            summary="summary text",
            trending_categories=["Design"],
            top_sellers=["UI Kit"],
            pricing_patterns=["bundle pricing"],
            sentiment_insights=["positive"],
            raw_output={"raw": True},
        )
    )

    labels = [call[0] for call in calls]
    assert "subheader" in labels
    assert ("json", {"raw": True}) in calls
