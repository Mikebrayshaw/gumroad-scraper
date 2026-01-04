"""Streamlit helpers for running CrewAI analysis and rendering insights."""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Iterable, List, Sequence

import streamlit as st

from analysis_engine import AnalysisResult, CrewAnalyzer, dataset_cache_key

_analyzer = CrewAnalyzer()


def _to_rows(products: Iterable[Any]) -> List[dict]:
    rows: List[dict] = []
    for product in products:
        if is_dataclass(product):
            rows.append(asdict(product))
        elif isinstance(product, dict):
            rows.append(product)
        else:
            rows.append(dict(product))
    return rows


@st.cache_data(show_spinner=False)
def _cached_analysis(cache_key: str, rows: Sequence[dict], source_label: str) -> AnalysisResult:
    return _analyzer.analyze(rows, cache_key, source_label)


def render_analysis_block(products: Iterable[Any], dataset_id: str, source_label: str, *, button_label: str = "🔎 Analyze"):
    """Render an Analyze button and display CrewAI insights when available."""

    rows = _to_rows(products)
    cache_key = dataset_cache_key(rows, dataset_id)
    session_key = f"analysis-result-{cache_key}"
    trigger_key = f"analysis-trigger-{cache_key}"

    if st.button(button_label, key=trigger_key, type="secondary"):
        st.session_state[trigger_key] = True

    if st.session_state.get(trigger_key):
        with st.spinner("Running CrewAI analysis... this may take a moment"):
            st.session_state[session_key] = _cached_analysis(cache_key, rows, source_label)
        st.session_state[trigger_key] = False

    result: AnalysisResult | None = st.session_state.get(session_key)
    if result:
        _render_result(result)
    else:
        st.info("Analysis results will appear here once you click Analyze.")


def _render_result(result: AnalysisResult):
    st.subheader("Insights")
    st.write(result.summary)

    col1, col2 = st.columns(2)
    _render_list(col1, "Trending categories", result.trending_categories)
    _render_list(col1, "Top sellers", result.top_sellers)
    _render_list(col2, "Pricing patterns", result.pricing_patterns)
    _render_list(col2, "Sentiment signals", result.sentiment_insights)

    with st.expander("Raw CrewAI output"):
        st.json(result.raw_output)


def _render_list(column, title: str, items: Sequence[str]):
    with column:
        st.markdown(f"**{title}**")
        if items:
            for item in items:
                st.write(f"• {item}")
        else:
            st.caption("No insights returned yet.")
