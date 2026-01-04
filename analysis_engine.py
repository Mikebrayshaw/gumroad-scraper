"""CrewAI-powered market analysis helpers for Gumroad datasets.

The module focuses on building structured prompts and normalising CrewAI
responses so that Streamlit pages can render insights reliably.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Sequence


@dataclass
class AnalysisResult:
    """Normalized insights returned from the CrewAI workflow."""

    dataset_id: str
    source_label: str
    summary: str
    trending_categories: List[str] = field(default_factory=list)
    top_sellers: List[str] = field(default_factory=list)
    pricing_patterns: List[str] = field(default_factory=list)
    sentiment_insights: List[str] = field(default_factory=list)
    raw_output: Any | None = None


class CrewAnalyzer:
    """Adapter that builds CrewAI tasks and parses the responses.

    The implementation accepts an optional ``crew_builder`` so tests can inject
    a lightweight fake Crew without pulling heavy dependencies.
    """

    def __init__(self, crew_builder: Callable[[List[Dict[str, str]]], Any] | None = None):
        self._crew_builder = crew_builder or self._default_crew_builder

    def analyze(self, products: Sequence[Dict[str, Any]], dataset_id: str, source_label: str) -> AnalysisResult:
        if not products:
            raise ValueError("No products available for analysis.")

        tasks = self._build_tasks(products, source_label)
        crew = self._crew_builder(tasks)
        raw_output = crew.kickoff()
        parsed = self._parse_output(raw_output)

        return AnalysisResult(
            dataset_id=dataset_id,
            source_label=source_label,
            summary=parsed.get("summary") or "Analysis completed.",
            trending_categories=_coerce_list(parsed.get("trending_categories")),
            top_sellers=_coerce_list(parsed.get("top_sellers")),
            pricing_patterns=_coerce_list(parsed.get("pricing_patterns")),
            sentiment_insights=_coerce_list(parsed.get("sentiment_insights")),
            raw_output=raw_output,
        )

    def _build_tasks(self, products: Sequence[Dict[str, Any]], source_label: str) -> List[Dict[str, str]]:
        context = json.dumps(_summarize_products(products), ensure_ascii=False, indent=2)
        return [
            {
                "prompt": (
                    "You are a marketplace intelligence analyst. Using the provided"
                    f" Gumroad dataset from {source_label}, craft short bullet list"
                    " insights for: (1) trending categories and subcategories,"
                    " (2) top sellers with why they stand out, (3) pricing patterns"
                    " and discounting behaviour, and (4) sentiment trends you can"
                    " infer from product descriptions or review metrics.\n\nContext:\n"
                    f"{context}\n\nRespond with compact JSON containing keys"
                    " summary, trending_categories, top_sellers, pricing_patterns,"
                    " sentiment_insights. Keep each list under 5 items."
                ),
                "expected_output": "JSON with summary + four insight arrays",
            }
        ]

    def _default_crew_builder(self, tasks: List[Dict[str, str]]):  # pragma: no cover - requires CrewAI runtime
        from crewai import Agent, Crew, Process, Task

        try:
            from langchain_openai import ChatOpenAI  # type: ignore

            llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.2)
        except Exception:
            # Fallback to letting CrewAI pick a default LLM if langchain-openai
            # is not available in the environment.
            llm = None

        analyst = Agent(
            role="Gumroad Market Analyst",
            goal="Spot patterns in scraped Gumroad listings and summarise them",
            backstory=(
                "You review digital product listings and synthesise insights about"
                " what categories are trending, which listings are outperforming,"
                " and how pricing or sentiment impacts performance."
            ),
            verbose=False,
            llm=llm,
        )

        crew_tasks = [
            Task(description=item["prompt"], expected_output=item["expected_output"], agent=analyst)
            for item in tasks
        ]

        return Crew(agents=[analyst], tasks=crew_tasks, process=Process.sequential)

    def _parse_output(self, raw_output: Any) -> Dict[str, Any]:
        if isinstance(raw_output, dict):
            return raw_output
        if isinstance(raw_output, str):
            try:
                return json.loads(raw_output)
            except json.JSONDecodeError:
                return {"summary": raw_output}
        return {"summary": "Analysis complete."}


def dataset_cache_key(products: Iterable[Dict[str, Any]], dataset_id: str) -> str:
    """Return a stable cache key for a dataset."""

    hasher = hashlib.sha256()
    hasher.update(dataset_id.encode("utf-8"))
    for row in products:
        serialized = json.dumps(row, sort_keys=True, ensure_ascii=False)
        hasher.update(serialized.encode("utf-8"))
    return hasher.hexdigest()


def _summarize_products(products: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Create a compact summary of the dataset for prompt conditioning."""

    sample = list(products)[:20]
    return {
        "rows_considered": len(sample),
        "fields": sorted({key for row in sample for key in row.keys()}),
        "sample_rows": sample,
    }


def _coerce_list(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    return [str(value)]


__all__ = ["AnalysisResult", "CrewAnalyzer", "dataset_cache_key"]
