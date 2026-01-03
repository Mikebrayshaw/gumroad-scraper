# Repository Review

## Overview
This project scrapes product data from Gumroad discover/category pages using Python and Playwright. The main scraper (`gumroad_scraper.py`) defines a `Product` dataclass, parsing helpers, asynchronous scraping workflow, and a CLI with options for categories, fast mode, and output selection. A lightweight test script (`test_scraper.py`) exercises a quick, non-detailed scrape, and the README gives a brief one-paragraph description.

## Strengths
- Uses structured data modeling (`Product` dataclass) and helper parsers for prices/ratings, which keeps the scrape output schema explicit and consistent across the pipeline.
- The main scraper offers configurable modes (category selection, product limit, fast mode to skip detailed pages, rate limiting) and summarizes results at the end, making it usable for exploratory runs.

## Issues & Risks
1. **Setup/operability gaps**: There is no dependency manifest (e.g., `requirements.txt`) or README guidance on installing Playwright browsers, so new users cannot easily reproduce the environment or know to run `playwright install chromium` before executing the scripts.
2. **Data extraction fragility**: Detailed metrics rely on scraping the entire page body text with regex (e.g., sales counts and per-star ratings) rather than DOM selectors, which risks false positives or missing data when unrelated text matches the patterns.
3. **Performance and resilience concerns**: The discover-page loop can attempt up to 100 scroll cycles with a ten-iteration backoff before aborting, while detailed scraping opens a fresh Playwright page per product with added sleeps; this could make runs very slow or appear hung when few new cards load.
4. **Data quality limitations**: Subcategory values are duplicated from the main category, and mixed-review percentages sum the percentage buckets without converting them to counts, which could mislead downstream analysis or CSV consumers.
5. **Testing/automation coverage**: Only an ad-hoc scraper smoke test exists; there are no assertions or CI hooks to guard core parsing helpers, CLI behavior, or regression detection when Gumroad markup shifts.

## Recommendations
- Add a dependency/installation guide (requirements file plus Playwright browser install step) and basic usage examples to the README.
- Prefer DOM-targeted selectors for sales counts and rating breakdowns, falling back to text regex only when structured elements are missing.
- Revisit scrolling/backoff parameters and reuse a single detail page or pool to reduce overhead; consider logging progress with elapsed time to make long runs transparent.
- Clarify or refactor data fields (e.g., true subcategory extraction, mixed-review computation from counts) to avoid ambiguous analytics.
- Introduce minimal automated tests for parser helpers and CLI argument handling, and wire them into CI to catch scraping drift early.
