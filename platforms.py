"""
Platform registry and dispatcher for scraping different marketplaces.

This module keeps a lightweight mapping of platform slugs to scraper
callables so ingestion jobs can target different sites (e.g., Gumroad
and Whop) without changing the runner logic. Scrapers should share a
compatible signature to keep cross-platform orchestration simple.
"""
from collections.abc import Awaitable, Callable
from typing import Dict, List

from gumroad_scraper import Product, scrape_discover_page
from whop_scraper import scrape_whop_search

ScraperFn = Callable[[str, int, bool, int], Awaitable[List[Product]]]

_SCRAPERS: Dict[str, ScraperFn] = {
    "gumroad": scrape_discover_page,
    "whop": scrape_whop_search,
}


def register_scraper(platform: str, scraper: ScraperFn) -> None:
    """Register or override a scraper for a platform slug."""
    _SCRAPERS[platform] = scraper


def get_scraper(platform: str) -> ScraperFn:
    """Return the scraper callable for the requested platform."""
    try:
        return _SCRAPERS[platform]
    except KeyError as exc:
        raise ValueError(f"Unsupported platform: {platform}") from exc
