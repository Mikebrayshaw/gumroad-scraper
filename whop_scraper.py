"""
Whop marketplace scraper (scaffolding).

This module mirrors the Gumroad scraper signature so it can plug into the
shared ingestion runner. Implementors should fill in the Playwright logic
and parsing selectors for Whop search/listing pages, then return Product
objects that map onto the shared schema.
"""
from __future__ import annotations

import asyncio
from typing import List

from gumroad_scraper import Product


async def scrape_whop_search(
    category_url: str,
    max_products: int = 100,
    get_detailed_ratings: bool = False,
    rate_limit_ms: int = 500,
    show_progress: bool = False,
) -> List[Product]:
    """Scrape Whop listings.

    The function currently raises a NotImplementedError and documents the
    contract expected by the ingestion runner. To add full Whop support,
    implement Playwright navigation for the marketplace URL provided in
    ``category_url`` and populate ``Product`` instances with the parsed
    fields (product name, creator, price, rating details, sales, and
    URL). Keep the signature intact so the platform registry can route
    jobs without special casing.
    """

    raise NotImplementedError(
        "Whop scraping is not implemented yet. Add Playwright navigation "
        "and selectors in whop_scraper.py to return Product objects."
    )


__all__ = ["scrape_whop_search"]
