"""
Whop marketplace scraper.

This module mirrors the Gumroad scraper signature so it can plug into the
shared ingestion runner. It uses Playwright to fetch a Whop search or listing
URL and extracts card-level metadata (name, creator, price, ratings, and URL).
Selectors are resilient and fall back to common text patterns so the scraper
degrades gracefully if Whop tweaks its markup. Whop does not expose per-star
rating breakdowns on listing cards, so the scraper records those fields as
``None`` and leaves ``mixed_review_percent`` unset for scoring exclusions.
"""
from __future__ import annotations

import asyncio
from typing import List, Optional
from urllib.parse import unquote, urljoin, urlparse

from playwright.async_api import TimeoutError as PlaywrightTimeout, async_playwright
from tqdm import tqdm

from gumroad_scraper import Product, parse_price, parse_rating, parse_sales


def _sanitize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return " ".join(value.split()).strip()


async def _first_text(card, selectors: list[str]) -> str:
    for selector in selectors:
        node = await card.query_selector(selector)
        if node:
            text = await node.inner_text()
            cleaned = _sanitize_text(text)
            if cleaned:
                return cleaned
    return ""


async def _extract_product_url(card, base_url: str) -> str:
    href = await card.get_attribute("href")
    if href:
        return urljoin(base_url, href)

    link = await card.query_selector("a[href]")
    if link:
        href = await link.get_attribute("href")
        if href:
            return urljoin(base_url, href)

    return base_url


def _format_category(text: str) -> str:
    cleaned = _sanitize_text(text)
    if not cleaned:
        return ""
    return cleaned.strip("#").strip()


async def _extract_category(card, category_url: str) -> str:
    category_text = await _first_text(
        card,
        [
            "[data-testid='listing-category']",
            "[data-testid='category']",
            "[data-testid='collection-name']",
            "[class*='category']",
            "[class*='Category']",
            "[class*='collection']",
            "[class*='Collection']",
        ],
    )
    if category_text:
        return _format_category(category_text)

    parsed = urlparse(category_url)
    segments = [segment for segment in parsed.path.split("/") if segment]
    if not segments:
        return ""

    category_markers = {"category", "categories", "collection", "collections", "discover"}
    for marker in category_markers:
        if marker in segments:
            marker_index = segments.index(marker)
            if marker_index + 1 < len(segments):
                return _format_category(unquote(segments[marker_index + 1]).replace("-", " ").replace("_", " "))

    fallback = segments[-1]
    if fallback.lower() in {"search", "listing", "listings", "product", "products"}:
        return ""
    return _format_category(unquote(fallback).replace("-", " ").replace("_", " "))


def _estimate_revenue(price_usd: float, sales_count: Optional[int]) -> Optional[float]:
    if sales_count is None:
        return None
    return round(price_usd * sales_count, 2)


async def _parse_card(
    card,
    base_url: str,
    get_detailed_ratings: bool,
) -> Product:
    product_url = await _extract_product_url(card, base_url)
    title = await _first_text(
        card,
        [
            "[data-testid='listing-title']",
            "[data-testid='product-title']",
            "h3",
            "h2",
            "h4",
        ],
    ) or "Unknown"

    creator = await _first_text(
        card,
        [
            "[data-testid='listing-creator']",
            "[data-testid='creator-name']",
            ".text-muted",
            ".creator",
            "span",
        ],
    ) or "Unknown"

    price_text = await _first_text(
        card,
        [
            "[data-testid='price']",
            "[data-testid='listing-price']",
            "[class*='price']",
            "[class*='Price']",
            "text=USD",
        ],
    )
    price_usd, original_price, currency = parse_price(price_text) if price_text else (0.0, "", "USD")

    rating_text = ""
    if get_detailed_ratings:
        rating_text = await _first_text(
            card,
            [
                "[data-testid='rating']",
                "[class*='rating']",
                "[class*='Rating']",
                "span:has-text('★')",
            ],
        )
    average_rating, total_reviews = parse_rating(rating_text)

    sales_text = await _first_text(
        card,
        [
            "[data-testid='sales']",
            "[class*='sales']",
            "[class*='Sales']",
            "span:has-text('sales')",
        ],
    )
    sales_count = parse_sales(sales_text)
    estimated_revenue = _estimate_revenue(price_usd, sales_count)
    category = await _extract_category(card, base_url) or "whop"

    return Product(
        product_name=title,
        creator_name=creator,
        category=category,
        subcategory="",
        price_usd=price_usd,
        original_price=original_price or price_text or "",
        currency=currency,
        average_rating=average_rating,
        total_reviews=total_reviews,
        rating_1_star=None,
        rating_2_star=None,
        rating_3_star=None,
        rating_4_star=None,
        rating_5_star=None,
        mixed_review_percent=None,
        sales_count=sales_count,
        estimated_revenue=estimated_revenue,
        product_url=product_url,
    )


async def scrape_whop_search(
    category_url: str,
    max_products: int = 100,
    get_detailed_ratings: bool = False,
    rate_limit_ms: int = 500,
    show_progress: bool = False,
) -> List[Product]:
    """Scrape Whop listings from a search or marketplace URL."""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(category_url, wait_until="networkidle")
        except PlaywrightTimeout:
            await browser.close()
            return []

        await page.wait_for_timeout(rate_limit_ms)

        product_cards = await page.query_selector_all(
            "a[href*='/product'], a[href*='/listing'], [data-testid='listing-card']"
        )

        results: List[Product] = []
        iterator = product_cards[:max_products]
        if show_progress:
            iterator = tqdm(iterator, desc="Whop products")

        for card in iterator:
            product = await _parse_card(card, category_url, get_detailed_ratings)
            results.append(product)
            if rate_limit_ms:
                await asyncio.sleep(rate_limit_ms / 1000)

        await browser.close()

    return results


__all__ = ["scrape_whop_search"]
