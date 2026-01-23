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
import re
from typing import List, Optional
from urllib.parse import unquote, urljoin, urlparse

from playwright.async_api import Page, TimeoutError as PlaywrightTimeout, async_playwright
from tqdm import tqdm

from gumroad_scraper import Product, parse_price, parse_rating, parse_rating_breakdown, parse_sales


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
    detail_page: Optional[Page],
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
    if price_text:
        price_usd, original_price, currency, price_is_pwyw = parse_price(price_text)
    else:
        price_usd, original_price, currency, price_is_pwyw = 0.0, "", "USD", False

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

    detail_data: dict[str, Optional[float] | Optional[int] | bool] = {}
    if get_detailed_ratings and detail_page and product_url:
        detail_data = await get_whop_product_details(
            detail_page,
            product_url,
            total_reviews_hint=total_reviews,
        )

    detail_fetched = bool(detail_data.get("fetched"))
    if detail_fetched:
        detail_average = detail_data.get("average_rating")
        detail_reviews = detail_data.get("total_reviews")
        detail_sales = detail_data.get("sales_count")
        if detail_average is not None:
            average_rating = float(detail_average)
        if isinstance(detail_reviews, int) and detail_reviews > 0:
            total_reviews = detail_reviews
        if isinstance(detail_sales, int):
            sales_count = detail_sales
            estimated_revenue = _estimate_revenue(price_usd, sales_count)

    rating_1_star = detail_data.get("rating_1_star") if detail_fetched else None
    rating_2_star = detail_data.get("rating_2_star") if detail_fetched else None
    rating_3_star = detail_data.get("rating_3_star") if detail_fetched else None
    rating_4_star = detail_data.get("rating_4_star") if detail_fetched else None
    rating_5_star = detail_data.get("rating_5_star") if detail_fetched else None
    mixed_review_percent = None
    if detail_fetched and all(isinstance(value, int) for value in (rating_1_star, rating_2_star, rating_3_star, rating_4_star, rating_5_star)):
        mixed_review_percent = round(float(rating_2_star + rating_3_star + rating_4_star), 2)

    return Product(
        product_name=title,
        creator_name=creator,
        category=category,
        subcategory="",
        price_usd=price_usd,
        original_price=original_price or price_text or "",
        price_is_pwyw=price_is_pwyw,
        currency=currency,
        average_rating=average_rating,
        total_reviews=total_reviews,
        rating_1_star=rating_1_star,
        rating_2_star=rating_2_star,
        rating_3_star=rating_3_star,
        rating_4_star=rating_4_star,
        rating_5_star=rating_5_star,
        mixed_review_percent=mixed_review_percent,
        sales_count=sales_count,
        estimated_revenue=estimated_revenue,
        product_url=product_url,
    )


def _parse_count_with_suffix(value: str, suffix: Optional[str]) -> int:
    numeric = float(value.replace(",", ""))
    if suffix:
        suffix = suffix.upper()
        if suffix == "K":
            numeric *= 1000
        elif suffix == "M":
            numeric *= 1_000_000
    return int(numeric)


def _select_rating_from_texts(texts: list[str]) -> tuple[Optional[float], int]:
    best_rating = None
    best_count = 0
    for text in texts:
        rating, count = parse_rating(text)
        if rating is None:
            continue
        if count > best_count:
            best_rating = rating
            best_count = count
        elif best_rating is None:
            best_rating = rating
    return best_rating, best_count


def _extract_total_reviews(text: str) -> Optional[int]:
    match = re.search(r"(\d[\d,]*)\s*(?:reviews|ratings)", text, re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


async def _collect_texts(page: Page, selectors: list[str]) -> list[str]:
    texts: list[str] = []
    for selector in selectors:
        nodes = await page.query_selector_all(selector)
        for node in nodes:
            text = _sanitize_text(await node.inner_text())
            if text:
                texts.append(text)
    return texts


async def get_whop_product_details(
    page: Page,
    product_url: str,
    max_retries: int = 3,
    total_reviews_hint: Optional[int] = None,
) -> dict[str, Optional[float] | Optional[int] | bool]:
    details: dict[str, Optional[float] | Optional[int] | bool] = {
        "average_rating": None,
        "total_reviews": 0,
        "rating_1_star": 0,
        "rating_2_star": 0,
        "rating_3_star": 0,
        "rating_4_star": 0,
        "rating_5_star": 0,
        "sales_count": None,
        "fetched": False,
    }

    for attempt in range(max_retries):
        try:
            await page.goto(product_url, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(1500)

            body_text = await page.inner_text("body")

            sales_count = None
            for pattern in (
                r"([\d,.]+)\s*([KkMm])?\s*sales?",
                r"([\d,.]+)\s*([KkMm])?\s*sold",
                r"([\d,.]+)\s*([KkMm])?\s*orders?",
            ):
                sales_match = re.search(pattern, body_text, re.IGNORECASE)
                if sales_match:
                    sales_count = _parse_count_with_suffix(sales_match.group(1), sales_match.group(2))
                    break
            details["sales_count"] = sales_count

            rating_texts = await _collect_texts(
                page,
                [
                    "[data-testid*='rating']",
                    "[data-testid*='review']",
                    "[class*='rating']",
                    "[class*='Rating']",
                    "text=/rating/i",
                    "text=/review/i",
                ],
            )

            rating_texts.append(body_text)
            average_rating, total_reviews = _select_rating_from_texts(rating_texts)

            if total_reviews == 0:
                total_reviews_match = _extract_total_reviews(body_text)
                if total_reviews_match is not None:
                    total_reviews = total_reviews_match

            if average_rating is None:
                rating_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:/|out of)\s*5", body_text, re.IGNORECASE)
                if rating_match:
                    average_rating = float(rating_match.group(1))

            if total_reviews == 0 and total_reviews_hint:
                total_reviews = total_reviews_hint

            details["average_rating"] = average_rating
            details["total_reviews"] = total_reviews

            total_reviews_for_calc = total_reviews or total_reviews_hint
            star_text_sources = []

            aria_texts = await page.eval_on_selector_all(
                '[aria-label*="star" i], [title*="star" i]',
                'els => els.map(el => (el.getAttribute("aria-label") || el.getAttribute("title") || ""))',
            )
            star_text_sources.extend([text for text in aria_texts if text])

            try:
                inner_texts = await page.locator(r"text=/[1-5]\\s*star/i").all_inner_texts()
                star_text_sources.extend([_sanitize_text(text) for text in inner_texts if text.strip()])
            except Exception:
                pass

            star_text_sources.append(body_text)

            details.update(
                parse_rating_breakdown(
                    star_text_sources,
                    total_reviews_for_calc=total_reviews_for_calc,
                )
            )

            details["fetched"] = True
            break

        except PlaywrightTimeout:
            if attempt < max_retries - 1:
                await asyncio.sleep((attempt + 1) * 2)
            else:
                break
        except Exception:
            if attempt < max_retries - 1:
                await asyncio.sleep((attempt + 1) * 2)
            else:
                break

    return details


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
        context = await browser.new_context()
        page = await context.new_page()
        detail_page = await context.new_page() if get_detailed_ratings else None
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
            product = await _parse_card(card, category_url, get_detailed_ratings, detail_page)
            results.append(product)
            if rate_limit_ms:
                await asyncio.sleep(rate_limit_ms / 1000)

        if detail_page:
            await detail_page.close()
        await context.close()
        await browser.close()

    return results


__all__ = ["scrape_whop_search"]
