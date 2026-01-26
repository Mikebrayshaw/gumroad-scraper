"""Workflow-friendly full Gumroad scrape runner."""
from __future__ import annotations

import argparse
import asyncio
import random
from dataclasses import asdict
from pathlib import Path

from categories import CATEGORY_TREE, build_discover_url
from gumroad_scraper import Product, scrape_discover_page, save_to_csv

MAX_PRODUCTS = 50
CATEGORY_DELAY_SECONDS = 60
SUBCATEGORY_DELAY_SECONDS = 30
FAILURE_COOLDOWN_SECONDS = 300


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run full Gumroad scrapes for GitHub Actions.")
    parser.add_argument(
        "--mode",
        choices=["full", "test"],
        default="full",
        help="Scrape mode: full (all categories) or test (two categories).",
    )
    return parser.parse_args()


def _apply_jitter(seconds: int) -> int:
    return seconds + random.randint(5, 15)


async def _wait_with_jitter(seconds: int, message: str) -> None:
    wait_time = _apply_jitter(seconds)
    print(f"{message} (jitter: {wait_time - seconds}s)")
    await asyncio.sleep(wait_time)


def _merge_product(existing: Product | None, incoming: Product) -> Product:
    if existing is None:
        return incoming
    if not existing.subcategory and incoming.subcategory:
        existing.subcategory = incoming.subcategory
    return existing


async def _scrape_with_retry(
    *,
    category_slug: str,
    subcategory_slug: str | None,
    url: str,
) -> list[Product]:
    for attempt in range(2):
        try:
            return await scrape_discover_page(
                category_url=url,
                category_slug=category_slug,
                subcategory_slug=subcategory_slug,
                max_products=MAX_PRODUCTS,
                get_detailed_ratings=False,
                rate_limit_ms=500,
                show_progress=False,
            )
        except Exception as exc:  # noqa: BLE001 - workflow resilience
            print(f"Scrape failed for {url}: {exc}")
            if attempt == 0:
                print(
                    f"Waiting {FAILURE_COOLDOWN_SECONDS // 60} minutes before retrying failed scrape..."
                )
                await asyncio.sleep(FAILURE_COOLDOWN_SECONDS)
            else:
                print("Retry failed; skipping this scrape.")
    return []


async def run() -> None:
    args = parse_args()
    categories = list(CATEGORY_TREE)
    if args.mode == "test":
        categories = categories[:2]

    output_dir = Path("scrape_outputs")
    output_dir.mkdir(parents=True, exist_ok=True)

    total_scraped = 0
    all_products: dict[str, Product] = {}

    for category_index, category in enumerate(categories, start=1):
        print(f"Starting category: {category.label} ({category_index} of {len(categories)})")
        category_products: dict[str, Product] = {}
        subcategories = category.subcategories

        for sub_index, subcategory in enumerate(subcategories, start=1):
            sub_slug = subcategory.slug or None
            sub_label = subcategory.label
            print(
                f"Starting subcategory: {sub_label} ({sub_index} of {len(subcategories)}) "
                f"for {category.label}"
            )
            url = build_discover_url(category.slug, sub_slug)
            products = await _scrape_with_retry(
                category_slug=category.slug,
                subcategory_slug=sub_slug,
                url=url,
            )
            total_scraped += len(products)
            for product in products:
                category_products[product.product_url] = _merge_product(
                    category_products.get(product.product_url),
                    product,
                )
                all_products[product.product_url] = _merge_product(
                    all_products.get(product.product_url),
                    product,
                )

            if sub_index < len(subcategories):
                await _wait_with_jitter(
                    SUBCATEGORY_DELAY_SECONDS,
                    f"Waiting {SUBCATEGORY_DELAY_SECONDS} seconds before next subcategory...",
                )

        category_csv = output_dir / f"{category.slug}.csv"
        save_to_csv(list(category_products.values()), str(category_csv))

        if category_index < len(categories):
            await _wait_with_jitter(
                CATEGORY_DELAY_SECONDS,
                f"Waiting {CATEGORY_DELAY_SECONDS} seconds before next category...",
            )

    master_csv = output_dir / "gumroad_full.csv"
    save_to_csv(list(all_products.values()), str(master_csv))
    print(f"Completed: {total_scraped} total products scraped")


if __name__ == "__main__":
    asyncio.run(run())
