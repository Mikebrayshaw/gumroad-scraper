"""Workflow-friendly full Gumroad scrape runner.

Supports both CLI/GitHub Actions usage and Streamlit UI integration.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import urllib.request
import urllib.error
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from categories import CATEGORY_TREE, build_discover_url
from gumroad_scraper import Product, scrape_discover_page, save_to_csv
from opportunity_scoring import score_product_dict
from supabase_utils import SupabasePersistence, SupabaseRunStore, get_supabase_client

MAX_PRODUCTS = 50
CATEGORY_DELAY_SECONDS = 60
SUBCATEGORY_DELAY_SECONDS = 30
FAILURE_COOLDOWN_SECONDS = 300
SECONDS_PER_MINUTE = 60


class AdaptiveDelayConfig:
    """Adaptive delay configuration that increases on failures."""
    def __init__(self):
        self.base_category_delay = 60
        self.base_subcategory_delay = 30
        self.failure_cooldown = 300
        self.consecutive_failures = 0
        self.max_multiplier = 4
    
    def record_success(self):
        """Reset failure counter on success."""
        self.consecutive_failures = max(0, self.consecutive_failures - 1)
    
    def record_failure(self):
        """Increase delays after failure."""
        self.consecutive_failures += 1
    
    @property
    def multiplier(self) -> float:
        """Get delay multiplier based on consecutive failures."""
        return min(1 + (self.consecutive_failures * 0.5), self.max_multiplier)
    
    def get_category_delay(self) -> int:
        return int(self.base_category_delay * self.multiplier)
    
    def get_subcategory_delay(self) -> int:
        return int(self.base_subcategory_delay * self.multiplier)
    
    def get_failure_cooldown(self) -> int:
        return int(self.failure_cooldown * self.multiplier)


def send_completion_notification(total_products: int, total_categories: int, errors: int) -> None:
    """Log completion and optionally send webhook notification."""
    # Always log to console with clear banner
    print("\n" + "=" * 60)
    print("[SUCCESS] SCRAPE COMPLETE [SUCCESS]")
    print("=" * 60)
    print(f"  Total products: {total_products:,}")
    print(f"  Categories:     {total_categories}")
    print(f"  Errors:         {errors}")
    print(f"  Completed at:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")

    # Optional webhook notification (set SCRAPE_WEBHOOK_URL in .env)
    webhook_url = os.environ.get("SCRAPE_WEBHOOK_URL")
    if webhook_url:
        try:
            payload = {
                "text": f"[SUCCESS] Gumroad scrape complete!\n• {total_products:,} products\n• {total_categories} categories\n• {errors} errors"
            }
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
            print("Webhook notification sent!")
        except urllib.error.URLError as e:
            print(f"Webhook failed: {e}")


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


async def _wait_with_jitter(seconds: int, label: str) -> None:
    wait_time = _apply_jitter(seconds)
    jitter = wait_time - seconds
    print(f"{label}: waiting {wait_time}s (base {seconds}s + {jitter}s jitter)")
    await asyncio.sleep(wait_time)


def _merge_product(existing: Product | None, incoming: Product) -> Product:
    if existing is None or existing.subcategory or not incoming.subcategory:
        return existing or incoming
    data = asdict(existing)
    data["subcategory"] = incoming.subcategory
    return Product(**data)


async def _scrape_with_retry(
    *,
    category_slug: str,
    subcategory_slug: str | None,
    url: str,
    delay_config: AdaptiveDelayConfig,
    max_retries: int = 3,
) -> tuple[list[Product], dict | None]:
    """Scrape with exponential backoff retry logic.
    
    Returns:
        Tuple of (products list, debug_info dict if failure occurred)
    """
    debug_info = None
    
    for attempt in range(max_retries):
        try:
            products = await scrape_discover_page(
                category_url=url,
                category_slug=category_slug,
                subcategory_slug=subcategory_slug,
                max_products=MAX_PRODUCTS,
                get_detailed_ratings=False,
                rate_limit_ms=500,
                show_progress=False,
            )
            
            # Check if we got zero products (possible block)
            if len(products) == 0:
                print(f"[WARN] Zero products returned for {url} - possible rate limit/block")
                delay_config.record_failure()
                # Don't immediately retry - let the main loop handle delay
                return products, {"zero_products": True, "url": url}
            
            delay_config.record_success()
            return products, None
            
        except Exception as exc:
            delay_config.record_failure()
            # Cap exponential backoff to prevent excessive wait times (max 30 minutes)
            backoff_time = min(delay_config.get_failure_cooldown() * (2 ** attempt), 1800)
            
            print(f"[ERROR] Scrape failed for {url}: {exc}")
            print(f"   Attempt {attempt + 1}/{max_retries}")
            print(f"   Consecutive failures: {delay_config.consecutive_failures}")
            
            if attempt < max_retries - 1:
                print(f"   Waiting {backoff_time}s before retry (exponential backoff, capped at 30min)...")
                await asyncio.sleep(backoff_time)
            else:
                print("   Max retries reached; skipping this scrape.")
                return [], {"error": str(exc), "url": url}
    
    return [], None


async def scrape_all_categories(
    max_per_category: int = 100,
    rate_limit_ms: int = 500,
    fast_mode: bool = False,
    progress_callback: Optional[Callable[[str, int, int, int], None]] = None,
) -> dict:
    """
    Scrape all Gumroad categories and save to Supabase.

    This function is designed for Streamlit UI integration with progress callbacks.

    Args:
        max_per_category: Max products to scrape per category
        rate_limit_ms: Delay between requests
        fast_mode: Skip detailed product pages if True
        progress_callback: Optional callback(category_label, current_idx, total_categories, products_so_far)

    Returns:
        Summary dict with totals
    """
    client = get_supabase_client()
    print(f"[DEBUG] Supabase client = {client}")
    print(f"[DEBUG] SUPABASE_URL = {os.getenv('SUPABASE_URL', 'NOT SET')[:20]}...")
    print(f"[DEBUG] SERVICE_ROLE_KEY present = {bool(os.getenv('SUPABASE_SERVICE_ROLE_KEY'))}")
    run_store = SupabaseRunStore(client)

    total_categories = len(CATEGORY_TREE)
    total_products = 0
    category_results = []

    for idx, category in enumerate(CATEGORY_TREE):
        category_label = category.label
        category_slug = category.slug

        if progress_callback:
            progress_callback(category_label, idx, total_categories, total_products)

        try:
            # Start a run for this category
            run_id = run_store.start_run(
                category=category_slug,
                subcategory="",
                max_products=max_per_category,
                fast_mode=fast_mode,
                rate_limit_ms=rate_limit_ms,
            )

            url = build_discover_url(category_slug, "")

            products = await scrape_discover_page(
                category_url=url,
                category_slug=category_slug,
                subcategory_slug="",
                max_products=max_per_category,
                get_detailed_ratings=not fast_mode,
                rate_limit_ms=rate_limit_ms,
            )

            # Score products
            product_dicts = [asdict(p) for p in products]
            scored_products = [score_product_dict(p) for p in product_dicts]

            # Save to Supabase
            persistence = SupabasePersistence(client)
            upsert_result = persistence.upsert_products(run_id, products)
            print(f"Upserted products for {category_slug}: {upsert_result}")
            totals = run_store.record_snapshots(run_id, products, scored_products)
            run_store.complete_run(run_id, totals={"total": len(products), **totals})

            total_products += len(products)
            category_results.append({
                "category": category_label,
                "slug": category_slug,
                "products": len(products),
                "status": "success",
            })

        except Exception as e:
            category_results.append({
                "category": category_label,
                "slug": category_slug,
                "products": 0,
                "status": "error",
                "error": str(e),
            })

        # Wait between categories to avoid rate limiting (critical!)
        if idx < total_categories - 1:
            await _wait_with_jitter(CATEGORY_DELAY_SECONDS, f"Waiting before next category")

    # Send completion notification
    errors = len([c for c in category_results if c["status"] == "error"])
    send_completion_notification(total_products, total_categories, errors)

    return {
        "total_categories": total_categories,
        "total_products": total_products,
        "completed_at": datetime.utcnow().isoformat(),
        "categories": category_results,
    }


async def run() -> None:
    """CLI entry point for GitHub Actions workflow."""
    args = parse_args()
    categories = list(CATEGORY_TREE)
    if args.mode == "test":
        categories = categories[:2]

    output_dir = Path("scrape_outputs")
    output_dir.mkdir(parents=True, exist_ok=True)

    total_scraped = 0
    all_products: dict[str, Product] = {}
    delay_config = AdaptiveDelayConfig()

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
            products, debug_info = await _scrape_with_retry(
                category_slug=category.slug,
                subcategory_slug=sub_slug,
                url=url,
                delay_config=delay_config,
            )
            
            if debug_info:
                print(f"[WARN] Debug info for {url}: {debug_info}")
            
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
                delay_seconds = delay_config.get_subcategory_delay()
                await _wait_with_jitter(
                    delay_seconds,
                    "Waiting before next subcategory",
                )

        category_csv = output_dir / f"{category.slug}.csv"
        save_to_csv(list(category_products.values()), str(category_csv))

        if category_index < len(categories):
            delay_seconds = delay_config.get_category_delay()
            await _wait_with_jitter(
                delay_seconds,
                "Waiting before next category",
            )

    master_csv = output_dir / "gumroad_full.csv"
    save_to_csv(list(all_products.values()), str(master_csv))
    print(f"Completed: {total_scraped} total products scraped")

    products = list(all_products.values())
    supabase_client = get_supabase_client()
    persistence = SupabasePersistence(supabase_client)
    run_store = SupabaseRunStore(supabase_client)
    run_id = run_store.start_run(
        category="all",
        subcategory="",
        max_products=MAX_PRODUCTS,
        fast_mode=False,
        rate_limit_ms=0,
    )
    upsert_totals = persistence.upsert_products(run_id, products)
    print(f"Supabase upsert results: {upsert_totals}")
    scored_products = [score_product_dict(asdict(product)) for product in products]
    snapshot_totals = run_store.record_snapshots(run_id, products, scored_products)
    run_store.complete_run(run_id, totals={"total": len(products), **snapshot_totals})


if __name__ == "__main__":
    asyncio.run(run())
