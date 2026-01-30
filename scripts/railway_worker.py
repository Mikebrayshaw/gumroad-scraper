"""Railway-friendly worker to run full Gumroad scrapes with status reporting."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from categories import CATEGORY_TREE, build_discover_url, should_skip_subcategory
from gumroad_scraper import Product, save_to_csv
from opportunity_scoring import score_product_dict
from supabase_utils import SupabasePersistence, SupabaseRunStore, get_supabase_client
from utils.progress import ProgressTracker, write_status_file

from scripts.full_gumroad_scrape import (
    AdaptiveDelayConfig,
    MAX_PRODUCTS,
    _merge_product,
    _scrape_with_retry,
    _wait_with_jitter,
)

DEFAULT_MAX_CONSECUTIVE_FATAL_ERRORS = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Railway worker for full Gumroad scrapes.")
    parser.add_argument(
        "--mode",
        choices=["full", "test"],
        default="full",
        help="Scrape mode: full (all categories) or test (two categories).",
    )
    parser.add_argument(
        "--max-consecutive-fatal-errors",
        type=int,
        default=None,
        help=(
            "Exit nonzero after this many consecutive fatal errors. "
            "Defaults to MAX_CONSECUTIVE_FATAL_ERRORS env var or 5."
        ),
    )
    return parser.parse_args()


def _get_max_consecutive_fatal_errors(args: argparse.Namespace) -> int:
    if args.max_consecutive_fatal_errors is not None:
        return args.max_consecutive_fatal_errors
    env_value = os.getenv("MAX_CONSECUTIVE_FATAL_ERRORS")
    if env_value is None:
        return DEFAULT_MAX_CONSECUTIVE_FATAL_ERRORS
    try:
        return int(env_value)
    except ValueError:
        return DEFAULT_MAX_CONSECUTIVE_FATAL_ERRORS


def _format_progress_line(snapshot: dict, category_slug: str) -> str:
    counts = snapshot["counts"]
    elapsed = _format_seconds(snapshot["elapsed_seconds"])
    eta = _format_seconds(snapshot["eta_seconds"]) if snapshot["eta_seconds"] is not None else "N/A"
    return (
        f"[PROGRESS] {snapshot['completed']}/{snapshot['planned_total']}"
        f" | products={snapshot['total_products']}"
        f" | invalid_route={counts['invalid_route']} zero_products={counts['zero_products']}"
        f" captcha_suspected={counts['captcha_suspected']} errors={counts['errors']}"
        f" | elapsed={elapsed} eta={eta}"
        f" | category={category_slug}"
    )


def _format_seconds(value: float | None) -> str:
    if value is None:
        return "N/A"
    total_seconds = int(round(value))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _write_status(
    *,
    url: str,
    category_slug: str,
    snapshot: dict,
) -> None:
    status = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "current_url": url,
        "category": category_slug,
        "index": snapshot["completed"],
        "total": snapshot["planned_total"],
        "products_total": snapshot["total_products"],
        "invalid_route": snapshot["counts"]["invalid_route"],
        "errors": snapshot["counts"]["errors"],
    }
    write_status_file(status)


async def run() -> None:
    args = parse_args()
    categories = list(CATEGORY_TREE)
    if args.mode == "test":
        categories = categories[:2]

    max_consecutive_fatal_errors = _get_max_consecutive_fatal_errors(args)

    output_dir = Path("scrape_outputs")
    output_dir.mkdir(parents=True, exist_ok=True)

    total_scraped = 0
    consecutive_fatal_errors = 0
    all_products: dict[str, Product] = {}
    delay_config = AdaptiveDelayConfig()
    planned_total = sum(len(category.subcategories) for category in categories)
    tracker = ProgressTracker(
        run_id=datetime.utcnow().strftime("railway_full_scrape_%Y%m%d_%H%M%S"),
        planned_total=planned_total,
    )

    for category in categories:
        category_products: dict[str, Product] = {}
        subcategories = category.subcategories

        for subcategory in subcategories:
            sub_slug = subcategory.slug or None

            if should_skip_subcategory(subcategory):
                snapshot = tracker.update(
                    category=category.slug,
                    subcategory=sub_slug,
                    products_delta=0,
                    completed_increment=1,
                )
                print(_format_progress_line(snapshot, category.slug))
                continue

            url = build_discover_url(category.slug, subcategory_slug=sub_slug, subcategory=subcategory)
            try:
                products, debug_info = await _scrape_with_retry(
                    category_slug=category.slug,
                    subcategory_slug=sub_slug,
                    url=url,
                    delay_config=delay_config,
                )
            except Exception as exc:
                debug_info = {"error": str(exc)}
                products = []

            fatal_error = bool(debug_info and debug_info.get("error"))
            if fatal_error:
                consecutive_fatal_errors += 1
            else:
                consecutive_fatal_errors = 0

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

            snapshot = tracker.update(
                category=category.slug,
                subcategory=sub_slug,
                products_delta=len(products),
                completed_increment=1,
                invalid_route=bool(debug_info and debug_info.get("invalid_route")),
                zero_products=bool(debug_info and debug_info.get("zero_products")),
                captcha_suspected=bool(debug_info and debug_info.get("possible_captcha")),
                error=fatal_error,
            )
            _write_status(url=url, category_slug=category.slug, snapshot=snapshot)
            print(_format_progress_line(snapshot, category.slug))

            if consecutive_fatal_errors > max_consecutive_fatal_errors:
                print(
                    "[ERROR] Exiting due to consecutive fatal errors "
                    f"({consecutive_fatal_errors}/{max_consecutive_fatal_errors})."
                )
                sys.exit(1)

            if subcategory != subcategories[-1]:
                delay_seconds = delay_config.get_subcategory_delay()
                await _wait_with_jitter(
                    delay_seconds,
                    "Waiting before next subcategory",
                )

        category_csv = output_dir / f"{category.slug}.csv"
        save_to_csv(list(category_products.values()), str(category_csv))

        if category != categories[-1]:
            delay_seconds = delay_config.get_category_delay()
            await _wait_with_jitter(
                delay_seconds,
                "Waiting before next category",
            )

    master_csv = output_dir / "gumroad_full.csv"
    save_to_csv(list(all_products.values()), str(master_csv))

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
    persistence.upsert_products(run_id, products)
    scored_products = [score_product_dict(asdict(product)) for product in products]
    snapshot_totals = run_store.record_snapshots(run_id, products, scored_products)
    run_store.complete_run(run_id, totals={"total": len(products), **snapshot_totals})


if __name__ == "__main__":
    asyncio.run(run())
