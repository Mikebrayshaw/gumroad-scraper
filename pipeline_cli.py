"""Command-line interface for the scrape -> ingest -> diff -> export pipeline."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, List
from uuid import uuid4

from categories import category_url_map
from gumroad_scraper import Product as RawGumroadProduct
from gumroad_scraper import scrape_discover_page
from models import ProductSnapshot, estimate_revenue
from pipeline import PipelineDatabase, configure_logging, load_snapshots_from_json, snapshots_to_json
from supabase_utils import extract_platform_product_id


def _snapshot_from_gumroad(product: RawGumroadProduct, scraped_at: datetime, category: str) -> ProductSnapshot:
    revenue, confidence = estimate_revenue(product.price_usd, product.sales_count, False, product.currency)
    snapshot = ProductSnapshot(
        platform="gumroad",
        product_id=extract_platform_product_id(product.product_url),
        url=product.product_url,
        title=product.product_name,
        creator_name=product.creator_name,
        creator_url=None,
        category=category,
        price_amount=product.price_usd,
        price_currency=product.currency,
        price_is_pwyw=False,
        rating_avg=product.average_rating,
        rating_count=product.total_reviews,
        sales_count=product.sales_count,
        revenue_estimate=revenue,
        revenue_confidence=confidence,
        tags=[],
        scraped_at=scraped_at,
        raw_source_hash="",
    )
    snapshot.raw_source_hash = snapshot.compute_hash()
    return snapshot


def cmd_scrape(args: argparse.Namespace) -> None:
    logger = configure_logging()
    scraped_at = datetime.utcnow()
    run_id = str(uuid4())
    logger.info("Starting scrape", extra={"run_id": run_id})
    categories = category_url_map()
    target = categories.get(args.category, args.category)
    products = asyncio.run(
        scrape_discover_page(
            category_url=target,
            max_products=args.max_products,
            get_detailed_ratings=not args.fast,
            rate_limit_ms=args.rate_limit,
            show_progress=not args.no_progress,
        )
    )

    snapshots: List[ProductSnapshot] = [
        _snapshot_from_gumroad(p, scraped_at, args.category) for p in products
    ]

    meta = {
        "started_at": scraped_at.isoformat(),
        "category": args.category,
        "max_products": args.max_products,
        "fast": args.fast,
    }
    payload = snapshots_to_json(run_id, meta, snapshots)

    output_path = Path(args.out or f"data/runs/{run_id}.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("Scrape finished", extra={"run_id": run_id})
    print(f"Run ID: {run_id}\nSaved: {output_path}")


def cmd_ingest(args: argparse.Namespace) -> None:
    logger = configure_logging(args.run_id)
    db = PipelineDatabase()
    run_id, snapshots, meta = load_snapshots_from_json(args.path)
    run_id = args.run_id or run_id
    logger.info("Starting ingest", extra={"run_id": run_id})
    db.start_run(platform="gumroad", category=meta.get("category"), source="scrape", config=meta, run_id=run_id)
    db.upsert_products(snapshots, run_id)
    db.complete_run(run_id, total_products=len(snapshots), summary={"source_path": args.path})
    logger.info("Ingest completed", extra={"run_id": run_id})
    print(f"Ingested run {run_id} with {len(snapshots)} products")


def cmd_diff(args: argparse.Namespace) -> None:
    logger = configure_logging(args.run_id)
    db = PipelineDatabase()
    logger.info("Computing diffs", extra={"run_id": args.run_id})
    diffs = db.compute_diffs(args.run_id)
    logger.info("Diffs stored", extra={"run_id": args.run_id})
    print(f"Computed {len(diffs)} diffs for run {args.run_id}")


def _export_csv(rows: Iterable[dict], out_path: Path) -> None:
    import csv

    if not rows:
        out_path.write_text("")
        return
    fieldnames = sorted({k for row in rows for k in row.keys()})
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def cmd_export(args: argparse.Namespace) -> None:
    db = PipelineDatabase()
    payload = db.export_run(args.run_id)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if args.format == "json":
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        _export_csv(payload["snapshots"], out_path)
    print(f"Exported run {args.run_id} to {out_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gumroad pipeline CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_scrape = sub.add_parser("scrape", help="Scrape Gumroad and write snapshots to disk")
    p_scrape.add_argument("--category", type=str, default="design", help="Category URL to scrape")
    p_scrape.add_argument("--max-products", type=int, default=50)
    p_scrape.add_argument("--fast", action="store_true")
    p_scrape.add_argument("--rate-limit", type=int, default=500)
    p_scrape.add_argument("--no-progress", action="store_true")
    p_scrape.add_argument("--out", type=str, help="Path to write snapshot JSON")
    p_scrape.set_defaults(func=cmd_scrape)

    p_ingest = sub.add_parser("ingest", help="Load a run JSON into the database")
    p_ingest.add_argument("path", type=str, help="Path to run JSON from scrape step")
    p_ingest.add_argument("--run-id", type=str, help="Override/force run id")
    p_ingest.set_defaults(func=cmd_ingest)

    p_diff = sub.add_parser("diff", help="Compute diffs for a run")
    p_diff.add_argument("--run-id", required=True, type=str)
    p_diff.set_defaults(func=cmd_diff)

    p_export = sub.add_parser("export", help="Export run snapshots or diffs")
    p_export.add_argument("--run-id", required=True, type=str)
    p_export.add_argument("--format", choices=["csv", "json"], default="json")
    p_export.add_argument("--out", required=True, type=str)
    p_export.set_defaults(func=cmd_export)

    return parser


def main(argv: List[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
