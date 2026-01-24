"""Command-line interface for the scrape -> ingest -> diff -> export pipeline."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence
from uuid import uuid4

from categories import category_url_map
from gumroad_scraper import Product as RawGumroadProduct
from gumroad_scraper import scrape_discover_page
from models import ProductSnapshot, estimate_revenue
from opportunity_engine import detect_alerts, load_config, render_alerts_markdown
from opportunity_scoring import score_product_dict
from pipeline import PipelineDatabase, configure_logging, load_snapshots_from_json, snapshots_to_json
from supabase_utils import extract_platform_product_id


def _snapshot_from_gumroad(product: RawGumroadProduct, scraped_at: datetime, category: str) -> ProductSnapshot:
    revenue, confidence = estimate_revenue(
        product.price_usd,
        product.sales_count,
        product.price_is_pwyw,
        product.currency,
    )
    snapshot = ProductSnapshot(
        platform="gumroad",
        product_id=extract_platform_product_id(product.product_url),
        url=product.product_url,
        title=product.product_name,
        creator_name=product.creator_name,
        creator_url=None,
        category=category,
        subcategory=product.subcategory,
        description=product.description,
        price_amount=product.price_usd,
        price_currency=product.currency,
        price_is_pwyw=product.price_is_pwyw,
        rating_avg=product.average_rating,
        rating_count=product.total_reviews,
        mixed_review_count=product.mixed_review_count,
        mixed_review_percent=product.mixed_review_percent,
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


def _snapshot_to_dict(rows: Iterable) -> List[dict]:
    return [
        {
            "platform": row.platform,
            "product_id": row.product_id,
            "run_id": row.run_id,
            "url": row.url,
            "title": row.title,
            "creator_name": row.creator_name,
            "category": row.category,
            "price_amount": row.price_amount,
            "price_currency": row.price_currency,
            "rating_avg": row.rating_avg,
            "rating_count": row.rating_count,
            "sales_count": row.sales_count,
            "revenue_estimate": row.revenue_estimate,
        }
        for row in rows
    ]


def _diffs_to_map(rows: Iterable) -> dict:
    mapping = {}
    for row in rows:
        mapping[(row.platform, row.product_id)] = {
            "rating_count_delta": row.rating_count_delta,
            "sales_count_delta": row.sales_count_delta,
            "price_delta": row.price_delta,
            "previous_run_id": row.previous_run_id,
        }
    return mapping


def _write_outputs(output_dir: Path, run_id: str, opportunities: List[dict], alerts: List[dict], top_k: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    import csv

    csv_path = output_dir / "opportunities.csv"
    json_path = output_dir / "opportunities.json"
    brief_path = output_dir / "opportunity_briefs.md"
    alerts_path = output_dir / "alerts.md"

    fieldnames = sorted({k for row in opportunities for k in row.keys() if k != "saturation_examples"})
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows([{k: v for k, v in row.items() if k in fieldnames} for row in opportunities])

    json_path.write_text(json.dumps(opportunities, indent=2), encoding="utf-8")
    brief_path.write_text(render_opportunity_briefs(opportunities, top_k=top_k), encoding="utf-8")
    alerts_path.write_text(render_alerts_markdown(alerts, run_id), encoding="utf-8")


def _score_snapshot(snapshot: Mapping[str, Optional[float | str | int]]) -> dict:
    scored = score_product_dict(
        {
            "product_name": snapshot.get("title", "") or "",
            "price_usd": snapshot.get("price_amount") or 0,
            "average_rating": snapshot.get("rating_avg"),
            "total_reviews": snapshot.get("rating_count") or 0,
            "mixed_review_percent": snapshot.get("mixed_review_percent") or 0,
            "sales_count": snapshot.get("sales_count"),
            "estimated_revenue": snapshot.get("revenue_estimate"),
        }
    )
    return scored


def render_opportunity_briefs(opportunities: Sequence[Mapping[str, Optional[float | str | int]]], top_k: int = 10) -> str:
    lines = ["# Opportunity Briefs\n"]
    for opp in opportunities[:top_k]:
        title = opp.get("title") or "Untitled"
        category = opp.get("category") or "Uncategorised"
        url = opp.get("url") or "N/A"
        score = opp.get("opportunity_score")
        price_amount = opp.get("price_amount")
        price_currency = opp.get("price_currency") or ""
        price_display = f"${price_amount:.2f}" if isinstance(price_amount, (int, float)) else "N/A"
        reason_summary = opp.get("reason_summary") or ""
        rating_avg = opp.get("rating_avg")
        rating_count = opp.get("rating_count") or 0
        sales_count = opp.get("sales_count")

        lines.append(f"## {title} ({category})")
        lines.append(f"URL: {url}")
        lines.append("")
        if score is not None:
            lines.append(f"**Opportunity score:** {score}/100.")
        if reason_summary:
            lines.append(f"**Signals:** {reason_summary}.")
        lines.append(f"**Price:** {price_display} {price_currency}".strip())
        lines.append(f"**Reviews:** {rating_count} | Rating: {rating_avg if rating_avg is not None else 'N/A'}")
        lines.append(f"**Sales:** {sales_count if sales_count is not None else 'N/A'}")
        lines.append("")
    return "\n".join(lines)


def cmd_generate_outputs(args: argparse.Namespace) -> None:
    logger = configure_logging(args.run_id)
    db = PipelineDatabase()
    config = load_config(args.config)

    snapshots_rows = db.get_snapshots(args.run_id)
    if not snapshots_rows:
        raise ValueError(f"Run {args.run_id} has no snapshots")

    diffs_rows = db.get_diffs(args.run_id)
    if not diffs_rows:
        logger.info("No diffs found, computing them now", extra={"run_id": args.run_id})
        diffs_rows = db.compute_diffs(args.run_id)

    current_run = db.get_run(args.run_id)
    previous_run = db.previous_run(args.run_id)

    snapshots = _snapshot_to_dict(snapshots_rows)
    diffs_map = _diffs_to_map(diffs_rows)

    opportunities: List[dict] = []
    for snap in snapshots:
        diff = diffs_map.get((snap.get("platform"), snap.get("product_id")), {})
        scored = _score_snapshot(snap)
        opportunities.append(
            {
                "run_id": snap.get("run_id"),
                "platform": snap.get("platform"),
                "product_id": snap.get("product_id"),
                "title": snap.get("title"),
                "url": snap.get("url"),
                "category": snap.get("category"),
                "creator_name": snap.get("creator_name"),
                "price_amount": snap.get("price_amount"),
                "price_currency": snap.get("price_currency"),
                "rating_avg": snap.get("rating_avg"),
                "rating_count": snap.get("rating_count"),
                "rating_count_delta": diff.get("rating_count_delta"),
                "sales_count": snap.get("sales_count"),
                "sales_count_delta": diff.get("sales_count_delta"),
                "opportunity_score": scored.get("opportunity_score"),
                "reason_summary": scored.get("score_notes"),
            }
        )

    opportunities.sort(key=lambda row: row.get("opportunity_score") or 0, reverse=True)
    db.upsert_opportunity_scores(opportunities)

    alerts = detect_alerts(args.run_id, snapshots, diffs_map, previous_run.id if previous_run else None, config)
    if alerts:
        db.insert_alerts(alerts)

    output_dir = Path(args.output_dir or f"data/opportunities/{args.run_id}")
    _write_outputs(output_dir, args.run_id, opportunities, alerts, args.top_k)
    logger.info("Generated opportunities", extra={"run_id": args.run_id})
    print(f"Wrote opportunities and alerts to {output_dir}")


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

    p_generate = sub.add_parser("generate_outputs", help="Create opportunity scores, briefs, and alerts for a run")
    p_generate.add_argument("--run-id", required=True, type=str)
    p_generate.add_argument("--top-k", type=int, default=10)
    p_generate.add_argument("--config", type=str, help="Path to opportunity config JSON")
    p_generate.add_argument("--output-dir", type=str, help="Directory for output files")
    p_generate.set_defaults(func=cmd_generate_outputs)

    return parser


def main(argv: List[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
