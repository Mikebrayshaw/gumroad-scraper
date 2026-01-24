"""
Scheduled ingestion runner for Gumroad scraping.

This CLI loads ingestion jobs from a JSON config, runs scrape_discover_page for
configured category or query URLs, and persists product data to a SQL database
(SQLite or PostgreSQL). Product URLs are used as the unique key and crawl
timestamps are recorded for change detection.
"""
import argparse
import asyncio
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from sqlalchemy import Column, DateTime, Float, Integer, String, UniqueConstraint, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from gumroad_scraper import Product, save_to_csv
from platforms import get_scraper
from supabase_utils import SupabasePersistence

Base = declarative_base()


class ProductRecord(Base):
    __tablename__ = "products"
    __table_args__ = (UniqueConstraint("product_url", name="uq_product_url"),)

    id = Column(Integer, primary_key=True)
    product_url = Column(String, nullable=False, index=True)
    product_name = Column(String, nullable=False)
    creator_name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    subcategory = Column(String, nullable=True)
    price_usd = Column(Float, nullable=True)
    original_price = Column(String, nullable=True)
    currency = Column(String, nullable=True)
    average_rating = Column(Float, nullable=True)
    total_reviews = Column(Integer, nullable=True)
    rating_1_star = Column(Integer, nullable=True)
    rating_2_star = Column(Integer, nullable=True)
    rating_3_star = Column(Integer, nullable=True)
    rating_4_star = Column(Integer, nullable=True)
    rating_5_star = Column(Integer, nullable=True)
    mixed_review_count = Column(Integer, nullable=True)
    mixed_review_percent = Column(Float, nullable=True)
    sales_count = Column(Integer, nullable=True)
    estimated_revenue = Column(Float, nullable=True)
    revenue_confidence = Column(String, nullable=True)
    description = Column(String, nullable=True)
    first_seen_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_crawled_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    @classmethod
    def from_product(cls, product: Product, crawled_at: datetime) -> "ProductRecord":
        payload = asdict(product)
        return cls(
            product_url=payload["product_url"],
            product_name=payload.get("product_name", ""),
            creator_name=payload.get("creator_name", ""),
            category=payload.get("category", ""),
            subcategory=payload.get("subcategory"),
            price_usd=payload.get("price_usd"),
            original_price=payload.get("original_price"),
            currency=payload.get("currency"),
            average_rating=payload.get("average_rating"),
            total_reviews=payload.get("total_reviews"),
            rating_1_star=payload.get("rating_1_star"),
            rating_2_star=payload.get("rating_2_star"),
            rating_3_star=payload.get("rating_3_star"),
            rating_4_star=payload.get("rating_4_star"),
            rating_5_star=payload.get("rating_5_star"),
            mixed_review_count=payload.get("mixed_review_count"),
            mixed_review_percent=payload.get("mixed_review_percent"),
            sales_count=payload.get("sales_count"),
            estimated_revenue=payload.get("estimated_revenue"),
            revenue_confidence=payload.get("revenue_confidence"),
            description=payload.get("description"),
            first_seen_at=crawled_at,
            last_crawled_at=crawled_at,
            last_updated_at=crawled_at,
        )

    def apply_updates(self, product: Product, crawled_at: datetime) -> bool:
        payload = asdict(product)
        changed = False
        for field in [
            "product_name",
            "creator_name",
            "category",
            "subcategory",
            "price_usd",
            "original_price",
            "currency",
            "average_rating",
            "total_reviews",
            "rating_1_star",
            "rating_2_star",
            "rating_3_star",
            "rating_4_star",
            "rating_5_star",
            "mixed_review_percent",
            "sales_count",
            "estimated_revenue",
        ]:
            new_value = payload.get(field)
            if getattr(self, field) != new_value:
                setattr(self, field, new_value)
                changed = True

        self.last_crawled_at = crawled_at
        if changed:
            self.last_updated_at = crawled_at
        return changed


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def pick_jobs(config: dict, schedule: Optional[str], names: Optional[set[str]]) -> List[dict]:
    jobs: List[dict] = config.get("jobs", [])
    if schedule and schedule != "all":
        jobs = [job for job in jobs if job.get("schedule") == schedule]
    if names:
        jobs = [job for job in jobs if job.get("name") in names]
    return jobs


def ensure_database(database_url: str):
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


def derive_category_labels(url: str) -> Tuple[str, str]:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    query_term = params.get("query", [None])[0]
    category = params.get("category", [None])[0]
    subcategory = params.get("subcategory", [""])[0]
    if query_term:
        return f"query:{query_term}", subcategory
    if category:
        return category, subcategory
    return "discover", subcategory


def upsert_products(session: Session, products: Iterable[Product], crawled_at: datetime) -> dict:
    summary = {"inserted": 0, "updated": 0, "unchanged": 0}
    for product in products:
        existing: Optional[ProductRecord] = (
            session.query(ProductRecord).filter_by(product_url=product.product_url).one_or_none()
        )
        if not existing:
            session.add(ProductRecord.from_product(product, crawled_at))
            summary["inserted"] += 1
            continue

        if existing.apply_updates(product, crawled_at):
            summary["updated"] += 1
        else:
            summary["unchanged"] += 1

    session.commit()
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scheduled Gumroad ingestion runner")
    parser.add_argument("--config", type=str, default="ingestion_config.json", help="Path to ingestion config")
    parser.add_argument(
        "--database-url",
        type=str,
        default="sqlite:///gumroad_ingestion.db",
        help="SQLAlchemy database URL (sqlite:///gumroad_ingestion.db or postgres+psycopg2://user:pass@host/db)",
    )
    parser.add_argument(
        "--schedule",
        type=str,
        default="all",
        choices=["all", "daily", "realtime"],
        help="Limit runs to a schedule bucket",
    )
    parser.add_argument(
        "--jobs",
        type=str,
        help="Comma-separated job names to run from the config",
    )
    parser.add_argument("--max-products", type=int, help="Override max_products for every job")
    parser.add_argument(
        "--rate-limit",
        type=int,
        help="Override rate_limit_ms for every job",
    )
    parser.add_argument(
        "--use-supabase",
        action="store_true",
        help="Persist runs and products to Supabase (requires SUPABASE_URL and keys)",
    )
    parser.add_argument(
        "--platform-slug",
        type=str,
        default="gumroad",
        help="Platform slug for Supabase platform records",
    )
    parser.add_argument(
        "--save-csv-dir",
        type=str,
        help="Optional directory to write a timestamped CSV for each job run",
    )
    return parser.parse_args()


async def run_job(
    job: dict,
    session_factory,
    args: argparse.Namespace,
    default_rate_limit_ms: int,
    persistence: Optional[SupabasePersistence],
):
    crawled_at = datetime.utcnow()
    max_products = args.max_products or job.get("max_products", 100)
    rate_limit_ms = args.rate_limit or job.get("rate_limit_ms", default_rate_limit_ms)
    get_details = job.get("get_detailed_ratings", True)
    url = job["category_url"]
    platform = job.get("platform", "gumroad")
    scraper = get_scraper(platform)
    category_label, subcategory_label = derive_category_labels(url)
    run_id = persistence.start_run(category_label, subcategory_label) if persistence else None

    print("=" * 80)
    print(f"Job: {job.get('name', 'unnamed')} | Schedule: {job.get('schedule', 'unspecified')}")
    print(f"URL: {url}")
    print(f"Platform: {platform}")
    print(f"Target products: {max_products}")
    print(f"Detailed ratings: {'yes' if get_details else 'no'}")
    print("=" * 80)

    products = await scraper(
        category_url=url,
        max_products=max_products,
        get_detailed_ratings=get_details,
        rate_limit_ms=rate_limit_ms,
    )

    csv_path = None
    if args.save_csv_dir:
        output_dir = Path(args.save_csv_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        safe_name = job.get("name") or category_label or platform
        safe_name = safe_name.replace("/", "_").replace(" ", "_")
        csv_path = output_dir / f"{safe_name}_{crawled_at.strftime('%Y%m%d_%H%M%S')}.csv"
        save_to_csv(products, str(csv_path))

    with session_factory() as session:
        summary = upsert_products(session, products, crawled_at)

    supabase_summary = None
    if persistence and run_id:
        supabase_summary = persistence.upsert_products(run_id, products)
        totals = {"total": len(products)}
        if supabase_summary:
            totals.update(supabase_summary)
        persistence.complete_run(run_id, totals)

    print(
        f"Stored {len(products)} products | Inserted: {summary['inserted']} | "
        f"Updated: {summary['updated']} | Unchanged: {summary['unchanged']}"
    )
    if csv_path:
        print(f"Saved CSV: {csv_path}")
    if supabase_summary:
        print("Supabase summary:")
        print(supabase_summary)


def main():
    args = parse_args()
    config = load_config(Path(args.config))
    names = set(args.jobs.split(",")) if args.jobs else None
    jobs = pick_jobs(config, args.schedule, names)

    if not jobs:
        print("No jobs matched the provided filters.")
        return

    session_factory = ensure_database(args.database_url)
    persistence = SupabasePersistence(platform_slug=args.platform_slug) if args.use_supabase else None
    default_rate_limit_ms = config.get("default_rate_limit_ms", 500)
    asyncio.run(
        asyncio.gather(
            *(
                run_job(job, session_factory, args, default_rate_limit_ms, persistence)
                for job in jobs
            )
        )
    )


if __name__ == "__main__":
    main()
