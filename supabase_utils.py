"""Supabase persistence helpers for Gumroad scraper.

This module encapsulates Supabase connections and helpers for recording
scrape runs and upserting products with platform-aware identifiers.
"""
from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime
from typing import Iterable, Optional
from urllib.parse import urlparse
from uuid import UUID, uuid4

from supabase import Client, create_client

from gumroad_scraper import Product


def _get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_supabase_client() -> Client:
    """Create a Supabase client from environment variables."""
    url = _get_env("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or _get_env("SUPABASE_ANON_KEY")
    return create_client(url, key)


def extract_platform_product_id(product_url: str) -> str:
    """Return a stable platform-specific product identifier from the URL."""
    parsed = urlparse(product_url)
    path_parts = [part for part in parsed.path.split("/") if part]
    if path_parts:
        return path_parts[-1]
    return product_url


class SupabaseRunStore:
    """Helper for recording run-scoped scrape data in Supabase."""

    def __init__(self, client: Optional[Client] = None, platform_slug: str = "gumroad"):
        self.client = client or get_supabase_client()
        self.platform_slug = platform_slug

    def start_run(
        self,
        *,
        category: str,
        subcategory: str,
        max_products: int,
        fast_mode: bool,
        rate_limit_ms: int,
    ) -> UUID:
        run_id = uuid4()
        self.client.table("runs").insert(
            {
                "id": str(run_id),
                "platform": self.platform_slug,
                "category": category,
                "subcategory": subcategory,
                "config": {
                    "max_products": max_products,
                    "fast_mode": fast_mode,
                    "rate_limit_ms": rate_limit_ms,
                },
                "status": "running",
                "started_at": datetime.utcnow().isoformat(),
            }
        ).execute()
        return run_id

    def complete_run(self, run_id: UUID, *, status: str = "completed", error: str | None = None, totals: dict | None = None):
        payload = {
            "completed_at": datetime.utcnow().isoformat(),
            "status": status,
            "error": error,
        }
        totals = totals or {}
        payload.update(
            {
                "total_products": totals.get("total", 0),
                "total_new": totals.get("inserted", 0),
                "total_updated": totals.get("updated", 0),
            }
        )
        self.client.table("runs").update(payload).eq("id", str(run_id)).execute()

    def record_snapshots(
        self,
        run_id: UUID,
        products: Iterable[Product],
        scored_products: Iterable[dict],
    ) -> dict:
        now = datetime.utcnow().isoformat()
        snapshots = []
        for product, scored in zip(products, scored_products):
            payload = asdict(product)
            platform_product_id = extract_platform_product_id(product.product_url)
            snapshots.append(
                {
                    "platform": self.platform_slug,
                    "product_id": platform_product_id,
                    "run_id": str(run_id),
                    "url": product.product_url,
                    "title": payload.get("product_name"),
                    "creator_name": payload.get("creator_name"),
                    "category": payload.get("category"),
                    "subcategory": payload.get("subcategory"),
                    "price_amount": payload.get("price_usd"),
                    "price_currency": payload.get("currency", "USD"),
                    "rating_avg": payload.get("average_rating"),
                    "rating_count": payload.get("total_reviews"),
                    "sales_count": payload.get("sales_count"),
                    "revenue_estimate": payload.get("estimated_revenue"),
                    "opportunity_score": scored.get("opportunity_score"),
                    "scraped_at": now,
                    "raw_source_hash": payload.get("product_url"),
                }
            )

        if not snapshots:
            return {"inserted": 0, "updated": 0, "unchanged": 0}

        response = (
            self.client.table("product_snapshots")
            .upsert(snapshots, on_conflict="platform,product_id,run_id")
            .execute()
        )
        inserted = len(response.data)
        return {"inserted": inserted, "updated": 0, "unchanged": 0}

    def fetch_snapshots(
        self,
        run_id: UUID,
        *,
        category: str | None = None,
        subcategory: str | None = None,
    ) -> list[dict]:
        query = (
            self.client.table("product_snapshots")
            .select(
                "url, title, creator_name, category, subcategory, price_amount, price_currency, "
                "rating_avg, rating_count, sales_count, revenue_estimate, opportunity_score"
            )
            .eq("run_id", str(run_id))
        )
        if category:
            query = query.eq("category", category)
        if subcategory:
            query = query.eq("subcategory", subcategory)

        response = query.order("opportunity_score", desc=True).execute()
        return response.data


class SupabasePersistence:
    """
    Backwards-compatible helper used by ingestion pipelines.

    This keeps the legacy interface while delegating run lifecycle to
    :class:`SupabaseRunStore` and product upserts to the ``products`` table
    for compatibility with existing data consumers.
    """

    def __init__(self, client: Optional[Client] = None, platform_slug: str = "gumroad"):
        self.client = client or get_supabase_client()
        self.platform_slug = platform_slug
        self.platform_id = self._ensure_platform()
        self._run_store = SupabaseRunStore(self.client, platform_slug=platform_slug)

    def _ensure_platform(self) -> int:
        existing = (
            self.client.table("platforms")
            .select("id")
            .eq("slug", self.platform_slug)
            .limit(1)
            .execute()
        )
        if existing.data:
            return existing.data[0]["id"]

        created = (
            self.client.table("platforms")
            .insert({"slug": self.platform_slug, "display_name": self.platform_slug.title()})
            .execute()
        )
        return created.data[0]["id"]

    def start_run(self, category: str, subcategory: str) -> UUID:
        return self._run_store.start_run(
            category=category,
            subcategory=subcategory,
            max_products=0,
            fast_mode=False,
            rate_limit_ms=0,
        )

    def complete_run(self, run_id: UUID, totals: dict):
        self._run_store.complete_run(run_id, totals=totals)

    def upsert_products(self, run_id: UUID, products: Iterable[Product]):
        now = datetime.utcnow().isoformat()
        records = []
        for product in products:
            payload = asdict(product)
            platform_product_id = extract_platform_product_id(product.product_url)
            records.append(
                {
                    "platform_id": self.platform_id,
                    "platform_product_id": platform_product_id,
                    "product_url": product.product_url,
                    "product_name": payload.get("product_name"),
                    "creator_name": payload.get("creator_name"),
                    "category": payload.get("category"),
                    "subcategory": payload.get("subcategory"),
                    "price_usd": payload.get("price_usd"),
                    "original_price": payload.get("original_price"),
                    "currency": payload.get("currency"),
                    "average_rating": payload.get("average_rating"),
                    "total_reviews": payload.get("total_reviews"),
                    "rating_1_star": payload.get("rating_1_star"),
                    "rating_2_star": payload.get("rating_2_star"),
                    "rating_3_star": payload.get("rating_3_star"),
                    "rating_4_star": payload.get("rating_4_star"),
                    "rating_5_star": payload.get("rating_5_star"),
                    "mixed_review_percent": payload.get("mixed_review_percent"),
                    "sales_count": payload.get("sales_count"),
                    "estimated_revenue": payload.get("estimated_revenue"),
                    "last_run_id": str(run_id),
                    "last_seen_at": now,
                }
            )

        if not records:
            return {"inserted": 0, "updated": 0, "unchanged": 0}

        response = (
            self.client.table("products")
            .upsert(records, on_conflict="platform_id,platform_product_id")
            .execute()
        )
        inserted = len(response.data)
        return {"inserted": inserted, "updated": 0, "unchanged": 0}


__all__ = [
    "SupabaseRunStore",
    "SupabasePersistence",
    "get_supabase_client",
    "extract_platform_product_id",
]
