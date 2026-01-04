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


class SupabasePersistence:
    """Helper for recording scrape runs and products in Supabase."""

    def __init__(self, client: Optional[Client] = None, platform_slug: str = "gumroad"):
        self.client = client or get_supabase_client()
        self.platform_slug = platform_slug
        self.platform_id = self._ensure_platform()

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
        run_id = uuid4()
        self.client.table("scrape_runs").insert(
            {
                "id": str(run_id),
                "platform_id": self.platform_id,
                "category": category,
                "subcategory": subcategory,
                "started_at": datetime.utcnow().isoformat(),
            }
        ).execute()
        return run_id

    def complete_run(self, run_id: UUID, totals: dict):
        payload = {
            "completed_at": datetime.utcnow().isoformat(),
            "total_products": totals.get("total", 0),
            "total_new": totals.get("inserted", 0),
            "total_updated": totals.get("updated", 0),
        }
        self.client.table("scrape_runs").update(payload).eq("id", str(run_id)).execute()

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
    "SupabasePersistence",
    "get_supabase_client",
    "extract_platform_product_id",
]
