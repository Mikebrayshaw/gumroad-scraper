import argparse
from datetime import datetime
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

import gumroad_scraper
from gumroad_scraper import Product
from ingestion_runner import run_job


class _SessionContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _session_factory():
    return _SessionContext()


def _build_product() -> Product:
    return Product(
        product_name="Example",
        creator_name="Creator",
        category="design",
        subcategory="ui",
        price_usd=10.0,
        original_price="$10",
        price_is_pwyw=False,
        currency="USD",
        average_rating=4.5,
        total_reviews=10,
        rating_1_star=0,
        rating_2_star=1,
        rating_3_star=2,
        rating_4_star=3,
        rating_5_star=4,
        mixed_review_count=3,
        mixed_review_percent=0.3,
        sales_count=100,
        estimated_revenue=1000.0,
        revenue_confidence="high",
        product_url="https://example.test/product",
        description="Sample",
        scraped_at=datetime.utcnow(),
    )


class TestRunJob(IsolatedAsyncioTestCase):
    async def test_run_job_preserves_debug_info(self):
        products = [_build_product()]
        debug_info = {"invalid_route": True, "possible_captcha": True}
        mock_scraper = AsyncMock(return_value=(products, debug_info))
        args = argparse.Namespace(max_products=None, rate_limit=None, save_csv_dir=None)
        job = {"category_url": "https://gumroad.com/invalid", "name": "invalid"}

        with (
            patch("gumroad_scraper.scrape_discover_page", mock_scraper),
            patch("ingestion_runner.get_scraper") as get_scraper_mock,
            patch(
                "ingestion_runner.upsert_products",
                return_value={"inserted": 1, "updated": 0, "unchanged": 0},
            ) as upsert_mock,
        ):
            get_scraper_mock.return_value = gumroad_scraper.scrape_discover_page
            result_products, result_debug_info = await run_job(
                job,
                _session_factory,
                args,
                default_rate_limit_ms=0,
                persistence=None,
            )

        mock_scraper.assert_awaited_once()
        upsert_mock.assert_called_once()
        self.assertEqual(result_products, products)
        self.assertEqual(result_debug_info, debug_info)
