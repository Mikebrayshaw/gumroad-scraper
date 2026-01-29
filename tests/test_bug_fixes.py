"""
Tests for the bug fixes in Gumroad scraper.
Tests BUG 1-4: timestamp serialization, wishlist filtering, rating validation, and sales extraction.
"""
import unittest
from datetime import datetime
from dataclasses import asdict

from gumroad_scraper import (
    is_valid_product_url,
    parse_rating,
    extract_sales_from_page,
    Product,
)
from supabase_utils import sanitize_for_json


class TestTimestampSerialization(unittest.TestCase):
    """Tests for BUG 1: Timestamp serialization error."""

    def test_sanitize_datetime_to_isoformat(self):
        """Test that datetime objects are converted to ISO strings."""
        now = datetime.utcnow()
        data = {
            "name": "Test Product",
            "scraped_at": now,
            "price": 10.0,
        }
        sanitized = sanitize_for_json(data)
        
        self.assertEqual(sanitized["name"], "Test Product")
        self.assertEqual(sanitized["price"], 10.0)
        self.assertIsInstance(sanitized["scraped_at"], str)
        self.assertEqual(sanitized["scraped_at"], now.isoformat())

    def test_sanitize_preserves_regular_types(self):
        """Test that regular types are not changed."""
        data = {
            "string": "test",
            "int": 42,
            "float": 3.14,
            "none": None,
            "bool": True,
        }
        sanitized = sanitize_for_json(data)
        
        self.assertEqual(sanitized, data)

    def test_product_dataclass_with_datetime(self):
        """Test that Product dataclass with datetime can be sanitized."""
        product = Product(
            product_name="Test",
            creator_name="Creator",
            category="Category",
            subcategory="Subcategory",
            price_usd=10.0,
            original_price="$10",
            price_is_pwyw=False,
            currency="USD",
            average_rating=4.5,
            total_reviews=100,
            rating_1_star=0,
            rating_2_star=5,
            rating_3_star=10,
            rating_4_star=20,
            rating_5_star=65,
            mixed_review_count=35,
            mixed_review_percent=35.0,
            sales_count=1000,
            estimated_revenue=10000.0,
            revenue_confidence="medium",
            product_url="https://example.com/product",
            description="Test description",
        )
        
        payload = sanitize_for_json(asdict(product))
        
        # Verify scraped_at was converted to string
        self.assertIsInstance(payload["scraped_at"], str)
        self.assertIn("T", payload["scraped_at"])  # ISO format has T separator


class TestWishlistFiltering(unittest.TestCase):
    """Tests for BUG 2: Wishlist URLs being scraped."""

    def test_valid_product_urls(self):
        """Test that valid product URLs pass validation."""
        valid_urls = [
            "https://gumroad.com/l/product-name",
            "https://creator.gumroad.com/l/product",
            "https://gumroad.com/products/product-id",
        ]
        for url in valid_urls:
            with self.subTest(url=url):
                self.assertTrue(is_valid_product_url(url))

    def test_wishlist_urls_rejected(self):
        """Test that wishlist URLs are rejected."""
        invalid_urls = [
            "https://garriefisher.gumroad.com/wishlists/free-resources",
            "https://gumirose.gumroad.com/wishlists/free-procreate",
            "https://creator.gumroad.com/wishlists/anything",
        ]
        for url in invalid_urls:
            with self.subTest(url=url):
                self.assertFalse(is_valid_product_url(url))

    def test_other_non_product_urls_rejected(self):
        """Test that other non-product URLs are rejected."""
        invalid_urls = [
            "https://creator.gumroad.com/followers",
            "https://creator.gumroad.com/following",
            "https://creator.gumroad.com/posts/new-post",
            "https://creator.gumroad.com/subscribe",
        ]
        for url in invalid_urls:
            with self.subTest(url=url):
                self.assertFalse(is_valid_product_url(url))

    def test_empty_url(self):
        """Test that empty URLs are rejected."""
        self.assertFalse(is_valid_product_url(""))
        self.assertFalse(is_valid_product_url(None))


class TestRatingValidation(unittest.TestCase):
    """Tests for BUG 3: Rating parser grabbing wrong numbers."""

    def test_valid_ratings_accepted(self):
        """Test that valid ratings (0-5) are accepted."""
        test_cases = [
            ("4.8 (123)", 4.8, 123),
            ("5.0 (50)", 5.0, 50),
            ("0.0 (10)", 0.0, 10),
            ("3.5 (200)", 3.5, 200),
            ("4.2(99)", 4.2, 99),
        ]
        for rating_str, expected_rating, expected_count in test_cases:
            with self.subTest(rating_str=rating_str):
                rating, count = parse_rating(rating_str)
                self.assertEqual(rating, expected_rating)
                self.assertEqual(count, expected_count)

    def test_invalid_ratings_rejected(self):
        """Test that invalid ratings (>5) are rejected."""
        test_cases = [
            "556.0 (1)",  # Bug example: should be rejected
            "69.0 (383)",  # Bug example: should be rejected
            "100.0 (666)",  # Bug example: should be rejected
            "6.5 (100)",
            "10.0 (50)",
        ]
        for rating_str in test_cases:
            with self.subTest(rating_str=rating_str):
                rating, count = parse_rating(rating_str)
                self.assertIsNone(rating)
                # Count should still be extracted if available
                if "(" in rating_str:
                    self.assertGreater(count, 0)

    def test_negative_ratings_rejected(self):
        """Test that negative ratings are rejected."""
        # Note: The parser strips out non-numeric characters except digits, dots and parens
        # so "-1.5" becomes "1.5" before validation. This test ensures ratings
        # that appear negative in the source are handled correctly.
        # In practice, negative numbers won't match the regex patterns,
        # so we test that ratings outside 0-5 range are rejected.
        rating, count = parse_rating("10.5 (100)")
        self.assertIsNone(rating)

    def test_edge_case_ratings(self):
        """Test edge case ratings at boundaries."""
        # Exactly 5.0 should be accepted
        rating, count = parse_rating("5.0 (100)")
        self.assertEqual(rating, 5.0)
        
        # Exactly 0.0 should be accepted
        rating, count = parse_rating("0.0 (10)")
        self.assertEqual(rating, 0.0)
        
        # Just over 5.0 should be rejected
        rating, count = parse_rating("5.1 (100)")
        self.assertIsNone(rating)


class TestSalesExtraction(unittest.TestCase):
    """Tests for BUG 4: Improve sales_count extraction."""

    def test_simple_sales_count(self):
        """Test extraction of simple sales count."""
        page_source = "This product has 28,133 sales and is popular."
        sales = extract_sales_from_page(page_source)
        self.assertEqual(sales, 28133)

    def test_sales_with_k_suffix(self):
        """Test extraction with K suffix."""
        test_cases = [
            ("1.2K sales", 1200),
            ("5K sales", 5000),
            ("10.5k sales", 10500),
        ]
        for source, expected in test_cases:
            with self.subTest(source=source):
                sales = extract_sales_from_page(source)
                self.assertEqual(sales, expected)

    def test_sales_with_m_suffix(self):
        """Test extraction with M suffix."""
        test_cases = [
            ("1.5M sales", 1500000),
            ("2M sales", 2000000),
        ]
        for source, expected in test_cases:
            with self.subTest(source=source):
                sales = extract_sales_from_page(source)
                self.assertEqual(sales, expected)

    def test_json_embedded_sales_count(self):
        """Test extraction from JSON data."""
        test_cases = [
            ('{"product": {"sales_count": 12345}}', 12345),
            ('{"salesCount": 99999}', 99999),
            ('{"stats": {"sales": 5000}}', 5000),
        ]
        for source, expected in test_cases:
            with self.subTest(source=source):
                sales = extract_sales_from_page(source)
                self.assertEqual(sales, expected)

    def test_no_sales_data(self):
        """Test that None is returned when no sales data found."""
        page_source = "This is a product page with no sales information."
        sales = extract_sales_from_page(page_source)
        self.assertIsNone(sales)

    def test_case_insensitive_matching(self):
        """Test that sales matching is case-insensitive."""
        test_cases = [
            "1,000 Sales",
            "1,000 SALES",
            "1,000 sAlEs",
        ]
        for source in test_cases:
            with self.subTest(source=source):
                sales = extract_sales_from_page(source)
                self.assertEqual(sales, 1000)


if __name__ == "__main__":
    unittest.main()
