"""
Unit tests for opportunity scoring and delta detection.
"""

import tempfile
import unittest
from pathlib import Path

from opportunity_scoring import (
    score_product,
    score_product_dict,
    get_top_scored_products,
    compute_rating_signal,
    compute_review_health_signal,
    compute_price_signal,
    compute_sales_velocity_signal,
    compute_revenue_signal,
)
from alerts import (
    init_database,
    create_saved_search,
    get_saved_searches,
    delete_saved_search,
    add_to_watchlist,
    get_watchlist,
    remove_from_watchlist,
    save_snapshot,
    get_latest_snapshot,
    get_previous_snapshot,
    detect_new_products,
    detect_price_changes,
    detect_rating_changes,
    detect_sales_changes,
    detect_all_changes,
    ProductSnapshot,
)


class TestOpportunityScoring(unittest.TestCase):
    """Tests for the opportunity scoring module."""

    def test_score_product_high_quality(self):
        """Test scoring a high-quality product."""
        scored = score_product(
            product_name="Great Product",
            price_usd=29.99,
            average_rating=4.8,
            total_reviews=150,
            mixed_review_percent=10,
            sales_count=5000,
            estimated_revenue=149950.0,
        )

        # High-quality product should score well
        self.assertGreater(scored.opportunity_score, 80)
        self.assertIn("excellent rating", scored.score_notes.lower())

    def test_score_product_low_quality(self):
        """Test scoring a low-quality product."""
        scored = score_product(
            product_name="Poor Product",
            price_usd=5.0,
            average_rating=2.5,
            total_reviews=3,
            mixed_review_percent=60,
            sales_count=10,
            estimated_revenue=50.0,
        )

        # Low-quality product should score poorly
        self.assertLess(scored.opportunity_score, 40)

    def test_score_product_missing_data(self):
        """Test scoring a product with missing data."""
        scored = score_product(
            product_name="Unknown Product",
            price_usd=0,
            average_rating=None,
            total_reviews=0,
            mixed_review_percent=0,
            sales_count=None,
            estimated_revenue=None,
        )

        # Should still produce a valid score
        self.assertGreaterEqual(scored.opportunity_score, 0)
        self.assertLessEqual(scored.opportunity_score, 100)
        self.assertIn("no rating", scored.score_notes.lower())

    def test_score_product_dict(self):
        """Test scoring from a dictionary."""
        product = {
            'product_name': 'Test Product',
            'price_usd': 49.99,
            'average_rating': 4.5,
            'total_reviews': 50,
            'mixed_review_percent': 15,
            'sales_count': 1000,
            'estimated_revenue': 49990.0,
        }

        scored = score_product_dict(product)

        # Should contain all original fields plus score fields
        self.assertIn('opportunity_score', scored)
        self.assertIn('score_notes', scored)
        self.assertIn('rating_signal', scored)
        self.assertEqual(scored['product_name'], 'Test Product')

    def test_get_top_scored_products(self):
        """Test getting top N scored products."""
        products = [
            {'product_name': 'Low', 'price_usd': 5, 'average_rating': 3.0,
             'total_reviews': 5, 'mixed_review_percent': 50,
             'sales_count': 10, 'estimated_revenue': 50},
            {'product_name': 'Medium', 'price_usd': 25, 'average_rating': 4.2,
             'total_reviews': 30, 'mixed_review_percent': 20,
             'sales_count': 200, 'estimated_revenue': 5000},
            {'product_name': 'High', 'price_usd': 39, 'average_rating': 4.9,
             'total_reviews': 100, 'mixed_review_percent': 5,
             'sales_count': 3000, 'estimated_revenue': 117000},
        ]

        top = get_top_scored_products(products, n=2)

        self.assertEqual(len(top), 2)
        self.assertEqual(top[0]['product_name'], 'High')
        self.assertEqual(top[1]['product_name'], 'Medium')

    def test_get_top_scored_with_filters(self):
        """Test top scored products with filters applied."""
        products = [
            {'product_name': 'Cheap', 'price_usd': 5, 'average_rating': 4.8,
             'total_reviews': 100, 'mixed_review_percent': 5,
             'sales_count': 5000, 'estimated_revenue': 25000, 'category': 'design'},
            {'product_name': 'Expensive', 'price_usd': 199, 'average_rating': 4.9,
             'total_reviews': 200, 'mixed_review_percent': 3,
             'sales_count': 1000, 'estimated_revenue': 199000, 'category': 'design'},
        ]

        # Filter by price range
        top = get_top_scored_products(products, n=10, min_price=10, max_price=100)
        self.assertEqual(len(top), 0)  # Both excluded by price filter

        # Filter by min_rating
        top = get_top_scored_products(products, n=10, min_rating=4.85)
        self.assertEqual(len(top), 1)
        self.assertEqual(top[0]['product_name'], 'Expensive')

    def test_rating_signal_thresholds(self):
        """Test rating signal at different thresholds."""
        # Excellent rating
        score, note = compute_rating_signal(4.8, 100)
        self.assertEqual(score, 1.0)
        self.assertIn("excellent", note)

        # Good rating
        score, note = compute_rating_signal(4.1, 50)
        self.assertEqual(score, 0.7)

        # No rating
        score, note = compute_rating_signal(None, 0)
        self.assertEqual(score, 0.3)

    def test_price_signal_sweet_spots(self):
        """Test price signal at different price points."""
        # Ideal range
        score, note = compute_price_signal(25.0)
        self.assertEqual(score, 1.0)
        self.assertIn("ideal", note)

        # Good range
        score, note = compute_price_signal(60.0)
        self.assertEqual(score, 0.85)

        # Free
        score, note = compute_price_signal(0)
        self.assertEqual(score, 0.3)
        self.assertIn("free", note)

        # Very high
        score, note = compute_price_signal(500.0)
        self.assertEqual(score, 0.35)

    def test_sales_velocity_signal(self):
        """Test sales velocity signal at different levels."""
        # Viral
        score, note = compute_sales_velocity_signal(15000)
        self.assertEqual(score, 1.0)
        self.assertIn("viral", note)

        # Strong
        score, note = compute_sales_velocity_signal(1500)
        self.assertEqual(score, 0.8)

        # No data
        score, note = compute_sales_velocity_signal(None)
        self.assertEqual(score, 0.3)


class TestDeltaDetection(unittest.TestCase):
    """Tests for the delta detection functions (pure functions)."""

    def setUp(self):
        """Set up test data."""
        self.previous_snapshot = [
            ProductSnapshot(
                product_url='https://gumroad.com/l/product1',
                product_name='Product 1',
                price_usd=29.99,
                average_rating=4.5,
                total_reviews=50,
                sales_count=1000,
                estimated_revenue=29990.0,
                opportunity_score=75.0,
                snapshot_at='2024-01-01T00:00:00',
            ),
            ProductSnapshot(
                product_url='https://gumroad.com/l/product2',
                product_name='Product 2',
                price_usd=19.99,
                average_rating=4.0,
                total_reviews=30,
                sales_count=500,
                estimated_revenue=9995.0,
                opportunity_score=65.0,
                snapshot_at='2024-01-01T00:00:00',
            ),
        ]

    def test_detect_new_products(self):
        """Test detection of new products."""
        current = [
            {'product_url': 'https://gumroad.com/l/product1', 'product_name': 'Product 1', 'price_usd': 29.99},
            {'product_url': 'https://gumroad.com/l/product3', 'product_name': 'Product 3', 'price_usd': 39.99},
        ]

        changes = detect_new_products(current, self.previous_snapshot)

        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].change_type, 'new')
        self.assertEqual(changes[0].product_name, 'Product 3')
        self.assertEqual(changes[0].new_value, '$39.99')

    def test_detect_no_new_products(self):
        """Test when no new products are added."""
        current = [
            {'product_url': 'https://gumroad.com/l/product1', 'product_name': 'Product 1', 'price_usd': 29.99},
        ]

        changes = detect_new_products(current, self.previous_snapshot)
        self.assertEqual(len(changes), 0)

    def test_detect_price_changes(self):
        """Test detection of price changes."""
        current = [
            {'product_url': 'https://gumroad.com/l/product1', 'product_name': 'Product 1', 'price_usd': 39.99},  # Price increased
            {'product_url': 'https://gumroad.com/l/product2', 'product_name': 'Product 2', 'price_usd': 19.99},  # Same price
        ]

        changes = detect_price_changes(current, self.previous_snapshot, threshold_percent=5.0)

        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].change_type, 'price_change')
        self.assertEqual(changes[0].old_value, '$29.99')
        self.assertEqual(changes[0].new_value, '$39.99')

    def test_detect_price_changes_below_threshold(self):
        """Test that small price changes are ignored."""
        current = [
            {'product_url': 'https://gumroad.com/l/product1', 'product_name': 'Product 1', 'price_usd': 30.49},  # ~2% change
        ]

        changes = detect_price_changes(current, self.previous_snapshot, threshold_percent=5.0)
        self.assertEqual(len(changes), 0)

    def test_detect_rating_changes(self):
        """Test detection of rating changes."""
        current = [
            {'product_url': 'https://gumroad.com/l/product1', 'product_name': 'Product 1',
             'price_usd': 29.99, 'average_rating': 4.8},  # Rating increased by 0.3
        ]

        changes = detect_rating_changes(current, self.previous_snapshot, threshold=0.2)

        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].change_type, 'rating_change')
        self.assertEqual(changes[0].old_value, '4.5')
        self.assertEqual(changes[0].new_value, '4.8')

    def test_detect_sales_changes(self):
        """Test detection of sales count changes."""
        current = [
            {'product_url': 'https://gumroad.com/l/product1', 'product_name': 'Product 1',
             'price_usd': 29.99, 'sales_count': 1200},  # 20% increase
        ]

        changes = detect_sales_changes(current, self.previous_snapshot, threshold_percent=10.0)

        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].change_type, 'sales_change')
        self.assertEqual(changes[0].old_value, '1000')
        self.assertEqual(changes[0].new_value, '1200')

    def test_detect_all_changes(self):
        """Test detection of all change types."""
        current = [
            # Product 1: price change
            {'product_url': 'https://gumroad.com/l/product1', 'product_name': 'Product 1',
             'price_usd': 49.99, 'average_rating': 4.5, 'total_reviews': 50, 'sales_count': 1000},
            # Product 2: no changes
            {'product_url': 'https://gumroad.com/l/product2', 'product_name': 'Product 2',
             'price_usd': 19.99, 'average_rating': 4.0, 'total_reviews': 30, 'sales_count': 500},
            # New product
            {'product_url': 'https://gumroad.com/l/product3', 'product_name': 'Product 3',
             'price_usd': 9.99, 'average_rating': 5.0, 'total_reviews': 10, 'sales_count': 100},
        ]

        changes = detect_all_changes(current, self.previous_snapshot)

        # Should detect: 1 new product + 1 price change
        self.assertEqual(len(changes), 2)
        change_types = {c.change_type for c in changes}
        self.assertIn('new', change_types)
        self.assertIn('price_change', change_types)

    def test_empty_previous_snapshot(self):
        """Test detection when previous snapshot is empty (all products are new)."""
        current = [
            {'product_url': 'https://gumroad.com/l/product1', 'product_name': 'Product 1', 'price_usd': 29.99},
            {'product_url': 'https://gumroad.com/l/product2', 'product_name': 'Product 2', 'price_usd': 19.99},
        ]

        changes = detect_new_products(current, [])

        self.assertEqual(len(changes), 2)
        self.assertTrue(all(c.change_type == 'new' for c in changes))


class TestAlertsPersistence(unittest.TestCase):
    """Tests for the alerts persistence layer (SQLite)."""

    def setUp(self):
        """Create a temporary database for testing."""
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.db_path = Path(self.temp_file.name)
        self.temp_file.close()
        init_database(self.db_path)

    def tearDown(self):
        """Clean up temporary database."""
        import gc
        gc.collect()  # Force garbage collection to release file handles
        try:
            self.db_path.unlink(missing_ok=True)
        except PermissionError:
            pass  # Windows may still have file locked; ignore

    def test_create_and_get_saved_search(self):
        """Test creating and retrieving saved searches."""
        search = create_saved_search(
            name="Test Search",
            category="design",
            subcategory="icons",
            min_price=10.0,
            max_price=50.0,
            db_path=self.db_path,
        )

        self.assertIsNotNone(search.id)
        self.assertEqual(search.name, "Test Search")
        self.assertEqual(search.category, "design")

        # Get all searches
        searches = get_saved_searches(self.db_path)
        self.assertEqual(len(searches), 1)
        self.assertEqual(searches[0].name, "Test Search")

    def test_delete_saved_search(self):
        """Test deleting a saved search."""
        search = create_saved_search(
            name="To Delete",
            category="3d",
            db_path=self.db_path,
        )

        deleted = delete_saved_search(search.id, self.db_path)
        self.assertTrue(deleted)

        searches = get_saved_searches(self.db_path)
        self.assertEqual(len(searches), 0)

    def test_add_to_watchlist(self):
        """Test adding items to watchlist."""
        item = add_to_watchlist(
            item_type="product",
            url="https://gumroad.com/l/test",
            name="Test Product",
            db_path=self.db_path,
        )

        self.assertIsNotNone(item)
        self.assertEqual(item.name, "Test Product")

        # Get watchlist
        watchlist = get_watchlist(self.db_path)
        self.assertEqual(len(watchlist), 1)

    def test_watchlist_no_duplicates(self):
        """Test that duplicate URLs are rejected."""
        add_to_watchlist("product", "https://gumroad.com/l/test", "Test", self.db_path)
        duplicate = add_to_watchlist("product", "https://gumroad.com/l/test", "Test2", self.db_path)

        self.assertIsNone(duplicate)

        watchlist = get_watchlist(self.db_path)
        self.assertEqual(len(watchlist), 1)

    def test_remove_from_watchlist(self):
        """Test removing items from watchlist."""
        item = add_to_watchlist("product", "https://gumroad.com/l/test", "Test", self.db_path)

        removed = remove_from_watchlist(item.id, self.db_path)
        self.assertTrue(removed)

        watchlist = get_watchlist(self.db_path)
        self.assertEqual(len(watchlist), 0)

    def test_save_and_get_snapshot(self):
        """Test saving and retrieving snapshots."""
        search = create_saved_search("Test", "design", db_path=self.db_path)

        products = [
            {'product_url': 'https://gumroad.com/l/p1', 'product_name': 'Product 1',
             'price_usd': 29.99, 'average_rating': 4.5, 'total_reviews': 50,
             'sales_count': 1000, 'estimated_revenue': 29990.0, 'opportunity_score': 75.0},
            {'product_url': 'https://gumroad.com/l/p2', 'product_name': 'Product 2',
             'price_usd': 19.99, 'average_rating': 4.0, 'total_reviews': 30,
             'sales_count': 500, 'estimated_revenue': 9995.0, 'opportunity_score': 65.0},
        ]

        count = save_snapshot(search.id, products, self.db_path)
        self.assertEqual(count, 2)

        snapshot = get_latest_snapshot(search.id, self.db_path)
        self.assertEqual(len(snapshot), 2)
        self.assertEqual(snapshot[0].product_name, 'Product 1')

    def test_multiple_snapshots(self):
        """Test getting previous snapshot when multiple exist."""
        search = create_saved_search("Test", "design", db_path=self.db_path)

        # First snapshot
        products1 = [
            {'product_url': 'https://gumroad.com/l/p1', 'product_name': 'Product 1',
             'price_usd': 29.99, 'average_rating': 4.5, 'total_reviews': 50,
             'sales_count': 1000, 'estimated_revenue': 29990.0, 'opportunity_score': 75.0},
        ]
        save_snapshot(search.id, products1, self.db_path)

        # Second snapshot (simulating time passing)
        import time
        time.sleep(0.1)  # Ensure different timestamp

        products2 = [
            {'product_url': 'https://gumroad.com/l/p1', 'product_name': 'Product 1',
             'price_usd': 39.99, 'average_rating': 4.5, 'total_reviews': 50,
             'sales_count': 1000, 'estimated_revenue': 39990.0, 'opportunity_score': 75.0},
            {'product_url': 'https://gumroad.com/l/p2', 'product_name': 'Product 2',
             'price_usd': 19.99, 'average_rating': 4.0, 'total_reviews': 30,
             'sales_count': 500, 'estimated_revenue': 9995.0, 'opportunity_score': 65.0},
        ]
        save_snapshot(search.id, products2, self.db_path)

        # Latest should have 2 products
        latest = get_latest_snapshot(search.id, self.db_path)
        self.assertEqual(len(latest), 2)

        # Previous should have 1 product with old price
        previous = get_previous_snapshot(search.id, self.db_path)
        self.assertEqual(len(previous), 1)
        self.assertEqual(previous[0].price_usd, 29.99)


if __name__ == '__main__':
    unittest.main()
