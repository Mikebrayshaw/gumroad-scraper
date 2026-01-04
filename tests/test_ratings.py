import re
from pathlib import Path
import unittest

from gumroad_scraper import parse_rating, parse_rating_breakdown


class RatingParsingTests(unittest.TestCase):
    def test_parse_rating_trims_symbols(self):
        rating_text = "★ 4.8 / 5 ( 123 ratings )"
        rating, count = parse_rating(rating_text)
        self.assertEqual(rating, 4.8)
        self.assertEqual(count, 123)

    def test_breakdown_from_fixture_is_non_zero(self):
        fixture_path = Path(__file__).parent / "fixtures" / "rating_section.html"
        html = fixture_path.read_text()

        star_text_sources = re.findall(r'[1-5]\s*stars?[^<]+', html, flags=re.IGNORECASE)
        breakdown = parse_rating_breakdown(star_text_sources, total_reviews_for_calc=120)

        expected = {
            'rating_5_star': 80,
            'rating_4_star': 10,
            'rating_3_star': 5,
            'rating_2_star': 3,
            'rating_1_star': 2,
        }

        self.assertEqual(breakdown, expected)
        self.assertTrue(all(value > 0 for value in breakdown.values()))


if __name__ == "__main__":
    unittest.main()
