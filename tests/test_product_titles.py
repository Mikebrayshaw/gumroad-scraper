import asyncio
from pathlib import Path
from unittest import IsolatedAsyncioTestCase

from playwright.async_api import Error as PlaywrightError, async_playwright

from gumroad_scraper import extract_product_name


class TestProductTitles(IsolatedAsyncioTestCase):
    async def test_visible_product_titles_are_extracted(self):
        html = Path("tests/fixtures/search_results.html").read_text()

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()

                await page.set_content(html)
                cards = await page.query_selector_all("article")
                self.assertEqual(len(cards), 2)

                titles = []
                try:
                    for card in cards:
                        title = await extract_product_name(card, "https://example.test")
                        titles.append(title)
                finally:
                    await browser.close()
        except PlaywrightError as exc:  # pragma: no cover - environment dependent
            self.skipTest(f"Playwright browser unavailable: {exc}")

        self.assertListEqual(titles, ["Sample Product One", "Second Product Visible"])
        self.assertTrue(all(title and title != "Unknown" for title in titles))
