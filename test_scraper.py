"""Quick test of Gumroad scraper - get 10 products without details."""
import asyncio
import os
import sys
import re

import pytest
from playwright.async_api import async_playwright

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)


pytestmark = pytest.mark.skipif(
    not os.getenv("RUN_SCRAPER_TEST"),
    reason="Network scrape disabled by default; set RUN_SCRAPER_TEST=1 to enable.",
)


async def test_scrape():
    print("Starting test scrape...", flush=True)

    async with async_playwright() as p:
        print("Launching browser...", flush=True)
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = await context.new_page()

        print("Navigating to Gumroad design category...", flush=True)
        await page.goto('https://gumroad.com/design', wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(2000)

        # Find product cards
        product_cards = await page.query_selector_all('article')
        print(f"Found {len(product_cards)} product cards", flush=True)

        products = []
        for i, card in enumerate(product_cards[:10]):
            try:
                # Get product link
                link = await card.query_selector('a.stretched-link, a[href*="/l/"]')
                if not link:
                    continue
                product_url = await link.get_attribute('href')

                # Get product name
                name_elem = await card.query_selector('h2')
                name = await name_elem.inner_text() if name_elem else "Unknown"

                # Get creator
                creator_elem = await card.query_selector('a[href$="?recommended_by=discover"]:not(.stretched-link)')
                creator = await creator_elem.inner_text() if creator_elem else "Unknown"

                # Get card text for price/rating
                card_text = await card.inner_text()

                # Extract price
                price_match = re.search(r'[€$£][\d,.]+', card_text)
                price = price_match.group(0) if price_match else "Free"

                # Extract rating
                rating_match = re.search(r'(\d+\.?\d*)\s*\n?\s*\((\d+)\)', card_text)
                rating = rating_match.group(1) if rating_match else "N/A"
                reviews = rating_match.group(2) if rating_match else "0"

                products.append({
                    'name': name.strip()[:50],
                    'creator': creator.strip(),
                    'price': price,
                    'rating': rating,
                    'reviews': reviews,
                    'url': product_url[:60] + '...'
                })
                print(f"[{i+1}] {name.strip()[:40]} | {price} | ★{rating} ({reviews})", flush=True)

            except Exception as e:
                print(f"Error: {e}", flush=True)

        await browser.close()
        print(f"\nSuccessfully scraped {len(products)} products!", flush=True)
        return products


if __name__ == '__main__':
    asyncio.run(test_scrape())
