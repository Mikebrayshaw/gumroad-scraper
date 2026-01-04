"""
Gumroad Discover Page Scraper
Extracts product data from Gumroad's discover page using Playwright.
"""

import argparse
import asyncio
import csv
import re
import json
import random
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional

from playwright.async_api import (
    Browser,
    ElementHandle,
    Page,
    TimeoutError as PlaywrightTimeout,
    async_playwright,
)
from tqdm import tqdm

from categories import CATEGORY_TREE, build_discover_url, category_url_map


@dataclass
class Product:
    """Data class for Gumroad product information."""
    product_name: str
    creator_name: str
    category: str
    subcategory: str
    price_usd: float
    original_price: str
    currency: str
    average_rating: Optional[float]
    total_reviews: int
    rating_1_star: int
    rating_2_star: int
    rating_3_star: int
    rating_4_star: int
    rating_5_star: int
    mixed_review_percent: float  # 2-4 star reviews / total
    sales_count: Optional[int]
    estimated_revenue: Optional[float]
    product_url: str


# Currency conversion rates to USD (approximate)
CURRENCY_TO_USD = {
    'USD': 1.0,
    '$': 1.0,
    'EUR': 1.10,
    '€': 1.10,
    'GBP': 1.27,
    '£': 1.27,
    'CAD': 0.74,
    'C$': 0.74,
    'AUD': 0.66,
    'A$': 0.66,
    'JPY': 0.0067,
    '¥': 0.0067,
    'INR': 0.012,
    '₹': 0.012,
}


async def extract_product_name(card: ElementHandle, product_url: str) -> str:
    """Extract a product name from a product card element.

    The function looks for common Gumroad heading patterns first (h2/h4 with
    itemprop markers), then falls back to generic headings and finally derives
    a readable name from the product URL slug. Returns "Unknown" if no
    reasonable name can be found.
    """

    title_selectors = [
        'h2[itemprop="name"]',
        'h4[itemprop="name"]',
        '[itemprop="name"] h2',
        '[itemprop="name"] h4',
        '[itemprop="name"]',
        'h2',
        'h3',
        'h4',
    ]

    for selector in title_selectors:
        title_elem = await card.query_selector(selector)
        if title_elem:
            title_text = await title_elem.inner_text()
            if title_text:
                cleaned = title_text.strip()
                if cleaned:
                    return cleaned

    # Fallback: infer a readable title from the URL slug
    if product_url:
        slug_match = re.search(r"/l/([\w-]+)", product_url)
        if slug_match:
            slug = slug_match.group(1)
            if slug:
                readable = slug.replace('-', ' ').strip()
                if readable:
                    return readable.title()

    return "Unknown"


def parse_price(price_str: str) -> tuple[float, str, str]:
    """
    Parse price string and convert to USD.
    Returns (usd_price, original_price, currency).
    """
    if not price_str or price_str.lower() in ['free', '$0', '0']:
        return 0.0, 'Free', 'USD'

    # Clean up the price string
    price_str = price_str.strip()
    original = price_str

    # Handle subscription prices (e.g., "$5 a month")
    if 'a month' in price_str.lower() or '/mo' in price_str.lower():
        price_str = re.sub(r'\s*(a month|/mo|per month).*', '', price_str, flags=re.IGNORECASE)

    # Extract currency symbol
    currency = 'USD'
    for curr_symbol in ['€', '£', '¥', '₹', 'C$', 'A$', '$']:
        if curr_symbol in price_str:
            if curr_symbol == '$':
                # Check for C$ or A$ first
                if 'C$' in price_str:
                    currency = 'CAD'
                elif 'A$' in price_str:
                    currency = 'AUD'
                else:
                    currency = 'USD'
            else:
                currency = curr_symbol
            break

    # Extract numeric value
    numbers = re.findall(r'[\d,]+\.?\d*', price_str)
    if numbers:
        price_value = float(numbers[0].replace(',', ''))
    else:
        return 0.0, original, currency

    # Convert to USD
    conversion_rate = CURRENCY_TO_USD.get(currency, 1.0)
    usd_price = round(price_value * conversion_rate, 2)

    return usd_price, original, currency


def parse_rating(rating_str: str) -> tuple[Optional[float], int]:
    """
    Parse rating string like "4.8(123)" or "4.5 (50)" or "4.0\n(2)".
    Returns (average_rating, total_reviews).
    """
    if not rating_str:
        return None, 0

    # Clean up - remove newlines and extra spaces
    rating_str = ' '.join(rating_str.split())
    rating_str = re.sub(r'[^\d().]+', ' ', rating_str)
    rating_str = re.sub(r'/\s*5', ' ', rating_str)
    rating_str = re.sub(r'([\d.]+)\s+\d+(?=\s*\()', r'\1', rating_str)
    rating_str = ' '.join(rating_str.split())
    # Trim leading/trailing non-numeric characters (e.g., stars or emojis)
    rating_str = re.sub(r'^[^\d]+', '', rating_str)
    rating_str = re.sub(r'[^\d)]+$', '', rating_str)

    # Try to extract rating and count
    primary_match = re.search(r'([\d.]+)[^\d]+\(\s*(\d+)', rating_str)
    if primary_match:
        rating = float(primary_match.group(1))
        count = int(primary_match.group(2))
        return rating, count

    # Pattern: "4.8(123)" or "4.8 (123)" or "4.8(123 ratings)"
    match = re.search(r'([\d.]+)\s*\(?(\d+)', rating_str)
    if match:
        rating = float(match.group(1))
        count = int(match.group(2))
        return rating, count

    # Just rating without count
    match = re.search(r'([\d.]+)', rating_str)
    if match:
        return float(match.group(1)), 0

    return None, 0


def parse_sales(sales_str: str) -> Optional[int]:
    """Parse sales count string like '1.2K sales' or '500 sales'."""
    if not sales_str:
        return None

    match = re.search(r'([\d.]+)\s*([KkMm])?\s*sales?', sales_str, re.IGNORECASE)
    if match:
        value = float(match.group(1))
        multiplier = match.group(2)
        if multiplier:
            if multiplier.upper() == 'K':
                value *= 1000
            elif multiplier.upper() == 'M':
                value *= 1000000
        return int(value)
    return None


def parse_rating_breakdown(
    star_text_sources: list[str],
    total_reviews_for_calc: Optional[int] = None,
) -> dict:
    """Parse rating breakdown percentages/counts from a collection of texts."""
    details = {
        'rating_1_star': 0,
        'rating_2_star': 0,
        'rating_3_star': 0,
        'rating_4_star': 0,
        'rating_5_star': 0,
    }

    counts_found: dict[int, float] = {}

    for text in star_text_sources:
        normalized = ' '.join(text.split())
        for star in range(1, 6):
            if details[f'rating_{star}_star']:
                continue  # Already found for this star

            if not re.search(rf'{star}\s*stars?', normalized, re.IGNORECASE):
                continue

            percent_match = re.search(r'(\d+(?:\.\d+)?)\s*%', normalized)
            if percent_match:
                details[f'rating_{star}_star'] = int(round(float(percent_match.group(1))))
                continue

            count_match = re.search(r'(\d+[\d,]*)\s*(?:reviews?|ratings?)', normalized, re.IGNORECASE)
            if count_match:
                value = float(count_match.group(1).replace(',', ''))
                counts_found[star] = value
                continue

            cleaned = re.sub(rf'^[^0-9]*{star}\s*stars?\s*', '', normalized, flags=re.IGNORECASE)
            number_candidates = [n for n in re.findall(r'\d+(?:\.\d+)?', cleaned) if int(float(n)) != star]
            if number_candidates:
                value = float(number_candidates[-1])
                counts_found[star] = value

    if counts_found:
        for star, count_value in counts_found.items():
            if details[f'rating_{star}_star']:
                continue
            if total_reviews_for_calc and total_reviews_for_calc > 0:
                percent = int(round((count_value / total_reviews_for_calc) * 100))
                details[f'rating_{star}_star'] = percent
            else:
                details[f'rating_{star}_star'] = int(round(count_value))

    return details


async def get_product_details(
    page: Page,
    product_url: str,
    max_retries: int = 3,
    total_reviews_hint: Optional[int] = None,
) -> dict:
    """
    Visit individual product page to get detailed rating breakdown and sales.
    Returns dict with rating breakdown and sales info.
    Includes retry logic for resilience.
    """
    details = {
        'rating_1_star': 0,
        'rating_2_star': 0,
        'rating_3_star': 0,
        'rating_4_star': 0,
        'rating_5_star': 0,
        'sales_count': None,
    }

    for attempt in range(max_retries):
        try:
            await page.goto(product_url, wait_until='domcontentloaded', timeout=20000)
            await page.wait_for_load_state('networkidle')
            await page.wait_for_timeout(2000)  # Wait for dynamic content

            # Get all visible text on the page
            body_text = await page.inner_text('body')

            # Find sales count - look for pattern like "28,133 sales" or "1.2K sales"
            sales_match = re.search(r'([\d,]+)\s*sales', body_text, re.IGNORECASE)
            if sales_match:
                sales_str = sales_match.group(1).replace(',', '')
                details['sales_count'] = int(sales_str)
            else:
                # Try K/M suffix patterns
                sales_match = re.search(r'([\d.]+)\s*([KkMm])\s*sales', body_text, re.IGNORECASE)
                if sales_match:
                    value = float(sales_match.group(1))
                    multiplier = sales_match.group(2).upper()
                    if multiplier == 'K':
                        value *= 1000
                    elif multiplier == 'M':
                        value *= 1000000
                    details['sales_count'] = int(value)

            # Try to find a reliable total reviews count on the detail page
            total_reviews_match = re.search(r'(\d+[\d,]*)\s*(?:reviews|ratings)', body_text, re.IGNORECASE)
            total_reviews_detail = int(total_reviews_match.group(1).replace(',', '')) if total_reviews_match else None
            total_reviews_for_calc = total_reviews_detail or total_reviews_hint

            # Proactively expand/hover rating breakdown sections
            rating_triggers = [
                'button:has-text("ratings")',
                'button:has-text("reviews")',
                '[data-testid*="rating"]',
                '.rating-summary button',
            ]
            for trigger in rating_triggers:
                try:
                    locator = page.locator(trigger).first
                    if await locator.count():
                        await locator.hover(timeout=1500)
                        await locator.click(timeout=1500)
                        await page.wait_for_timeout(500)
                except Exception:
                    continue

            # Wait for rating breakdown to render if it is lazy-loaded
            for _ in range(3):
                try:
                    await page.wait_for_selector('text=/[1-5] star/', timeout=1500)
                    break
                except PlaywrightTimeout:
                    await page.wait_for_timeout(500)

            # Collect candidate texts for rating breakdown
            star_text_sources = []

            aria_texts = await page.eval_on_selector_all(
                '[aria-label*="star" i], [title*="star" i]',
                'els => els.map(el => (el.getAttribute("aria-label") || el.getAttribute("title") || ""))'
            )
            star_text_sources.extend([t for t in aria_texts if t])

            try:
                inner_texts = await page.locator(r"text=/[1-5]\s*star/i").all_inner_texts()
                star_text_sources.extend([' '.join(t.split()) for t in inner_texts if t.strip()])
            except Exception:
                pass

            star_text_sources.append(body_text)

            details.update(
                parse_rating_breakdown(
                    star_text_sources,
                    total_reviews_for_calc=total_reviews_for_calc,
                )
            )

            if not any(details[f'rating_{star}_star'] for star in range(1, 6)):
                print(f"  Rating breakdown missing or collapsed for {product_url[:50]}...")

            # Success - break out of retry loop
            break

        except PlaywrightTimeout:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(f"  Timeout, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait_time)
            else:
                print(f"  Warning: Timeout after {max_retries} attempts for {product_url[:50]}...")
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(f"  Error: {e}, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait_time)
            else:
                print(f"  Warning: Could not get details for {product_url[:50]}... ({e})")

    return details


async def scrape_discover_page(
    category_url: str,
    max_products: int = 100,
    get_detailed_ratings: bool = True,
    rate_limit_ms: int = 500,
    show_progress: bool = False,
) -> list[Product]:
    """
    Scrape products from a Gumroad discover/category page.

    Args:
        category_url: URL of the category page to scrape
        max_products: Maximum number of products to scrape
        get_detailed_ratings: Whether to visit each product page for rating breakdown
        rate_limit_ms: Delay between product page requests in milliseconds

    Returns:
        List of Product objects
    """
    products = []
    seen_urls = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = await context.new_page()

        print(f"Navigating to {category_url}...")
        await page.goto(category_url, wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(3000)

        # Ensure product cards have rendered before scraping
        try:
            await page.wait_for_selector(
                'article h2[itemprop="name"], article h3[itemprop="name"], article h4[itemprop="name"], article h2.line-clamp-3, article h4.line-clamp-4',
                timeout=15000,
            )
        except PlaywrightTimeout:
            print("Warning: Product title selector did not appear before timeout; continuing with best effort.")

        # Extract category from URL
        category_match = re.search(r'gumroad\.com/([^/?]+)', category_url)
        main_category = category_match.group(1) if category_match else 'discover'
        if main_category == 'discover':
            # Check for query param
            query_match = re.search(r'query=([^&]+)', category_url)
            if query_match:
                main_category = query_match.group(1)

        progress = tqdm(
            total=max_products,
            desc=f"{main_category} products",
            unit="product",
            leave=True,
            disable=not show_progress,
        )

        # Scroll to load more products - aggressive scrolling for better coverage
        scroll_attempts = 0
        max_scroll_attempts = 200  # Increased for large scrapes
        no_new_cards_count = 0
        last_product_count = 0

        while len(products) < max_products and scroll_attempts < max_scroll_attempts:
            # Find all product cards using article elements
            product_cards = await page.query_selector_all('article')

            if not product_cards:
                print("No product cards found, trying alternative selectors...")
                product_cards = await page.query_selector_all('[class*="product-card"], a[href*="/l/"]')

            current_card_count = len(product_cards)
            print(f"Found {current_card_count} product cards on page (scraped: {len(products)}/{max_products})...")

            for card in product_cards:
                if len(products) >= max_products:
                    break

                try:
                    # Extract product URL from the stretched-link
                    link = await card.query_selector('a.stretched-link, a[href*="/l/"]')
                    if not link:
                        continue

                    product_url = await link.get_attribute('href')
                    if not product_url:
                        continue

                    # Make URL absolute
                    if product_url.startswith('/'):
                        product_url = f'https://gumroad.com{product_url}'

                    # Skip if already scraped
                    if product_url in seen_urls:
                        continue
                    seen_urls.add(product_url)

                    # Extract product name - handle both discover grid cards (h2 titles)
                    # and search result cards (h4 titles inside the stretched link).
                    product_name = await extract_product_name(card, product_url)

                    # If still unknown, try to get from the link aria-label or title
                    if product_name == "Unknown":
                        link = await card.query_selector('a.stretched-link, a[href*="/l/"]')
                        if link:
                            aria_label = await link.get_attribute('aria-label')
                            title = await link.get_attribute('title')
                            product_name = aria_label or title or "Unknown"

                    # Extract creator name - look for the link with user-avatar img
                    # Structure: <a href="...?recommended_by=..."><img class="user-avatar">CreatorName</a>
                    creator_elem = await card.query_selector('a[href*="?recommended_by="]:not(.stretched-link)')
                    creator_name = "Unknown"
                    if creator_elem:
                        # Get the text content (will be after the img)
                        creator_name = await creator_elem.inner_text()
                        creator_name = creator_name.strip()

                    # Fallback: try to get from itemprop="seller"
                    if creator_name == "Unknown" or not creator_name:
                        seller_elem = await card.query_selector('[itemprop="seller"] [itemprop="name"]')
                        if seller_elem:
                            creator_name = await seller_elem.inner_text()
                            creator_name = creator_name.strip()

                    # Extract price using itemprop for accuracy
                    price_elem = await card.query_selector('[itemprop="price"]')
                    currency_elem = await card.query_selector('[itemprop="priceCurrency"]')

                    price_usd = 0.0
                    original_price = "Free"
                    currency = "USD"

                    if price_elem:
                        # Get the content attribute for numeric value
                        price_content = await price_elem.get_attribute('content')
                        price_text = await price_elem.inner_text()
                        original_price = price_text.strip() if price_text else "Free"

                        if price_content:
                            try:
                                price_value = float(price_content)
                            except ValueError:
                                price_value = 0.0
                        else:
                            # Parse from text
                            _, original_price, _ = parse_price(original_price)
                            price_value = 0.0
                            numbers = re.findall(r'[\d,]+\.?\d*', original_price)
                            if numbers:
                                price_value = float(numbers[0].replace(',', ''))

                        # Get currency
                        if currency_elem:
                            currency = (await currency_elem.inner_text()).strip().upper()

                        # Convert to USD
                        conversion_rate = CURRENCY_TO_USD.get(currency, 1.0)
                        price_usd = round(price_value * conversion_rate, 2)

                    # Extract rating using class selector
                    rating_elem = await card.query_selector('.rating-average, [class*="rating-average"]')
                    reviews_elem = await card.query_selector('span[title*="rating"]')

                    average_rating = None
                    total_reviews = 0

                    if rating_elem:
                        rating_text = await rating_elem.inner_text()
                        parsed_rating, _ = parse_rating(rating_text)
                        average_rating = parsed_rating if parsed_rating is not None else average_rating

                    if reviews_elem:
                        reviews_text = await reviews_elem.inner_text()
                        _, parsed_total_reviews = parse_rating(reviews_text)
                        total_reviews = parsed_total_reviews or total_reviews

                    # Fallback: get rating from footer text
                    if average_rating is None:
                        footer = await card.query_selector('footer')
                        if footer:
                            footer_text = await footer.inner_text()
                            rating_guess, total_guess = parse_rating(footer_text)
                            if rating_guess is not None:
                                average_rating = rating_guess
                                total_reviews = total_guess or total_reviews

                    if average_rating is None and total_reviews == 0:
                        print(f"  No rating metadata found for {product_name[:40]} ({product_url[:50]}...)")

                    # Initialize rating breakdown
                    rating_breakdown = {
                        'rating_1_star': 0,
                        'rating_2_star': 0,
                        'rating_3_star': 0,
                        'rating_4_star': 0,
                        'rating_5_star': 0,
                        'sales_count': None,
                    }

                    # Get detailed info if requested
                    if get_detailed_ratings and product_url:
                        # Rate limiting - add random jitter to avoid detection
                        jitter = random.randint(0, rate_limit_ms // 2)
                        await asyncio.sleep((rate_limit_ms + jitter) / 1000)

                        detail_page = await context.new_page()
                        rating_breakdown = await get_product_details(
                            detail_page,
                            product_url,
                            total_reviews_hint=total_reviews,
                        )
                        await detail_page.close()

                    # Calculate mixed review percentage (2-4 stars)
                    # Note: rating_breakdown values are already percentages from the page
                    mixed_percent = (
                        rating_breakdown['rating_2_star'] +
                        rating_breakdown['rating_3_star'] +
                        rating_breakdown['rating_4_star']
                    )

                    # Estimate revenue
                    sales = rating_breakdown['sales_count']
                    estimated_revenue = round(sales * price_usd, 2) if sales and price_usd else None

                    product = Product(
                        product_name=product_name,
                        creator_name=creator_name,
                        category=main_category,
                        subcategory=main_category,  # Same as category for now
                        price_usd=price_usd,
                        original_price=original_price,
                        currency=currency,
                        average_rating=average_rating,
                        total_reviews=total_reviews,
                        rating_1_star=rating_breakdown['rating_1_star'],
                        rating_2_star=rating_breakdown['rating_2_star'],
                        rating_3_star=rating_breakdown['rating_3_star'],
                        rating_4_star=rating_breakdown['rating_4_star'],
                        rating_5_star=rating_breakdown['rating_5_star'],
                        mixed_review_percent=round(mixed_percent, 2),
                        sales_count=sales,
                        estimated_revenue=estimated_revenue,
                        product_url=product_url,
                    )
                    products.append(product)
                    progress.update(1)
                    if show_progress:
                        progress.set_postfix(
                            {
                                "last": product_name[:32],
                                "detail": "yes" if get_detailed_ratings else "no",
                            },
                            refresh=False,
                        )
                    print(f"[{len(products)}/{max_products}] {product_name[:40]}... | ${price_usd} | Rating: {average_rating or 'N/A'} ({total_reviews} reviews)")

                except Exception as e:
                    print(f"  Error parsing product card: {e}")
                    continue

            # Check if we need more products
            if len(products) >= max_products:
                break

            # Scroll to load more - use multiple scroll strategies
            scroll_attempts += 1

            # Strategy 1: Scroll by viewport height (multiple times)
            for _ in range(3):
                await page.evaluate('window.scrollBy(0, window.innerHeight)')
                await page.wait_for_timeout(500)

            # Strategy 2: Scroll to bottom of page
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(2000)

            # Strategy 3: Try clicking "Load More" button if present
            try:
                load_more = await page.query_selector('button:has-text("Load more"), button:has-text("Show more"), [class*="load-more"]')
                if load_more and await load_more.is_visible():
                    await load_more.click()
                    await page.wait_for_timeout(2500)
            except:
                pass

            # Wait for any network requests to complete
            try:
                await page.wait_for_load_state('networkidle', timeout=5000)
            except:
                pass  # Continue even if timeout

            # Check if we're making progress (new unique products scraped)
            if len(products) > last_product_count:
                no_new_cards_count = 0
                new_count = len(products)
                added = new_count - last_product_count
                last_product_count = new_count
                print(f"  Added {added} new products (unique total: {len(products)})")
            else:
                no_new_cards_count += 1
                # Try more aggressive scrolling when stuck
                if no_new_cards_count >= 3:
                    # Scroll up slightly then back down to trigger lazy loading
                    await page.evaluate('window.scrollBy(0, -500)')
                    await page.wait_for_timeout(500)
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    await page.wait_for_timeout(2000)

                # If stuck for 15 consecutive attempts, give up
                if no_new_cards_count >= 15:
                    print(
                        "No new products discovered after "
                        f"{no_new_cards_count} scroll attempts. Reached end of results."
                    )
                    break

        await browser.close()

        progress.close()

    return products


def save_to_csv(products: list[Product], filename: str):
    """Save products to CSV file."""
    if not products:
        print("No products to save.")
        return

    fieldnames = list(asdict(products[0]).keys())

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for product in products:
            writer.writerow(asdict(product))

    print(f"\nSaved {len(products)} products to {filename}")


# Gumroad category URLs aligned with the Streamlit dropdown
CATEGORY_URLS = category_url_map()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Scrape product data from Gumroad discover pages.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  python gumroad_scraper.py                     # Scrape 'design' category (default)
  python gumroad_scraper.py -c software         # Scrape 'software' category
  python gumroad_scraper.py -c 3d -n 50         # Scrape 50 products from '3d' category
  python gumroad_scraper.py --all -n 25         # Scrape 25 products from ALL categories
  python gumroad_scraper.py -c design --fast    # Fast mode (no detailed product pages)

Available categories:
  {', '.join(CATEGORY_URLS.keys())}
        """
    )
    parser.add_argument(
        '-c', '--category',
        type=str,
        default='design',
        choices=list(CATEGORY_URLS.keys()),
        help='Category to scrape (default: design)'
    )
    parser.add_argument(
        '-n', '--max-products',
        type=int,
        default=100,
        help='Maximum number of products to scrape per category (default: 100)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Scrape all categories'
    )
    parser.add_argument(
        '--fast',
        action='store_true',
        help='Fast mode: skip detailed product page scraping (no sales data)'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Output CSV filename (default: auto-generated with timestamp)'
    )
    parser.add_argument(
        '--rate-limit',
        type=int,
        default=500,
        help='Rate limit in milliseconds between requests (default: 500)'
    )
    parser.add_argument(
        '--no-progress',
        action='store_true',
        help='Disable the progress bar (useful for CI logs)'
    )
    return parser.parse_args()


async def scrape_category(category: str, args) -> list[Product]:
    """Scrape a single category and return products."""
    url = CATEGORY_URLS.get(category, 'https://gumroad.com/discover')

    print("=" * 60)
    print(f"SCRAPING: {category.upper()}")
    print("=" * 60)
    print(f"URL: {url}")
    print(f"Target: {args.max_products} products")
    print(f"Detailed scraping: {'No (fast mode)' if args.fast else 'Yes'}")
    if not args.fast:
        print(
            "Note: Visiting each product page with spacing to avoid rate limits; "
            "use --fast to skip detail pages if you only need card metadata."
        )
    print("=" * 60 + "\n")

    products = await scrape_discover_page(
        category_url=url,
        max_products=args.max_products,
        get_detailed_ratings=not args.fast,
        rate_limit_ms=args.rate_limit,
        show_progress=not args.no_progress,
    )

    return products


async def main():
    """Main entry point."""
    args = parse_args()

    all_products = []

    if args.all:
        # Scrape all categories
        categories = [c for c in CATEGORY_URLS.keys() if c != 'discover']
        print(f"Scraping {len(categories)} categories...\n")

        for category in categories:
            products = await scrape_category(category, args)
            all_products.extend(products)
            print(f"\nCompleted {category}: {len(products)} products\n")

    else:
        # Scrape single category
        products = await scrape_category(args.category, args)
        all_products.extend(products)

    # Save to CSV
    if args.output:
        filename = args.output
    else:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if args.all:
            filename = f'gumroad_all_{timestamp}.csv'
        else:
            filename = f'gumroad_{args.category}_{timestamp}.csv'

    save_to_csv(all_products, filename)

    # Print summary
    print("\n" + "=" * 60)
    print("SCRAPE COMPLETE")
    print("=" * 60)
    print(f"Total products scraped: {len(all_products)}")

    if all_products:
        avg_price = sum(p.price_usd for p in all_products) / len(all_products)
        rated_products = [p for p in all_products if p.average_rating]
        avg_rating = sum(p.average_rating for p in rated_products) / len(rated_products) if rated_products else 0

        products_with_sales = [p for p in all_products if p.sales_count]
        total_sales = sum(p.sales_count for p in products_with_sales)
        total_revenue = sum(p.estimated_revenue for p in all_products if p.estimated_revenue)

        print(f"Average price: ${avg_price:.2f}")
        print(f"Average rating: {avg_rating:.2f} ({len(rated_products)} rated products)")
        print(f"Products with sales data: {len(products_with_sales)}")
        print(f"Total sales (visible): {total_sales:,}")
        print(f"Total estimated revenue: ${total_revenue:,.2f}")
        print(f"Output file: {filename}")

    print("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
