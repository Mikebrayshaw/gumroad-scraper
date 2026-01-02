"""
Gumroad Discover Page Scraper
Extracts product data from Gumroad's discover page using Playwright.
"""

import asyncio
import csv
import re
import json
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional
from playwright.async_api import async_playwright, Page, Browser


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

    # Try to extract rating and count
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


async def get_product_details(page: Page, product_url: str) -> dict:
    """
    Visit individual product page to get detailed rating breakdown and sales.
    Returns dict with rating breakdown and sales info.
    """
    details = {
        'rating_1_star': 0,
        'rating_2_star': 0,
        'rating_3_star': 0,
        'rating_4_star': 0,
        'rating_5_star': 0,
        'sales_count': None,
    }

    try:
        await page.goto(product_url, wait_until='domcontentloaded', timeout=15000)
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

        # Try to find rating breakdown from the page
        # Look for star distribution sections
        for star in range(1, 6):
            # Pattern: "5 stars (123)" or "5-star: 123" or similar
            patterns = [
                rf'{star}\s*stars?\s*\(?(\d+)\)?',
                rf'{star}\s*[-★]\s*(\d+)',
            ]
            for pattern in patterns:
                match = re.search(pattern, body_text, re.IGNORECASE)
                if match:
                    details[f'rating_{star}_star'] = int(match.group(1))
                    break

    except Exception as e:
        print(f"  Warning: Could not get details for {product_url[:50]}... ({e})")

    return details


async def scrape_discover_page(
    category_url: str,
    max_products: int = 100,
    get_detailed_ratings: bool = True
) -> list[Product]:
    """
    Scrape products from a Gumroad discover/category page.

    Args:
        category_url: URL of the category page to scrape
        max_products: Maximum number of products to scrape
        get_detailed_ratings: Whether to visit each product page for rating breakdown

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
        await page.wait_for_timeout(2000)

        # Extract category from URL
        category_match = re.search(r'gumroad\.com/([^/?]+)', category_url)
        main_category = category_match.group(1) if category_match else 'discover'
        if main_category == 'discover':
            # Check for query param
            query_match = re.search(r'query=([^&]+)', category_url)
            if query_match:
                main_category = query_match.group(1)

        # Scroll to load more products
        scroll_attempts = 0
        max_scroll_attempts = 50

        while len(products) < max_products and scroll_attempts < max_scroll_attempts:
            # Find all product cards using article elements
            product_cards = await page.query_selector_all('article')

            if not product_cards:
                print("No product cards found, trying alternative selectors...")
                product_cards = await page.query_selector_all('[class*="product-card"], a[href*="/l/"]')

            print(f"Found {len(product_cards)} product cards on page...")

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

                    # Extract product name from h2 or h4 with itemprop="name"
                    # Discover results use h2, search results use h4
                    name_elem = await card.query_selector('[itemprop="name"], h2, h4')
                    product_name = "Unknown"
                    if name_elem:
                        product_name = await name_elem.inner_text()
                        product_name = product_name.strip()
                        # Decode HTML entities
                        product_name = product_name.replace('&amp;', '&').replace('&#39;', "'")

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
                        try:
                            average_rating = float(rating_text.strip())
                        except ValueError:
                            pass

                    if reviews_elem:
                        reviews_text = await reviews_elem.inner_text()
                        match = re.search(r'\((\d+)\)', reviews_text)
                        if match:
                            total_reviews = int(match.group(1))

                    # Fallback: get rating from footer text
                    if average_rating is None:
                        footer = await card.query_selector('footer')
                        if footer:
                            footer_text = await footer.inner_text()
                            rating_match = re.search(r'(\d+\.?\d*)\s*\n?\s*\((\d+)\)', footer_text)
                            if rating_match:
                                average_rating = float(rating_match.group(1))
                                total_reviews = int(rating_match.group(2))

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
                        detail_page = await context.new_page()
                        rating_breakdown = await get_product_details(detail_page, product_url)
                        await detail_page.close()

                    # Calculate mixed review percentage (2-4 stars / total)
                    mixed_reviews = (
                        rating_breakdown['rating_2_star'] +
                        rating_breakdown['rating_3_star'] +
                        rating_breakdown['rating_4_star']
                    )
                    mixed_percent = (mixed_reviews / total_reviews * 100) if total_reviews > 0 else 0.0

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
                    print(f"[{len(products)}/{max_products}] {product_name[:40]}... | ${price_usd} | Rating: {average_rating or 'N/A'} ({total_reviews} reviews)")

                except Exception as e:
                    print(f"  Error parsing product card: {e}")
                    continue

            # Check if we need more products
            if len(products) >= max_products:
                break

            # Scroll to load more
            prev_count = len(products)
            await page.evaluate('window.scrollBy(0, window.innerHeight)')
            await page.wait_for_timeout(1500)

            # Try clicking "Load More" button if present
            try:
                load_more = await page.query_selector('button:has-text("Load more"), button:has-text("Show more")')
                if load_more:
                    await load_more.click()
                    await page.wait_for_timeout(2000)
            except:
                pass

            # Check if we're making progress
            if len(products) == prev_count:
                scroll_attempts += 1
            else:
                scroll_attempts = 0

        await browser.close()

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


async def main():
    """Main entry point."""
    # Gumroad category URLs - using direct category pages
    CATEGORY_URLS = {
        'design': 'https://gumroad.com/design',
        '3d': 'https://gumroad.com/3d',
        'drawing': 'https://gumroad.com/drawing-and-painting',
        'software': 'https://gumroad.com/software',
        'music': 'https://gumroad.com/music-and-sound-design',
        'writing': 'https://gumroad.com/writing-and-publishing',
        'education': 'https://gumroad.com/education',
        'photography': 'https://gumroad.com/photography',
        'comics': 'https://gumroad.com/comics-and-graphic-novels',
        'fitness': 'https://gumroad.com/fitness-and-health',
        'films': 'https://gumroad.com/films',
        'audio': 'https://gumroad.com/audio',
        'games': 'https://gumroad.com/gaming',
        'discover': 'https://gumroad.com/discover',
    }

    # Choose category to scrape
    category = 'design'  # Change this to scrape different categories
    url = CATEGORY_URLS.get(category, 'https://gumroad.com/discover')

    print("=" * 60)
    print("GUMROAD DISCOVER PAGE SCRAPER")
    print("=" * 60)
    print(f"Category: {category}")
    print(f"URL: {url}")
    print(f"Target: 100 products")
    print("=" * 60 + "\n")

    # Scrape products
    products = await scrape_discover_page(
        category_url=url,
        max_products=100,
        get_detailed_ratings=True  # Get sales count from product pages
    )

    # Save to CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'gumroad_{category}_{timestamp}.csv'
    save_to_csv(products, filename)

    # Print summary
    print("\n" + "=" * 60)
    print("SCRAPE COMPLETE")
    print("=" * 60)
    print(f"Total products scraped: {len(products)}")

    if products:
        avg_price = sum(p.price_usd for p in products) / len(products)
        rated_products = [p for p in products if p.average_rating]
        avg_rating = sum(p.average_rating for p in rated_products) / len(rated_products) if rated_products else 0

        products_with_sales = [p for p in products if p.sales_count]
        total_sales = sum(p.sales_count for p in products_with_sales)
        total_revenue = sum(p.estimated_revenue for p in products if p.estimated_revenue)

        print(f"Average price: ${avg_price:.2f}")
        print(f"Average rating: {avg_rating:.2f} ({len(rated_products)} rated products)")
        print(f"Products with sales data: {len(products_with_sales)}")
        print(f"Total sales (visible): {total_sales:,}")
        print(f"Total estimated revenue: ${total_revenue:,.2f}")

    print("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
