"""
Gumroad Discover Page Scraper
Extracts product data from Gumroad's discover page using Playwright.
"""

import argparse
import asyncio
import csv
import re
import json
import os
import random
from datetime import datetime
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional

from playwright.async_api import (
    Browser,
    ElementHandle,
    Page,
    TimeoutError as PlaywrightTimeout,
    async_playwright,
)
from tqdm import tqdm

from categories import CATEGORY_BY_SLUG, CATEGORY_TREE, build_discover_url, category_url_map
from models import estimate_revenue
from utils.progress import ProgressTracker


@dataclass
class Product:
    """Data class for Gumroad product information."""
    product_name: str
    creator_name: str
    category: str
    subcategory: str
    price_usd: float
    original_price: str
    price_is_pwyw: bool
    currency: str
    average_rating: Optional[float]
    total_reviews: int
    rating_1_star: Optional[int]
    rating_2_star: Optional[int]
    rating_3_star: Optional[int]
    rating_4_star: Optional[int]
    rating_5_star: Optional[int]
    mixed_review_count: Optional[int]
    mixed_review_percent: Optional[float]  # 2-4 star reviews / total
    sales_count: Optional[int]
    estimated_revenue: Optional[float]
    revenue_confidence: Optional[str]
    product_url: str
    description: Optional[str] = None
    scraped_at: datetime = field(default_factory=datetime.utcnow)


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

# User agents for rotation to avoid detection
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
]


def get_random_user_agent() -> str:
    """Get a random user agent string."""
    return random.choice(USER_AGENTS)


def get_proxy_config() -> dict | None:
    """Get proxy configuration from environment variables."""
    proxy_url = os.environ.get("SCRAPER_PROXY_URL")
    if not proxy_url:
        return None
    
    config = {"server": proxy_url}
    
    proxy_user = os.environ.get("SCRAPER_PROXY_USER")
    proxy_pass = os.environ.get("SCRAPER_PROXY_PASS")
    
    # Warn if only one credential is provided
    if (proxy_user and not proxy_pass) or (proxy_pass and not proxy_user):
        print("[WARN] Only one proxy credential (user or pass) is set. Both are required for authentication.")
    
    if proxy_user and proxy_pass:
        config["username"] = proxy_user
        config["password"] = proxy_pass
    
    return config


async def capture_debug_info(page: Page, category_slug: str, reason: str) -> dict:
    """Capture debug information when scraping fails."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    debug_dir = Path("debug_screenshots")
    debug_dir.mkdir(exist_ok=True)
    
    # Sanitize category slug for filesystem safety
    safe_category_slug = re.sub(r'[^\w\-]', '_', category_slug)
    
    info = {
        "timestamp": timestamp,
        "category": category_slug,
        "reason": reason,
        "page_title": "unknown",
        "page_url": page.url,
    }
    
    # Get page title with error handling
    try:
        info["page_title"] = await page.title()
    except Exception as e:
        print(f"Could not get page title: {e}")
    
    # Take screenshot with error handling
    try:
        screenshot_path = debug_dir / f"{safe_category_slug}_{timestamp}.png"
        await page.screenshot(path=str(screenshot_path), full_page=True)
        info["screenshot"] = str(screenshot_path)
        print(f"[DEBUG] Debug screenshot captured: {screenshot_path}")
    except Exception as e:
        print(f"Could not capture screenshot: {e}")
        info["screenshot"] = None
    
    # Save HTML with error handling
    try:
        html_path = debug_dir / f"{safe_category_slug}_{timestamp}.html"
        html_content = await page.content()
        html_path.write_text(html_content)
        info["html"] = str(html_path)
    except Exception as e:
        print(f"Could not save HTML: {e}")
        info["html"] = None
    
    # Check for CAPTCHA indicators with error handling
    try:
        captcha_indicators = [
            "captcha", "challenge", "verify you're human", "robot",
            "cloudflare", "access denied", "blocked", "rate limit"
        ]
        page_text = await page.inner_text("body")
        info["possible_captcha"] = any(ind in page_text.lower() for ind in captcha_indicators)
        
        if info["possible_captcha"]:
            print(f"🚨 CAPTCHA/BLOCK DETECTED! Page title: {info['page_title']}")
    except Exception as e:
        print(f"Could not check for CAPTCHA indicators: {e}")
        info["possible_captcha"] = False

    
    return info


def is_valid_product_url(url: str | None) -> bool:
    """Check if URL is a valid product page, not a wishlist or other non-product page."""
    if not url:
        return False
    invalid_patterns = [
        '/wishlists/',
        '/followers',
        '/following',
        '/posts/',
        '/subscribe',
    ]
    return not any(pattern in url.lower() for pattern in invalid_patterns)


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


def is_pwyw_price(price_str: str) -> bool:
    """Return True if the price text implies pay-what-you-want pricing."""
    if not price_str:
        return False
    normalized = price_str.strip().lower()
    return bool(
        re.search(
            r"(name your price|pay what you want|pay-what-you-want|pwyw|\$?\s*0\s*\+)",
            normalized,
        )
    )


def parse_price(price_str: str) -> tuple[float, str, str, bool]:
    """
    Parse price string and convert to USD.
    Returns (usd_price, original_price, currency, price_is_pwyw).
    """
    if not price_str or price_str.lower() in ['free', '$0', '0']:
        return 0.0, 'Free', 'USD', False

    # Clean up the price string
    price_str = price_str.strip()
    original = price_str
    price_is_pwyw = is_pwyw_price(price_str)

    # Handle subscription prices (e.g., "$5 a month")
    if 'a month' in price_str.lower() or '/mo' in price_str.lower():
        price_str = re.sub(r'\s*(a month|/mo|per month).*', '', price_str, flags=re.IGNORECASE)

    currency = 'UNKNOWN'
    code_map = {
        "USD": "USD",
        "EUR": "EUR",
        "GBP": "GBP",
        "CAD": "CAD",
        "AUD": "AUD",
        "JPY": "JPY",
        "INR": "INR",
    }
    code_match = re.search(r"\b(USD|EUR|GBP|CAD|AUD|JPY|INR)\b", price_str, re.IGNORECASE)
    if code_match:
        currency = code_map[code_match.group(1).upper()]

    # Extract currency symbol
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
        return 0.0, original, currency, price_is_pwyw

    # Convert to USD
    conversion_rate = CURRENCY_TO_USD.get(currency, 1.0)
    usd_price = round(price_value * conversion_rate, 2)

    return usd_price, original, currency, price_is_pwyw


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
        # Validate rating is in valid range (0-5)
        if rating < 0 or rating > 5:
            # Invalid rating - likely parsed wrong element, return None
            return None, count
        return rating, count

    # Pattern: "4.8(123)" or "4.8 (123)" or "4.8(123 ratings)"
    match = re.search(r'([\d.]+)\s*\(?(\d+)', rating_str)
    if match:
        rating = float(match.group(1))
        count = int(match.group(2))
        # Validate rating is in valid range (0-5)
        if rating < 0 or rating > 5:
            # Invalid rating - likely parsed wrong element, return None
            return None, count
        return rating, count

    # Just rating without count
    match = re.search(r'([\d.]+)', rating_str)
    if match:
        rating = float(match.group(1))
        # Validate rating is in valid range (0-5)
        if rating < 0 or rating > 5:
            # Invalid rating - likely parsed wrong element, return None
            return None, 0
        return rating, 0

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


def compute_mixed_review_stats(
    total_reviews: int,
    rating_breakdown: dict,
) -> tuple[Optional[int], Optional[float]]:
    """Compute mixed review count/percent for 2-4 star ratings."""
    mixed_sum = (
        rating_breakdown.get('rating_2_star', 0) +
        rating_breakdown.get('rating_3_star', 0) +
        rating_breakdown.get('rating_4_star', 0)
    )
    total_sum = sum(
        rating_breakdown.get(f'rating_{star}_star', 0) for star in range(1, 6)
    )

    if total_reviews <= 0 and total_sum == 0:
        return None, None

    if total_sum <= 100:
        mixed_percent = round(float(mixed_sum), 2)
        mixed_count = None
        if total_reviews > 0:
            mixed_count = int(round(total_reviews * (mixed_percent / 100)))
        return mixed_count, mixed_percent

    mixed_count = int(round(mixed_sum))
    mixed_percent = None
    if total_reviews > 0:
        mixed_percent = round((mixed_count / total_reviews) * 100, 2)
    return mixed_count, mixed_percent


def extract_sales_from_page(page_source: str) -> int | None:
    """Extract sales_count from page HTML source, checking multiple sources.
    
    Tries multiple extraction strategies in order:
    1. JSON embedded data (more reliable)
    2. Visible text patterns like "28,133 sales" or "1.2K sales"
    
    Args:
        page_source: Raw HTML content of the page
        
    Returns:
        Sales count as integer, or None if not found
    """
    if not page_source:
        return None
    
    # Pattern 1: Check for JSON embedded data first (more reliable)
    json_patterns = [
        r'"sales_count"\s*:\s*(\d+)',
        r'"salesCount"\s*:\s*(\d+)',
    ]
    
    for pattern in json_patterns:
        match = re.search(pattern, page_source)
        if match:
            return int(match.group(1))
    
    # Pattern 2: Visible text like "28,133 sales" or "1.2K sales"
    patterns = [
        r'\b([\d,]+)\s+sales\b',
        r'\b([\d.]+)\s*([KkMm])\s+sales\b',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, page_source, re.IGNORECASE)
        if match:
            groups = match.groups()
            if len(groups) == 1:
                return int(groups[0].replace(',', ''))
            elif len(groups) == 2:
                value = float(groups[0])
                multiplier = groups[1].upper()
                if multiplier == 'K':
                    value *= 1000
                elif multiplier == 'M':
                    value *= 1000000
                return int(value)
    
    return None


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
        'description': None,
    }

    for attempt in range(max_retries):
        try:
            await page.goto(product_url, wait_until='domcontentloaded', timeout=20000)
            await page.wait_for_load_state("domcontentloaded")

            await page.wait_for_timeout(2000)  # Wait for dynamic content

            # Get all visible text on the page
            body_text = await page.inner_text('body')

            description = None
            meta_selectors = [
                'meta[name="description"]',
                'meta[property="og:description"]',
                'meta[name="twitter:description"]',
            ]
            for selector in meta_selectors:
                element = await page.query_selector(selector)
                if element:
                    content = await element.get_attribute("content")
                    if content:
                        description = content.strip()
                        break
            if not description:
                for selector in (
                    '[itemprop="description"]',
                    '[data-testid*="description"]',
                    '.product-description',
                    '.description',
                ):
                    try:
                        element = await page.query_selector(selector)
                        if element:
                            text = (await element.inner_text()).strip()
                            if text:
                                description = text
                                break
                    except Exception:
                        continue
            details["description"] = description

            # Get page source for enhanced sales extraction
            page_source = await page.content()
            
            # Find sales count using enhanced extraction
            sales_count = extract_sales_from_page(page_source)
            if sales_count is not None:
                details['sales_count'] = sales_count

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
    category_slug: str | None = None,
    subcategory_slug: str | None = None,
    max_products: int = 100,
    get_detailed_ratings: bool = True,
    rate_limit_ms: int = 500,
    show_progress: bool = False,
) -> tuple[list[Product], dict | None]:
    """
    Scrape products from a Gumroad discover/category page.

    Args:
        category_url: URL of the category page to scrape
        max_products: Maximum number of products to scrape
        get_detailed_ratings: Whether to visit each product page for rating breakdown
        rate_limit_ms: Delay between product page requests in milliseconds

    Returns:
        Tuple of (products list, debug_info dict)
    """
    products = []
    seen_urls = set()

    def _invalid_route_debug(reason: str, status: int | None = None) -> dict:
        debug = {
            "invalid_route": True,
            "url": category_url,
            "reason": reason,
        }
        if status is not None:
            debug["status"] = status
        return debug
    
    # Helper function to setup browser, context, and page with request interception
    async def setup_browser_and_page(p):
        browser = await p.chromium.launch(headless=True, args=["--disable-gpu"])
        
        # Configure proxy and user agent rotation
        proxy_config = get_proxy_config()
        context_options = {
            'viewport': {'width': 1920, 'height': 1080},
            'user_agent': get_random_user_agent()
        }
        if proxy_config:
            context_options['proxy'] = proxy_config
            print(f"Using proxy: {proxy_config['server']}")
        
        context = await browser.new_context(**context_options)
        page = await context.new_page()

        # Block resource-heavy requests to reduce memory usage and prevent crashes
        async def block_resources(route):
            if route.request.resource_type in ("image", "media", "font"):
                await route.abort()
            else:
                await route.continue_()

        await page.route("**/*", block_resources)
        
        return browser, context, page
    
    # Inner function containing the main scraping logic
    async def perform_scrape(p):
        browser, context, page = await setup_browser_and_page(p)
        debug_info = None

        print(f"Navigating to {category_url}...")
        response = await page.goto(category_url, wait_until='domcontentloaded', timeout=60000)
        
        # Check for 404 or 410 status codes
        if response and response.status in [404, 410]:
            print(f"[WARN] Invalid route detected: {category_url} returned {response.status}")
            await browser.close()
            return [], _invalid_route_debug("http_status", response.status)
        
        # Check page content for "Page not found" indicators (Gumroad may return 200 with error template)
        try:
            page_text = await page.inner_text("body")
            page_not_found_indicators = [
                "page not found",
                "404",
                "not found",
                "this page doesn't exist",
                "couldn't find that page",
            ]
            if any(indicator in page_text.lower() for indicator in page_not_found_indicators):
                # Check if it's a genuine 404 page (not just product descriptions containing these words)
                # Look for title or heading indicators
                try:
                    page_title = await page.title()
                    if "not found" in page_title.lower() or "404" in page_title.lower():
                        print(f"[WARN] Invalid route detected: {category_url} shows 'Page not found' content")
                        await browser.close()
                        debug_info = _invalid_route_debug("page_not_found")
                        debug_info["page_title"] = page_title
                        return [], debug_info
                except Exception:
                    pass
        except Exception as e:
            print(f"[DEBUG] Could not check for 'Page not found' indicators: {e}")
        
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
        main_category = category_slug or (category_match.group(1) if category_match else 'discover')
        if main_category == 'discover':
            # Check for query param
            query_match = re.search(r'query=([^&]+)', category_url)
            if query_match:
                main_category = query_match.group(1)

        selected_subcategory = subcategory_slug or main_category

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

            # If this is the first attempt and no cards found, capture debug info
            if not product_cards and scroll_attempts == 0:
                debug_info = await capture_debug_info(page, main_category, "no_products_on_load")
                debug_info["zero_products"] = True
                if debug_info.get("possible_captcha"):
                    print("🚨 Detected possible CAPTCHA/block - aborting this category")
                    progress.close()
                    await browser.close()
                    return products, debug_info  # Return empty list with debug info

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

                    # Skip non-product URLs (wishlists, follower pages, etc.)
                    if not is_valid_product_url(product_url):
                        continue

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
                    price_is_pwyw = False

                    if price_elem:
                        # Get the content attribute for numeric value
                        price_content = await price_elem.get_attribute('content')
                        price_text = await price_elem.inner_text()
                        original_price = price_text.strip() if price_text else "Free"
                        price_is_pwyw = is_pwyw_price(original_price)

                        if price_content:
                            try:
                                price_value = float(price_content)
                            except ValueError:
                                price_value = 0.0
                        else:
                            # Parse from text
                            _, original_price, _, price_is_pwyw = parse_price(original_price)
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
                    else:
                        price_is_pwyw = is_pwyw_price(original_price)

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
                            # Only use if valid (parse_rating now validates 0-5 range)
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

                    mixed_count, mixed_percent = compute_mixed_review_stats(
                        total_reviews,
                        rating_breakdown,
                    )

                    # Estimate revenue (conservative)
                    sales = rating_breakdown['sales_count']
                    estimated_revenue, revenue_confidence = estimate_revenue(
                        price_usd,
                        sales,
                        price_is_pwyw,
                        currency,
                    )

                    product = Product(
                        product_name=product_name,
                        creator_name=creator_name,
                        category=main_category,
                        subcategory=selected_subcategory,
                        price_usd=price_usd,
                        original_price=original_price,
                        price_is_pwyw=price_is_pwyw,
                        currency=currency,
                        average_rating=average_rating,
                        total_reviews=total_reviews,
                        rating_1_star=rating_breakdown['rating_1_star'],
                        rating_2_star=rating_breakdown['rating_2_star'],
                        rating_3_star=rating_breakdown['rating_3_star'],
                        rating_4_star=rating_breakdown['rating_4_star'],
                        rating_5_star=rating_breakdown['rating_5_star'],
                        mixed_review_count=mixed_count,
                        mixed_review_percent=mixed_percent,
                        sales_count=sales,
                        estimated_revenue=estimated_revenue,
                        revenue_confidence=revenue_confidence,
                        product_url=product_url,
                        description=rating_breakdown.get('description'),
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
                await page.wait_for_load_state("domcontentloaded", timeout=5000)

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

        try:
            await browser.close()
        except Exception:
            pass  # Ignore errors during cleanup

        progress.close()
        
        return products, debug_info
    
    # Retry logic for page crash errors
    max_attempts = 2
    
    async with async_playwright() as p:
        for attempt in range(1, max_attempts + 1):
            try:
                products, debug_info = await perform_scrape(p)
                return products, debug_info
            except Exception as e:
                error_msg = str(e).lower()
                if "page crash" in error_msg and attempt < max_attempts:
                    print(f"⚠️ Page crashed (attempt {attempt}/{max_attempts}), retrying...")
                    # Cleanup is handled by perform_scrape closing the browser
                    continue
                else:
                    # Either not a page crash error, or we're out of retries
                    raise


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
    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument(
        '-c', '--category',
        type=str,
        default='design',
        choices=list(CATEGORY_URLS.keys()),
        help='Category to scrape (default: design)'
    )
    base_parser.add_argument(
        '--all',
        action='store_true',
        help='Scrape all categories'
    )

    base_args, _ = base_parser.parse_known_args()
    subcategory_choices = []
    category_meta = CATEGORY_BY_SLUG.get(base_args.category)
    if category_meta:
        subcategory_choices = [sub.slug for sub in category_meta.subcategories if sub.slug]

    parser = argparse.ArgumentParser(
        description='Scrape product data from Gumroad discover pages.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[base_parser],
        epilog=f"""
Examples:
  python gumroad_scraper.py                     # Scrape 'design' category (default)
  python gumroad_scraper.py -c software         # Scrape 'software' category
  python gumroad_scraper.py -c 3d -n 50         # Scrape 50 products from '3d' category
  python gumroad_scraper.py -c design --subcategory icons  # Scrape a category subcategory
  python gumroad_scraper.py --all -n 25         # Scrape 25 products from ALL categories
  python gumroad_scraper.py -c design --fast    # Fast mode (no detailed product pages)

Available categories:
  {', '.join(CATEGORY_URLS.keys())}
        """
    )
    parser.add_argument(
        '-n', '--max-products',
        type=int,
        default=100,
        help='Maximum number of products to scrape per category (default: 100)'
    )
    parser.add_argument(
        '--subcategory',
        type=str,
        choices=subcategory_choices if subcategory_choices else None,
        help='Optional subcategory slug for the selected category'
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
    args = parser.parse_args()
    if args.all and args.subcategory:
        parser.error('--subcategory cannot be used with --all')

    if args.subcategory:
        category_meta = CATEGORY_BY_SLUG.get(args.category)
        if not category_meta:
            parser.error(f"Subcategories are not available for category '{args.category}'.")
        allowed_subcategories = {sub.slug for sub in category_meta.subcategories if sub.slug}
        if args.subcategory not in allowed_subcategories:
            parser.error(
                f"Invalid subcategory '{args.subcategory}' for category '{args.category}'. "
                f"Valid options: {', '.join(sorted(allowed_subcategories))}"
            )

    return args


async def scrape_category(category: str, args) -> tuple[list[Product], dict | None]:
    """Scrape a single category and return products plus debug info."""
    url = build_discover_url(category, args.subcategory)

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

    products, debug_info = await scrape_discover_page(
        category_url=url,
        max_products=args.max_products,
        get_detailed_ratings=not args.fast,
        rate_limit_ms=args.rate_limit,
        show_progress=not args.no_progress,
    )

    return products, debug_info


async def main():
    """Main entry point."""
    args = parse_args()
    run_id = datetime.now().strftime("gumroad_cli_%Y%m%d_%H%M%S")

    all_products = []

    if args.all:
        # Scrape all categories
        categories = [c for c in CATEGORY_URLS.keys() if c != 'discover']
        print(f"Scraping {len(categories)} categories...\n")
        tracker = ProgressTracker(run_id=run_id, planned_total=len(categories))

        for category in categories:
            products, debug_info = await scrape_category(category, args)
            all_products.extend(products)
            print(f"\nCompleted {category}: {len(products)} products\n")
            snapshot = tracker.update(
                category=category,
                subcategory=args.subcategory,
                products_delta=len(products),
                invalid_route=bool(debug_info and debug_info.get("invalid_route")),
                zero_products=bool(debug_info and debug_info.get("zero_products")),
                captcha_suspected=bool(debug_info and debug_info.get("possible_captcha")),
                error=bool(debug_info and debug_info.get("error")),
            )
            print(tracker.format_line(snapshot))

    else:
        # Scrape single category
        tracker = ProgressTracker(run_id=run_id, planned_total=1)
        products, debug_info = await scrape_category(args.category, args)
        all_products.extend(products)
        snapshot = tracker.update(
            category=args.category,
            subcategory=args.subcategory,
            products_delta=len(products),
            invalid_route=bool(debug_info and debug_info.get("invalid_route")),
            zero_products=bool(debug_info and debug_info.get("zero_products")),
            captcha_suspected=bool(debug_info and debug_info.get("possible_captcha")),
            error=bool(debug_info and debug_info.get("error")),
        )
        print(tracker.format_line(snapshot))

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
