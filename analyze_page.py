"""Quick script to analyze Gumroad page structure."""
import asyncio
from playwright.async_api import async_playwright


async def analyze_gumroad():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print("Loading Gumroad discover page...")
        await page.goto('https://gumroad.com/discover', wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(3000)

        # Take screenshot
        await page.screenshot(path='gumroad_screenshot.png', full_page=False)
        print("Screenshot saved to gumroad_screenshot.png")

        # Get page HTML structure
        html = await page.content()
        with open('gumroad_page.html', 'w', encoding='utf-8') as f:
            f.write(html)
        print("HTML saved to gumroad_page.html")

        # Look for product containers
        print("\n=== Analyzing page structure ===\n")

        # Try various selectors
        selectors_to_try = [
            'article',
            '[class*="product"]',
            '[class*="Product"]',
            '[class*="card"]',
            '[class*="Card"]',
            'a[href*="/l/"]',
            '[data-component]',
            'section article',
            'main article',
            '.discover',
            '[class*="discover"]',
            '[class*="grid"] > *',
        ]

        for selector in selectors_to_try:
            elements = await page.query_selector_all(selector)
            if elements:
                print(f"'{selector}': {len(elements)} elements found")
                if len(elements) > 0 and len(elements) < 50:
                    first = elements[0]
                    class_name = await first.get_attribute('class')
                    tag = await first.evaluate('el => el.tagName')
                    print(f"  First element: <{tag}> class='{class_name}'")

        # Find links to products
        print("\n=== Product links ===")
        links = await page.query_selector_all('a')
        product_links = []
        for link in links:
            href = await link.get_attribute('href')
            if href and '/l/' in href:
                product_links.append(href)
        print(f"Found {len(product_links)} product links")
        if product_links:
            print(f"Sample: {product_links[:3]}")

        # Get all text from first visible product-like element
        print("\n=== Sample product content ===")

        # Try to find a product card
        sample_card = await page.query_selector('article, [class*="product-card"], [class*="ProductCard"]')
        if sample_card:
            text = await sample_card.inner_text()
            print(f"Sample card text:\n{text[:500]}")
            html = await sample_card.inner_html()
            print(f"\nSample card HTML:\n{html[:1000]}")

        await browser.close()


if __name__ == '__main__':
    asyncio.run(analyze_gumroad())
