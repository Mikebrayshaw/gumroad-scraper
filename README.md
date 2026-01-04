# gumroad-scraper

A Playwright-powered scraper that collects product details from Gumroad discover and category pages. It extracts the product name, creator, price, rating data, sales counts (when available), and an estimated revenue figure and writes the results to CSV.

## Prerequisites
- Python 3.10+
- Playwright browsers installed (Chromium is required)

Install Python dependencies and the Chromium browser once before running the scraper:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

## Usage
Run the scraper from the repository root.

### Single category (default: `design`)
```bash
python gumroad_scraper.py
```

### Choose a specific category
```bash
python gumroad_scraper.py --category software --max-products 50
```

### Scrape all categories
```bash
python gumroad_scraper.py --all --max-products 25
```

### Fast mode (skip detailed product pages)
```bash
python gumroad_scraper.py --fast
```

### Custom output filename
```bash
python gumroad_scraper.py --output gumroad_design.csv
```

The scraper saves a CSV with all collected fields and prints a run summary that includes averages, sales totals, and the output path.
