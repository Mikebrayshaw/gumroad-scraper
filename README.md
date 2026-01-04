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

### Progress bar (on by default)
```bash
python gumroad_scraper.py --no-progress  # disable if your log sink dislikes TTY updates
```

### Custom output filename
```bash
python gumroad_scraper.py --output gumroad_design.csv
```

The scraper saves a CSV with all collected fields and prints a run summary that includes averages, sales totals, and the output path. Detailed runs open each product page (with a delay between requests) to capture ratings and sales data, which is the slowest part of a full scrape—use `--fast` when you only need card metadata.

## Supabase persistence & Railway deployment
1. Create a Supabase project and run `supabase_schema.sql` in the SQL editor to provision the `platforms`, `scrape_runs`, and `products` tables (with indexes on product IDs and timestamps).
2. Copy `.env.example` to `.env` and fill in `SUPABASE_URL`, `SUPABASE_ANON_KEY`, and `SUPABASE_SERVICE_ROLE_KEY` locally. Railway uses the same variable names—add them in the Railway dashboard so deployments can reach Supabase.
3. Enable persistence in the ingestion runner with:
   ```bash
   python ingestion_runner.py --use-supabase --platform-slug gumroad
   ```
   The runner records each scrape run and upserts/deduplicates products by the platform-specific product ID parsed from the URL.
4. To review historical data, launch the Streamlit history view (requires the same Supabase env vars):
   ```bash
   streamlit run history_app.py
   ```
   Filter runs, inspect products, and export CSV directly from the UI.

### Save CSVs from scheduled jobs
- Pass `--save-csv-dir ./ingestion_results` to `ingestion_runner.py` to write a timestamped CSV for every job run. Filenames use the
  job name (or platform slug) plus the run timestamp so repeated searches are preserved alongside database or Supabase storage.

## Adding new marketplaces (Whop scaffolding included)
- The ingestion runner now routes jobs through a platform registry (`platforms.py`).
  Each job in `ingestion_config.json` may declare a `"platform"` (defaults to `gumroad`).
- Gumroad scrapes continue to work unchanged. A Whop scraper now lives in `whop_scraper.py` and
  matches the ingestion runner’s call signature. Point any job at a Whop listing or search URL with
  `"platform": "whop"` to ingest it. The `Product` dataclass is shared so persistence and CSV exports
  continue to work across platforms.
