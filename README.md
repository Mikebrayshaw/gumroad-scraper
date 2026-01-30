# gumroad-scraper

A Playwright-powered scraper that collects product details from Gumroad discover and category pages. It extracts the product name, creator, price, rating data, sales counts (when available), and an estimated revenue figure and writes the results to CSV. The repository also includes a production-minded pipeline for registering scrape runs, normalizing products into a canonical schema, storing per-run snapshots, computing diffs, and exporting results.

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

### Choose a subcategory within a category
```bash
python gumroad_scraper.py --category design --subcategory icons --max-products 50
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

## Pipeline overview (runs, snapshots, diffs)
The pipeline makes every scrape a first-class run and preserves facts for change tracking:

- **Runs**: each scrape is a run with `run_id`, timestamps, input config (category, max products, fast/slow), and summary totals.
- **Products**: stable identity table keyed by `platform` + `product_id` (the Gumroad slug). Identity fields include URL, title, creator, and category.
- **Product snapshots**: each run records the canonical facts for every product (`price_amount`, `price_currency`, `price_is_pwyw`, ratings, sales, `revenue_estimate`, `revenue_confidence`, `scraped_at`, `raw_source_hash`). Snapshots are keyed by `(platform, product_id, run_id)`.
- **Product diffs**: after ingest, the system compares the newest snapshot to the previous one for the same product and stores deltas (`price_delta`, `rating_count_delta`, `sales_count_delta`, `revenue_delta`, `raw_source_changed`).

Revenue estimation is intentionally conservative: `price * sales_count` when both are present, otherwise `None` with `low` confidence. Pay-what-you-want pricing or unknown currency downgrades confidence. See inline comments in `models.py` for the assumptions.

### Canonical Product model
The canonical schema (see `models.ProductSnapshot`) includes:

- platform (e.g., `gumroad`), product_id, url, title
- creator_name, creator_url (nullable), category
- price_amount, price_currency, price_is_pwyw (bool)
- rating_avg, rating_count, sales_count (nullable)
- revenue_estimate (nullable), revenue_confidence (`low`/`med`/`high`)
- tags/keywords (optional list)
- scraped_at (timestamp), raw_source_hash (sha256 of the serialized facts)

## Pipeline CLI
The CLI walks a scrape through the full lifecycle: scrape → ingest → diff → export. All commands live in `pipeline_cli.py`.

```bash
# Scrape a Gumroad category and save canonical snapshots
python pipeline_cli.py scrape --category design --max-products 25 --fast --out data/runs/latest.json

# Ingest a saved run into the local pipeline database (or DATABASE_URL)
python pipeline_cli.py ingest data/runs/latest.json

# Compute diffs against the previous snapshots for each product
python pipeline_cli.py diff --run-id <run_id_from_scrape>

# Export the run as JSON or CSV (snapshots)
python pipeline_cli.py export --run-id <run_id> --format json --out exports/run.json
python pipeline_cli.py export --run-id <run_id> --format csv --out exports/run.csv
```

- `DATABASE_URL` controls persistence (defaults to `sqlite:///data/gumroad_pipeline.db` for local dev).
- Structured logs include the `run_id` for traceability.

## Local dev vs production
- **Local**: default SQLite database at `data/gumroad_pipeline.db`, browserless tests can use `--fast` to avoid product-page fetches.
- **Production (Supabase/Railway)**: set `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`/`SUPABASE_ANON_KEY`, and `DATABASE_URL` in Railway or the environment. Run the migrations in `supabase_schema.sql` to provision `runs`, `product_snapshots`, and `product_diffs` in addition to the existing tables. The ingestion runner (`ingestion_runner.py`) and Streamlit apps continue to work with the same env vars.
- Logging remains structured; retries/backoff are implemented in `gumroad_scraper.py` when fetching product detail pages. Debug artifacts (HTML/screenshot) can be saved by extending the Playwright selectors when they fail.

## Why results are run-scoped
- Every Streamlit scrape issues a new `run_id` (UUID) and writes run metadata (category, subcategory, rate limits, etc.) to the `runs` table.
- Products for that scrape are stored in `product_snapshots` keyed by `(run_id, platform, product_id)`. UI tables filter by the active `run_id` and category/subcategory so stale/global results never bleed across runs.
- The scrape view logs the selected category, the exact discover URL, the `run_id`, and the first few scraped product URLs to make debugging mismatches easy. After a page refresh the last `run_id` is re-used to reload the same snapshots from the database.

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
5. Recommended Railway worker command for full scrapes:
   ```bash
   PYTHONUNBUFFERED=1 DIAG_IP_CHECK=1 python -m scripts.railway_worker --mode full
   ```

### Save CSVs from scheduled jobs
- Pass `--save-csv-dir ./ingestion_results` to `ingestion_runner.py` to write a timestamped CSV for every job run. Filenames use the
  job name (or platform slug) plus the run timestamp so repeated searches are preserved alongside database or Supabase storage.

### Weekly Railway cron job
- Use a dedicated Railway cron service (see `railway.cron.toml`) to run the weekly scrape/ingest job.
- Schedule: `0 3 * * 1` (Monday 03:00 UTC).
- Command: `python ingestion_runner.py --use-supabase --platform-slug gumroad --database-url "$DATABASE_URL"`.
- Ensure the cron service includes `SUPABASE_URL`, `SUPABASE_ANON_KEY`, `SUPABASE_SERVICE_ROLE_KEY`, `DATABASE_URL`, and
  `PLAYWRIGHT_BROWSERS_PATH=/ms-playwright` so Playwright and Supabase work in the job.
- See `ops/railway-cron.md` for setup steps.

## Adding new marketplaces (Whop scaffolding included)
- The ingestion runner now routes jobs through a platform registry (`platforms.py`).
  Each job in `ingestion_config.json` may declare a `"platform"` (defaults to `gumroad`).
- Gumroad scrapes continue to work unchanged. A Whop scraper now lives in `whop_scraper.py` and
  matches the ingestion runner’s call signature. Point any job at a Whop listing or search URL with
  `"platform": "whop"` to ingest it. The `Product` dataclass is shared so persistence and CSV exports
  continue to work across platforms.
