# gumroad-scraper

scrapes product data from gumroad discover pages.

extracts: product name, creator, price, rating, sales count, estimated revenue

built with python, playwright, beautifulsoup

work in progress.

## Scheduled ingestion runner

Use `ingestion_runner.py` to run daily or real-time scraping jobs defined in
`ingestion_config.json`. Results are upserted into a SQL database (SQLite by
default) keyed by product URL and timestamped for change detection.

```bash
# Install new dependencies
pip install -r requirements.txt

# Run all configured jobs against a local SQLite file
python ingestion_runner.py --config ingestion_config.json --database-url sqlite:///gumroad_ingestion.db

# Run only the daily jobs to ship to Postgres
python ingestion_runner.py --schedule daily --database-url "postgres+psycopg2://user:pass@host/dbname"

# Target specific jobs and override batch size
python ingestion_runner.py --jobs design-daily,ai-query-realtime --max-products 50
```
