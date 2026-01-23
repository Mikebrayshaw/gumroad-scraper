# Railway cron service

Use a dedicated Railway cron service to run the weekly Gumroad scrape/ingest.

## Schedule
- **Weekly cadence**: `0 3 * * 1` (every Monday at 03:00 UTC)

## Command
Run the ingestion runner with Supabase persistence and the shared database URL:

```bash
python ingestion_runner.py --use-supabase --platform-slug gumroad --database-url "$DATABASE_URL"
```

This uses the jobs in `ingestion_config.json`. Adjust flags (e.g., `--save-csv-dir ./ingestion_results`) as needed.

## Required environment variables
Configure the cron service with the same env vars as the main Railway service:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `DATABASE_URL` (Postgres URL for SQLAlchemy)
- `PLAYWRIGHT_BROWSERS_PATH=/ms-playwright` (matches the Dockerfile install path)

## Railway setup notes
- Create a new Railway service from this repo and set its config path to `railway.cron.toml`.
- Copy the environment variables from the primary service so Playwright and Supabase are available to the job.
