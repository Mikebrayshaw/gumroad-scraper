-- Supabase schema for Gumroad scraper persistence
create extension if not exists "pgcrypto";

create table if not exists platforms (
    id bigint generated always as identity primary key,
    slug text not null unique,
    display_name text not null
);

create table if not exists scrape_runs (
    id uuid primary key default gen_random_uuid(),
    platform_id bigint not null references platforms(id),
    category text,
    subcategory text,
    started_at timestamptz not null default now(),
    completed_at timestamptz,
    total_products integer,
    total_new integer,
    total_updated integer
);

create table if not exists products (
    id bigint generated always as identity primary key,
    platform_id bigint not null references platforms(id),
    platform_product_id text not null,
    product_url text not null,
    product_name text not null,
    creator_name text,
    category text,
    subcategory text,
    price_usd numeric,
    original_price text,
    currency text,
    average_rating numeric,
    total_reviews integer,
    rating_1_star integer,
    rating_2_star integer,
    rating_3_star integer,
    rating_4_star integer,
    rating_5_star integer,
    mixed_review_percent numeric,
    sales_count integer,
    estimated_revenue numeric,
    last_run_id uuid references scrape_runs(id),
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    unique(platform_id, platform_product_id)
);

create index if not exists idx_products_platform_product on products(platform_id, platform_product_id);
create index if not exists idx_products_last_seen on products(last_seen_at desc);
create index if not exists idx_runs_started_at on scrape_runs(started_at desc);

-- Canonical run/snapshot tables for pipeline
create table if not exists runs (
    id uuid primary key default gen_random_uuid(),
    platform text not null,
    category text,
    source text,
    config jsonb,
    started_at timestamptz not null default now(),
    completed_at timestamptz,
    total_products integer,
    summary jsonb
);

create table if not exists product_snapshots (
    id bigint generated always as identity primary key,
    platform text not null,
    product_id text not null,
    run_id uuid not null references runs(id),
    url text not null,
    title text not null,
    creator_name text,
    creator_url text,
    category text,
    price_amount numeric,
    price_currency text,
    price_is_pwyw boolean default false,
    rating_avg numeric,
    rating_count integer,
    sales_count integer,
    revenue_estimate numeric,
    revenue_confidence text,
    tags jsonb,
    scraped_at timestamptz not null,
    raw_source_hash text not null,
    unique(platform, product_id, run_id)
);

create table if not exists product_diffs (
    id bigint generated always as identity primary key,
    platform text not null,
    product_id text not null,
    run_id uuid not null references runs(id),
    previous_run_id uuid,
    price_delta numeric,
    rating_count_delta integer,
    sales_count_delta integer,
    revenue_delta numeric,
    raw_source_changed boolean default false,
    computed_at timestamptz not null default now(),
    unique(platform, product_id, run_id)
);
