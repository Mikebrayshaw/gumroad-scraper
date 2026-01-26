"""
Gumroad Scraper - Streamlit App
Web UI for scraping product data from Gumroad discover pages.
Includes opportunity scoring, saved searches, and watchlists.
"""

import asyncio
import sys
from datetime import datetime

# Fix for Python 3.14+ on Windows - ensure ProactorEventLoop is used for subprocess support
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import pandas as pd
import streamlit as st
from dataclasses import asdict

from analysis_ui import render_analysis_block
from categories import CATEGORY_BY_LABEL, CATEGORY_TREE, Category, Subcategory, build_discover_url
from gumroad_scraper import (
    scrape_discover_page,
    Product,
)
from opportunity_scoring import (
    score_product_dict,
    get_top_scored_products,
    get_score_breakdown,
)
from alerts import (
    create_saved_search,
    get_saved_searches,
    delete_saved_search,
    add_to_watchlist,
    get_watchlist,
    remove_from_watchlist,
    save_snapshot,
    get_latest_snapshot,
    check_for_updates,
    send_digest,
    SavedSearch,
)
from supabase_utils import SupabaseRunStore, get_supabase_client
from scripts.full_gumroad_scrape import scrape_all_categories


st.set_page_config(
    page_title="Gumroad Scraper",
    page_icon="🛒",
    layout="wide",
)

st.title("🛒 Gumroad Scraper")
st.markdown("Scrape product data from Gumroad discover pages with opportunity scoring.")

# Sidebar controls
st.sidebar.header("Settings")


category_options = [cat.label for cat in CATEGORY_TREE]
category_label = st.sidebar.selectbox(
    "Category",
    options=category_options,
    index=0,
)

selected_category: Category = CATEGORY_BY_LABEL[category_label]
subcategory_options: tuple[Subcategory, ...] = selected_category.subcategories
category_slug = selected_category.slug

# Reset subcategory and clear old results when category changes
if "last_category" not in st.session_state:
    st.session_state.last_category = category_label
if "subcategory_choice" not in st.session_state:
    st.session_state.subcategory_choice = subcategory_options[0].label
if st.session_state.last_category != category_label:
    st.session_state.last_category = category_label
    st.session_state.subcategory_choice = subcategory_options[0].label
    # Clear previous results when category changes to prevent showing stale data
    st.session_state.results = None
    st.session_state.scored_results = None
    st.session_state.current_run_id = None
    st.session_state.current_category_slug = None
    st.session_state.current_subcategory_slug = None

subcategory_label = st.sidebar.selectbox(
    "Subcategory",
    options=[subcategory.label for subcategory in subcategory_options],
    format_func=lambda value: value if value == "All Subcategories" else f" • {value}",
    key="subcategory_choice",
)

subcategory_slug_lookup = {subcategory.label: subcategory.slug for subcategory in subcategory_options}
subcategory_slug = subcategory_slug_lookup.get(subcategory_label, "")

max_products = st.sidebar.number_input(
    "Max Products",
    min_value=10,
    max_value=1000,
    value=100,
    step=10,
    help="Maximum number of products to scrape (higher values take longer)",
)

fast_mode = st.sidebar.checkbox(
    "Fast Mode",
    value=False,
    help="Skip detailed product pages (no sales data)",
)

rate_limit = st.sidebar.slider(
    "Rate Limit (ms)",
    min_value=100,
    max_value=2000,
    value=500,
    step=100,
    help="Delay between requests to avoid detection",
)

st.sidebar.markdown("---")
if st.sidebar.button("Clear Cache", use_container_width=True):
    st.cache_data.clear()
    st.session_state.results = None
    st.session_state.scored_results = None
    st.session_state.current_run_id = None
    st.sidebar.success("Cache cleared!")

# Initialize session state
if "results" not in st.session_state:
    st.session_state.results = None
if "scored_results" not in st.session_state:
    st.session_state.scored_results = None
if "scraping" not in st.session_state:
    st.session_state.scraping = False
if "detected_changes" not in st.session_state:
    st.session_state.detected_changes = None
if "current_run_id" not in st.session_state:
    st.session_state.current_run_id = None
if "current_category_slug" not in st.session_state:
    st.session_state.current_category_slug = None
if "current_subcategory_slug" not in st.session_state:
    st.session_state.current_subcategory_slug = None


@st.cache_resource
def get_run_store() -> tuple[SupabaseRunStore, str]:
    client = get_supabase_client()
    if client:
        return SupabaseRunStore(client), "supabase"
    return SupabaseRunStore(None), "local"


run_store, storage_mode = get_run_store()
storage_label = "Supabase" if storage_mode == "supabase" else "Local (no persistence)"
badge_color = "#16a34a" if storage_mode == "supabase" else "#f97316"
st.markdown(
    f"<div style='display:inline-block;padding:6px 10px;border-radius:8px;background:{badge_color};color:white;font-weight:600;'>"
    f"Storage: {storage_label}</div>",
    unsafe_allow_html=True,
)
if st.session_state.current_run_id:
    st.caption(f"Current run ID: {st.session_state.current_run_id}")


def load_run_results(run_id: str, category_slug: str, subcategory_slug: str | None, store: SupabaseRunStore) -> pd.DataFrame:
    data = store.fetch_snapshots(run_id, category=category_slug, subcategory=subcategory_slug or None)
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df = df.rename(
        columns={
            "title": "product_name",
            "url": "product_url",
            "price_amount": "price_usd",
            "rating_avg": "average_rating",
            "rating_count": "total_reviews",
            "revenue_estimate": "estimated_revenue",
        }
    )
    for required in [
        "product_name",
        "product_url",
        "creator_name",
        "category",
        "subcategory",
        "price_usd",
        "average_rating",
        "total_reviews",
        "sales_count",
        "estimated_revenue",
        "opportunity_score",
    ]:
        if required not in df:
            df[required] = None
    for numeric_col in [
        "price_usd",
        "average_rating",
        "total_reviews",
        "sales_count",
        "estimated_revenue",
        "opportunity_score",
    ]:
        df[numeric_col] = pd.to_numeric(df[numeric_col], errors="coerce") if numeric_col in df else None
    return df


def run_scraper(
    category_slug: str,
    subcategory_slug: str,
    max_products: int,
    fast_mode: bool,
    rate_limit: int,
    run_id: str,
    storage_mode: str,
) -> list[Product]:
    """Run the scraper and return products."""

    url = build_discover_url(category_slug, subcategory_slug)

    # Console debug: what is actually being passed to the scraper
    print(f"\n>>> run_scraper() called with:")
    print(f">>>   category_slug: '{category_slug}'")
    print(f">>>   subcategory_slug: '{subcategory_slug}'")
    print(f">>>   Built URL: {url}")

    st.write(
        f"Scraping with run_id={run_id} | category={category_slug} | subcategory={subcategory_slug or 'all'} | storage={storage_mode}"
    )

    # Run async scraper in sync context
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        products = loop.run_until_complete(
            scrape_discover_page(
                category_url=url,
                category_slug=category_slug,
                subcategory_slug=subcategory_slug,
                max_products=max_products,
                get_detailed_ratings=not fast_mode,
                rate_limit_ms=rate_limit,
            )
        )
    finally:
        loop.close()

    return products


def to_dataframe(products: list[Product], scored: list[dict]) -> pd.DataFrame:
    """Combine scraped products and scores into a dataframe for display."""

    if not products or not scored:
        return pd.DataFrame()

    rows = []
    for product, score in zip(products, scored):
        payload = asdict(product)
        payload.update(score)
        rows.append(payload)

    df = pd.DataFrame(rows)
    for numeric_col in [
        "price_usd",
        "average_rating",
        "total_reviews",
        "sales_count",
        "estimated_revenue",
        "opportunity_score",
    ]:
        if numeric_col in df:
            df[numeric_col] = pd.to_numeric(df[numeric_col], errors="coerce")

    return df


# Create tabs for different features
tab_scrape, tab_saved, tab_watchlist, tab_full_scrape = st.tabs(["Scrape", "Saved Searches", "Watchlist", "Full Scrape"])

with tab_scrape:
    # Main scraping area
    col1, col2 = st.columns([1, 4])

    with col1:
        scrape_button = st.button(
            "Scrape",
            type="primary",
            use_container_width=True,
        )

    if scrape_button:
        # Clear all caches before scraping to ensure fresh data
        st.cache_data.clear()

        st.session_state.scraping = True
        st.session_state.results = None
        st.session_state.scored_results = None

        # DEBUG: Show timestamp and exactly what we're about to scrape
        scrape_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.warning(f"🕐 SCRAPE STARTED: {scrape_timestamp}")

        # Print to console for debugging
        print(f"\n{'='*60}")
        print(f"SCRAPE DEBUG - {scrape_timestamp}")
        print(f"{'='*60}")
        print(f"UI Selection:")
        print(f"  - category_label (dropdown): '{category_label}'")
        print(f"  - subcategory_label (dropdown): '{subcategory_label}'")
        print(f"Resolved Values:")
        print(f"  - category_slug: '{category_slug}'")
        print(f"  - subcategory_slug: '{subcategory_slug}'")
        print(f"{'='*60}\n")

        st.info(f"**UI Selection:** Category='{category_label}', Subcategory='{subcategory_label}'")

        subcategory_text = f" / {subcategory_label}" if subcategory_slug else ""
        run_id = run_store.start_run(
            category=category_slug,
            subcategory=subcategory_slug,
            max_products=max_products,
            fast_mode=fast_mode,
            rate_limit_ms=rate_limit,
        )
        st.session_state.current_run_id = str(run_id)
        st.session_state.current_category_slug = category_slug
        st.session_state.current_subcategory_slug = subcategory_slug

        scrape_url = build_discover_url(category_slug, subcategory_slug)

        # Print exact URL to console
        print(f">>> SCRAPING URL: {scrape_url}")
        print(f">>> Passed to run_scraper: category_slug='{category_slug}', subcategory_slug='{subcategory_slug}'")

        st.info(f"Run ID: {run_id} | Storage: {storage_label}")
        st.code(f"🔗 EXACT URL BEING SCRAPED:\n{scrape_url}")
        st.write(
            f"Selected category: **{category_label}** | Selected subcategory: **{subcategory_label}** | Mode: {storage_mode}"
        )

        with st.spinner(
            f"Scraping {category_label}{subcategory_text}... This may take a few minutes."
        ):
            try:
                products = run_scraper(
                    category_slug=category_slug,
                    subcategory_slug=subcategory_slug,
                    max_products=max_products,
                    fast_mode=fast_mode,
                    rate_limit=rate_limit,
                    run_id=str(run_id),
                    storage_mode=storage_mode,
                )
                st.session_state.results = products

                # Score all products
                product_dicts = [asdict(p) for p in products]
                scored_products = [score_product_dict(p) for p in product_dicts]
                st.session_state.scored_results = scored_products

                totals = run_store.record_snapshots(run_id, products, scored_products)
                run_store.complete_run(run_id, totals={"total": len(products), **totals})

                first_urls = [p.product_url for p in products[:3]]
                if first_urls:
                    st.write("First scraped product URLs:")
                    st.code("\n".join(first_urls))

                complete_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"\n>>> SCRAPE COMPLETED: {complete_timestamp}")
                print(f">>> Products scraped: {len(products)}")
                if products:
                    print(f">>> First product: {products[0].product_name}")
                    print(f">>> First product URL: {products[0].product_url}")

                st.success(f"✅ Scraped {len(products)} products at {complete_timestamp}")
            except Exception as e:
                st.error(f"Error: {e}")
                run_store.complete_run(run_id, status="failed", error=str(e))
                st.session_state.results = None
                st.session_state.scored_results = None

        st.session_state.scraping = False

    # Prefer freshly scraped results in memory; fall back to persisted snapshots
    df = to_dataframe(st.session_state.results or [], st.session_state.scored_results or [])

    if df.empty and st.session_state.current_run_id:
        try:
            df = load_run_results(
                st.session_state.current_run_id,
                st.session_state.current_category_slug or category_slug,
                st.session_state.current_subcategory_slug or subcategory_slug,
                run_store,
            )
            if not df.empty:
                st.session_state.scored_results = df.to_dict(orient="records")
        except Exception as exc:
            st.error(f"Unable to load results for run {st.session_state.current_run_id}: {exc}")

    # Display results
    if not df.empty:
        scored_products = df.to_dict(orient="records")

        # Summary metrics
        st.markdown("---")
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric("Products", len(df))

        with col2:
            avg_price = df["price_usd"].mean()
            st.metric("Avg Price", f"${avg_price:.2f}")

        with col3:
            rated = df[df["average_rating"].notna()]
            avg_rating = rated["average_rating"].mean() if len(rated) > 0 else 0
            st.metric("Avg Rating", f"{avg_rating:.2f}")

        with col4:
            total_sales = df["sales_count"].sum()
            st.metric("Total Sales", f"{total_sales:,.0f}" if pd.notna(total_sales) else "N/A")

        with col5:
            avg_score = df["opportunity_score"].mean()
            st.metric("Avg Score", f"{avg_score:.1f}")

        st.markdown("---")

        # Top 10 by Opportunity Score
        st.subheader("Top 10 by Opportunity Score")

        top_10 = get_top_scored_products(scored_products, n=10)
        if top_10:
            top_df = pd.DataFrame(top_10)
            top_display_cols = [
                "product_name",
                "opportunity_score",
                "price_usd",
                "average_rating",
                "total_reviews",
                "sales_count",
                "estimated_revenue",
            ]

            st.dataframe(
                top_df[top_display_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "product_name": st.column_config.TextColumn("Product", width="large"),
                    "opportunity_score": st.column_config.NumberColumn("Score", format="%.1f"),
                    "price_usd": st.column_config.NumberColumn("Price (USD)", format="$%.2f"),
                    "average_rating": st.column_config.NumberColumn("Rating", format="%.1f"),
                    "total_reviews": st.column_config.NumberColumn("Reviews"),
                    "sales_count": st.column_config.NumberColumn("Sales", format="%d"),
                    "estimated_revenue": st.column_config.NumberColumn("Est. Revenue", format="$%.0f"),
                },
            )

            # Score breakdown for top product
            with st.expander("View Score Breakdown for Top Product"):
                if top_10:
                    st.code(get_score_breakdown(top_10[0]))

        st.markdown("---")

        # Full Results table
        st.subheader("All Results")

        # Select columns to display
        display_cols = [
            "product_name",
            "creator_name",
            "opportunity_score",
            "price_usd",
            "average_rating",
            "total_reviews",
            "sales_count",
            "estimated_revenue",
        ]

        st.dataframe(
            df[display_cols].sort_values("opportunity_score", ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                "product_name": st.column_config.TextColumn("Product", width="large"),
                "creator_name": st.column_config.TextColumn("Creator", width="medium"),
                "opportunity_score": st.column_config.NumberColumn("Score", format="%.1f"),
                "price_usd": st.column_config.NumberColumn("Price (USD)", format="$%.2f"),
                "average_rating": st.column_config.NumberColumn("Rating", format="%.1f"),
                "total_reviews": st.column_config.NumberColumn("Reviews"),
                "sales_count": st.column_config.NumberColumn("Sales", format="%d"),
                "estimated_revenue": st.column_config.NumberColumn("Est. Revenue", format="$%.0f"),
            },
        )

        # Download button
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"gumroad_{category_slug}{f'_{subcategory_slug}' if subcategory_slug else ''}.csv",
            mime="text/csv",
        )

        # Expandable full data view
        with st.expander("View All Columns"):
            st.dataframe(df, use_container_width=True, hide_index=True)

        # Save Search button
        st.markdown("---")
        st.subheader("Save This Search")

        save_col1, save_col2 = st.columns([3, 1])
        with save_col1:
            search_name = st.text_input(
                "Search Name",
                value=f"{category_label} - {subcategory_label}" if subcategory_slug else category_label,
                key="save_search_name",
            )
        with save_col2:
            if st.button("Save Search", use_container_width=True):
                saved = create_saved_search(
                    name=search_name,
                    category=category_slug,
                    subcategory=subcategory_slug,
                )
                # Save initial snapshot
                save_snapshot(saved.id, scored_products)
                st.success(f"Saved search '{search_name}' with {len(scored_products)} products!")

    else:
        st.info("Select a category and click **Scrape** to get started.")


with tab_saved:
    st.subheader("Saved Searches")

    saved_searches = get_saved_searches()

    if not saved_searches:
        st.info("No saved searches yet. Scrape some products and save a search to get started.")
    else:
        for search in saved_searches:
            with st.container():
                col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

                with col1:
                    st.write(f"**{search.name}**")
                    st.caption(f"Category: {search.category}" + (f" / {search.subcategory}" if search.subcategory else ""))

                with col2:
                    if search.last_checked_at:
                        st.caption(f"Last checked: {search.last_checked_at[:16]}")
                    else:
                        st.caption("Never checked")

                with col3:
                    if st.button("Check Updates", key=f"check_{search.id}", use_container_width=True):
                        with st.spinner("Checking for updates..."):
                            # Run a new scrape
                            products = run_scraper(
                                category_slug=search.category,
                                subcategory_slug=search.subcategory or "",
                                max_products=100,
                                fast_mode=False,
                                rate_limit=500,
                                run_id=f"saved-search-{search.id}",
                            )
                            product_dicts = [asdict(p) for p in products]
                            scored_products = [score_product_dict(p) for p in product_dicts]

                            # Check for changes
                            changes = check_for_updates(search.id, scored_products)
                            st.session_state.detected_changes = changes

                            if changes:
                                st.success(f"Found {len(changes)} changes!")
                                # Send digest (prints to console)
                                send_digest(changes)
                            else:
                                st.info("No changes detected since last check.")

                with col4:
                    if st.button("Delete", key=f"del_{search.id}", type="secondary"):
                        delete_saved_search(search.id)
                        st.rerun()

                st.markdown("---")

        # Show detected changes if any
        if st.session_state.detected_changes:
            st.subheader("Detected Changes")
            changes = st.session_state.detected_changes

            for change in changes:
                icon = {
                    'new': '🆕',
                    'price_change': '💰',
                    'rating_change': '⭐',
                    'sales_change': '📈',
                }.get(change.change_type, '🔔')

                with st.container():
                    if change.change_type == 'new':
                        st.write(f"{icon} **New Product**: {change.product_name}")
                        st.caption(f"Price: {change.new_value}")
                    else:
                        st.write(f"{icon} **{change.change_type.replace('_', ' ').title()}**: {change.product_name}")
                        st.caption(f"{change.old_value} → {change.new_value}")
                    st.caption(f"[View Product]({change.product_url})")


with tab_watchlist:
    st.subheader("Watchlist")

    # Add to watchlist form
    with st.expander("Add to Watchlist"):
        add_col1, add_col2 = st.columns([2, 1])

        with add_col1:
            watch_url = st.text_input("Product URL", placeholder="https://gumroad.com/l/...")
            watch_name = st.text_input("Name (optional)", placeholder="My Product")

        with add_col2:
            watch_type = st.selectbox("Type", ["product", "category"])
            if st.button("Add to Watchlist", use_container_width=True):
                if watch_url:
                    name = watch_name or watch_url[:50]
                    result = add_to_watchlist(watch_type, watch_url, name)
                    if result:
                        st.success(f"Added '{name}' to watchlist!")
                        st.rerun()
                    else:
                        st.warning("Item already in watchlist.")
                else:
                    st.error("Please enter a URL.")

    st.markdown("---")

    # Display watchlist
    watchlist = get_watchlist()

    if not watchlist:
        st.info("Your watchlist is empty. Add products or categories to track them.")
    else:
        for item in watchlist:
            col1, col2, col3 = st.columns([4, 2, 1])

            with col1:
                st.write(f"**{item.name}**")
                st.caption(f"Type: {item.item_type} | Added: {item.created_at[:10]}")

            with col2:
                st.markdown(f"[Open Link]({item.url})")

            with col3:
                if st.button("Remove", key=f"rm_{item.id}", type="secondary"):
                    remove_from_watchlist(item.id)
                    st.rerun()

    # Data table
    st.subheader("Results")

    if st.session_state.scored_results:
        df = pd.DataFrame(st.session_state.scored_results)

        # Select columns to display
        display_cols = [
            "product_name",
            "creator_name",
            "price_usd",
            "average_rating",
            "total_reviews",
            "sales_count",
            "estimated_revenue",
        ]

        st.dataframe(
            df[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "product_name": st.column_config.TextColumn("Product", width="large"),
                "creator_name": st.column_config.TextColumn("Creator", width="medium"),
                "price_usd": st.column_config.NumberColumn("Price (USD)", format="$%.2f"),
                "average_rating": st.column_config.NumberColumn("Rating", format="%.1f ⭐"),
                "total_reviews": st.column_config.NumberColumn("Reviews"),
                "sales_count": st.column_config.NumberColumn("Sales", format="%d"),
                "estimated_revenue": st.column_config.NumberColumn("Est. Revenue", format="$%.0f"),
            },
        )

        # Download button
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"gumroad_{category_slug}{f'_{subcategory_slug}' if subcategory_slug else ''}.csv",
            mime="text/csv",
        )

        # Expandable full data view
        with st.expander("View All Columns"):
            st.dataframe(df, use_container_width=True, hide_index=True)

        st.subheader("Analyze with CrewAI")
        render_analysis_block(
            df.to_dict(orient="records"),
            dataset_id=f"scrape-{category_slug}-{subcategory_slug or 'all'}",
            source_label="Current scrape run",
        )
    else:
        st.info("Select a category and click **Scrape** to get started.")


with tab_full_scrape:
    st.subheader("Full Scrape - All Categories")

    st.warning(
        "**This will scrape ALL categories.** Takes 3-4 hours. "
        "Make sure you have a stable connection and Supabase is configured."
    )

    # Initialize session state for full scrape
    if "full_scrape_running" not in st.session_state:
        st.session_state.full_scrape_running = False
    if "full_scrape_result" not in st.session_state:
        st.session_state.full_scrape_result = None

    # Settings
    col1, col2 = st.columns(2)
    with col1:
        full_max_products = st.number_input(
            "Max products per category",
            min_value=10,
            max_value=500,
            value=100,
            step=10,
            key="full_scrape_max",
        )
    with col2:
        full_rate_limit = st.slider(
            "Rate limit (ms)",
            min_value=300,
            max_value=2000,
            value=500,
            step=100,
            key="full_scrape_rate",
        )

    full_fast_mode = st.checkbox("Fast mode (skip detailed pages)", value=False, key="full_scrape_fast")

    st.markdown("---")

    # Big start button
    if st.button(
        "Start Full Scrape",
        type="primary",
        use_container_width=True,
        disabled=st.session_state.full_scrape_running,
    ):
        st.session_state.full_scrape_running = True
        st.session_state.full_scrape_result = None

        progress_bar = st.progress(0)
        status_text = st.empty()
        category_status = st.empty()

        from categories import CATEGORY_TREE
        total_categories = len(CATEGORY_TREE)

        def update_progress(category: str, idx: int, total: int, products: int):
            progress = (idx) / total
            progress_bar.progress(progress)
            status_text.write(f"**Progress:** {idx}/{total} categories | {products} products scraped")
            category_status.write(f"Currently scraping: **{category}**")

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            result = loop.run_until_complete(
                scrape_all_categories(
                    max_per_category=full_max_products,
                    rate_limit_ms=full_rate_limit,
                    fast_mode=full_fast_mode,
                    progress_callback=update_progress,
                )
            )
            loop.close()

            progress_bar.progress(1.0)
            status_text.write(f"**Completed!** {result['total_products']} products from {result['total_categories']} categories")
            category_status.empty()

            st.session_state.full_scrape_result = result
            st.success(f"Full scrape completed! {result['total_products']} products saved to Supabase.")

        except Exception as e:
            st.error(f"Full scrape failed: {e}")

        finally:
            st.session_state.full_scrape_running = False

    # Show previous result if available
    if st.session_state.full_scrape_result:
        result = st.session_state.full_scrape_result

        st.markdown("---")
        st.subheader("Last Full Scrape Results")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Products", result["total_products"])
        with col2:
            st.metric("Categories", result["total_categories"])
        with col3:
            errors = len([c for c in result["categories"] if c["status"] == "error"])
            st.metric("Errors", errors)

        # Show category breakdown
        with st.expander("Category Breakdown"):
            for cat in result["categories"]:
                status_icon = "+" if cat["status"] == "success" else "x"
                error_msg = f" - {cat.get('error', '')}" if cat["status"] == "error" else ""
                st.text(f"[{status_icon}] {cat['category']}: {cat['products']} products{error_msg}")
