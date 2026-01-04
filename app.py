"""
Gumroad Scraper - Streamlit App
Web UI for scraping product data from Gumroad discover pages.
Includes opportunity scoring, saved searches, and watchlists.
"""

import asyncio

import pandas as pd
import streamlit as st
from dataclasses import asdict

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

# Reset subcategory when category changes
if "last_category" not in st.session_state:
    st.session_state.last_category = category_label
if "subcategory_choice" not in st.session_state:
    st.session_state.subcategory_choice = subcategory_options[0].label
if st.session_state.last_category != category_label:
    st.session_state.last_category = category_label
    st.session_state.subcategory_choice = subcategory_options[0].label

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

# Initialize session state
if "results" not in st.session_state:
    st.session_state.results = None
if "scored_results" not in st.session_state:
    st.session_state.scored_results = None
if "scraping" not in st.session_state:
    st.session_state.scraping = False
if "detected_changes" not in st.session_state:
    st.session_state.detected_changes = None


def run_scraper(
    category_slug: str,
    subcategory_slug: str,
    max_products: int,
    fast_mode: bool,
    rate_limit: int,
) -> list[Product]:
    """Run the scraper and return products."""

    url = build_discover_url(category_slug, subcategory_slug)

    # Run async scraper in sync context
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        products = loop.run_until_complete(
            scrape_discover_page(
                category_url=url,
                max_products=max_products,
                get_detailed_ratings=not fast_mode,
                rate_limit_ms=rate_limit,
            )
        )
    finally:
        loop.close()

    return products


# Create tabs for different features
tab_scrape, tab_saved, tab_watchlist = st.tabs(["Scrape", "Saved Searches", "Watchlist"])

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
        st.session_state.scraping = True

        subcategory_text = f" / {subcategory_label}" if subcategory_slug else ""

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
                )
                st.session_state.results = products

                # Score all products
                product_dicts = [asdict(p) for p in products]
                scored_products = [score_product_dict(p) for p in product_dicts]
                st.session_state.scored_results = scored_products

                st.success(f"Scraped {len(products)} products!")
            except Exception as e:
                st.error(f"Error: {e}")
                st.session_state.results = None
                st.session_state.scored_results = None

        st.session_state.scraping = False

    # Display results
    if st.session_state.scored_results:
        scored_products = st.session_state.scored_results

        # Convert to DataFrame
        df = pd.DataFrame(scored_products)

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

            st.markdown("---")
