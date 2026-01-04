"""
Gumroad Scraper - Streamlit App
Web UI for scraping product data from Gumroad discover pages.
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


st.set_page_config(
    page_title="Gumroad Scraper",
    page_icon="🛒",
    layout="wide",
)

st.title("🛒 Gumroad Scraper")
st.markdown("Scrape product data from Gumroad discover pages.")

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
if "scraping" not in st.session_state:
    st.session_state.scraping = False


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


# Main area
col1, col2 = st.columns([1, 4])

with col1:
    scrape_button = st.button(
        "🚀 Scrape",
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
            st.success(f"Scraped {len(products)} products!")
        except Exception as e:
            st.error(f"Error: {e}")
            st.session_state.results = None

    st.session_state.scraping = False

# Display results
if st.session_state.results:
    products = st.session_state.results

    # Convert to DataFrame
    df = pd.DataFrame([asdict(p) for p in products])

    # Summary metrics
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Products", len(df))

    with col2:
        avg_price = df["price_usd"].mean()
        st.metric("Avg Price", f"${avg_price:.2f}")

    with col3:
        rated = df[df["average_rating"].notna()]
        avg_rating = rated["average_rating"].mean() if len(rated) > 0 else 0
        st.metric("Avg Rating", f"{avg_rating:.2f} ⭐")

    with col4:
        total_sales = df["sales_count"].sum()
        st.metric("Total Sales", f"{total_sales:,.0f}" if pd.notna(total_sales) else "N/A")

    st.markdown("---")

    # Data table
    st.subheader("Results")

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

else:
    st.info("Select a category and click **Scrape** to get started.")
