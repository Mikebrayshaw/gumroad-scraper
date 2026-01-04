"""
Gumroad Scraper - Streamlit App
Web UI for scraping product data from Gumroad discover pages.
Includes opportunity scoring, saved searches, and watchlists.
"""

import asyncio
from urllib.parse import urlencode

import pandas as pd
import streamlit as st
from dataclasses import asdict

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


CATEGORY_STRUCTURE = {
    "3D": {
        "slug": "3d",
        "subcategories": [
            ("All Subcategories", ""),
            ("Assets", "assets"),
            ("Characters", "characters"),
            ("Environments", "environments"),
            ("Materials & Textures", "materials-and-textures"),
            ("Models", "models"),
            ("Props", "props"),
        ],
    },
    "Audio": {
        "slug": "audio",
        "subcategories": [
            ("All Subcategories", ""),
            ("Beats", "beats"),
            ("Loops & Samples", "loops-and-samples"),
            ("Mixing & Mastering", "mixing-and-mastering"),
            ("Sound Effects", "sound-effects"),
            ("Vocal Presets", "vocal-presets"),
        ],
    },
    "Business & Money": {
        "slug": "business-and-money",
        "subcategories": [
            ("All Subcategories", ""),
            ("Entrepreneurship", "entrepreneurship"),
            ("Finance & Investing", "finance-and-investing"),
            ("Freelancing", "freelancing"),
            ("Marketing", "marketing"),
            ("Sales", "sales"),
            ("Startups", "startups"),
        ],
    },
    "Comics & Graphic Novels": {
        "slug": "comics-and-graphic-novels",
        "subcategories": [
            ("All Subcategories", ""),
            ("Anthologies", "anthologies"),
            ("Graphic Novels", "graphic-novels"),
            ("Manga", "manga"),
            ("Webcomics", "webcomics"),
            ("Zines", "zines"),
        ],
    },
    "Design": {
        "slug": "design",
        "subcategories": [
            ("All Subcategories", ""),
            ("Icons", "icons"),
            ("Templates", "templates"),
            ("Fonts", "fonts"),
            ("UI Kits", "ui-kits"),
            ("Illustrations", "illustrations"),
            ("Mockups", "mockups"),
        ],
    },
    "Drawing & Painting": {
        "slug": "drawing-and-painting",
        "subcategories": [
            ("All Subcategories", ""),
            ("Brushes", "brushes"),
            ("Procreate Brushes", "procreate-brushes"),
            ("Photoshop Brushes", "photoshop-brushes"),
            ("Tutorials", "tutorials"),
            ("Coloring Pages", "coloring-pages"),
        ],
    },
    "Education": {
        "slug": "education",
        "subcategories": [
            ("All Subcategories", ""),
            ("Courses", "courses"),
            ("Study Guides", "study-guides"),
            ("Homeschool", "homeschool"),
            ("Lesson Plans", "lesson-plans"),
            ("Worksheets", "worksheets"),
        ],
    },
    "Fiction Books": {
        "slug": "fiction-books",
        "subcategories": [
            ("All Subcategories", ""),
            ("Fantasy", "fantasy"),
            ("Romance", "romance"),
            ("Science Fiction", "science-fiction"),
            ("Mystery & Thriller", "mystery-and-thriller"),
            ("Young Adult", "young-adult"),
        ],
    },
    "Films": {
        "slug": "films",
        "subcategories": [
            ("All Subcategories", ""),
            ("Filmmaking", "filmmaking"),
            ("Editing", "editing"),
            ("VFX", "vfx"),
            ("Color Grading", "color-grading"),
            ("Screenwriting", "screenwriting"),
        ],
    },
    "Fitness & Health": {
        "slug": "fitness-and-health",
        "subcategories": [
            ("All Subcategories", ""),
            ("Workout Programs", "workout-programs"),
            ("Nutrition", "nutrition"),
            ("Yoga", "yoga"),
            ("Meditation", "meditation"),
            ("Meal Plans", "meal-plans"),
        ],
    },
    "Games": {
        "slug": "gaming",
        "subcategories": [
            ("All Subcategories", ""),
            ("Game Assets", "game-assets"),
            ("Game Templates", "game-templates"),
            ("Tabletop RPGs", "tabletop-rpgs"),
            ("Rulebooks", "rulebooks"),
            ("Tools & Plugins", "tools-and-plugins"),
        ],
    },
    "Music & Sound Design": {
        "slug": "music-and-sound-design",
        "subcategories": [
            ("All Subcategories", ""),
            ("Sample Packs", "sample-packs"),
            ("Presets", "presets"),
            ("Plugins", "plugins"),
            ("Loops", "loops"),
            ("Beatmaking", "beatmaking"),
        ],
    },
    "Nonfiction Books": {
        "slug": "nonfiction-books",
        "subcategories": [
            ("All Subcategories", ""),
            ("Biography & Memoir", "biography-and-memoir"),
            ("Business", "business"),
            ("Self Help", "self-help"),
            ("History & Politics", "history-and-politics"),
            ("Guides & Manuals", "guides-and-manuals"),
        ],
    },
    "Photography": {
        "slug": "photography",
        "subcategories": [
            ("All Subcategories", ""),
            ("Presets", "presets"),
            ("LUTs", "luts"),
            ("Overlays", "overlays"),
            ("Tutorials", "tutorials"),
            ("Stock Photos", "stock-photos"),
        ],
    },
    "Podcasts": {
        "slug": "podcasts",
        "subcategories": [
            ("All Subcategories", ""),
            ("Business", "business"),
            ("Education", "education"),
            ("Entertainment", "entertainment"),
            ("News", "news"),
            ("Technology", "technology"),
        ],
    },
    "Productivity": {
        "slug": "productivity",
        "subcategories": [
            ("All Subcategories", ""),
            ("Notion Templates", "notion-templates"),
            ("Planners", "planners"),
            ("Journals", "journals"),
            ("Trackers", "trackers"),
            ("Spreadsheets", "spreadsheets"),
        ],
    },
    "Programming & Tech": {
        "slug": "programming-and-tech",
        "subcategories": [
            ("All Subcategories", ""),
            ("Web Development", "web-development"),
            ("AI & Machine Learning", "ai-and-machine-learning"),
            ("Data Science", "data-science"),
            ("Automation", "automation"),
            ("Game Development", "game-development"),
        ],
    },
    "Self Improvement": {
        "slug": "self-improvement",
        "subcategories": [
            ("All Subcategories", ""),
            ("Mindfulness", "mindfulness"),
            ("Habits", "habits"),
            ("Relationships", "relationships"),
            ("Mental Health", "mental-health"),
            ("Productivity", "productivity"),
        ],
    },
    "Software": {
        "slug": "software",
        "subcategories": [
            ("All Subcategories", ""),
            ("Apps", "apps"),
            ("Plugins", "plugins"),
            ("Scripts", "scripts"),
            ("SaaS", "saas"),
            ("Tools", "tools"),
        ],
    },
    "Worldbuilding": {
        "slug": "worldbuilding",
        "subcategories": [
            ("All Subcategories", ""),
            ("Maps", "maps"),
            ("Lore", "lore"),
            ("Characters", "characters"),
            ("RPG Systems", "rpg-systems"),
            ("Reference Packs", "reference-packs"),
        ],
    },
}

category_options = list(CATEGORY_STRUCTURE.keys())
category_label = st.sidebar.selectbox(
    "Category",
    options=category_options,
    index=0,
)

selected_category = CATEGORY_STRUCTURE[category_label]
subcategory_options = selected_category["subcategories"]
category_slug = selected_category["slug"]

# Reset subcategory when category changes
if "last_category" not in st.session_state:
    st.session_state.last_category = category_label
if "subcategory_choice" not in st.session_state:
    st.session_state.subcategory_choice = subcategory_options[0][0]
if st.session_state.last_category != category_label:
    st.session_state.last_category = category_label
    st.session_state.subcategory_choice = subcategory_options[0][0]

subcategory_label = st.sidebar.selectbox(
    "Subcategory",
    options=[label for label, _ in subcategory_options],
    key="subcategory_choice",
)

subcategory_slug_lookup = {label: slug for label, slug in subcategory_options}
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

    query_params = {"category": category_slug}
    if subcategory_slug:
        query_params["subcategory"] = subcategory_slug

    url = f"https://gumroad.com/discover?{urlencode(query_params)}"

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
