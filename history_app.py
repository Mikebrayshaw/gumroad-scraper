"""Streamlit view for browsing historical Gumroad scrape data stored in Supabase."""

import pandas as pd
import pandas as pd
import streamlit as st

from analysis_ui import render_analysis_block
from supabase_utils import get_supabase_client

st.set_page_config(page_title="Gumroad Scraper History", page_icon="🗂️", layout="wide")
st.title("🗂️ Gumroad Scraper History")
st.markdown("Browse historical scrape runs persisted in Supabase.")


@st.cache_resource
def supabase_client():
    return get_supabase_client()


@st.cache_data(ttl=60)
def load_runs(limit: int = 200):
    client = supabase_client()
    response = (
        client.table("runs")
        .select("id, category, subcategory, started_at, completed_at, total_products, total_new, total_updated, status")
        .order("started_at", desc=True)
        .limit(limit)
        .execute()
    )
    return pd.DataFrame(response.data)


@st.cache_data(ttl=60)
def load_products(run_id: str, limit: int = 500):
    client = supabase_client()
    response = (
        client.table("product_snapshots")
        .select(
            "title, creator_name, category, subcategory, price_amount, price_currency, rating_avg, rating_count, sales_count, revenue_estimate, url, opportunity_score, scraped_at"
        )
        .eq("run_id", run_id)
        .order("opportunity_score", desc=True)
        .limit(limit)
        .execute()
    )
    return pd.DataFrame(response.data)


try:
    runs_df = load_runs()
except Exception as exc:  # pragma: no cover - UI feedback
    st.error(f"Unable to load Supabase data: {exc}")
    st.stop()

if runs_df.empty:
    st.info("No scrape runs found in Supabase yet.")
    st.stop()

with st.sidebar:
    st.subheader("Filters")
    selected_run = st.selectbox(
        "Scrape run",
        runs_df["id"],
        format_func=lambda rid: f"{rid[:8]}... | {runs_df.set_index('id').loc[rid, 'started_at']}"
    )
    max_products = st.slider("Rows to load", min_value=50, max_value=2000, value=500, step=50)

run_meta = runs_df[runs_df["id"] == selected_run].iloc[0]
st.markdown(
    f"**Category**: {run_meta.get('category', 'n/a')} &nbsp;&nbsp; "
    f"**Subcategory**: {run_meta.get('subcategory', 'n/a')} &nbsp;&nbsp; "
    f"**Started**: {run_meta.get('started_at')} &nbsp;&nbsp; "
    f"**Completed**: {run_meta.get('completed_at', 'n/a')}"
)

products_df = load_products(selected_run, limit=max_products)
products_df = products_df.rename(
    columns={
        "title": "product_name",
        "url": "product_url",
        "price_amount": "price_usd",
        "rating_avg": "average_rating",
        "rating_count": "total_reviews",
        "revenue_estimate": "estimated_revenue",
    }
)

if products_df.empty:
    st.warning("No products recorded for this run yet.")
    st.stop()

st.dataframe(products_df, use_container_width=True, hide_index=True)

csv_data = products_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download CSV",
    data=csv_data,
    file_name=f"gumroad_scrape_{selected_run}.csv",
    mime="text/csv",
    type="primary",
)

st.subheader("Analyze run with CrewAI")
render_analysis_block(
    products_df.to_dict(orient="records"),
    dataset_id=selected_run,
    source_label=f"Supabase run {selected_run}",
)
