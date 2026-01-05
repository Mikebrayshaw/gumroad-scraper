"""Pipeline storage and orchestration utilities.

This module provides a lightweight persistence layer for local development
and production deployments. It tracks runs, product identities, snapshots,
and diffs so every scrape can be replayed and compared over time.
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from models import ProductSnapshot

Base = declarative_base()
_LOG_CONTEXT = {"run_id": "-"}


class Run(Base):
    __tablename__ = "runs"

    id = Column(String, primary_key=True)
    platform = Column(String, nullable=False)
    category = Column(String, nullable=True)
    source = Column(String, nullable=True)
    config = Column(JSON, nullable=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    total_products = Column(Integer, nullable=True)
    summary = Column(JSON, nullable=True)


class Product(Base):
    __tablename__ = "products"
    __table_args__ = (UniqueConstraint("platform", "product_id", name="uq_products_identity"),)

    id = Column(Integer, primary_key=True)
    platform = Column(String, nullable=False)
    product_id = Column(String, nullable=False)
    url = Column(Text, nullable=False)
    title = Column(Text, nullable=False)
    creator_name = Column(Text, nullable=True)
    creator_url = Column(Text, nullable=True)
    category = Column(Text, nullable=True)
    first_seen_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class ProductSnapshotRow(Base):
    __tablename__ = "product_snapshots"
    __table_args__ = (UniqueConstraint("platform", "product_id", "run_id", name="uq_snapshots_run"),)

    id = Column(Integer, primary_key=True)
    platform = Column(String, nullable=False)
    product_id = Column(String, nullable=False)
    run_id = Column(String, nullable=False)
    url = Column(Text, nullable=False)
    title = Column(Text, nullable=False)
    creator_name = Column(Text, nullable=True)
    creator_url = Column(Text, nullable=True)
    category = Column(Text, nullable=True)
    price_amount = Column(Float, nullable=True)
    price_currency = Column(String, nullable=True)
    price_is_pwyw = Column(Boolean, default=False)
    rating_avg = Column(Float, nullable=True)
    rating_count = Column(Integer, nullable=True)
    sales_count = Column(Integer, nullable=True)
    revenue_estimate = Column(Float, nullable=True)
    revenue_confidence = Column(String, nullable=False, default="low")
    tags = Column(JSON, nullable=True)
    scraped_at = Column(DateTime, nullable=False)
    raw_source_hash = Column(String, nullable=False)


class ProductDiff(Base):
    __tablename__ = "product_diffs"
    __table_args__ = (UniqueConstraint("platform", "product_id", "run_id", name="uq_diffs_run"),)

    id = Column(Integer, primary_key=True)
    platform = Column(String, nullable=False)
    product_id = Column(String, nullable=False)
    run_id = Column(String, nullable=False)
    previous_run_id = Column(String, nullable=True)
    price_delta = Column(Float, nullable=True)
    rating_count_delta = Column(Integer, nullable=True)
    sales_count_delta = Column(Integer, nullable=True)
    revenue_delta = Column(Float, nullable=True)
    raw_source_changed = Column(Boolean, default=False)
    computed_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class OpportunityScoreRow(Base):
    __tablename__ = "opportunity_scores"
    __table_args__ = (
        UniqueConstraint("platform", "product_id", "run_id", name="uq_opportunity_scores_run"),
    )

    id = Column(Integer, primary_key=True)
    run_id = Column(String, nullable=False)
    platform = Column(String, nullable=False)
    product_id = Column(String, nullable=False)
    title = Column(Text, nullable=False)
    url = Column(Text, nullable=False)
    category = Column(Text, nullable=True)
    creator_name = Column(Text, nullable=True)
    price_amount = Column(Float, nullable=True)
    price_currency = Column(String, nullable=True)
    rating_avg = Column(Float, nullable=True)
    rating_count = Column(Integer, nullable=True)
    rating_count_delta = Column(Integer, nullable=True)
    sales_count = Column(Integer, nullable=True)
    sales_count_delta = Column(Integer, nullable=True)
    opportunity_score = Column(Float, nullable=False)
    velocity_score = Column(Float, nullable=True)
    novelty_score = Column(Float, nullable=True)
    copyability_score = Column(Float, nullable=True)
    price_to_value_score = Column(Float, nullable=True)
    saturation_penalty = Column(Float, nullable=True)
    confidence = Column(String, nullable=True)
    reason_summary = Column(Text, nullable=True)
    saturation_examples = Column(JSON, nullable=True)


class AlertRow(Base):
    __tablename__ = "alerts"
    id = Column(Integer, primary_key=True)
    run_id = Column(String, nullable=False)
    platform = Column(String, nullable=False)
    product_id = Column(String, nullable=True)
    alert_type = Column(String, nullable=False)
    message = Column(Text, nullable=False)
    metadata = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


def configure_logging(run_id: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger("pipeline")
    _LOG_CONTEXT["run_id"] = run_id or "-"
    if logger.handlers:
        return logger
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s [run:%(run_id)s] %(message)s")

    class ContextFilter(logging.Filter):
        def filter(self, record):
            record.run_id = _LOG_CONTEXT.get("run_id", "-")
            return True

    handler.addFilter(ContextFilter())
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


class PipelineDatabase:
    """Small helper around SQLAlchemy sessions for runs and snapshots."""

    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv("DATABASE_URL", "sqlite:///data/gumroad_pipeline.db")
        if self.database_url.startswith("sqlite"):
            Path("data").mkdir(exist_ok=True)
        self.engine = create_engine(self.database_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    @contextmanager
    def session(self) -> Session:
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def start_run(self, platform: str, category: Optional[str], source: str, config: dict, run_id: Optional[str] = None) -> str:
        run_identifier = run_id or str(uuid4())
        with self.session() as session:
            existing = session.query(Run).filter_by(id=run_identifier).one_or_none()
            if not existing:
                session.add(
                    Run(
                        id=run_identifier,
                        platform=platform,
                        category=category,
                        source=source,
                        config=config,
                        started_at=datetime.utcnow(),
                    )
                )
        return run_identifier

    def complete_run(self, run_id: str, total_products: int, summary: Optional[dict] = None) -> None:
        with self.session() as session:
            session.query(Run).filter_by(id=run_id).update(
                {
                    Run.completed_at: datetime.utcnow(),
                    Run.total_products: total_products,
                    Run.summary: summary or {},
                }
            )

    def upsert_products(self, snapshots: Iterable[ProductSnapshot], run_id: str) -> None:
        with self.session() as session:
            for snapshot in snapshots:
                product = session.query(Product).filter_by(
                    platform=snapshot.platform, product_id=snapshot.product_id
                ).one_or_none()
                now = datetime.utcnow()
                if not product:
                    session.add(
                        Product(
                            platform=snapshot.platform,
                            product_id=snapshot.product_id,
                            url=snapshot.url,
                            title=snapshot.title,
                            creator_name=snapshot.creator_name,
                            creator_url=snapshot.creator_url,
                            category=snapshot.category,
                            first_seen_at=now,
                            last_seen_at=now,
                        )
                    )
                else:
                    product.url = snapshot.url
                    product.title = snapshot.title
                    product.creator_name = snapshot.creator_name
                    product.creator_url = snapshot.creator_url
                    product.category = snapshot.category
                    product.last_seen_at = now

                session.add(
                    ProductSnapshotRow(
                        platform=snapshot.platform,
                        product_id=snapshot.product_id,
                        run_id=run_id,
                        url=snapshot.url,
                        title=snapshot.title,
                        creator_name=snapshot.creator_name,
                        creator_url=snapshot.creator_url,
                        category=snapshot.category,
                        price_amount=snapshot.price_amount,
                        price_currency=snapshot.price_currency,
                        price_is_pwyw=snapshot.price_is_pwyw,
                        rating_avg=snapshot.rating_avg,
                        rating_count=snapshot.rating_count,
                        sales_count=snapshot.sales_count,
                        revenue_estimate=snapshot.revenue_estimate,
                        revenue_confidence=snapshot.revenue_confidence,
                        tags=snapshot.tags,
                        scraped_at=snapshot.scraped_at,
                        raw_source_hash=snapshot.raw_source_hash,
                    )
                )

    def _previous_snapshot(self, session: Session, snapshot: ProductSnapshotRow) -> Optional[ProductSnapshotRow]:
        return (
            session.query(ProductSnapshotRow)
            .filter(
                ProductSnapshotRow.platform == snapshot.platform,
                ProductSnapshotRow.product_id == snapshot.product_id,
                ProductSnapshotRow.run_id != snapshot.run_id,
                ProductSnapshotRow.scraped_at <= snapshot.scraped_at,
            )
            .order_by(ProductSnapshotRow.scraped_at.desc())
            .first()
        )

    def compute_diffs(self, run_id: str) -> List[ProductDiff]:
        diffs: List[ProductDiff] = []
        with self.session() as session:
            snapshots = session.query(ProductSnapshotRow).filter_by(run_id=run_id).all()
            for snap in snapshots:
                previous = self._previous_snapshot(session, snap)
                price_delta = None
                rating_delta = None
                sales_delta = None
                revenue_delta = None
                raw_changed = False

                if previous:
                    price_delta = _delta(previous.price_amount, snap.price_amount)
                    rating_delta = _delta(previous.rating_count, snap.rating_count)
                    sales_delta = _delta(previous.sales_count, snap.sales_count)
                    revenue_delta = _delta(previous.revenue_estimate, snap.revenue_estimate)
                    raw_changed = previous.raw_source_hash != snap.raw_source_hash

                diff_row = ProductDiff(
                    platform=snap.platform,
                    product_id=snap.product_id,
                    run_id=run_id,
                    previous_run_id=previous.run_id if previous else None,
                    price_delta=price_delta,
                    rating_count_delta=rating_delta,
                    sales_count_delta=sales_delta,
                    revenue_delta=revenue_delta,
                    raw_source_changed=raw_changed,
                    computed_at=datetime.utcnow(),
                )
                session.add(diff_row)
                diffs.append(diff_row)
        return diffs

    def get_run(self, run_id: str) -> Optional[Run]:
        with self.session() as session:
            return session.query(Run).filter_by(id=run_id).one_or_none()

    def previous_run(self, run_id: str) -> Optional[Run]:
        current = self.get_run(run_id)
        if not current or not current.started_at:
            return None
        with self.session() as session:
            return (
                session.query(Run)
                .filter(Run.started_at < current.started_at)
                .order_by(Run.started_at.desc())
                .first()
            )

    def get_snapshots(self, run_id: str) -> List[ProductSnapshotRow]:
        with self.session() as session:
            return session.query(ProductSnapshotRow).filter_by(run_id=run_id).all()

    def get_diffs(self, run_id: str) -> List[ProductDiff]:
        with self.session() as session:
            return session.query(ProductDiff).filter_by(run_id=run_id).all()

    def upsert_opportunity_scores(self, rows: Iterable[dict]) -> None:
        with self.session() as session:
            for row in rows:
                existing = (
                    session.query(OpportunityScoreRow)
                    .filter_by(platform=row["platform"], product_id=row["product_id"], run_id=row["run_id"])
                    .one_or_none()
                )
                if existing:
                    for key, value in row.items():
                        setattr(existing, key, value)
                else:
                    session.add(OpportunityScoreRow(**row))

    def insert_alerts(self, rows: Iterable[dict]) -> None:
        with self.session() as session:
            for row in rows:
                session.add(AlertRow(**row))

    def recent_titles_by_category(self, category: Optional[str], exclude_run_id: Optional[str], limit_runs: int) -> list[str]:
        with self.session() as session:
            run_query = session.query(Run.id).order_by(Run.started_at.desc())
            if limit_runs:
                run_query = run_query.limit(limit_runs)
            run_ids = [r[0] for r in run_query.all()]

            query = session.query(ProductSnapshotRow.title).filter(ProductSnapshotRow.run_id.in_(run_ids))
            if category:
                query = query.filter_by(category=category)
            if exclude_run_id:
                query = query.filter(ProductSnapshotRow.run_id != exclude_run_id)
            return [row[0] for row in query.all()]

    def export_run(self, run_id: str) -> dict:
        with self.session() as session:
            run = session.query(Run).filter_by(id=run_id).first()
            if not run:
                raise ValueError(f"Run {run_id} not found")
            snapshots = session.query(ProductSnapshotRow).filter_by(run_id=run_id).all()
            diffs = session.query(ProductDiff).filter_by(run_id=run_id).all()
            return {
                "run": {
                    "id": run_id,
                    "platform": run.platform,
                    "category": run.category,
                    "started_at": run.started_at.isoformat() if run.started_at else None,
                    "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                    "total_products": run.total_products,
                    "summary": run.summary,
                },
                "snapshots": [self._snapshot_to_dict(s) for s in snapshots],
                "diffs": [self._diff_to_dict(d) for d in diffs],
            }

    def _snapshot_to_dict(self, snap: ProductSnapshotRow) -> dict:
        return {
            "platform": snap.platform,
            "product_id": snap.product_id,
            "run_id": snap.run_id,
            "url": snap.url,
            "title": snap.title,
            "creator_name": snap.creator_name,
            "creator_url": snap.creator_url,
            "category": snap.category,
            "price_amount": snap.price_amount,
            "price_currency": snap.price_currency,
            "price_is_pwyw": snap.price_is_pwyw,
            "rating_avg": snap.rating_avg,
            "rating_count": snap.rating_count,
            "sales_count": snap.sales_count,
            "revenue_estimate": snap.revenue_estimate,
            "revenue_confidence": snap.revenue_confidence,
            "tags": snap.tags,
            "scraped_at": snap.scraped_at.isoformat() if snap.scraped_at else None,
            "raw_source_hash": snap.raw_source_hash,
        }

    def _diff_to_dict(self, diff: ProductDiff) -> dict:
        return {
            "platform": diff.platform,
            "product_id": diff.product_id,
            "run_id": diff.run_id,
            "previous_run_id": diff.previous_run_id,
            "price_delta": diff.price_delta,
            "rating_count_delta": diff.rating_count_delta,
            "sales_count_delta": diff.sales_count_delta,
            "revenue_delta": diff.revenue_delta,
            "raw_source_changed": diff.raw_source_changed,
            "computed_at": diff.computed_at.isoformat() if diff.computed_at else None,
        }


def _delta(previous: Optional[float], current: Optional[float]) -> Optional[float]:
    if previous is None or current is None:
        return None
    return round(current - previous, 2)


def load_snapshots_from_json(path: str) -> tuple[str, List[ProductSnapshot], dict]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    run_id = payload.get("run_id") or str(uuid4())
    meta = payload.get("run_meta", {})
    snapshots = []
    for item in payload.get("products", []):
        snapshot = ProductSnapshot(
            platform=item["platform"],
            product_id=item["product_id"],
            url=item["url"],
            title=item["title"],
            creator_name=item.get("creator_name", ""),
            creator_url=item.get("creator_url"),
            category=item.get("category"),
            price_amount=item.get("price_amount"),
            price_currency=item.get("price_currency"),
            price_is_pwyw=item.get("price_is_pwyw", False),
            rating_avg=item.get("rating_avg"),
            rating_count=item.get("rating_count"),
            sales_count=item.get("sales_count"),
            revenue_estimate=item.get("revenue_estimate"),
            revenue_confidence=item.get("revenue_confidence", "low"),
            tags=item.get("tags", []),
            scraped_at=datetime.fromisoformat(item["scraped_at"]),
            raw_source_hash=item.get("raw_source_hash", ""),
        )
        snapshots.append(snapshot)

    return run_id, snapshots, meta


def snapshots_to_json(run_id: str, meta: dict, snapshots: Iterable[ProductSnapshot]) -> dict:
    return {
        "run_id": run_id,
        "run_meta": meta,
        "products": [s.to_dict() for s in snapshots],
    }

