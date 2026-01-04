"""
Alerts, Saved Searches, and Watchlists Module

Provides persistence (SQLite), delta detection, and notification hooks
for tracking Gumroad product changes.
"""

import json
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# Default database path
DEFAULT_DB_PATH = Path(__file__).parent / "gumroad_alerts.db"


@dataclass
class SavedSearch:
    """A saved search with query and filters."""
    id: Optional[int]
    name: str
    category: str
    subcategory: str
    min_price: Optional[float]
    max_price: Optional[float]
    min_rating: Optional[float]
    min_reviews: Optional[int]
    created_at: str
    last_checked_at: Optional[str]


@dataclass
class WatchlistItem:
    """A product URL or category to watch."""
    id: Optional[int]
    item_type: str  # 'product' or 'category'
    url: str
    name: str
    created_at: str


@dataclass
class ProductSnapshot:
    """A snapshot of a product at a point in time."""
    product_url: str
    product_name: str
    price_usd: float
    average_rating: Optional[float]
    total_reviews: int
    sales_count: Optional[int]
    estimated_revenue: Optional[float]
    opportunity_score: Optional[float]
    snapshot_at: str


@dataclass
class ProductChange:
    """Represents a change detected in a product."""
    product_url: str
    product_name: str
    change_type: str  # 'new', 'price_change', 'rating_change', 'sales_change'
    old_value: Optional[str]
    new_value: Optional[str]
    detected_at: str


# =============================================================================
# Database Operations
# =============================================================================

def init_database(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Initialize the SQLite database with required tables."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    cursor = conn.cursor()

    # Saved searches table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS saved_searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT DEFAULT '',
            min_price REAL,
            max_price REAL,
            min_rating REAL,
            min_reviews INTEGER,
            created_at TEXT NOT NULL,
            last_checked_at TEXT
        )
    """)

    # Watchlist table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_type TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # Product snapshots table (for delta detection)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_id INTEGER,
            product_url TEXT NOT NULL,
            product_name TEXT NOT NULL,
            price_usd REAL,
            average_rating REAL,
            total_reviews INTEGER,
            sales_count INTEGER,
            estimated_revenue REAL,
            opportunity_score REAL,
            snapshot_at TEXT NOT NULL,
            FOREIGN KEY (search_id) REFERENCES saved_searches(id)
        )
    """)

    # Create indexes for faster lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_snapshots_search_id
        ON product_snapshots(search_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_snapshots_url
        ON product_snapshots(product_url)
    """)

    conn.commit()
    return conn


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Get a database connection, initializing if needed."""
    if not db_path.exists():
        return init_database(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# Saved Searches CRUD
# =============================================================================

def create_saved_search(
    name: str,
    category: str,
    subcategory: str = "",
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_rating: Optional[float] = None,
    min_reviews: Optional[int] = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> SavedSearch:
    """Create a new saved search."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    now = datetime.now(timezone.utc).isoformat()

    cursor.execute("""
        INSERT INTO saved_searches
        (name, category, subcategory, min_price, max_price, min_rating, min_reviews, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, category, subcategory, min_price, max_price, min_rating, min_reviews, now))

    search_id = cursor.lastrowid
    conn.commit()
    conn.close()

    return SavedSearch(
        id=search_id,
        name=name,
        category=category,
        subcategory=subcategory,
        min_price=min_price,
        max_price=max_price,
        min_rating=min_rating,
        min_reviews=min_reviews,
        created_at=now,
        last_checked_at=None,
    )


def get_saved_searches(db_path: Path = DEFAULT_DB_PATH) -> list[SavedSearch]:
    """Get all saved searches."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM saved_searches ORDER BY created_at DESC")
    rows = cursor.fetchall()
    conn.close()

    return [
        SavedSearch(
            id=row['id'],
            name=row['name'],
            category=row['category'],
            subcategory=row['subcategory'],
            min_price=row['min_price'],
            max_price=row['max_price'],
            min_rating=row['min_rating'],
            min_reviews=row['min_reviews'],
            created_at=row['created_at'],
            last_checked_at=row['last_checked_at'],
        )
        for row in rows
    ]


def get_saved_search(search_id: int, db_path: Path = DEFAULT_DB_PATH) -> Optional[SavedSearch]:
    """Get a saved search by ID."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM saved_searches WHERE id = ?", (search_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    return SavedSearch(
        id=row['id'],
        name=row['name'],
        category=row['category'],
        subcategory=row['subcategory'],
        min_price=row['min_price'],
        max_price=row['max_price'],
        min_rating=row['min_rating'],
        min_reviews=row['min_reviews'],
        created_at=row['created_at'],
        last_checked_at=row['last_checked_at'],
    )


def delete_saved_search(search_id: int, db_path: Path = DEFAULT_DB_PATH) -> bool:
    """Delete a saved search and its snapshots."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM product_snapshots WHERE search_id = ?", (search_id,))
    cursor.execute("DELETE FROM saved_searches WHERE id = ?", (search_id,))

    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()

    return deleted


def update_search_last_checked(
    search_id: int,
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    """Update the last_checked_at timestamp for a search."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    now = datetime.now(timezone.utc).isoformat()
    cursor.execute(
        "UPDATE saved_searches SET last_checked_at = ? WHERE id = ?",
        (now, search_id)
    )

    conn.commit()
    conn.close()


# =============================================================================
# Watchlist CRUD
# =============================================================================

def add_to_watchlist(
    item_type: str,
    url: str,
    name: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> Optional[WatchlistItem]:
    """Add an item to the watchlist. Returns None if already exists."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    now = datetime.now(timezone.utc).isoformat()

    try:
        cursor.execute("""
            INSERT INTO watchlist (item_type, url, name, created_at)
            VALUES (?, ?, ?, ?)
        """, (item_type, url, name, now))

        item_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return WatchlistItem(
            id=item_id,
            item_type=item_type,
            url=url,
            name=name,
            created_at=now,
        )
    except sqlite3.IntegrityError:
        conn.close()
        return None


def get_watchlist(db_path: Path = DEFAULT_DB_PATH) -> list[WatchlistItem]:
    """Get all watchlist items."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM watchlist ORDER BY created_at DESC")
    rows = cursor.fetchall()
    conn.close()

    return [
        WatchlistItem(
            id=row['id'],
            item_type=row['item_type'],
            url=row['url'],
            name=row['name'],
            created_at=row['created_at'],
        )
        for row in rows
    ]


def remove_from_watchlist(item_id: int, db_path: Path = DEFAULT_DB_PATH) -> bool:
    """Remove an item from the watchlist."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM watchlist WHERE id = ?", (item_id,))

    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()

    return deleted


# =============================================================================
# Snapshot Operations
# =============================================================================

def save_snapshot(
    search_id: int,
    products: list[dict],
    db_path: Path = DEFAULT_DB_PATH,
) -> int:
    """
    Save a snapshot of products for a search.
    Returns the number of products saved.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    now = datetime.now(timezone.utc).isoformat()

    for p in products:
        cursor.execute("""
            INSERT INTO product_snapshots
            (search_id, product_url, product_name, price_usd, average_rating,
             total_reviews, sales_count, estimated_revenue, opportunity_score, snapshot_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            search_id,
            p.get('product_url', ''),
            p.get('product_name', ''),
            p.get('price_usd', 0),
            p.get('average_rating'),
            p.get('total_reviews', 0),
            p.get('sales_count'),
            p.get('estimated_revenue'),
            p.get('opportunity_score'),
            now,
        ))

    conn.commit()
    conn.close()

    # Update the last checked timestamp
    update_search_last_checked(search_id, db_path)

    return len(products)


def get_latest_snapshot(
    search_id: int,
    db_path: Path = DEFAULT_DB_PATH,
) -> list[ProductSnapshot]:
    """Get the most recent snapshot for a search."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Get the latest snapshot timestamp for this search
    cursor.execute("""
        SELECT MAX(snapshot_at) as latest FROM product_snapshots
        WHERE search_id = ?
    """, (search_id,))
    result = cursor.fetchone()

    if not result or not result['latest']:
        conn.close()
        return []

    latest_time = result['latest']

    # Get all products from that snapshot
    cursor.execute("""
        SELECT * FROM product_snapshots
        WHERE search_id = ? AND snapshot_at = ?
    """, (search_id, latest_time))

    rows = cursor.fetchall()
    conn.close()

    return [
        ProductSnapshot(
            product_url=row['product_url'],
            product_name=row['product_name'],
            price_usd=row['price_usd'],
            average_rating=row['average_rating'],
            total_reviews=row['total_reviews'],
            sales_count=row['sales_count'],
            estimated_revenue=row['estimated_revenue'],
            opportunity_score=row['opportunity_score'],
            snapshot_at=row['snapshot_at'],
        )
        for row in rows
    ]


def get_previous_snapshot(
    search_id: int,
    db_path: Path = DEFAULT_DB_PATH,
) -> list[ProductSnapshot]:
    """Get the second-most-recent snapshot for a search (for comparison)."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Get distinct snapshot times, ordered descending
    cursor.execute("""
        SELECT DISTINCT snapshot_at FROM product_snapshots
        WHERE search_id = ?
        ORDER BY snapshot_at DESC
        LIMIT 2
    """, (search_id,))
    results = cursor.fetchall()

    if len(results) < 2:
        conn.close()
        return []

    previous_time = results[1]['snapshot_at']

    # Get all products from that snapshot
    cursor.execute("""
        SELECT * FROM product_snapshots
        WHERE search_id = ? AND snapshot_at = ?
    """, (search_id, previous_time))

    rows = cursor.fetchall()
    conn.close()

    return [
        ProductSnapshot(
            product_url=row['product_url'],
            product_name=row['product_name'],
            price_usd=row['price_usd'],
            average_rating=row['average_rating'],
            total_reviews=row['total_reviews'],
            sales_count=row['sales_count'],
            estimated_revenue=row['estimated_revenue'],
            opportunity_score=row['opportunity_score'],
            snapshot_at=row['snapshot_at'],
        )
        for row in rows
    ]


# =============================================================================
# Delta Detection (Pure Functions)
# =============================================================================

def detect_new_products(
    current_products: list[dict],
    previous_snapshot: list[ProductSnapshot],
) -> list[ProductChange]:
    """
    Detect new products that weren't in the previous snapshot.
    Pure function - no side effects.
    """
    previous_urls = {p.product_url for p in previous_snapshot}
    now = datetime.now(timezone.utc).isoformat()

    changes = []
    for product in current_products:
        url = product.get('product_url', '')
        if url and url not in previous_urls:
            changes.append(ProductChange(
                product_url=url,
                product_name=product.get('product_name', 'Unknown'),
                change_type='new',
                old_value=None,
                new_value=f"${product.get('price_usd', 0):.2f}",
                detected_at=now,
            ))

    return changes


def detect_price_changes(
    current_products: list[dict],
    previous_snapshot: list[ProductSnapshot],
    threshold_percent: float = 5.0,
) -> list[ProductChange]:
    """
    Detect significant price changes.
    Pure function - no side effects.
    """
    previous_by_url = {p.product_url: p for p in previous_snapshot}
    now = datetime.now(timezone.utc).isoformat()

    changes = []
    for product in current_products:
        url = product.get('product_url', '')
        if url not in previous_by_url:
            continue

        prev = previous_by_url[url]
        old_price = prev.price_usd or 0
        new_price = product.get('price_usd', 0) or 0

        if old_price == 0 and new_price == 0:
            continue

        if old_price > 0:
            change_percent = abs(new_price - old_price) / old_price * 100
        else:
            change_percent = 100  # From free to paid

        if change_percent >= threshold_percent:
            changes.append(ProductChange(
                product_url=url,
                product_name=product.get('product_name', 'Unknown'),
                change_type='price_change',
                old_value=f"${old_price:.2f}",
                new_value=f"${new_price:.2f}",
                detected_at=now,
            ))

    return changes


def detect_rating_changes(
    current_products: list[dict],
    previous_snapshot: list[ProductSnapshot],
    threshold: float = 0.2,
) -> list[ProductChange]:
    """
    Detect significant rating changes.
    Pure function - no side effects.
    """
    previous_by_url = {p.product_url: p for p in previous_snapshot}
    now = datetime.now(timezone.utc).isoformat()

    changes = []
    for product in current_products:
        url = product.get('product_url', '')
        if url not in previous_by_url:
            continue

        prev = previous_by_url[url]
        old_rating = prev.average_rating
        new_rating = product.get('average_rating')

        if old_rating is None or new_rating is None:
            continue

        if abs(new_rating - old_rating) >= threshold:
            changes.append(ProductChange(
                product_url=url,
                product_name=product.get('product_name', 'Unknown'),
                change_type='rating_change',
                old_value=f"{old_rating:.1f}",
                new_value=f"{new_rating:.1f}",
                detected_at=now,
            ))

    return changes


def detect_sales_changes(
    current_products: list[dict],
    previous_snapshot: list[ProductSnapshot],
    threshold_percent: float = 10.0,
) -> list[ProductChange]:
    """
    Detect significant sales count changes.
    Pure function - no side effects.
    """
    previous_by_url = {p.product_url: p for p in previous_snapshot}
    now = datetime.now(timezone.utc).isoformat()

    changes = []
    for product in current_products:
        url = product.get('product_url', '')
        if url not in previous_by_url:
            continue

        prev = previous_by_url[url]
        old_sales = prev.sales_count
        new_sales = product.get('sales_count')

        if old_sales is None or new_sales is None:
            continue

        if old_sales == 0:
            if new_sales > 0:
                change_percent = 100
            else:
                continue
        else:
            change_percent = (new_sales - old_sales) / old_sales * 100

        if change_percent >= threshold_percent:
            changes.append(ProductChange(
                product_url=url,
                product_name=product.get('product_name', 'Unknown'),
                change_type='sales_change',
                old_value=str(old_sales),
                new_value=str(new_sales),
                detected_at=now,
            ))

    return changes


def detect_all_changes(
    current_products: list[dict],
    previous_snapshot: list[ProductSnapshot],
) -> list[ProductChange]:
    """
    Detect all types of changes between current products and previous snapshot.
    Pure function - no side effects.
    """
    changes = []
    changes.extend(detect_new_products(current_products, previous_snapshot))
    changes.extend(detect_price_changes(current_products, previous_snapshot))
    changes.extend(detect_rating_changes(current_products, previous_snapshot))
    changes.extend(detect_sales_changes(current_products, previous_snapshot))
    return changes


def check_for_updates(
    search_id: int,
    current_products: list[dict],
    db_path: Path = DEFAULT_DB_PATH,
) -> list[ProductChange]:
    """
    Check for updates by comparing current products to the previous snapshot.
    Saves the current products as a new snapshot.
    Returns list of detected changes.
    """
    # Get previous snapshot
    previous = get_previous_snapshot(search_id, db_path)

    # If no previous snapshot, get the latest (which will be the only one)
    if not previous:
        previous = get_latest_snapshot(search_id, db_path)

    # Detect changes
    changes = detect_all_changes(current_products, previous)

    # Save new snapshot
    save_snapshot(search_id, current_products, db_path)

    return changes


# =============================================================================
# Notification Hooks (Placeholder Functions)
# =============================================================================

def notify_email(
    to_address: str,
    subject: str,
    changes: list[ProductChange],
) -> bool:
    """
    Placeholder for email notification.
    In production, this would send an actual email.
    Returns True if notification was "sent" (logged).
    """
    print(f"\n{'='*60}")
    print(f"EMAIL NOTIFICATION (placeholder)")
    print(f"{'='*60}")
    print(f"To: {to_address}")
    print(f"Subject: {subject}")
    print(f"\nChanges Detected: {len(changes)}")
    print("-" * 40)

    for change in changes:
        if change.change_type == 'new':
            print(f"  [NEW] {change.product_name}")
            print(f"        Price: {change.new_value}")
        else:
            print(f"  [{change.change_type.upper()}] {change.product_name}")
            print(f"        {change.old_value} -> {change.new_value}")
        print(f"        URL: {change.product_url}")
        print()

    print(f"{'='*60}\n")
    return True


def notify_slack(
    webhook_url: str,
    channel: str,
    changes: list[ProductChange],
) -> bool:
    """
    Placeholder for Slack notification.
    In production, this would POST to a Slack webhook.
    Returns True if notification was "sent" (logged).
    """
    print(f"\n{'='*60}")
    print(f"SLACK NOTIFICATION (placeholder)")
    print(f"{'='*60}")
    print(f"Webhook: {webhook_url[:30]}...")
    print(f"Channel: {channel}")
    print(f"\nChanges Detected: {len(changes)}")
    print("-" * 40)

    # Format as Slack-like message
    message_lines = []
    for change in changes:
        emoji = {
            'new': ':new:',
            'price_change': ':moneybag:',
            'rating_change': ':star:',
            'sales_change': ':chart_with_upwards_trend:',
        }.get(change.change_type, ':bell:')

        if change.change_type == 'new':
            message_lines.append(
                f"{emoji} *New Product*: {change.product_name} ({change.new_value})"
            )
        else:
            message_lines.append(
                f"{emoji} *{change.change_type.replace('_', ' ').title()}*: "
                f"{change.product_name} ({change.old_value} -> {change.new_value})"
            )

    for line in message_lines:
        print(f"  {line}")

    print(f"\n{'='*60}\n")
    return True


def send_digest(
    changes: list[ProductChange],
    email_to: Optional[str] = None,
    slack_webhook: Optional[str] = None,
    slack_channel: Optional[str] = None,
) -> dict:
    """
    Send a digest of changes via configured notification channels.
    Returns a dict with status for each channel.
    """
    results = {}

    if not changes:
        print("[Digest] No changes to report.")
        return {'status': 'no_changes'}

    print(f"[Digest] Sending notifications for {len(changes)} changes...")

    if email_to:
        subject = f"Gumroad Alert: {len(changes)} product changes detected"
        results['email'] = notify_email(email_to, subject, changes)

    if slack_webhook:
        channel = slack_channel or "#gumroad-alerts"
        results['slack'] = notify_slack(slack_webhook, channel, changes)

    # If no channels configured, just print to console
    if not email_to and not slack_webhook:
        print("\n[Digest] Console output (no notification channels configured):")
        for change in changes:
            print(f"  - [{change.change_type}] {change.product_name}")
            if change.old_value and change.new_value:
                print(f"    {change.old_value} -> {change.new_value}")
            elif change.new_value:
                print(f"    Value: {change.new_value}")
        results['console'] = True

    return results
