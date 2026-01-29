"""Centralized Gumroad category metadata."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from urllib.parse import urlencode


@dataclass(frozen=True)
class Subcategory:
    label: str
    slug: str
    path_suffix: Optional[str] = None
    query_params: Optional[Dict[str, str]] = None
    absolute_url: Optional[str] = None
    skip_scraping: bool = False


@dataclass(frozen=True)
class Category:
    label: str
    slug: str
    subcategories: Tuple[Subcategory, ...]


CATEGORY_TREE: Tuple[Category, ...] = (
    Category(
        label="3D",
        slug="3d",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Assets", "assets", skip_scraping=True),
            Subcategory("Characters", "characters", skip_scraping=True),
            Subcategory("Environments", "environments", skip_scraping=True),
            Subcategory("Materials & Textures", "materials-and-textures", skip_scraping=True),
            Subcategory("Models", "models", skip_scraping=True),
            Subcategory("Props", "props", skip_scraping=True),
        ),
    ),
    Category(
        label="Audio",
        slug="audio",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Beats", "beats", skip_scraping=True),
            Subcategory("Loops & Samples", "loops-and-samples", skip_scraping=True),
            Subcategory("Mixing & Mastering", "mixing-and-mastering", skip_scraping=True),
            Subcategory("Sound Effects", "sound-effects", skip_scraping=True),
            Subcategory("Vocal Presets", "vocal-presets", skip_scraping=True),
        ),
    ),
    Category(
        label="Business & Money",
        slug="business-and-money",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Entrepreneurship", "entrepreneurship"),
            Subcategory("Finance & Investing", "finance-and-investing"),
            Subcategory("Freelancing", "freelancing"),
            Subcategory("Marketing", "marketing"),
            Subcategory("Sales", "sales"),
            Subcategory("Startups", "startups"),
        ),
    ),
    Category(
        label="Comics & Graphic Novels",
        slug="comics-and-graphic-novels",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Anthologies", "anthologies"),
            Subcategory("Graphic Novels", "graphic-novels"),
            Subcategory("Manga", "manga"),
            Subcategory("Webcomics", "webcomics"),
            Subcategory("Zines", "zines"),
        ),
    ),
    Category(
        label="Design",
        slug="design",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Icons", "icons"),
            Subcategory("Templates", "templates"),
            Subcategory("Fonts", "fonts"),
            Subcategory("UI Kits", "ui-kits"),
            Subcategory("Illustrations", "illustrations"),
            Subcategory("Mockups", "mockups"),
        ),
    ),
    Category(
        label="Drawing & Painting",
        slug="drawing-and-painting",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Brushes", "brushes"),
            Subcategory("Procreate Brushes", "procreate-brushes"),
            Subcategory("Photoshop Brushes", "photoshop-brushes"),
            Subcategory("Tutorials", "tutorials"),
            Subcategory("Coloring Pages", "coloring-pages"),
        ),
    ),
    Category(
        label="Education",
        slug="education",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Courses", "courses"),
            Subcategory("Study Guides", "study-guides"),
            Subcategory("Homeschool", "homeschool"),
            Subcategory("Lesson Plans", "lesson-plans"),
            Subcategory("Worksheets", "worksheets"),
        ),
    ),
    Category(
        label="Fiction Books",
        slug="fiction-books",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Fantasy", "fantasy"),
            Subcategory("Romance", "romance"),
            Subcategory("Science Fiction", "science-fiction"),
            Subcategory("Mystery & Thriller", "mystery-and-thriller"),
            Subcategory("Young Adult", "young-adult"),
        ),
    ),
    Category(
        label="Films",
        slug="films",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Filmmaking", "filmmaking"),
            Subcategory("Editing", "editing"),
            Subcategory("VFX", "vfx"),
            Subcategory("Color Grading", "color-grading"),
            Subcategory("Screenwriting", "screenwriting"),
        ),
    ),
    Category(
        label="Fitness & Health",
        slug="fitness-and-health",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Workout Programs", "workout-programs"),
            Subcategory("Nutrition", "nutrition"),
            Subcategory("Yoga", "yoga"),
            Subcategory("Meditation", "meditation"),
            Subcategory("Meal Plans", "meal-plans"),
        ),
    ),
    Category(
        label="Games",
        slug="gaming",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Game Assets", "game-assets"),
            Subcategory("Game Templates", "game-templates"),
            Subcategory("Tabletop RPGs", "tabletop-rpgs"),
            Subcategory("Rulebooks", "rulebooks"),
            Subcategory("Tools & Plugins", "tools-and-plugins"),
        ),
    ),
    Category(
        label="Music & Sound Design",
        slug="music-and-sound-design",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Sample Packs", "sample-packs"),
            Subcategory("Presets", "presets"),
            Subcategory("Plugins", "plugins"),
            Subcategory("Loops", "loops"),
            Subcategory("Beatmaking", "beatmaking"),
        ),
    ),
    Category(
        label="Nonfiction Books",
        slug="nonfiction-books",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Biography & Memoir", "biography-and-memoir"),
            Subcategory("Business", "business"),
            Subcategory("Self Help", "self-help"),
            Subcategory("History & Politics", "history-and-politics"),
            Subcategory("Guides & Manuals", "guides-and-manuals"),
        ),
    ),
    Category(
        label="Photography",
        slug="photography",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Presets", "presets"),
            Subcategory("LUTs", "luts"),
            Subcategory("Overlays", "overlays"),
            Subcategory("Tutorials", "tutorials"),
            Subcategory("Stock Photos", "stock-photos"),
        ),
    ),
    Category(
        label="Podcasts",
        slug="podcasts",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Business", "business"),
            Subcategory("Education", "education"),
            Subcategory("Entertainment", "entertainment"),
            Subcategory("News", "news"),
            Subcategory("Technology", "technology"),
        ),
    ),
    Category(
        label="Productivity",
        slug="productivity",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Notion Templates", "notion-templates"),
            Subcategory("Planners", "planners"),
            Subcategory("Journals", "journals"),
            Subcategory("Trackers", "trackers"),
            Subcategory("Spreadsheets", "spreadsheets"),
        ),
    ),
    Category(
        label="Software Development",
        slug="software-development",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Web Development", "web-development"),
            Subcategory("AI & Machine Learning", "ai-and-machine-learning"),
            Subcategory("Data Science", "data-science"),
            Subcategory("Automation", "automation"),
            Subcategory("Game Development", "game-development"),
        ),
    ),
    Category(
        label="Self Improvement",
        slug="self-improvement",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Mindfulness", "mindfulness"),
            Subcategory("Habits", "habits"),
            Subcategory("Relationships", "relationships"),
            Subcategory("Mental Health", "mental-health"),
            Subcategory("Productivity", "productivity"),
        ),
    ),
    Category(
        label="Software",
        slug="software",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Apps", "apps"),
            Subcategory("Plugins", "plugins"),
            Subcategory("Scripts", "scripts"),
            Subcategory("SaaS", "saas"),
            Subcategory("Tools", "tools"),
        ),
    ),
    Category(
        label="Worldbuilding",
        slug="worldbuilding",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Maps", "maps"),
            Subcategory("Lore", "lore"),
            Subcategory("Characters", "characters"),
            Subcategory("RPG Systems", "rpg-systems"),
            Subcategory("Reference Packs", "reference-packs"),
        ),
    ),
    Category(
        label="Writing & Publishing",
        slug="writing-and-publishing",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Fiction", "fiction"),
            Subcategory("Nonfiction", "nonfiction"),
            Subcategory("Poetry", "poetry"),
            Subcategory("Scripts", "scripts"),
            Subcategory("Short Stories", "short-stories"),
        ),
    ),
    Category(
        label="Other",
        slug="other",
        subcategories=(Subcategory("All Subcategories", ""),),
    ),
)

CATEGORY_BY_LABEL: Dict[str, Category] = {cat.label: cat for cat in CATEGORY_TREE}
CATEGORY_BY_SLUG: Dict[str, Category] = {cat.slug: cat for cat in CATEGORY_TREE}

CATEGORY_SLUG_ALIASES: Dict[str, str] = {
    "programming-and-tech": "software-development",
    "software": "software-development",
}

# Known invalid URL patterns that return 404 on Gumroad
INVALID_PATH_PATTERNS = [
    "/3d/assets",
    "/3d/characters",
    "/3d/environments",
    "/3d/materials-and-textures",
    "/3d/models",
    "/3d/props",
    "/audio/beats",
    "/audio/loops-and-samples",
    "/audio/mixing-and-mastering",
    "/audio/sound-effects",
    "/audio/vocal-presets",
]


def validate_url(url: str) -> bool:
    """Check if a URL is valid (not in known invalid patterns)."""
    for pattern in INVALID_PATH_PATTERNS:
        if pattern in url:
            return False
    return True


def should_skip_subcategory(subcategory: Subcategory) -> bool:
    """Check if a subcategory should be skipped during scraping."""
    return subcategory.skip_scraping


def build_discover_url(
    category_slug: str,
    subcategory_slug: str | None = None,
    subcategory: Subcategory | None = None,
) -> str:
    """Construct a Gumroad discover URL for the given category and subcategory.
    
    Args:
        category_slug: The category slug
        subcategory_slug: Optional subcategory slug (for backwards compatibility)
        subcategory: Optional Subcategory object with routing information
        
    Returns:
        A valid Gumroad discover URL
        
    Routing priority:
        1. subcategory.absolute_url - use exact URL if provided
        2. subcategory.query_params - construct URL with query parameters
        3. subcategory.path_suffix - use as path segment
        4. subcategory_slug or subcategory.slug - traditional path-based routing
        5. category_slug only - fallback to category-only URL
    """
    if not category_slug:
        return "https://gumroad.com/discover"
    
    resolved_slug = CATEGORY_SLUG_ALIASES.get(category_slug, category_slug)
    base_url = f"https://gumroad.com/{resolved_slug}"
    
    # If subcategory object is provided, use its routing information
    if subcategory:
        # Priority 1: absolute_url overrides everything
        if subcategory.absolute_url:
            return subcategory.absolute_url
        
        # Priority 2: query_params
        if subcategory.query_params:
            query_string = urlencode(subcategory.query_params)
            return f"{base_url}?{query_string}"
        
        # Priority 3: path_suffix
        if subcategory.path_suffix:
            url = f"{base_url}/{subcategory.path_suffix}"
            # Validate before returning
            if validate_url(url):
                return url
            else:
                # Fall back to category-only URL if invalid
                return base_url
        
        # Priority 4: use subcategory.slug
        if subcategory.slug:
            url = f"{base_url}/{subcategory.slug}"
            # Validate before returning
            if validate_url(url):
                return url
            else:
                # Fall back to category-only URL if invalid
                return base_url
    
    # Legacy path: subcategory_slug parameter
    if subcategory_slug:
        url = f"{base_url}/{subcategory_slug}"
        # Validate before returning
        if validate_url(url):
            return url
        else:
            # Fall back to category-only URL if invalid
            return base_url
    
    return base_url


def category_url_map() -> Dict[str, str]:
    """Return mapping suitable for CLI category choices."""
    urls = {cat.slug: build_discover_url(cat.slug) for cat in CATEGORY_TREE}
    urls["discover"] = "https://gumroad.com/discover"
    return urls


def build_search_url(query: str) -> str:
    """Construct a Gumroad search URL for the given query."""
    params = {"query": query}
    return f"https://gumroad.com/discover?{urlencode(params)}"


def get_all_category_slugs() -> list[str]:
    """Return a list of all category slugs (excluding 'discover')."""
    return [cat.slug for cat in CATEGORY_TREE]


# Sanity checks: Validate URLs at module import time
def _run_sanity_checks():
    """Run validation checks on CATEGORY_TREE at module import."""
    for category in CATEGORY_TREE:
        for subcategory in category.subcategories:
            # Skip validation for subcategories marked as skip_scraping
            if subcategory.skip_scraping:
                continue
            
            # Check that URLs constructed are valid
            if subcategory.slug:
                url = build_discover_url(category.slug, subcategory_slug=subcategory.slug)
                if not validate_url(url):
                    print(f"[WARN] Invalid URL pattern detected but not marked for skipping: {url}")


_run_sanity_checks()
