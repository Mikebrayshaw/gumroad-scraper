"""Centralized Gumroad category metadata."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple
from urllib.parse import urlencode


@dataclass(frozen=True)
class Subcategory:
    label: str
    slug: str


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
            Subcategory("Assets", "assets"),
            Subcategory("Characters", "characters"),
            Subcategory("Environments", "environments"),
            Subcategory("Materials & Textures", "materials-and-textures"),
            Subcategory("Models", "models"),
            Subcategory("Props", "props"),
        ),
    ),
    Category(
        label="Audio",
        slug="audio",
        subcategories=(
            Subcategory("All Subcategories", ""),
            Subcategory("Beats", "beats"),
            Subcategory("Loops & Samples", "loops-and-samples"),
            Subcategory("Mixing & Mastering", "mixing-and-mastering"),
            Subcategory("Sound Effects", "sound-effects"),
            Subcategory("Vocal Presets", "vocal-presets"),
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
        label="Programming & Tech",
        slug="programming-and-tech",
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


def build_discover_url(category_slug: str, subcategory_slug: str | None = None) -> str:
    """Construct a Gumroad discover URL for the given category and subcategory."""
    params = {"category": category_slug}
    if subcategory_slug:
        params["subcategory"] = subcategory_slug
    return f"https://gumroad.com/discover?{urlencode(params)}"


def category_url_map() -> Dict[str, str]:
    """Return mapping suitable for CLI category choices."""
    urls = {cat.slug: build_discover_url(cat.slug) for cat in CATEGORY_TREE}
    urls["discover"] = "https://gumroad.com/discover"
    return urls
