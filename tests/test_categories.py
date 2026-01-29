from categories import (
    build_discover_url,
    validate_url,
    should_skip_subcategory,
    Subcategory,
    CATEGORY_TREE,
)


def test_build_discover_url_uses_category_paths():
    assert build_discover_url("design") == "https://gumroad.com/design"
    assert build_discover_url("design", "icons") == "https://gumroad.com/design/icons"
    assert build_discover_url("business-and-money") == "https://gumroad.com/business-and-money"


def test_build_discover_url_applies_aliases():
    assert build_discover_url("programming-and-tech") == "https://gumroad.com/software-development"
    assert build_discover_url("software") == "https://gumroad.com/software-development"


def test_validate_url_rejects_invalid_patterns():
    """Test that validate_url correctly identifies invalid URL patterns."""
    # Invalid patterns
    assert validate_url("https://gumroad.com/3d/assets") is False
    assert validate_url("https://gumroad.com/3d/characters") is False
    assert validate_url("https://gumroad.com/audio/beats") is False
    assert validate_url("https://gumroad.com/audio/loops-and-samples") is False
    
    # Valid patterns
    assert validate_url("https://gumroad.com/3d") is True
    assert validate_url("https://gumroad.com/audio") is True
    assert validate_url("https://gumroad.com/design/icons") is True
    assert validate_url("https://gumroad.com/business-and-money") is True


def test_should_skip_subcategory():
    """Test that should_skip_subcategory correctly identifies subcategories to skip."""
    skip_sub = Subcategory("Test", "test", skip_scraping=True)
    assert should_skip_subcategory(skip_sub) is True
    
    normal_sub = Subcategory("Test", "test", skip_scraping=False)
    assert should_skip_subcategory(normal_sub) is False
    
    default_sub = Subcategory("Test", "test")
    assert should_skip_subcategory(default_sub) is False


def test_build_discover_url_with_invalid_subcategory_falls_back():
    """Test that build_discover_url falls back to category-only URL for invalid subcategories."""
    # Should fall back to category-only URL for invalid patterns
    assert build_discover_url("3d", "assets") == "https://gumroad.com/3d"
    assert build_discover_url("3d", "characters") == "https://gumroad.com/3d"
    assert build_discover_url("audio", "beats") == "https://gumroad.com/audio"


def test_build_discover_url_with_subcategory_object_absolute_url():
    """Test that absolute_url has highest priority."""
    sub = Subcategory(
        "Test",
        "test-slug",
        absolute_url="https://gumroad.com/special/url",
        path_suffix="suffix",
        query_params={"key": "value"}
    )
    assert build_discover_url("design", subcategory=sub) == "https://gumroad.com/special/url"


def test_build_discover_url_with_subcategory_object_query_params():
    """Test that query_params work when absolute_url is not provided."""
    sub = Subcategory(
        "Test",
        "test-slug",
        query_params={"filter": "popular", "sort": "recent"}
    )
    url = build_discover_url("design", subcategory=sub)
    assert url.startswith("https://gumroad.com/design?")
    assert "filter=popular" in url
    assert "sort=recent" in url


def test_build_discover_url_with_subcategory_object_path_suffix():
    """Test that path_suffix works when absolute_url and query_params are not provided."""
    sub = Subcategory(
        "Test",
        "test-slug",
        path_suffix="custom-path"
    )
    assert build_discover_url("design", subcategory=sub) == "https://gumroad.com/design/custom-path"


def test_build_discover_url_with_subcategory_object_slug():
    """Test that slug is used when no other routing info is provided."""
    sub = Subcategory("Test", "test-slug")
    assert build_discover_url("design", subcategory=sub) == "https://gumroad.com/design/test-slug"


def test_build_discover_url_with_subcategory_object_invalid_path_fallback():
    """Test that invalid path_suffix falls back to category-only URL."""
    # Create a subcategory with path_suffix that would create an invalid URL
    sub = Subcategory("Assets", "assets", path_suffix="assets")
    url = build_discover_url("3d", subcategory=sub)
    # Should fall back to category-only URL
    assert url == "https://gumroad.com/3d"


def test_category_tree_has_skip_scraping_marked():
    """Test that known invalid subcategories are marked with skip_scraping=True."""
    # Find 3D category
    category_3d = next(cat for cat in CATEGORY_TREE if cat.slug == "3d")
    
    # Check that invalid subcategories are marked
    assets_sub = next(sub for sub in category_3d.subcategories if sub.slug == "assets")
    assert assets_sub.skip_scraping is True
    
    characters_sub = next(sub for sub in category_3d.subcategories if sub.slug == "characters")
    assert characters_sub.skip_scraping is True
    
    # Check that "All Subcategories" is not marked
    all_sub = next(sub for sub in category_3d.subcategories if sub.slug == "")
    assert all_sub.skip_scraping is False


def test_backwards_compatibility_with_subcategory_slug():
    """Test that the old API (subcategory_slug parameter) still works."""
    # Valid subcategories should work as before
    assert build_discover_url("design", subcategory_slug="icons") == "https://gumroad.com/design/icons"
    
    # Invalid subcategories should fall back to category-only
    assert build_discover_url("3d", subcategory_slug="assets") == "https://gumroad.com/3d"
