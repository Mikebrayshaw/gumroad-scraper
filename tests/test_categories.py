from categories import build_discover_url


def test_build_discover_url_uses_category_paths():
    assert build_discover_url("design") == "https://gumroad.com/design"
    assert build_discover_url("design", "icons") == "https://gumroad.com/design/icons"
    assert build_discover_url("business-and-money") == "https://gumroad.com/business-and-money"


def test_build_discover_url_applies_aliases():
    assert build_discover_url("programming-and-tech") == "https://gumroad.com/software-development"
    assert build_discover_url("software") == "https://gumroad.com/software-development"
