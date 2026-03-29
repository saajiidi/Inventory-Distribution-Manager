import re

def _normalize(value) -> str:
    return str(value or "").strip().lower()


def _has_any(text: str, keywords: list[str]) -> bool:
    for kw in keywords:
        # Use word boundaries to prevent substring overlaps (like 'bag' in 'cabbage')
        # We handle multi-word keywords correctly here as well.
        pattern = r"\b" + re.escape(kw.lower()) + r"\b"
        if re.search(pattern, text.lower()):
            return True
    return False


def _is_full_sleeve_orders(text: str) -> bool:
    return "full sleeve" in text


def _is_full_sleeve_sales(text: str) -> bool:
    return _has_any(text, ["full sleeve", "long sleeve", "fs", "l/s"])


def _tshirt_label(text: str, mode: str) -> str | None:
    tshirt_keywords = (
        ["t-shirt", "t shirt"] if mode == "orders" else ["t-shirt", "t shirt", "tee"]
    )
    if not _has_any(text, tshirt_keywords):
        return None

    if mode == "orders":
        return "FS T-Shirt" if _is_full_sleeve_orders(text) else "HS T-Shirt"
    return "FS T-Shirt" if _is_full_sleeve_sales(text) else "T-Shirt"


def _shirt_label(text: str, mode: str) -> str | None:
    if "shirt" not in text:
        return None

    if mode == "orders":
        return "FS Shirt" if _is_full_sleeve_orders(text) else "HS Shirt"
    return "FS Shirt" if _is_full_sleeve_sales(text) else "HS Shirt"


_ORDER_RULES = [
    ("Boxer", ["boxer"]),
    ("Jeans", ["jeans"]),
    ("Denim", ["denim"]),
    ("Flannel", ["flannel"]),
    ("Polo", ["polo"]),
    ("Panjabi", ["panjabi"]),
    ("Trousers", ["trouser"]),
    ("Twill", ["twill", "chino"]),
    ("Sweatshirt", ["sweatshirt"]),
    ("Tank Top", ["tank top"]),
    ("Pants", ["gabardine", "pant"]),
    ("Contrast Shirt", ["contrast"]),
    ("Turtleneck", ["turtleneck"]),
    ("Wallet", ["wallet"]),
    ("Kaftan", ["kaftan"]),
    ("Active", ["active"]),
    ("1 Pack Mask", ["mask"]),
    ("Bag", ["bag"]),
    ("Bottle", ["bottle"]),
]

_SALES_RULES = [
    ("Boxer", ["boxer"]),
    ("Jeans", ["jeans"]),
    ("Denim", ["denim"]),
    ("Flannel", ["flannel"]),
    ("Polo Shirt", ["polo"]),
    ("Panjabi", ["panjabi", "punjabi"]),
    (
        "Trousers",
        ["trousers", "pant", "cargo", "trouser", "joggers", "track pant", "jogger"],
    ),
    ("Twill Chino", ["twill chino"]),
    ("Mask", ["mask"]),
    ("Water Bottle", ["water bottle"]),
    ("Contrast Shirt", ["contrast"]),
    ("Turtleneck", ["turtleneck", "mock neck"]),
    ("Drop Shoulder", ["drop", "shoulder"]),
    ("Wallet", ["wallet"]),
    ("Kaftan", ["kaftan"]),
    ("Active Wear", ["active wear"]),
    ("Jersy", ["jersy"]),
    ("Sweatshirt", ["sweatshirt", "hoodie", "pullover"]),
    ("Jacket", ["jacket", "outerwear", "coat"]),
    ("Belt", ["belt"]),
    ("Sweater", ["sweater", "cardigan", "knitwear"]),
    ("Passport Holder", ["passport holder"]),
    ("Cap", ["cap"]),
    ("Tank Top", ["tank top"]),
    ("Bag", ["bag", "backpack"]),
]


def get_category_for_orders(name) -> str:
    text = _normalize(name)
    if not text:
        return "Items"

    for label, keywords in _ORDER_RULES:
        if _has_any(text, keywords):
            return label

    tshirt = _tshirt_label(text, "orders")
    if tshirt:
        return tshirt

    shirt = _shirt_label(text, "orders")
    if shirt:
        return shirt

    words = text.split()
    if len(words) >= 2:
        return f"{words[0].title()} {words[1].title()}"
    return "Items"


def get_category_for_sales(name) -> str:
    text = _normalize(name)
    if not text:
        return "Others"

    for label, keywords in _SALES_RULES:
        if _has_any(text, keywords):
            return label

    tshirt = _tshirt_label(text, "sales")
    if tshirt:
        return tshirt

    shirt = _shirt_label(text, "sales")
    if shirt:
        return shirt

    return "Others"
