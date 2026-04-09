import re

def _normalize(value) -> str:
    return str(value or "").strip().lower()

def _has_any(keywords, text):
    return any(
        re.search(rf"\b{re.escape(kw.lower())}\b", text, re.IGNORECASE)
        for kw in keywords
    )

def get_category_for_orders(name) -> str:
    """Old order categorization (maintained for backward compatibility if needed)."""
    text = _normalize(name)
    if not text:
        return "Items"

    order_rules = [
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

    for label, keywords in order_rules:
        if _has_any(keywords, text):
            return label

    fs_keywords = ["full sleeve"]
    if _has_any(["t-shirt", "t shirt"], text):
        return "FS T-Shirt" if any(kw in text for kw in fs_keywords) else "HS T-Shirt"

    if "shirt" in text:
        return "FS Shirt" if any(kw in text for kw in fs_keywords) else "HS Shirt"

    words = text.split()
    if len(words) >= 2:
        return f"{words[0].title()} {words[1].title()}"
    return "Items"

# Master Category Priority (Determines the 'Flow' in UI Dropdowns)
CATEGORIES_PRIORITY = [
    "Tank Top", "Boxer", "Jeans", "Denim Shirt", "Flannel Shirt", "Polo Shirt",
    "Panjabi", "Trousers", "Joggers", "Twill Chino", "Mask", "Leather Bag",
    "Water Bottle", "Contrast Shirt", "Turtleneck", "Drop Shoulder", "Wallet",
    "Kaftan Shirt", "Active Wear", "Jersy", "Sweatshirt", "Jacket", "Belt",
    "Sweater", "Passport Holder", "Card Holder", "Cap", "FS T-Shirt", "HS T-Shirt",
    "FS Shirt", "HS Shirt", "Others"
]

def sort_categories(cats):
    """Sorts a list of categories based on the defined priority."""
    def sort_key(cat):
        try:
            return CATEGORIES_PRIORITY.index(cat)
        except (ValueError, KeyError):
            return 999
    return sorted(cats, key=sort_key)

def get_category_for_sales(name) -> str:
    """Categorizes products based on keywords in their names (v9.5 Expert Rules)."""
    name_str = _normalize(name)
    if not name_str:
        return "Others"

    specific_cats = {
        "Tank Top": ["tank top"],
        "Boxer": ["boxer"],
        "Jeans": ["jeans"],
        "Denim Shirt": ["denim"],
        "Flannel Shirt": ["flannel"],
        "Polo Shirt": ["polo"],
        "Panjabi": ["panjabi", "punjabi"],
        "Trousers": ["trousers", "trouser"],
        "Joggers": ["joggers", "jogger", "track pant"],
        "Twill Chino": ["twill chino", "chino", "twill"],
        "Mask": ["mask"],
        "Leather Bag": ["bag", "backpack"],
        "Water Bottle": ["water bottle"],
        "Contrast Shirt": ["contrast"],
        "Turtleneck": ["turtleneck", "mock neck"],
        "Drop Shoulder": ["drop", "shoulder"],
        "Wallet": ["wallet"],
        "Kaftan Shirt": ["kaftan"],
        "Active Wear": ["active wear"],
        "Jersy": ["jersy"],
        "Sweatshirt": ["sweatshirt", "hoodie", "pullover"],
        "Jacket": ["jacket", "outerwear", "coat"],
        "Belt": ["belt"],
        "Sweater": ["sweater", "cardigan", "knitwear"],
        "Passport Holder": ["passport holder"],
        "Card Holder": ["card holder"],
        "Cap": ["cap"],
    }

    for cat, keywords in specific_cats.items():
        if _has_any(keywords, name_str):
            return cat

    fs_keywords = ["full sleeve", "long sleeve", "fs", "l/s"]
    if _has_any(["t-shirt", "t shirt", "tee"], name_str):
        return "FS T-Shirt" if _has_any(fs_keywords, name_str) else "HS T-Shirt"

    if _has_any(["shirt"], name_str):
        return "FS Shirt" if _has_any(fs_keywords, name_str) else "HS Shirt"

    return "Others"
    

def parse_sku_variants(name: str) -> tuple[str, str]:
    """Extracts Color and Size from an e-commerce product name string using regex heuristics."""
    name_str = str(name).strip()
    if not name_str:
        return "Unknown", "Unknown"

    # Common patterns for e-commerce sizes
    # Matches: S, M, L, XL, 2XL, XXL, 3XL, XXXL, 4XL, 5XL, XS, 
    # Also matches numeric sizes from 26 to 52 (standard for pants/shoes)
    # Added some flexibility for common formats (e.g. Size 38, XL-Slim)
    size_pattern = r"\b(XS|S|M|L|XL|XXL|2XL|3XL|XXXL|4XL|5XL|2[6-9]|3[0-9]|4[0-9]|5[0-2])\b"
    
    parts = [p.strip() for p in name_str.split("-") if p.strip()]
    
    found_size = "Unknown"
    found_color = "Unknown"

    # Strategy 1: Check all segments for size patterns (don't guess by position)
    for i, part in enumerate(reversed(parts)):
        size_match = re.search(size_pattern, part, re.IGNORECASE)
        if size_match:
            found_size = size_match.group(0).upper()
            # If we found a size, usually the segment BEFORE it is the color
            if i + 1 < len(parts):
                rev_idx = len(parts) - 1 - i
                found_color = parts[rev_idx - 1]
            break

    # Strategy 2: If size is still unknown, search the entire string regardless of hyphens
    if found_size == "Unknown":
        match = re.search(size_pattern, name_str, re.IGNORECASE)
        if match:
            found_size = match.group(0).upper()

    # Final cleanup: Remove any surrounding noise
    found_size = re.sub(r"[()\[\]]", "", found_size).strip()
    found_color = re.sub(r"[()\[\]]", "", found_color).strip()

    # If the color is recognized as a size, it shouldn't be a color
    if re.search(size_pattern, found_color, re.IGNORECASE):
        found_color = "Unknown"

    return found_color, found_size

def get_clean_product_name(name):
    """Strips size/color segments from product name using robust detection."""
    name_str = str(name).strip()
    color, size = parse_sku_variants(name_str)
    
    clean = name_str
    # Remove markers if they are part of a dashed segment at the end
    if size != "Unknown":
        # Matches " - XL" or "-XL" at the end
        clean = re.sub(rf"\s*-\s*{re.escape(size)}\s*$", "", clean, flags=re.IGNORECASE)
    if color != "Unknown":
        # Matches " - Blue" or "-Blue" at the end
        clean = re.sub(rf"\s*-\s*{re.escape(color)}\s*$", "", clean, flags=re.IGNORECASE)
        
    # If no segments were removed, fall back to dash split if there are many dashes
    if clean == name_str:
        parts = [p.strip() for p in name_str.split("-") if p.strip()]
        if len(parts) >= 3:
            return "-".join(parts[:-2]).strip()
        elif len(parts) == 2:
            return parts[0]
            
    return clean.strip("- ")
