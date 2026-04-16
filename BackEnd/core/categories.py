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
    "Jeans", "Jeans - Regular Fit", "Jeans - Slim Fit", "Jeans - Straight Fit",
    "T-Shirt", "T-Shirt - HS T-Shirt", "T-Shirt - FS T-Shirt", "T-Shirt - Drop Shoulder", "T-Shirt - Tank Top", "T-Shirt - Active Wear", "T-Shirt - Jersey",
    "FS Shirt", "FS Shirt - Flannel Shirt", "FS Shirt - Denim Shirt", "FS Shirt - Oxford Shirt", "FS Shirt - Kaftan Shirt", "FS Shirt - FS Casual Shirt",
    "HS Shirt", "HS Shirt - Contrast Stitch Shirt", "HS Shirt - HS Casual Shirt",
    "Wallet", "Wallet - Passport Holder", "Wallet - Card Holder", "Wallet - Long Wallet", "Wallet - Bifold Wallet", "Wallet - Trifold Wallet",
    "Panjabi", "Panjabi - Panjabi", "Panjabi - Old Panjabi",
    "Sweatshirt", "Sweatshirt - Cotton Terry Sweatshirt", "Sweatshirt - French Terry Sweatshirt",
    "Polo Shirt", "Turtle-Neck",
    "Twill Chino", "Twill Chino - Twill Chino Pant", "Twill Chino - Twill Joggers", "Twill Chino - Five Pockets",
    "Trousers", "Trousers - Trousers", "Trousers - Joggers", "Trousers - Cotton Trousers",
    "Boxer", "Leather Bag", "Belt", "Mask", "Water Bottle", "Bundles", "Others"
]

def format_category_label(cat: str) -> str:
    """Formats a category string for hierarchical display in UI dropdowns."""
    if not cat: return cat
    if " - " in cat:
        return f"   └─ {cat.split(' - ', 1)[1]}"
    return cat

def sort_categories(cats):
    """Sorts a list of categories based on the defined priority."""
    def sort_key(cat):
        try:
            # Match base category if sub-category not in priority
            if cat not in CATEGORIES_PRIORITY and " - " in cat:
                base = cat.split(" - ")[0]
                return CATEGORIES_PRIORITY.index(base) + 0.5
            return CATEGORIES_PRIORITY.index(cat)
        except (ValueError, KeyError):
            return 999
    return sorted(cats, key=sort_key)

_FUZZY_CACHE = {}

def get_subcategory_name(full_cat: str) -> str:
    """Extracts only the sub-category part for specific reporting needs."""
    if not full_cat or full_cat == "Others": return "Others"
    if " - " in full_cat:
        return full_cat.split(" - ", 1)[1]
    return full_cat

def get_display_category(full_cat: str, selected_cats: list[str]) -> str:
    """
    Returns the appropriate name for reporting based on filter context:
    1. If All or nothing selected, return Main Category.
    2. If a specific sub-category is selected, return Sub-Category.
    """
    if not selected_cats or "All" in selected_cats:
        # Default to main category name
        return full_cat.split(" - ")[0] if " - " in full_cat else full_cat
    
    # If the exact full_cat is in selection, or if its parent is NOT in selection but it matches, 
    # we show the sub-category part.
    if full_cat in selected_cats:
        return get_subcategory_name(full_cat)
        
    return full_cat.split(" - ")[0] if " - " in full_cat else full_cat

def get_category_for_sales(name) -> str:
    """Categorizes products based on keywords in their names (v16.0 Comprehensive Mapping)."""
    name_str = _normalize(name)
    if not name_str:
        return "Others"

    # 1. HIGH PRIORITY SPECIAL CATEGORIES (Check before 'Shirt' overlap)
    
    # Sweatshirt 
    if _has_any(["sweatshirt", "hoodie", "pullover"], name_str):
        if _has_any(["cotton terry"], name_str): return "Sweatshirt - Cotton Terry Sweatshirt"
        if _has_any(["french terry"], name_str): return "Sweatshirt - French Terry Sweatshirt"
        return "Sweatshirt"

    # Polo Shirt 
    if _has_any(["polo"], name_str):
        return "Polo Shirt"

    # Turtle-Neck 
    if _has_any(["turtleneck", "mock neck", "turtle-neck"], name_str):
        return "Turtle-Neck"

    # 2. MAIN CLUSTERS

    # Jeans
    if _has_any(["jeans"], name_str):
        if _has_any(["regular"], name_str): return "Jeans - Regular Fit"
        if _has_any(["slim"], name_str): return "Jeans - Slim Fit"
        if _has_any(["straight"], name_str): return "Jeans - Straight Fit"
        return "Jeans"

    # T-Shirt (Must be before general Shirt)
    if _has_any(["t-shirt", "t shirt", "tee"], name_str):
        if _has_any(["drop shoulder"], name_str): return "T-Shirt - Drop Shoulder"
        if _has_any(["tank top"], name_str): return "T-Shirt - Tank Top"
        if _has_any(["active wear", "activewear"], name_str): return "T-Shirt - Active Wear"
        if _has_any(["jersey", "jersy"], name_str): return "T-Shirt - Jersey"
        
        fs_keywords = ["full sleeve", "long sleeve", "fs", "l/s"]
        if _has_any(fs_keywords, name_str): return "T-Shirt - FS T-Shirt"
        return "T-Shirt - HS T-Shirt"

    # FS Shirt
    fs_keywords = ["full sleeve", "long sleeve", "fs", "l/s"]
    if _has_any(["shirt"], name_str) and _has_any(fs_keywords, name_str):
        if _has_any(["flannel"], name_str): return "FS Shirt - Flannel Shirt"
        if _has_any(["denim"], name_str): return "FS Shirt - Denim Shirt"
        if _has_any(["oxford"], name_str): return "FS Shirt - Oxford Shirt"
        if _has_any(["kaftan"], name_str): return "FS Shirt - Kaftan Shirt"
        if _has_any(["casual"], name_str): return "FS Shirt - FS Casual Shirt"
        return "FS Shirt"

    # HS Shirt
    if _has_any(["shirt"], name_str):
        if _has_any(["contrast", "stitch"], name_str): return "HS Shirt - Contrast Stitch Shirt"
        if _has_any(["casual"], name_str): return "HS Shirt - HS Casual Shirt"
        return "HS Shirt"

    # Wallet
    if _has_any(["wallet", "card holder", "passport holder"], name_str):
        if _has_any(["passport"], name_str): return "Wallet - Passport Holder"
        if _has_any(["card"], name_str): return "Wallet - Card Holder"
        if _has_any(["long"], name_str): return "Wallet - Long Wallet"
        if _has_any(["bifold"], name_str): return "Wallet - Bifold Wallet"
        if _has_any(["trifold"], name_str): return "Wallet - Trifold Wallet"
        return "Wallet"

    # Panjabi
    if _has_any(["panjabi", "punjabi"], name_str):
        if _has_any(["embroidered cotton"], name_str): return "Panjabi - Old Panjabi"
        return "Panjabi - Panjabi"

    # Twill Chino
    if _has_any(["twill", "chino"], name_str):
        if _has_any(["jogger"], name_str): return "Twill Chino - Twill Joggers"
        if _has_any(["five pocket", "5 pocket", "5-pocket"], name_str): return "Twill Chino - Five Pockets"
        return "Twill Chino - Twill Chino Pant"

    # Trousers
    if _has_any(["trouser", "jogger", "pants", "gabardine"], name_str):
        if _has_any(["regular"], name_str) and _has_any(["fit"], name_str): return "Trousers - Cotton Trousers"
        if _has_any(["jogger"], name_str): return "Trousers - Joggers"
        return "Trousers - Trousers"

    # 3. STATIC / BUNDLES
    if "bundle" in name_str:
        detected = []
        if _has_any(["t-shirt", "t shirt", "tee"], name_str): detected.append("T-Shirt")
        if _has_any(["jeans", "denim"], name_str): detected.append("Jeans")
        if _has_any(["boxer"], name_str): detected.append("Boxer")
        return f"Bundles - {' + '.join(detected)}" if detected else "Bundles"

    specific_cats = {
        "Boxer": ["boxer"],
        "Leather Bag": ["bag", "backpack", "tote"],
        "Mask": ["mask"],
        "Water Bottle": ["bottle"],
        "Belt": ["belt"],
    }

    for cat, keywords in specific_cats.items():
        if _has_any(keywords, name_str):
            return cat

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
