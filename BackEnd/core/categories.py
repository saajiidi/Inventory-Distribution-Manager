import re
import pandas as pd
import numpy as np


def _normalize(value) -> str:
    return str(value or "").strip().lower()

def _has_any(keywords, text):
    return any(
        re.search(rf"\b{re.escape(kw.lower())}\b", text, re.IGNORECASE)
        for kw in keywords
    )

def get_category_for_orders(name) -> str:
    """Redirect to the unified hierarchical categorization logic."""
    return get_category_for_sales(name)

# Master Category Priority (Determines the 'Flow' in UI Dropdowns)
CATEGORIES_PRIORITY = [
    "Jeans", "Jeans - Regular Fit", "Jeans - Slim Fit", "Jeans - Straight Fit",
    "T-Shirt", "T-Shirt - HS T-Shirt", "T-Shirt - FS T-Shirt", "T-Shirt - Drop Shoulder", "T-Shirt - Tank Top", "T-Shirt - Active Wear", "T-Shirt - Jersey",
    "FS Shirt", "FS Shirt - Flannel Shirt", "FS Shirt - Denim Shirt", "FS Shirt - Oxford Shirt", "FS Shirt - Kaftan Shirt", "FS Shirt - Executive Formal Shirt", "FS Shirt - FS Casual Shirt",
    "HS Shirt", "HS Shirt - Contrast Shirt", "HS Shirt - HS Casual Shirt",
    "Wallet", "Wallet - Passport Holder", "Wallet - Card Holder", "Wallet - Long Wallet", "Wallet - Bifold Wallet", "Wallet - Trifold Wallet",
    "Panjabi", "Panjabi - Panjabi", "Panjabi - Embroidered Panjabi",
    "Sweatshirt", "Sweatshirt - Cotton Terry Sweatshirt", "Sweatshirt - French Terry Sweatshirt",
    "Polo Shirt", "Turtle-Neck",
    "Twill", "Twill - Twill Chino", "Twill - Twill Joggers", "Twill - Five Pockets",
    "Trousers", "Trousers - Trousers", "Trousers - Joggers", "Trousers - Cotton Trousers", "Trousers - French Terry Trousers",
    "Boxer", "Leather Bag", "Belt", "Jacket", "Sweater", "Cap", "Mask", "Water Bottle",
    "Co-ords", "Shorts", "Socks", "Footwear", "Perfume & Fragrance", "Accessories", "Gift Box",
    "Bundles", "Bundles - Combo", "Bundles - Choose Any", "Others"
]

def format_category_label(cat: str) -> str:
    """Formats a category string for hierarchical display in UI dropdowns."""
    if not cat: return cat
    if " - " in cat:
        return f"↳ {cat.split(' - ', 1)[1]}"
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

def classify_velocity_trend(velocity_series: pd.Series) -> pd.Series:
    """Standardized global velocity trend classification."""
    return pd.Series(np.select(
        [
            velocity_series > 3.0,
            velocity_series > 0.8,
            velocity_series > 0.01
        ],
        ["🔥 Fast Moving", "⚖️ Regular", "🐌 Slow Moving"],
        default="❄️ Non-Moving"
    ), index=velocity_series.index)


def get_master_category_list() -> list[str]:
    """Returns the complete master category list for UI dropdowns.
    
    This ensures the dropdown always shows all categories in the defined
    priority order, regardless of whether data exists for each category.
    """
    return CATEGORIES_PRIORITY.copy()

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
    if _has_any(["jeans", "denim pant", "denim pants", "denim long"], name_str):
        if _has_any(["regular"], name_str): return "Jeans - Regular Fit"
        if _has_any(["slim"], name_str): return "Jeans - Slim Fit"
        if _has_any(["straight"], name_str): return "Jeans - Straight Fit"
        return "Jeans"

    # T-Shirt (Must be before general Shirt)
    if _has_any(["t-shirt", "t shirt", "tee", "tank top", "tanktop", "tank", "active wear", "activewear", "jersey", "jersy", "drop shoulder", "oversized", "oversize", "crewneck", "crew neck", "v-neck", "henley"], name_str):
        if _has_any(["drop shoulder", "oversized", "oversize"], name_str): return "T-Shirt - Drop Shoulder"
        if _has_any(["tank top", "tanktop", "tank"], name_str): return "T-Shirt - Tank Top"
        if _has_any(["active wear", "activewear"], name_str): return "T-Shirt - Active Wear"
        if _has_any(["jersey", "jersy"], name_str): return "T-Shirt - Jersey"
        
        fs_keywords = ["full sleeve", "long sleeve", "fs", "l/s"]
        if _has_any(fs_keywords, name_str): return "T-Shirt - FS T-Shirt"
        return "T-Shirt - HS T-Shirt"

    # FS Shirt
    fs_keywords = ["full sleeve", "long sleeve", "fs", "l/s", "full-sleeve"]
    is_shirt = _has_any(["shirt", "overshirt"], name_str)
    
    # Force certain types into FS Shirt even if 'full sleeve' is missing
    if is_shirt and (_has_any(fs_keywords, name_str) or _has_any(["flannel", "denim", "oxford", "kaftan", "executive", "formal", "linen", "corduroy"], name_str)):
        if _has_any(["flannel"], name_str): return "FS Shirt - Flannel Shirt"
        if _has_any(["denim"], name_str): return "FS Shirt - Denim Shirt"
        if _has_any(["oxford"], name_str): return "FS Shirt - Oxford Shirt"
        if _has_any(["kaftan"], name_str): return "FS Shirt - Kaftan Shirt"
        if _has_any(["executive", "formal"], name_str): return "FS Shirt - Executive Formal Shirt"
        if _has_any(["casual"], name_str): return "FS Shirt - FS Casual Shirt"
        return "FS Shirt"

    # HS Shirt
    if is_shirt:
        if _has_any(["contrast", "stitch"], name_str): return "HS Shirt - Contrast Shirt"
        if _has_any(["half sleeve", "hs", "casual", "cuban", "resort", "camp collar", "hawaiian"], name_str): return "HS Shirt - HS Casual Shirt"
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
    if _has_any(["panjabi", "punjabi", "fatua", "kurta", "kameez", "kabli"], name_str):
        if _has_any(["embroidered cotton", "embroidered", "embroidery"], name_str): return "Panjabi - Embroidered Panjabi"
        return "Panjabi - Panjabi"

    # Twill Chino
    if _has_any(["twill", "chino", "chinos"], name_str):
        if _has_any(["jogger"], name_str): return "Twill - Twill Joggers"
        if _has_any(["five pocket", "5 pocket", "5-pocket"], name_str): return "Twill - Five Pockets"
        return "Twill - Twill Chino"

    # Trousers
    if _has_any(["trouser", "jogger", "pants", "pant", "gabardine", "cargo", "sweatpant", "track pant", "track pants"], name_str):
        if _has_any(["french terry"], name_str): return "Trousers - French Terry Trousers"
        if _has_any(["regular", "fit"], name_str) and not _has_any(["jogger", "cargo"], name_str): return "Trousers - Cotton Trousers"
        if _has_any(["jogger"], name_str): return "Trousers - Joggers"
        return "Trousers - Trousers"

    # 3. STATIC / BUNDLES
    if _has_any(["bundle", "combo", "cambo", "choose any"], name_str):
        detected = []
        if _has_any(["t-shirt", "t shirt", "tee"], name_str): detected.append("T-Shirt")
        if _has_any(["jeans", "denim"], name_str): detected.append("Jeans")
        if _has_any(["boxer"], name_str): detected.append("Boxer")
        if detected:
            return f"Bundles - {' + '.join(detected)}"
        if _has_any(["choose any"], name_str): return "Bundles - Choose Any"
        if _has_any(["combo", "cambo"], name_str): return "Bundles - Combo"
        return "Bundles"

    # Co-ords / Sets
    if _has_any(["co-ord", "coord", "matching set", "tracksuit", "co ord", "two piece"], name_str):
        return "Co-ords"

    specific_cats = {
        "Boxer": ["boxer", "underwear", "brief", "trunk"],
        "Leather Bag": ["bag", "backpack", "tote", "purse", "messenger", "sling", "crossbody"],
        "Mask": ["mask"],
        "Water Bottle": ["bottle", "flask", "tumbler"],
        "Belt": ["belt"],
        "Jacket": ["jacket", "outerwear", "coat", "windbreaker", "blazer", "shacket", "bomber"],
        "Sweater": ["sweater", "cardigan", "knitwear", "jumper"],
        "Cap": ["cap", "hat", "beanie"],
        "Shorts": ["short", "half pant", "swim trunk"],
        "Socks": ["sock", "socks", "anklet"],
        "Footwear": ["shoe", "sneaker", "sandal", "slipper", "loafer", "boot", "slides"],
        "Perfume & Fragrance": ["perfume", "fragrance", "attar", "cologne", "body spray", "mist"],
        "Gift Box": ["gift box", "gift packaging", "wrapping"],
        "Accessories": ["sunglass", "watch", "bracelet", "ring", "necklace", "pendant"],
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

def get_densed_name(name: str, category: str) -> str:
    """Robust density formatting for long product names on charts."""
    name_str = str(name).strip()
    cat_str = str(category).strip()
    if len(name_str) > 22 and " - " in cat_str:
        main, sub = cat_str.split(" - ", 1)
        densed = re.sub(rf"\b{re.escape(main)}\b", "", name_str, flags=re.IGNORECASE).strip("- ")
        # Include sub-category if not already present
        if sub.lower() not in densed.lower():
            return f"{sub} {densed}".strip()
        return densed
    return name_str


def apply_category_expert_rules(df: pd.DataFrame, name_col: str = "item_name") -> pd.DataFrame:
    """
    Modular application of categorization rules to a DataFrame.
    Resilient to missing name columns.
    """
    if df is None or df.empty:
        return df
        
    # Standardize column search
    target_col = name_col
    if target_col not in df.columns:
        # Check standard aliases if name_col is missing
        aliases = ["item_name", "Product Name", "Product", "Item", "item"]
        for alias in aliases:
            if alias in df.columns:
                target_col = alias
                break
        else:
            return df
            
    df['Category'] = df[target_col].apply(get_category_for_sales)
    return df
