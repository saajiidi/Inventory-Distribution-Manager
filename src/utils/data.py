import re
import pandas as pd
from src.core.categories import get_category_for_orders


def find_columns(df):
    """Detects primary columns using exact and then partial matching with expanded aliases."""
    mapping = {
        "name": [
            "item name",
            "product name",
            "product",
            "item",
            "title",
            "description",
            "name",
            "internal_name",
        ],
        "customer_name": [
            "first name (shipping)",
            "first name",
            "customer name",
            "billing name",
            "full name",
            "client name",
            "customer",
            "recipient",
        ],
        "cost": [
            "item cost",
            "price",
            "unit price",
            "cost",
            "rate",
            "mrp",
            "selling price",
            "total",
            "internal_cost",
            "line_total",
        ],
        "qty": [
            "quantity",
            "qty",
            "units",
            "sold",
            "count",
            "total quantity",
            "internal_qty",
        ],
        "date": [
            "date",
            "order date",
            "month",
            "time",
            "created at",
            "date_created",
            "date_paid",
        ],
        "order_id": [
            "order id",
            "order #",
            "invoice number",
            "invoice #",
            "order number",
            "transaction id",
            "id",
            "number",
        ],
        "phone": [
            "phone",
            "contact",
            "mobile",
            "cell",
            "phone number",
            "customer phone",
            "billing.phone",
        ],
        "email": [
            "email",
            "customer email",
            "email address",
            "e-mail",
            "billing.email",
        ],
    }
    found = {}
    actual_cols = [c.strip() for c in df.columns]
    lower_cols = [c.lower() for c in actual_cols]

    # Priority 1: Exact mapping
    for key, aliases in mapping.items():
        for alias in aliases:
            if alias in lower_cols:
                idx = lower_cols.index(alias)
                found[key] = actual_cols[idx]
                break

    # Priority 2: Fuzzy mapping
    for key, aliases in mapping.items():
        if key not in found:
            for col, l_col in zip(actual_cols, lower_cols):
                # 🛡️ PROTECT: Identity fields must not match metadata/instructional columns
                if key in ["customer_name", "name", "phone", "email"]:
                    if any(
                        bad in l_col
                        for bad in [
                            "note",
                            "status",
                            "detail",
                            "review",
                            "comment",
                            "flag",
                            "msg",
                            "message",
                            "instruction",
                            "memo",
                            "info",
                            "desc",
                        ]
                    ):
                        continue

                # Special Case: 'customer' alias for customer_name should be EXACT ONLY (already handled in Priority 1)
                # We skip it here to avoid matching 'customer notes', 'customer info', etc.
                fuzzy_aliases = [
                    a
                    for a in aliases
                    if not (key == "customer_name" and a == "customer")
                ]

                if any(alias in l_col for alias in fuzzy_aliases):
                    found[key] = col
                    break
    return found


def pick_column(df: pd.DataFrame, candidates: list[str]) -> str:
    """Returns the first existing column from a list of candidates."""
    for col in candidates:
        if col in df.columns:
            return col
    return ""


def parse_dates(values: pd.Series) -> pd.Series:
    """Parse mixed order-date formats while keeping failed values as NaT."""

    def _parse_single(value):
        text = str(value).strip()
        if text in {"", "nan", "NaT", "None"}:
            return pd.NaT
        iso_patterns = (
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
        )
        for fmt in iso_patterns:
            try:
                return pd.to_datetime(text, format=fmt, errors="raise")
            except:
                continue
        for dayfirst in (True, False):
            try:
                parsed = pd.to_datetime(text, errors="raise", dayfirst=dayfirst)
                if pd.notna(parsed):
                    return parsed
            except:
                continue
        return pd.NaT

    return values.apply(_parse_single)


def get_category_from_name(name):
    return get_category_for_orders(name)


# --- Address Logic ---
def normalize_city_name(city_name):
    if not city_name:
        return ""
    c = city_name.strip().lower()
    if "brahmanbaria" in c:
        return "B. Baria"
    if "narsingdi" in c or "narsinghdi" in c:
        return "Narshingdi"
    if "bagura" in c or "bogura" in c:
        return "Bogra"
    if "chattogram" in c:
        return "Chittagong"
    if "cox" in c and "bazar" in c:
        return "Cox's Bazar"
    return city_name.strip().title()


def extract_best_zone(address, KNOWN_ZONES):
    if not isinstance(address, str) or not address:
        return ""
    addr_l = address.lower()
    matches = [z for z in KNOWN_ZONES if z.lower() in addr_l]
    if not matches:
        return ""
    matches.sort(key=len, reverse=True)
    return matches[0]


def format_address_logic(raw_addr, city_norm, extracted_zone, raw_city_val):
    addr = " ".join(raw_addr.split()).title()
    if raw_city_val and city_norm and raw_city_val.lower() != city_norm.lower():
        addr = re.compile(re.escape(raw_city_val), re.IGNORECASE).sub(city_norm, addr)
    parts = [p.strip() for p in re.split(r"[,;]\s*", addr) if p.strip()]
    cleaned = []
    seen = set()
    for p in parts:
        pl = p.lower()
        if (
            pl in seen
            or (city_norm and pl == city_norm.lower())
            or (extracted_zone and pl == extracted_zone.lower())
        ):
            continue
        cleaned.append(p)
        seen.add(pl)
    if extracted_zone and (
        extracted_zone.lower() not in ["sadar", "city"] or not cleaned
    ):
        if not any(extracted_zone.lower() in p.lower() for p in cleaned):
            cleaned.append(extracted_zone)
    if city_norm:
        cleaned.append(city_norm)
    return ", ".join(cleaned)
