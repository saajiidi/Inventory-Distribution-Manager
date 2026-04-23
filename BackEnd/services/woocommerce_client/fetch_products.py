"""Product data fetching service with caching.

Provides functions to fetch product data from WooCommerce REST API
with Streamlit caching for performance optimization.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
from typing import Optional, Callable

from .api_client import WooCommerceAPI, get_woocommerce_api
from .base_api_client import APIError
from BackEnd.core.logging_config import get_logger


logger = get_logger("fetch_products")


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_products(
    status: str = "publish",
    category: Optional[int] = None,
    search: Optional[str] = None,
    per_page: int = 100,
    _progress_callback: Optional[Callable] = None,
) -> pd.DataFrame:
    """Fetch all products from WooCommerce with caching.
    
    This function fetches products using pagination and returns
    a normalized DataFrame with product information.
    
    Args:
        status: Product status filter (default: publish)
        category: Filter by category ID
        search: Search term for product name
        per_page: Items per page for pagination
        _progress_callback: Internal callback for progress updates (not cached)
        
    Returns:
        DataFrame with product columns:
        - product_id: WooCommerce product ID
        - name: Product name
        - slug: Product slug
        - sku: Product SKU
        - type: Product type (simple, variable, etc.)
        - status: Product status
        - price: Current price
        - regular_price: Regular price
        - sale_price: Sale price
        - stock_status: Stock status
        - stock_quantity: Stock quantity
        - manage_stock: Whether stock is managed
        - categories: List of category names
        - tags: List of tag names
        - date_created: Creation date
        - date_modified: Last modified date
        
    Raises:
        st.error: If API credentials are missing or request fails
    """
    api = get_woocommerce_api()
    
    if not api:
        st.error(
            "🔐 **WooCommerce API Not Connected**\n\n"
            "Please configure your WooCommerce credentials in `.streamlit/secrets.toml`"
        )
        return pd.DataFrame()
    
    try:
        params = {"status": status}
        if category:
            params["category"] = category
        if search:
            params["search"] = search
        
        with st.spinner("📦 Fetching products from WooCommerce..."):
            products = api.get_all_products(
                per_page=per_page,
                progress_callback=_progress_callback,
                **params
            )
        
        if not products:
            return pd.DataFrame()
        
        df = _normalize_products(products)
        logger.info(f"Fetched {len(df)} products from WooCommerce")
        return df
        
    except APIError as e:
        logger.error(f"API error fetching products: {e}")
        st.error(
            f"❌ **Failed to fetch products**\n\n"
            f"{str(e)}\n\n"
            f"**Quick checks:**\n"
            f"1. Open `{e.url}` in browser - should show JSON, not HTML\n"
            f"2. Verify permalinks are enabled in WordPress (not 'Plain')\n"
            f"3. Check API key has 'read' permission for products"
        )
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Failed to fetch products: {e}")
        st.error(
            f"❌ **Failed to fetch products**\n\n"
            f"Error: {str(e)}\n\n"
            f"**Possible fixes:**\n"
            f"- Verify API credentials are correct\n"
            f"- Check store URL accessibility\n"
            f"- Ensure 'products' read permission is enabled"
        )
        return pd.DataFrame()


def _normalize_products(products: list[dict]) -> pd.DataFrame:
    """Normalize raw WooCommerce product JSON to DataFrame.
    
    Args:
        products: List of product dictionaries from API
        
    Returns:
        Normalized DataFrame
    """
    normalized = []
    
    for product in products:
        # Extract categories
        categories = [c.get("name", "") for c in product.get("categories", [])]
        
        # Extract tags
        tags = [t.get("name", "") for t in product.get("tags", [])]
        
        # Handle variations for variable products
        variations = product.get("variations", [])
        
        normalized.append({
            "product_id": product.get("id"),
            "name": product.get("name", ""),
            "slug": product.get("slug", ""),
            "sku": product.get("sku", ""),
            "type": product.get("type", "simple"),
            "status": product.get("status", ""),
            "featured": product.get("featured", False),
            "catalog_visibility": product.get("catalog_visibility", ""),
            "description": _clean_html(product.get("description", "")),
            "short_description": _clean_html(product.get("short_description", "")),
            "price": _parse_price(product.get("price")),
            "regular_price": _parse_price(product.get("regular_price")),
            "sale_price": _parse_price(product.get("sale_price")),
            "on_sale": product.get("on_sale", False),
            "stock_status": product.get("stock_status", "instock"),
            "stock_quantity": product.get("stock_quantity") or 0,
            "manage_stock": product.get("manage_stock", False),
            "backorders": product.get("backorders", "no"),
            "categories": categories,
            "category_names": ", ".join(categories),
            "tags": tags,
            "tag_names": ", ".join(tags),
            "images_count": len(product.get("images", [])),
            "variations_count": len(variations),
            "date_created": product.get("date_created"),
            "date_modified": product.get("date_modified"),
            "total_sales": product.get("total_sales", 0),
        })
    
    df = pd.DataFrame(normalized)
    
    # Parse dates
    date_cols = ["date_created", "date_modified"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    
    return df


def _parse_price(price: Optional[str]) -> float:
    """Parse price string to float.
    
    Args:
        price: Price string or None
        
    Returns:
        Price as float, 0 if invalid
    """
    if not price:
        return 0.0
    try:
        return float(price)
    except (ValueError, TypeError):
        return 0.0


def _clean_html(html: str) -> str:
    """Remove HTML tags from text.
    
    Args:
        html: HTML string
        
    Returns:
        Plain text without HTML tags
    """
    if not html:
        return ""
    import re
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', html)
    # Decode HTML entities
    import html as html_module
    text = html_module.unescape(text)
    return text.strip()


def get_product_options() -> list[dict]:
    """Get products formatted for Streamlit selectbox/multiselect.
    
    Returns:
        List of dictionaries with 'id' and 'label' keys
    """
    df = fetch_products()
    
    if df.empty:
        return []
    
    return [
        {"id": row["product_id"], "label": f"{row['name']} (SKU: {row['sku']})"}
        for _, row in df.iterrows()
    ]


def get_product_by_id(product_id: int) -> Optional[dict]:
    """Get a single product by ID.
    
    Args:
        product_id: Product ID
        
    Returns:
        Product dictionary or None
    """
    df = fetch_products()
    
    if df.empty:
        return None
    
    match = df[df["product_id"] == product_id]
    
    if match.empty:
        return None
    
    return match.iloc[0].to_dict()


def clear_products_cache() -> None:
    """Clear the products data cache."""
    fetch_products.clear()
    logger.info("Cleared products cache")
    st.success("🗑️ Products cache cleared. Data will refresh on next load.")
