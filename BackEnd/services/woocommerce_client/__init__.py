"""WooCommerce API services for customer insight module."""

from .api_client import WooCommerceAPI, get_woocommerce_api
from .fetch_customers import fetch_customers, fetch_customer_by_id
from .fetch_orders import fetch_orders, fetch_customer_orders
from .fetch_products import fetch_products

__all__ = [
    "WooCommerceAPI",
    "get_woocommerce_api",
    "fetch_customers",
    "fetch_customer_by_id",
    "fetch_orders",
    "fetch_customer_orders",
    "fetch_products",
]
