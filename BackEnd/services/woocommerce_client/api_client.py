"""WooCommerce API Client implementing OAuth1.0a authentication.

This module provides a WooCommerce-specific API client that inherits from
BaseAPIClient and handles WooCommerce REST API authentication and endpoints.
"""

from __future__ import annotations

import streamlit as st
from typing import Optional
from urllib.parse import urlencode, parse_qsl, urlparse, urlunparse
import hashlib
import hmac
import base64
import time
import uuid

import requests

from .base_api_client import BaseAPIClient, APIConfig, APIError
from BackEnd.core.logging_config import get_logger


logger = get_logger("woocommerce_api")


class WooCommerceAPI(BaseAPIClient):
    """WooCommerce REST API client with OAuth1.0a authentication.
    
    This client handles WooCommerce-specific authentication and provides
    methods for common WooCommerce endpoints like customers, orders, and products.
    
    Attributes:
        consumer_key: WooCommerce API consumer key
        consumer_secret: WooCommerce API consumer secret
        version: API version (default: wc/v3)
    """
    
    def __init__(
        self,
        store_url: str,
        consumer_key: str,
        consumer_secret: str,
        version: str = "wc/v3",
        timeout: int = 120,
        max_retries: int = 3,
    ):
        """Initialize WooCommerce API client.
        
        Args:
            store_url: Full store URL (e.g., https://example.com)
            consumer_key: WooCommerce REST API consumer key
            consumer_secret: WooCommerce REST API consumer secret
            version: API version (default: wc/v3)
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts for failed requests
        """
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.version = version
        
        # Ensure store_url ends with /wp-json/ for API calls
        base_url = store_url.rstrip("/")
        if not base_url.endswith("/wp-json"):
            base_url = f"{base_url}/wp-json"
        
        config = APIConfig(
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=1.5,
            rate_limit_delay=2.0,
            verify_ssl=True,
        )
        
        super().__init__(config)
        
        logger.info(f"Initialized WooCommerce API client for {store_url}")
    
    def _get_auth_headers(self) -> dict[str, str]:
        """Return headers for WooCommerce API requests.
        
        WooCommerce uses query parameter authentication for HTTPS.
        Headers include content type and user agent.
        
        Returns:
            Dictionary with request headers
        """
        return {
            "Content-Type": "application/json",
            "User-Agent": "DEEN-BI/1.0 (WooCommerce Integration)",
        }
    
    def _get_sensitive_keys(self) -> list[str]:
        """Return sensitive key names to redact in logs.
        
        Returns:
            List of sensitive key names
        """
        return ["consumer_key", "consumer_secret", "oauth_consumer_key"]
    
    def _add_auth_params(self, url: str, method: str = "GET") -> str:
        """Add OAuth1.0a authentication parameters to URL.
        
        WooCommerce REST API uses OAuth1.0a for authentication.
        For HTTPS connections, this adds the consumer key as a query parameter.
        
        Args:
            url: Original URL
            method: HTTP method
            
        Returns:
            URL with authentication parameters
        """
        parsed = urlparse(url)
        params = dict(parse_qsl(parsed.query))
        
        # Add OAuth parameters
        params["oauth_consumer_key"] = self.consumer_key
        params["oauth_timestamp"] = str(int(time.time()))
        params["oauth_nonce"] = hashlib.sha1(str(uuid.uuid4()).encode()).hexdigest()[:16]
        params["oauth_signature_method"] = "HMAC-SHA256"
        
        # Build base string for signature
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        param_string = urlencode(sorted(params.items()))
        base_string = f"{method.upper()}&{base_url}&{param_string}"
        
        # Generate signature
        signing_key = f"{self.consumer_secret}&"
        signature = base64.b64encode(
            hmac.new(
                signing_key.encode(),
                base_string.encode(),
                hashlib.sha256
            ).digest()
        ).decode()
        
        params["oauth_signature"] = signature
        
        # Rebuild URL with auth params
        new_query = urlencode(params)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    
    def _handle_json_error(
        self,
        response: requests.Response,
        endpoint: str,
        original_error: ValueError
    ) -> None:
        """Handle JSON decode errors with helpful diagnostics.
        
        Args:
            response: The HTTP response that failed JSON parsing
            endpoint: The API endpoint being called
            original_error: The original ValueError from json()
            
        Raises:
            APIError: With detailed diagnostic information
        """
        content_type = response.headers.get("Content-Type", "")
        response_text = response.text[:500] if response.text else "Empty response"
        
        logger.error(
            f"JSON decode error for {endpoint}. "
            f"Content-Type: {content_type}, "
            f"Status: {response.status_code}, "
            f"URL: {response.url}, "
            f"Preview: {response_text[:200]}"
        )
        
        # Check for common issues
        if "text/html" in content_type:
            if response.status_code == 404 or "404" in response_text:
                raise APIError(
                    f"WooCommerce REST API not found at endpoint: {endpoint}\n\n"
                    f"Please check:\n"
                    f"1. Store URL is correct in secrets.toml\n"
                    f"2. WordPress permalinks are enabled (not 'Plain')\n"
                    f"3. WooCommerce plugin is active\n"
                    f"4. The endpoint {endpoint} exists",
                    status_code=404,
                    response_text="HTML 404 page received - REST API may be disabled",
                    url=response.url
                )
            elif "authentication" in response_text.lower() or "login" in response_text.lower():
                raise APIError(
                    f"Authentication failed for {endpoint}.\n\n"
                    f"Please check:\n"
                    f"1. Consumer key and secret are correct\n"
                    f"2. API key has 'Read' permission for this endpoint\n"
                    f"3. Consumer key is active in WooCommerce settings",
                    status_code=401,
                    response_text="Authentication page detected",
                    url=response.url
                )
            elif "captcha" in response_text.lower() or "security" in response_text.lower():
                raise APIError(
                    f"Security check blocking API access to {endpoint}.\n\n"
                    f"A security plugin may be blocking REST API requests. "
                    f"Check Wordfence, iThemes Security, or similar plugins.",
                    status_code=403,
                    response_text="Security/captcha page detected",
                    url=response.url
                )
            else:
                raise APIError(
                    f"API returned HTML instead of JSON for endpoint: {endpoint}\n\n"
                    f"This usually means:\n"
                    f"- The REST API is not enabled\n"
                    f"- A plugin is blocking API access\n"
                    f"- The URL is incorrect\n\n"
                    f"Response preview: {response_text[:100]}...",
                    status_code=response.status_code,
                    response_text=f"HTML response received",
                    url=response.url
                )
        else:
            raise APIError(
                f"Invalid JSON response from {endpoint}: {str(original_error)}\n\n"
                f"Content-Type: {content_type}\n"
                f"Response preview: {response_text[:200]}...",
                status_code=response.status_code,
                response_text=response_text[:200],
                url=response.url
            )
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make request with WooCommerce authentication.
        
        Overrides base method to add OAuth authentication.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            **kwargs: Additional request arguments
            
        Returns:
            Response object
        """
        # Build full URL for auth
        url = f"{self.config.base_url.rstrip('/')}/{self.version.lstrip('/')}/{endpoint.lstrip('/')}"
        
        # Add authentication
        auth_url = self._add_auth_params(url, method)
        
        # Extract components for request
        parsed = urlparse(auth_url)
        kwargs["params"] = dict(parse_qsl(parsed.query))
        
        # Call parent's _make_request with authenticated URL (without query params)
        clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, "", parsed.fragment))
        
        # Temporarily override the base_url for this request
        original_base = self.config.base_url
        self.config.base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        try:
            endpoint_path = f"/{self.version.lstrip('/')}/{endpoint.lstrip('/')}"
            return super()._make_request(method, endpoint_path, **kwargs)
        finally:
            self.config.base_url = original_base
    
    def _build_endpoint(self, resource: str, resource_id: Optional[int] = None) -> str:
        """Build API endpoint path.
        
        Args:
            resource: Resource type (customers, orders, products)
            resource_id: Optional specific resource ID
            
        Returns:
            Endpoint path string
        """
        endpoint = resource
        if resource_id:
            endpoint = f"{resource}/{resource_id}"
        return endpoint
    
    def get_customers(
        self,
        page: int = 1,
        per_page: int = 100,
        search: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        **kwargs
    ) -> list[dict]:
        """Fetch customers from WooCommerce.
        
        Args:
            page: Page number
            per_page: Items per page
            search: Search term for customer name or email
            after: ISO8601 date to filter customers registered after
            before: ISO8601 date to filter customers registered before
            **kwargs: Additional filter parameters
            
        Returns:
            List of customer dictionaries
        """
        params = {"page": page, "per_page": per_page}
        if search:
            params["search"] = search
        if after:
            params["after"] = after
        if before:
            params["before"] = before
        params.update(kwargs)
        
        response = self.get("customers", params=params)
        try:
            return response.json()
        except ValueError as e:
            self._handle_json_error(response, "customers", e)
    
    def get_customer(self, customer_id: int) -> dict:
        """Fetch a single customer by ID.
        
        Args:
            customer_id: Customer ID
            
        Returns:
            Customer dictionary
        """
        response = self.get(f"customers/{customer_id}")
        try:
            return response.json()
        except ValueError as e:
            self._handle_json_error(response, f"customers/{customer_id}", e)
    
    def get_orders(
        self,
        page: int = 1,
        per_page: int = 100,
        customer: Optional[int] = None,
        status: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        product: Optional[int] = None,
        **kwargs
    ) -> list[dict]:
        """Fetch orders from WooCommerce.
        
        Args:
            page: Page number
            per_page: Items per page
            customer: Filter by customer ID
            status: Filter by order status
            after: ISO8601 date to filter orders created after
            before: ISO8601 date to filter orders created before
            product: Filter by product ID
            **kwargs: Additional filter parameters
            
        Returns:
            List of order dictionaries
        """
        params = {"page": page, "per_page": per_page}
        if customer:
            params["customer"] = customer
        if status:
            params["status"] = status
        if after:
            params["after"] = after
        if before:
            params["before"] = before
        if product:
            params["product"] = product
        params.update(kwargs)
        
        response = self.get("orders", params=params)
        try:
            return response.json()
        except ValueError as e:
            self._handle_json_error(response, "orders", e)
    
    def get_products(
        self,
        page: int = 1,
        per_page: int = 100,
        status: str = "publish",
        **kwargs
    ) -> list[dict]:
        """Fetch products from WooCommerce.
        
        Args:
            page: Page number
            per_page: Items per page
            status: Product status filter (default: publish)
            **kwargs: Additional filter parameters
            
        Returns:
            List of product dictionaries
        """
        params = {"page": page, "per_page": per_page, "status": status}
        params.update(kwargs)
        
        response = self.get("products", params=params)
        try:
            return response.json()
        except ValueError as e:
            self._handle_json_error(response, "products", e)
    
    def get_all_customers(
        self,
        per_page: int = 100,
        max_pages: int = 100,
        progress_callback: Optional[callable] = None,
        **kwargs
    ) -> list[dict]:
        """Fetch all customers using pagination.
        
        Args:
            per_page: Items per page
            max_pages: Maximum pages to fetch
            progress_callback: Optional callback(page, total_pages)
            **kwargs: Filter parameters
            
        Returns:
            Combined list of all customers
        """
        return self.paginated_get(
            "customers",
            params=kwargs,
            per_page=per_page,
            max_pages=max_pages,
            progress_callback=progress_callback,
        )
    
    def get_all_orders(
        self,
        per_page: int = 100,
        max_pages: int = 100,
        progress_callback: Optional[callable] = None,
        **kwargs
    ) -> list[dict]:
        """Fetch all orders using pagination.
        
        Args:
            per_page: Items per page
            max_pages: Maximum pages to fetch
            progress_callback: Optional callback(page, total_pages)
            **kwargs: Filter parameters
            
        Returns:
            Combined list of all orders
        """
        return self.paginated_get(
            "orders",
            params=kwargs,
            per_page=per_page,
            max_pages=max_pages,
            progress_callback=progress_callback,
        )
    
    def get_all_products(
        self,
        per_page: int = 100,
        max_pages: int = 200,
        progress_callback: Optional[callable] = None,
        **kwargs
    ) -> list[dict]:
        """Fetch all products using pagination.
        
        Args:
            per_page: Items per page
            max_pages: Maximum pages to fetch
            progress_callback: Optional callback(page, total_pages)
            **kwargs: Filter parameters
            
        Returns:
            Combined list of all products
        """
        return self.paginated_get(
            "products",
            params=kwargs,
            per_page=per_page,
            max_pages=max_pages,
            progress_callback=progress_callback,
        )


def get_woocommerce_credentials_from_secrets() -> dict[str, str]:
    """Load WooCommerce credentials from Streamlit secrets.
    
    Returns:
        Dictionary with store_url, consumer_key, consumer_secret
        or empty dict if credentials are missing
    """
    try:
        woo = st.secrets.get("woocommerce", {})
    except Exception:
        return {}
    
    if not woo:
        return {}
    
    credentials = dict(woo)
    required = {"store_url", "consumer_key", "consumer_secret"}
    
    if not required.issubset(credentials):
        missing = required - set(credentials.keys())
        logger.warning(f"Missing WooCommerce credentials: {missing}")
        return {}
    
    return credentials


def get_woocommerce_api() -> Optional[WooCommerceAPI]:
    """Factory function to create WooCommerce API client from secrets.
    
    Returns:
        Configured WooCommerceAPI instance or None if credentials missing
    """
    credentials = get_woocommerce_credentials_from_secrets()
    
    if not credentials:
        return None
    
    try:
        return WooCommerceAPI(
            store_url=credentials["store_url"],
            consumer_key=credentials["consumer_key"],
            consumer_secret=credentials["consumer_secret"],
        )
    except Exception as e:
        logger.error(f"Failed to initialize WooCommerce API: {e}")
        return None
