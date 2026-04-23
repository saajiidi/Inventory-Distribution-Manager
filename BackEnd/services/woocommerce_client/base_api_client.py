"""Base API Client with retry, logging, and error handling.

This module provides a foundational HTTP client with:
- Exponential backoff retry logic for transient failures
- Structured logging integration
- Rate limiting handling (429 responses)
- Request/response sanitization for security
"""

from __future__ import annotations

import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Callable
from functools import wraps

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from BackEnd.core.logging_config import get_logger, log_performance


logger = get_logger("base_api_client")


@dataclass
class APIConfig:
    """Configuration for API client behavior."""
    
    base_url: str
    timeout: int = 120
    max_retries: int = 3
    backoff_factor: float = 1.0
    rate_limit_delay: float = 2.0
    verify_ssl: bool = True


class APIError(Exception):
    """Custom exception for API-related errors."""
    
    def __init__(
        self, 
        message: str, 
        status_code: Optional[int] = None, 
        response_text: Optional[str] = None,
        url: Optional[str] = None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text
        self.url = url


class AuthenticationError(APIError):
    """Raised when API authentication fails."""
    pass


class RateLimitError(APIError):
    """Raised when rate limit is exceeded."""
    pass


class NotFoundError(APIError):
    """Raised when resource is not found."""
    pass


def retry_with_backoff(
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    exceptions: tuple = (requests.exceptions.RequestException,)
):
    """Decorator for retrying function calls with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Multiplier for delay between retries
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        raise
                    delay = backoff_factor * (2 ** attempt)
                    logger.warning(
                        f"Retry {attempt + 1}/{max_retries} for {func.__name__} "
                        f"after {delay:.1f}s delay. Error: {e}"
                    )
                    time.sleep(delay)
            return None
        return wrapper
    return decorator


class BaseAPIClient(ABC):
    """Abstract base class for API clients with retry and logging.
    
    Attributes:
        config: APIConfig instance with connection settings
        session: requests.Session with configured retry strategy
        _last_request_time: Timestamp of last request for rate limiting
    """
    
    def __init__(self, config: APIConfig):
        """Initialize base API client.
        
        Args:
            config: API configuration settings
        """
        self.config = config
        self.session = requests.Session()
        self._last_request_time: Optional[float] = None
        self._setup_session()
        
        logger.info(f"Initialized {self.__class__.__name__} for {config.base_url}")
    
    def _setup_session(self) -> None:
        """Configure session with retry strategy."""
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def _handle_rate_limit(self, response: requests.Response) -> None:
        """Handle rate limit response with exponential backoff.
        
        Args:
            response: Response object that triggered rate limit
        """
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", self.config.rate_limit_delay))
            logger.warning(f"Rate limited. Waiting {retry_after}s before retry.")
            time.sleep(retry_after)
            raise RateLimitError(
                "Rate limit exceeded",
                status_code=429,
                response_text=response.text,
                url=response.url
            )
    
    def _sanitize_error(self, error_text: str, sensitive_keys: list[str]) -> str:
        """Remove sensitive data from error messages.
        
        Args:
            error_text: Original error message
            sensitive_keys: List of key names to redact
            
        Returns:
            Sanitized error text
        """
        if not error_text:
            return "Unknown API error"
        
        safe_text = str(error_text)
        for key in sensitive_keys:
            # Try to redact common patterns
            import re
            pattern = rf'{key}["\']?\s*[:=]\s*["\']?([^"\'\s]+)'
            safe_text = re.sub(pattern, f'{key}=[REDACTED]', safe_text, flags=re.IGNORECASE)
        
        return safe_text
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
        headers: Optional[dict] = None,
        **kwargs
    ) -> requests.Response:
        """Make HTTP request with logging and error handling.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            params: Query parameters
            data: Request body data
            headers: Additional headers
            **kwargs: Additional requests arguments
            
        Returns:
            Response object
            
        Raises:
            APIError: For API-related errors
            AuthenticationError: For authentication failures
            NotFoundError: For 404 responses
        """
        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        request_headers = self._get_auth_headers()
        if headers:
            request_headers.update(headers)
        
        start_time = time.perf_counter()
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=request_headers,
                timeout=self.config.timeout,
                verify=self.config.verify_ssl,
                **kwargs
            )
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            log_performance(
                f"api_request_{method.lower()}",
                duration_ms,
                success=response.status_code < 400,
                metadata={
                    "endpoint": endpoint,
                    "status_code": response.status_code,
                    "url": url
                }
            )
            
            # Handle specific status codes
            if response.status_code == 429:
                self._handle_rate_limit(response)
            elif response.status_code == 401:
                raise AuthenticationError(
                    "Authentication failed. Check API credentials.",
                    status_code=401,
                    response_text=self._sanitize_error(response.text, self._get_sensitive_keys()),
                    url=url
                )
            elif response.status_code == 404:
                raise NotFoundError(
                    f"Resource not found: {endpoint}",
                    status_code=404,
                    url=url
                )
            elif response.status_code >= 400:
                raise APIError(
                    f"API request failed: {response.status_code}",
                    status_code=response.status_code,
                    response_text=self._sanitize_error(response.text, self._get_sensitive_keys()),
                    url=url
                )
            
            return response
            
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {url}")
            raise APIError(f"Request timeout after {self.config.timeout}s", url=url)
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error for {url}: {e}")
            raise APIError(f"Connection error: {str(e)}", url=url)
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise APIError(f"Request failed: {str(e)}", url=url)
    
    @abstractmethod
    def _get_auth_headers(self) -> dict[str, str]:
        """Return authentication headers for API requests.
        
        Returns:
            Dictionary of header key-value pairs
        """
        pass
    
    @abstractmethod
    def _get_sensitive_keys(self) -> list[str]:
        """Return list of sensitive key names to redact in logs.
        
        Returns:
            List of sensitive key names
        """
        return []
    
    def get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        **kwargs
    ) -> requests.Response:
        """Make GET request."""
        return self._make_request("GET", endpoint, params=params, headers=headers, **kwargs)
    
    def post(
        self,
        endpoint: str,
        data: Optional[dict] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        **kwargs
    ) -> requests.Response:
        """Make POST request."""
        return self._make_request("POST", endpoint, params=params, data=data, headers=headers, **kwargs)
    
    def put(
        self,
        endpoint: str,
        data: Optional[dict] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        **kwargs
    ) -> requests.Response:
        """Make PUT request."""
        return self._make_request("PUT", endpoint, params=params, data=data, headers=headers, **kwargs)
    
    def delete(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        **kwargs
    ) -> requests.Response:
        """Make DELETE request."""
        return self._make_request("DELETE", endpoint, params=params, headers=headers, **kwargs)
    
    def paginated_get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        per_page: int = 100,
        max_pages: int = 100,
        headers: Optional[dict] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> list[dict]:
        """Fetch all pages of a paginated endpoint.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters (will add page/per_page)
            per_page: Items per page
            max_pages: Maximum pages to fetch (safety limit)
            headers: Additional headers
            progress_callback: Optional callback(page, total_pages) for progress updates
            
        Returns:
            Combined list of all items from all pages
        """
        all_items: list[dict] = []
        page = 1
        total_pages = 1
        
        query_params = params.copy() if params else {}
        query_params["per_page"] = per_page
        
        while page <= min(total_pages, max_pages):
            query_params["page"] = page
            
            response = self.get(endpoint, params=query_params, headers=headers)
            
            # Extract total pages from headers if available
            if "X-WP-TotalPages" in response.headers:
                total_pages = int(response.headers.get("X-WP-TotalPages", 1))
            elif "x-wp-totalpages" in response.headers:
                total_pages = int(response.headers.get("x-wp-totalpages", 1))
            
            if progress_callback:
                progress_callback(page, total_pages)
            
            # Try to parse JSON response
            try:
                items = response.json()
            except ValueError as e:
                # Response is not JSON - likely an HTML error page
                content_type = response.headers.get("Content-Type", "")
                response_preview = response.text[:500] if response.text else "Empty response"
                
                logger.error(
                    f"Non-JSON response from {endpoint}. "
                    f"Content-Type: {content_type}, "
                    f"Status: {response.status_code}, "
                    f"Preview: {response_preview[:200]}..."
                )
                
                # Check for common issues
                if "text/html" in content_type:
                    if "404" in response.text or response.status_code == 404:
                        raise APIError(
                            f"WooCommerce REST API not found. "
                            f"Please check:\n"
                            f"1. Store URL is correct\n"
                            f"2. WordPress permalinks are enabled (not 'Plain')\n"
                            f"3. WooCommerce plugin is active",
                            status_code=404,
                            response_text="HTML 404 page received",
                            url=response.url
                        )
                    elif "captcha" in response.text.lower() or "security" in response.text.lower():
                        raise APIError(
                            f"Security check blocking API access. "
                            f"A security plugin may be blocking REST API requests.",
                            status_code=response.status_code,
                            response_text="Security page detected",
                            url=response.url
                        )
                    else:
                        raise APIError(
                            f"API returned HTML instead of JSON. "
                            f"Check that the store URL and API endpoint are correct.",
                            status_code=response.status_code,
                            response_text=f"HTML response: {response_preview[:100]}...",
                            url=response.url
                        )
                else:
                    raise APIError(
                        f"Invalid JSON response: {str(e)}",
                        status_code=response.status_code,
                        response_text=response_preview[:200],
                        url=response.url
                    )
            
            if not items:
                break
            
            if isinstance(items, list):
                all_items.extend(items)
            elif isinstance(items, dict) and "data" in items:
                all_items.extend(items["data"])
            else:
                logger.warning(f"Unexpected response format from {endpoint}")
                break
            
            page += 1
            
            # Safety check: if no total pages header, stop when we get empty results
            if "X-WP-TotalPages" not in response.headers and len(items) < per_page:
                break
        
        logger.info(f"Fetched {len(all_items)} items from {page - 1} pages of {endpoint}")
        return all_items
    
    def close(self) -> None:
        """Close the session and release resources."""
        self.session.close()
        logger.info(f"Closed {self.__class__.__name__} session")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
