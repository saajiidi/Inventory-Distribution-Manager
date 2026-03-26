import json
from datetime import date, datetime, time
from io import BytesIO

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

from src.core.errors import log_error
from src.ui.components import section_card

DEFAULT_WP_ORDERS_SITE_URL = "https://deencommerce.com/"
DEFAULT_WP_ORDERS_ENDPOINT_PATH = "/wp-json/wc/v3/orders"
LEGACY_WP_ORDERS_ENDPOINT_PATH = "/wp-json/wp/v2/orders"

AUTH_MODE_APPLICATION_PASSWORD = "WP User + Application Password"
AUTH_MODE_WOO_KEYS = "WooCommerce Consumer Key / Secret"
AUTH_MODE_NONE = "No Auth / Public Route"
AUTH_MODE_OPTIONS = [
    AUTH_MODE_APPLICATION_PASSWORD,
    AUTH_MODE_WOO_KEYS,
    AUTH_MODE_NONE,
]

KNOWN_ORDER_ENDPOINTS = [
    ("/wp-json/wc/v3/orders", "WooCommerce Orders v3"),
    ("/wp-json/wc/v2/orders", "WooCommerce Orders v2"),
    ("/wp-json/wc-analytics/reports/orders", "WooCommerce Analytics Orders Report"),
    ("/wp-json/wp/v2/orders", "WordPress Orders Route"),
]

WP_API_ORDERS_RESULT_KEY = "wp_api_orders_result_df"
WP_API_ORDERS_META_KEY = "wp_api_orders_result_meta"
WP_API_ORDERS_DIAGNOSTICS_KEY = "wp_api_orders_diagnostics"
WP_API_ORDERS_DISCOVERY_KEY = "wp_api_orders_discovery"
WP_API_ORDERS_USER_ACCESS_KEY = "wp_api_orders_user_access"


def get_wp_api_orders_tab_label():
    return "🌐 WooCommerce Orders"


def build_wp_api_orders_endpoint(site_url: str, endpoint_path: str) -> str:
    """Build a full API URL from a site URL and endpoint path."""
    site_url = (site_url or "").strip()
    endpoint_path = (endpoint_path or "").strip()
    if not site_url:
        raise ValueError("Site URL is required.")
    if not endpoint_path:
        raise ValueError("API endpoint path is required.")

    if endpoint_path.startswith(("http://", "https://")):
        return endpoint_path.rstrip("/")

    return f"{site_url.rstrip('/')}/{endpoint_path.lstrip('/')}"


def build_wp_api_orders_params(
    start_date: date, end_date: date, per_page: int = 100, page: int = 1
) -> dict:
    """Build the date-range query params expected by WooCommerce order endpoints."""
    safe_per_page = max(1, min(int(per_page), 100))
    safe_page = max(1, int(page))
    return {
        "after": datetime.combine(start_date, time(0, 0, 0)).isoformat(),
        "before": datetime.combine(end_date, time(23, 59, 59)).isoformat(),
        "per_page": safe_per_page,
        "page": safe_page,
    }


def build_wp_api_auth_request_options(
    auth_mode: str,
    username: str = "",
    app_password: str = "",
    consumer_key: str = "",
    consumer_secret: str = "",
):
    """Return requests auth tuple and extra query params for the selected auth mode."""
    auth = None
    extra_params = {}

    if auth_mode == AUTH_MODE_APPLICATION_PASSWORD:
        if username.strip() and app_password.strip():
            auth = (username.strip(), app_password.strip())
    elif auth_mode == AUTH_MODE_WOO_KEYS:
        if consumer_key.strip():
            extra_params["consumer_key"] = consumer_key.strip()
        if consumer_secret.strip():
            extra_params["consumer_secret"] = consumer_secret.strip()

    return auth, extra_params


def extract_wp_api_records(payload) -> list[dict]:
    """Extract order rows from common WordPress-style JSON payloads."""
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in ("orders", "data", "results", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        if any(
            key in payload for key in ("id", "date", "date_created", "link", "title")
        ):
            return [payload]

    raise ValueError(
        "Unsupported API response format. Expected a list of order objects."
    )


def _coerce_title(value):
    if isinstance(value, dict):
        for key in ("rendered", "raw"):
            if value.get(key):
                return str(value[key])
        return json.dumps(value, ensure_ascii=False)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value)


def make_wp_orders_export_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert nested cells to strings so export and preview remain stable."""
    safe_df = df.copy()
    for col in safe_df.columns:
        if safe_df[col].dtype == "object":
            safe_df[col] = safe_df[col].apply(
                lambda value: (
                    json.dumps(value, ensure_ascii=False)
                    if isinstance(value, (dict, list))
                    else value
                )
            )
    return safe_df


def normalize_wp_orders_dataframe(records: list[dict]) -> pd.DataFrame:
    """Flatten API records and expose friendly top-level columns for preview/export."""
    if not records:
        return pd.DataFrame()

    df = pd.json_normalize(records, sep=".")

    if "title.rendered" in df.columns:
        df["title"] = df["title.rendered"].fillna("")
    elif "title" in df.columns:
        df["title"] = df["title"].apply(_coerce_title)

    if "date" not in df.columns:
        for candidate in (
            "date_created",
            "date_created_gmt",
            "date_paid",
            "date_paid_gmt",
            "date_completed",
            "date_completed_gmt",
            "date_gmt",
        ):
            if candidate in df.columns:
                df["date"] = df[candidate]
                break

    if "link" not in df.columns:
        for candidate in ("permalink", "_links.self.0.href"):
            if candidate in df.columns:
                df["link"] = df[candidate]
                break

    if "title" not in df.columns and "number" in df.columns:
        df["title"] = "Order #" + df["number"].astype(str)

    if "date" in df.columns:
        parsed_dates = pd.to_datetime(df["date"], errors="coerce")
        if parsed_dates.notna().any():
            normalized_text = parsed_dates.dt.strftime("%Y-%m-%d %H:%M:%S")
            raw_date_text = df["date"].astype(str)
            df["date"] = normalized_text.where(parsed_dates.notna(), raw_date_text)

    priority_cols = [
        "id",
        "number",
        "title",
        "date",
        "status",
        "total",
        "currency",
        "billing.first_name",
        "billing.last_name",
        "billing.phone",
        "billing.email",
        "billing.city",
        "shipping.city",
        "payment_method_title",
        "link",
        "customer_id",
        "line_items",
    ]
    ordered_cols = [col for col in priority_cols if col in df.columns]
    ordered_cols.extend(col for col in df.columns if col not in ordered_cols)
    return make_wp_orders_export_safe(df[ordered_cols])


def get_wp_orders_preview_columns(df: pd.DataFrame) -> list[str]:
    preferred = [
        "id",
        "number",
        "title",
        "date",
        "status",
        "total",
        "currency",
        "billing.city",
        "payment_method_title",
    ]
    preview_cols = [col for col in preferred if col in df.columns]
    if preview_cols:
        return preview_cols
    return list(df.columns[: min(10, len(df.columns))])


def _extract_wp_api_error_details(response: requests.Response) -> dict:
    try:
        payload = response.json()
    except ValueError:
        payload = None

    code = ""
    message = ""
    if isinstance(payload, dict):
        code = str(payload.get("code", "") or "")
        message = str(payload.get("message", "") or "")

    detail = response.text.strip()
    return {
        "status_code": response.status_code,
        "code": code,
        "message": message,
        "detail": detail[:400] if detail else "",
    }


def _build_wp_orders_hint(
    status_code: int, code: str, auth_mode: str, endpoint_path: str
) -> str:
    if status_code == 404:
        return (
            "This route does not exist on the site. On March 24, 2026, "
            "deencommerce.com returned 404 for /wp-json/wp/v2/orders and exposed "
            "/wp-json/wc/v3/orders instead."
        )

    if status_code in (401, 403):
        if endpoint_path.startswith("/wp-json/wc/"):
            if auth_mode == AUTH_MODE_NONE:
                return "The WooCommerce route exists, but it requires authenticated access."
            if auth_mode == AUTH_MODE_APPLICATION_PASSWORD:
                return (
                    "The WooCommerce route exists, but the application-password login was not accepted "
                    "for order access. Try a WooCommerce consumer key/secret or an account with "
                    "WooCommerce order permissions."
                )
            if auth_mode == AUTH_MODE_WOO_KEYS:
                return (
                    "The WooCommerce route exists, but the consumer key/secret was rejected or does not "
                    "have permission to read orders."
                )
        return "The route exists, but this account does not currently have access to read the resource."

    if status_code == 200:
        return "Endpoint verified successfully."

    if code:
        return f"The server returned {status_code} with code '{code}'."
    return f"The server returned status {status_code}."


def discover_wp_orders_endpoints(site_url: str, timeout_seconds: int = 20) -> dict:
    """Inspect /wp-json/ and suggest the most likely orders endpoint."""
    root_url = build_wp_api_orders_endpoint(site_url, "/wp-json/")
    response = requests.get(root_url, timeout=timeout_seconds)

    if response.status_code != 200:
        detail = _extract_wp_api_error_details(response)
        raise RuntimeError(
            f"Could not read the REST index. Status {response.status_code}: {detail['message'] or detail['detail']}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("The REST index returned a non-JSON response.") from exc

    routes = payload.get("routes", {})
    if not isinstance(routes, dict):
        raise RuntimeError("The REST index did not include a route list.")

    discovered_routes = []
    known_route_set = set()
    for endpoint_path, label in KNOWN_ORDER_ENDPOINTS:
        if endpoint_path in routes:
            discovered_routes.append(
                {
                    "endpoint_path": endpoint_path,
                    "label": label,
                    "source": "known",
                }
            )
            known_route_set.add(endpoint_path)

    extra_routes = sorted(
        route
        for route in routes
        if "order" in route.lower() and route not in known_route_set
    )
    for route in extra_routes:
        discovered_routes.append(
            {
                "endpoint_path": route,
                "label": "Additional order-related route",
                "source": "discovered",
            }
        )

    suggested_endpoint_path = ""
    for preferred in (
        "/wp-json/wc/v3/orders",
        "/wp-json/wc/v2/orders",
        "/wp-json/wp/v2/orders",
    ):
        if preferred in {row["endpoint_path"] for row in discovered_routes}:
            suggested_endpoint_path = preferred
            break
    if not suggested_endpoint_path and discovered_routes:
        suggested_endpoint_path = discovered_routes[0]["endpoint_path"]

    supports_application_passwords = bool(
        payload.get("authentication", {}).get("application-passwords")
    )

    return {
        "root_url": root_url,
        "supports_application_passwords": supports_application_passwords,
        "route_count": len(routes),
        "routes": discovered_routes,
        "suggested_endpoint_path": suggested_endpoint_path,
    }


def verify_wp_orders_endpoint_access(
    site_url: str,
    endpoint_path: str,
    auth_mode: str,
    username: str = "",
    app_password: str = "",
    consumer_key: str = "",
    consumer_secret: str = "",
    timeout_seconds: int = 20,
) -> dict:
    """Probe the selected endpoint and explain whether the route exists and whether auth worked."""
    endpoint = build_wp_api_orders_endpoint(site_url, endpoint_path)
    auth, auth_params = build_wp_api_auth_request_options(
        auth_mode=auth_mode,
        username=username,
        app_password=app_password,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
    )
    params = {"per_page": 1}
    params.update(auth_params)
    response = requests.get(endpoint, params=params, auth=auth, timeout=timeout_seconds)
    detail = _extract_wp_api_error_details(response)
    hint = _build_wp_orders_hint(
        response.status_code, detail["code"], auth_mode, endpoint_path
    )
    return {
        "endpoint": endpoint,
        "endpoint_path": endpoint_path,
        "auth_mode": auth_mode,
        "status_code": response.status_code,
        "code": detail["code"],
        "message": detail["message"],
        "detail": detail["detail"],
        "hint": hint,
        "verified_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def verify_wp_application_user_access(
    site_url: str,
    endpoint_path: str,
    username: str,
    app_password: str,
    timeout_seconds: int = 20,
) -> dict:
    """Check whether a WP application-password login authenticates and can read WooCommerce orders."""
    auth, _ = build_wp_api_auth_request_options(
        auth_mode=AUTH_MODE_APPLICATION_PASSWORD,
        username=username,
        app_password=app_password,
    )
    if not auth:
        raise ValueError("Username and application password are required.")

    user_endpoint = build_wp_api_orders_endpoint(site_url, "/wp-json/wp/v2/users/me")
    orders_endpoint = build_wp_api_orders_endpoint(
        site_url, endpoint_path or DEFAULT_WP_ORDERS_ENDPOINT_PATH
    )

    user_response = requests.get(
        user_endpoint,
        params={"context": "edit"},
        auth=auth,
        timeout=timeout_seconds,
    )
    user_detail = _extract_wp_api_error_details(user_response)
    try:
        user_payload = user_response.json() if user_response.status_code == 200 else {}
    except ValueError:
        user_payload = {}

    orders_response = requests.get(
        orders_endpoint,
        params={"per_page": 1},
        auth=auth,
        timeout=timeout_seconds,
    )
    orders_detail = _extract_wp_api_error_details(orders_response)

    authenticated = user_response.status_code == 200
    orders_access = orders_response.status_code == 200
    roles = user_payload.get("roles") if isinstance(user_payload, dict) else []
    if not isinstance(roles, list):
        roles = []

    user_name = username.strip()
    if isinstance(user_payload, dict):
        user_name = str(
            user_payload.get("name")
            or user_payload.get("slug")
            or user_payload.get("username")
            or user_name
        )

    if authenticated and orders_access:
        hint = "The WordPress user authenticated successfully and can read WooCommerce orders."
    elif authenticated:
        hint = (
            "The WordPress user authenticated, but WooCommerce order access was denied. "
            "Use an Administrator or Shop Manager account with permission to read orders."
        )
    else:
        hint = (
            "The WordPress application-password login was not accepted by the site. "
            "The credentials may be wrong, the user may not have access, or the server may be blocking the Authorization header."
        )

    return {
        "user_endpoint": user_endpoint,
        "orders_endpoint": orders_endpoint,
        "authenticated": authenticated,
        "orders_access": orders_access,
        "user_status_code": user_response.status_code,
        "orders_status_code": orders_response.status_code,
        "user_code": user_detail["code"],
        "orders_code": orders_detail["code"],
        "user_message": user_detail["message"] or user_detail["detail"],
        "orders_message": orders_detail["message"] or orders_detail["detail"],
        "user_name": user_name,
        "roles": roles,
        "hint": hint,
        "verified_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def fetch_wp_api_orders(
    site_url: str,
    endpoint_path: str,
    auth_mode: str,
    username: str,
    app_password: str,
    consumer_key: str,
    consumer_secret: str,
    start_date: date,
    end_date: date,
    per_page: int = 100,
    timeout_seconds: int = 30,
):
    """Fetch paginated order data from a WooCommerce-compatible REST endpoint."""
    endpoint = build_wp_api_orders_endpoint(site_url, endpoint_path)
    params = build_wp_api_orders_params(start_date, end_date, per_page=per_page, page=1)
    auth, auth_params = build_wp_api_auth_request_options(
        auth_mode=auth_mode,
        username=username,
        app_password=app_password,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
    )
    params.update(auth_params)

    all_records = []
    pages_fetched = 0
    total_pages_header = None

    while True:
        response = requests.get(
            endpoint,
            params=params,
            auth=auth,
            timeout=timeout_seconds,
        )

        if response.status_code != 200:
            detail = _extract_wp_api_error_details(response)
            hint = _build_wp_orders_hint(
                response.status_code, detail["code"], auth_mode, endpoint_path
            )
            message = (
                detail["message"]
                or detail["detail"]
                or "No API error message returned."
            )
            raise RuntimeError(
                f"API request failed with status {response.status_code}: {message} Hint: {hint}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("The API returned a non-JSON response.") from exc

        records = extract_wp_api_records(payload)
        if not records:
            break

        all_records.extend(records)
        pages_fetched += 1

        total_pages_header = response.headers.get("X-WP-TotalPages")
        if total_pages_header and params["page"] >= int(total_pages_header):
            break
        if len(records) < params["per_page"]:
            break
        if params["page"] >= 200:
            raise RuntimeError(
                "Stopped after 200 pages to avoid an endless pagination loop."
            )

        params["page"] += 1

    df = normalize_wp_orders_dataframe(all_records)
    meta = {
        "endpoint": endpoint,
        "endpoint_path": endpoint_path,
        "pages_fetched": pages_fetched,
        "records_found": len(df),
        "date_range": f"{start_date} to {end_date}",
        "total_pages_header": total_pages_header or "",
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "auth_mode": auth_mode,
    }
    return df, meta


def _pick_first_existing_column(df: pd.DataFrame, candidates) -> str:
    for col in candidates:
        if col in df.columns:
            return col
    return ""


def _get_wp_orders_date_series(df: pd.DataFrame):
    for col in (
        "date_created",
        "date_created_gmt",
        "date_paid",
        "date_paid_gmt",
        "date_completed",
        "date_completed_gmt",
        "date",
    ):
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notna().any():
                return parsed, col
    return pd.Series(pd.NaT, index=df.index), ""


def _parse_nested_json(value):
    if isinstance(value, (list, dict)):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    text = str(value).strip()
    if not text:
        return []
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return []


def extract_wp_order_line_items_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if "line_items" not in df.columns:
        return pd.DataFrame()

    rows = []
    for _, order_row in df.iterrows():
        items = _parse_nested_json(order_row.get("line_items"))
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "order_id": order_row.get("id", ""),
                    "order_date": order_row.get("_order_date"),
                    "product_name": item.get("name")
                    or item.get("product_name")
                    or "Unknown product",
                    "sku": item.get("sku") or "",
                    "quantity": pd.to_numeric(item.get("quantity"), errors="coerce"),
                    "line_total": pd.to_numeric(item.get("total"), errors="coerce"),
                }
            )

    if not rows:
        return pd.DataFrame()

    items_df = pd.DataFrame(rows)
    items_df["quantity"] = items_df["quantity"].fillna(0)
    items_df["line_total"] = items_df["line_total"].fillna(0.0)
    return items_df


def build_wp_orders_dashboard_frames(df: pd.DataFrame) -> dict:
    """Create summary tables and metrics for the WooCommerce orders dashboard."""
    working_df = df.copy()

    parsed_dates, detected_date_col = _get_wp_orders_date_series(working_df)
    working_df["_parsed_order_ts"] = parsed_dates
    working_df["_order_date"] = parsed_dates.dt.normalize()

    total_col = _pick_first_existing_column(working_df, ["total"])
    if total_col:
        working_df["_order_total"] = pd.to_numeric(
            working_df[total_col], errors="coerce"
        ).fillna(0.0)
    else:
        working_df["_order_total"] = 0.0

    status_col = _pick_first_existing_column(working_df, ["status"])
    payment_col = _pick_first_existing_column(
        working_df, ["payment_method_title", "payment_method"]
    )
    city_col = _pick_first_existing_column(
        working_df, ["shipping.city", "billing.city"]
    )
    customer_col = _pick_first_existing_column(
        working_df, ["billing.phone", "billing.email", "customer_id"]
    )
    currency_col = _pick_first_existing_column(working_df, ["currency"])

    currency = ""
    if currency_col:
        currency_values = working_df[currency_col].replace("", pd.NA).dropna()
        if not currency_values.empty:
            currency = str(currency_values.iloc[0])

    valid_dates_df = working_df[working_df["_order_date"].notna()].copy()
    if not valid_dates_df.empty:
        daily_sales = (
            valid_dates_df.groupby("_order_date", as_index=False)
            .agg(
                orders=(
                    ("id", "count")
                    if "id" in valid_dates_df.columns
                    else ("_order_total", "size")
                ),
                gross_sales=("_order_total", "sum"),
            )
            .rename(columns={"_order_date": "order_date"})
        )
        daily_sales["order_date"] = pd.to_datetime(daily_sales["order_date"])
    else:
        daily_sales = pd.DataFrame(columns=["order_date", "orders", "gross_sales"])

    if status_col:
        status_summary = (
            working_df.groupby(status_col, dropna=False, as_index=False)
            .agg(
                orders=(
                    ("id", "count")
                    if "id" in working_df.columns
                    else ("_order_total", "size")
                ),
                gross_sales=("_order_total", "sum"),
            )
            .rename(columns={status_col: "status"})
            .sort_values(["orders", "gross_sales"], ascending=False)
        )
        status_summary["status"] = (
            status_summary["status"].fillna("Unknown").astype(str)
        )
    else:
        status_summary = pd.DataFrame(columns=["status", "orders", "gross_sales"])

    if payment_col:
        payment_summary = (
            working_df.groupby(payment_col, dropna=False, as_index=False)
            .agg(
                orders=(
                    ("id", "count")
                    if "id" in working_df.columns
                    else ("_order_total", "size")
                ),
                gross_sales=("_order_total", "sum"),
            )
            .rename(columns={payment_col: "payment_method"})
            .sort_values(["gross_sales", "orders"], ascending=False)
        )
        payment_summary["payment_method"] = (
            payment_summary["payment_method"].fillna("Unknown").astype(str)
        )
    else:
        payment_summary = pd.DataFrame(
            columns=["payment_method", "orders", "gross_sales"]
        )

    if city_col:
        city_summary = (
            working_df.groupby(city_col, dropna=False, as_index=False)
            .agg(
                orders=(
                    ("id", "count")
                    if "id" in working_df.columns
                    else ("_order_total", "size")
                ),
                gross_sales=("_order_total", "sum"),
            )
            .rename(columns={city_col: "city"})
            .sort_values(["orders", "gross_sales"], ascending=False)
        )
        city_summary["city"] = city_summary["city"].fillna("Unknown").astype(str)
    else:
        city_summary = pd.DataFrame(columns=["city", "orders", "gross_sales"])

    line_items_df = extract_wp_order_line_items_dataframe(working_df)
    if not line_items_df.empty:
        product_summary = (
            line_items_df.groupby(["product_name", "sku"], dropna=False, as_index=False)
            .agg(
                quantity=("quantity", "sum"),
                gross_sales=("line_total", "sum"),
            )
            .sort_values(["gross_sales", "quantity"], ascending=False)
        )
    else:
        product_summary = pd.DataFrame(
            columns=["product_name", "sku", "quantity", "gross_sales"]
        )

    unique_customers = 0
    if customer_col:
        unique_customers = int(
            working_df[customer_col].replace("", pd.NA).dropna().astype(str).nunique()
        )

    metrics = {
        "orders": int(len(working_df)),
        "gross_sales": float(working_df["_order_total"].sum()),
        "avg_order_value": (
            float(working_df["_order_total"].mean()) if len(working_df) else 0.0
        ),
        "unique_customers": unique_customers,
        "items_sold": (
            float(line_items_df["quantity"].sum()) if not line_items_df.empty else 0.0
        ),
        "currency": currency,
        "detected_date_column": detected_date_col,
    }

    return {
        "working_df": working_df,
        "daily_sales": daily_sales,
        "status_summary": status_summary,
        "payment_summary": payment_summary,
        "city_summary": city_summary,
        "product_summary": product_summary,
        "line_items_df": line_items_df,
        "metrics": metrics,
    }


def build_wp_orders_dashboard_export_bytes(
    raw_df: pd.DataFrame,
    dashboard_frames: dict,
) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        raw_df.to_excel(writer, sheet_name="Orders", index=False)
        dashboard_frames["daily_sales"].to_excel(
            writer, sheet_name="Daily Sales", index=False
        )
        dashboard_frames["status_summary"].to_excel(
            writer, sheet_name="Status Summary", index=False
        )
        dashboard_frames["payment_summary"].to_excel(
            writer, sheet_name="Payment Summary", index=False
        )
        dashboard_frames["city_summary"].to_excel(
            writer, sheet_name="City Summary", index=False
        )
        dashboard_frames["product_summary"].to_excel(
            writer, sheet_name="Product Summary", index=False
        )
    return buf.getvalue()


def _format_money(value: float, currency: str) -> str:
    prefix = f"{currency} " if currency else ""
    return f"{prefix}{value:,.2f}"


def _render_chart(fig, key: str):
    st.plotly_chart(
        fig,
        width="stretch",
        config={"scrollZoom": True, "displayModeBar": True},
        key=key,
    )


def _clear_wp_api_orders_state():
    for key in (
        WP_API_ORDERS_RESULT_KEY,
        WP_API_ORDERS_META_KEY,
        WP_API_ORDERS_DIAGNOSTICS_KEY,
        WP_API_ORDERS_DISCOVERY_KEY,
        WP_API_ORDERS_USER_ACCESS_KEY,
    ):
        st.session_state.pop(key, None)


def _render_discovery_results(discovery: dict):
    st.subheader("Endpoint Discovery")
    st.caption(f"REST index: {discovery.get('root_url', 'N/A')}")
    st.caption(f"Routes in index: {discovery.get('route_count', 0):,}")
    st.caption(
        "Application passwords advertised: "
        + ("Yes" if discovery.get("supports_application_passwords") else "No")
    )

    routes = discovery.get("routes", [])
    if not routes:
        st.warning("No order-related routes were found in the REST index.")
        return

    route_df = pd.DataFrame(routes).rename(
        columns={
            "endpoint_path": "Endpoint Path",
            "label": "Route",
            "source": "Source",
        }
    )
    st.dataframe(route_df, width="stretch", hide_index=True)

    suggested_endpoint = discovery.get("suggested_endpoint_path")
    if suggested_endpoint:
        st.success(f"Suggested orders endpoint: {suggested_endpoint}")


def _render_verification_result(verification: dict):
    status_code = verification.get("status_code", 0)
    message = (
        verification.get("message")
        or verification.get("detail")
        or "No message returned."
    )
    summary = (
        f"Verification result: HTTP {status_code} for {verification.get('endpoint_path', '')}. "
        f"{verification.get('hint', '')}"
    )

    if status_code == 200:
        st.success(summary)
    elif status_code == 404:
        st.error(summary)
    else:
        st.warning(summary)

    diagnostics = pd.DataFrame(
        [
            {"Field": "Verified at", "Value": verification.get("verified_at", "N/A")},
            {"Field": "Endpoint", "Value": verification.get("endpoint", "N/A")},
            {"Field": "Auth mode", "Value": verification.get("auth_mode", "N/A")},
            {"Field": "Status", "Value": status_code},
            {"Field": "Code", "Value": verification.get("code", "") or "-"},
            {"Field": "Message", "Value": message},
            {"Field": "Hint", "Value": verification.get("hint", "")},
        ]
    )
    with st.expander("Verification diagnostics", expanded=status_code != 200):
        st.dataframe(diagnostics, width="stretch", hide_index=True)


def _render_user_access_result(access: dict):
    if access.get("authenticated") and access.get("orders_access"):
        st.success(
            access.get("hint", "The WordPress user can read WooCommerce orders.")
        )
    elif access.get("authenticated"):
        st.warning(
            access.get(
                "hint",
                "The WordPress user authenticated, but orders access was denied.",
            )
        )
    else:
        st.error(access.get("hint", "The WordPress user could not be authenticated."))

    roles = ", ".join(access.get("roles", [])) or "Unavailable"
    diagnostics = pd.DataFrame(
        [
            {"Field": "Verified at", "Value": access.get("verified_at", "N/A")},
            {"Field": "User", "Value": access.get("user_name", "N/A")},
            {"Field": "Roles", "Value": roles},
            {
                "Field": "Core auth status",
                "Value": access.get("user_status_code", "N/A"),
            },
            {
                "Field": "Orders access status",
                "Value": access.get("orders_status_code", "N/A"),
            },
            {
                "Field": "Core auth message",
                "Value": access.get("user_message", "") or "-",
            },
            {
                "Field": "Orders access message",
                "Value": access.get("orders_message", "") or "-",
            },
            {"Field": "Hint", "Value": access.get("hint", "")},
        ]
    )
    with st.expander(
        "WP user access diagnostics", expanded=not access.get("orders_access", False)
    ):
        st.dataframe(diagnostics, width="stretch", hide_index=True)


def _render_wp_orders_dashboard(result_df: pd.DataFrame, result_meta: dict):
    dashboard_frames = build_wp_orders_dashboard_frames(result_df)
    metrics = dashboard_frames["metrics"]
    currency = metrics.get("currency", "")

    metric_1, metric_2, metric_3, metric_4, metric_5 = st.columns(5)
    from src.ui.components import render_metric_hud

    with metric_1:
        render_metric_hud("Orders", f"{metrics['orders']:,}", "🛒")
    with metric_2:
        render_metric_hud(
            "Gross Sales", _format_money(metrics["gross_sales"], currency), "💰"
        )
    with metric_3:
        render_metric_hud(
            "Avg Order Value", _format_money(metrics["avg_order_value"], currency), "🛍️"
        )
    with metric_4:
        render_metric_hud("Unique Customers", f"{metrics['unique_customers']:,}", "👥")
    with metric_5:
        render_metric_hud("Items Sold", f"{metrics['items_sold']:,.0f}", "📦")

    st.caption(f"Date range: {result_meta.get('date_range', 'N/A')}")
    st.caption(f"Endpoint: {result_meta.get('endpoint', 'N/A')}")
    st.caption(f"Auth mode: {result_meta.get('auth_mode', 'N/A')}")
    if metrics.get("detected_date_column"):
        st.caption(f"Detected order date column: {metrics['detected_date_column']}")

    daily_sales = dashboard_frames["daily_sales"]
    status_summary = dashboard_frames["status_summary"]
    payment_summary = dashboard_frames["payment_summary"]
    city_summary = dashboard_frames["city_summary"]
    product_summary = dashboard_frames["product_summary"]

    if not daily_sales.empty:
        chart_left, chart_right = st.columns(2)
        with chart_left:
            fig_daily = px.line(
                daily_sales,
                x="order_date",
                y="gross_sales",
                markers=True,
                title="Daily Gross Sales",
            )
            fig_daily.update_layout(
                margin=dict(l=12, r=12, t=50, b=12),
                xaxis_title="Order Date",
                yaxis_title="Gross Sales",
            )
            _render_chart(fig_daily, "wp_api_orders_daily_sales_chart")
        with chart_right:
            fig_daily_orders = px.bar(
                daily_sales,
                x="order_date",
                y="orders",
                title="Daily Order Count",
                text_auto=".0f",
            )
            fig_daily_orders.update_layout(
                margin=dict(l=12, r=12, t=50, b=12),
                xaxis_title="Order Date",
                yaxis_title="Orders",
            )
            _render_chart(fig_daily_orders, "wp_api_orders_daily_orders_chart")

    summary_left, summary_right = st.columns(2)
    with summary_left:
        if not status_summary.empty:
            fig_status = px.pie(
                status_summary,
                values="orders",
                names="status",
                hole=0.55,
                title="Orders by Status",
            )
            fig_status.update_layout(margin=dict(l=12, r=12, t=50, b=12))
            _render_chart(fig_status, "wp_api_orders_status_chart")
        else:
            st.info("No status data was returned by the API.")
    with summary_right:
        if not payment_summary.empty:
            payment_chart = payment_summary.head(10)
            fig_payment = px.bar(
                payment_chart,
                x="payment_method",
                y="gross_sales",
                title="Sales by Payment Method",
                text_auto=".2s",
            )
            fig_payment.update_layout(
                margin=dict(l=12, r=12, t=50, b=12),
                xaxis_title="Payment Method",
                yaxis_title="Gross Sales",
            )
            _render_chart(fig_payment, "wp_api_orders_payment_chart")
        else:
            st.info("No payment-method data was returned by the API.")

    detail_left, detail_right = st.columns(2)
    with detail_left:
        if not city_summary.empty:
            city_chart = city_summary.head(10).sort_values("orders", ascending=True)
            fig_city = px.bar(
                city_chart,
                x="orders",
                y="city",
                orientation="h",
                title="Top Cities by Orders",
                text_auto=".0f",
            )
            fig_city.update_layout(
                margin=dict(l=12, r=12, t=50, b=12),
                xaxis_title="Orders",
                yaxis_title="",
            )
            _render_chart(fig_city, "wp_api_orders_city_chart")
        else:
            st.info("No city information was returned by the API.")
    with detail_right:
        if not product_summary.empty:
            product_chart = product_summary.head(10).sort_values(
                "gross_sales", ascending=True
            )
            fig_products = px.bar(
                product_chart,
                x="gross_sales",
                y="product_name",
                orientation="h",
                title="Top Products by Sales",
                text_auto=".2s",
            )
            fig_products.update_layout(
                margin=dict(l=12, r=12, t=50, b=12),
                xaxis_title="Gross Sales",
                yaxis_title="",
            )
            _render_chart(fig_products, "wp_api_orders_products_chart")
        else:
            st.info("No line-item product details were returned by the API.")

    csv_bytes = result_df.to_csv(index=False).encode("utf-8-sig")
    excel_bytes = build_wp_orders_dashboard_export_bytes(result_df, dashboard_frames)
    date_range_label = (
        result_meta.get("date_range", "").replace(" ", "").replace("to", "_")
    )

    export_1, export_2 = st.columns(2)
    export_1.download_button(
        "Download Orders CSV",
        csv_bytes,
        file_name=f"woocommerce_orders_{date_range_label or 'report'}.csv",
        mime="text/csv",
        width="stretch",
        type="primary",
        key="wp_api_orders_download_csv",
    )
    export_2.download_button(
        "Download Dashboard Excel",
        excel_bytes,
        file_name=f"woocommerce_orders_dashboard_{date_range_label or 'report'}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
        key="wp_api_orders_download_excel",
    )

    preview_cols = get_wp_orders_preview_columns(result_df)
    preview_tabs = st.tabs(["Orders Preview", "Status Summary", "Products", "Raw Data"])
    with preview_tabs[0]:
        st.dataframe(
            result_df[preview_cols].head(200), width="stretch", hide_index=True
        )
    with preview_tabs[1]:
        st.dataframe(status_summary, width="stretch", hide_index=True)
    with preview_tabs[2]:
        st.dataframe(product_summary.head(100), width="stretch", hide_index=True)
    with preview_tabs[3]:
        st.dataframe(result_df.head(200), width="stretch", hide_index=True)

    with st.expander("Returned columns", expanded=False):
        st.write(", ".join(result_df.columns))


def render_wp_api_orders_tab():
    if st.session_state.get("wp_api_orders_site_url") == "http://deencommerce.com/":
        st.session_state["wp_api_orders_site_url"] = DEFAULT_WP_ORDERS_SITE_URL
    if st.session_state.get("wp_api_orders_endpoint_path") in (
        "",
        LEGACY_WP_ORDERS_ENDPOINT_PATH,
    ):
        st.session_state["wp_api_orders_endpoint_path"] = (
            DEFAULT_WP_ORDERS_ENDPOINT_PATH
        )

    section_card(
        "WooCommerce Orders Dashboard",
        "Verify the orders endpoint, fetch orders for any date range, and turn the result into a dashboard with sales and status analytics.",
    )
    st.info(
        "Credentials stay in the current browser session only. The app's Save session state feature does not write them to disk."
    )

    today = datetime.now().date()
    month_start = today.replace(day=1)

    site_url = st.text_input(
        "Site URL",
        value=st.session_state.get(
            "wp_api_orders_site_url", DEFAULT_WP_ORDERS_SITE_URL
        ),
        key="wp_api_orders_site_url",
        help="Use the site root, for example https://deencommerce.com/.",
    )

    if "deencommerce.com" in (site_url or "").lower():
        st.caption(
            "Verified on March 24, 2026: deencommerce.com returned 404 for /wp-json/wp/v2/orders, "
            "and exposed /wp-json/wc/v3/orders instead."
        )

    endpoint_path = st.text_input(
        "API endpoint path",
        value=st.session_state.get(
            "wp_api_orders_endpoint_path", DEFAULT_WP_ORDERS_ENDPOINT_PATH
        ),
        key="wp_api_orders_endpoint_path",
        help="The verified default for deencommerce.com is /wp-json/wc/v3/orders.",
    )
    if site_url and endpoint_path:
        try:
            st.caption(
                f"Resolved endpoint: {build_wp_api_orders_endpoint(site_url, endpoint_path)}"
            )
        except ValueError:
            pass

    auth_mode = st.selectbox(
        "Authentication mode",
        options=AUTH_MODE_OPTIONS,
        index=AUTH_MODE_OPTIONS.index(
            st.session_state.get(
                "wp_api_orders_auth_mode", AUTH_MODE_APPLICATION_PASSWORD
            )
            if st.session_state.get(
                "wp_api_orders_auth_mode", AUTH_MODE_APPLICATION_PASSWORD
            )
            in AUTH_MODE_OPTIONS
            else AUTH_MODE_APPLICATION_PASSWORD
        ),
        key="wp_api_orders_auth_mode",
    )

    username = ""
    app_password = ""
    consumer_key = ""
    consumer_secret = ""

    if auth_mode == AUTH_MODE_APPLICATION_PASSWORD:
        c1, c2 = st.columns(2)
        username = c1.text_input(
            "WP username / email",
            value=st.session_state.get("wp_api_orders_username", ""),
            key="wp_api_orders_username",
        )
        app_password = c2.text_input(
            "Application password",
            value="",
            type="password",
            key="wp_api_orders_app_password",
        )
        st.caption(
            "Use a WordPress user that can read WooCommerce orders, ideally Administrator or Shop Manager. "
            "If auth still fails, the site may be blocking the Authorization header."
        )
    elif auth_mode == AUTH_MODE_WOO_KEYS:
        c1, c2 = st.columns(2)
        consumer_key = c1.text_input(
            "Consumer key",
            value=st.session_state.get("wp_api_orders_consumer_key", ""),
            key="wp_api_orders_consumer_key",
        )
        consumer_secret = c2.text_input(
            "Consumer secret",
            value="",
            type="password",
            key="wp_api_orders_consumer_secret",
        )
        st.caption(
            "Use WooCommerce REST API keys with at least read access. This mode sends the keys as query parameters."
        )
    else:
        st.caption(
            "Use this only for public or custom routes that expose orders without authentication."
        )

    c3, c4, c5, c6 = st.columns(4)
    start_date = c3.date_input(
        "Start date",
        value=st.session_state.get("wp_api_orders_start_date", month_start),
        key="wp_api_orders_start_date",
    )
    end_date = c4.date_input(
        "End date",
        value=st.session_state.get("wp_api_orders_end_date", today),
        key="wp_api_orders_end_date",
    )
    per_page = c5.number_input(
        "Per page",
        min_value=1,
        max_value=100,
        value=int(st.session_state.get("wp_api_orders_per_page", 100)),
        step=1,
        key="wp_api_orders_per_page",
        help="WooCommerce REST endpoints commonly cap this at 100.",
    )
    timeout_seconds = c6.number_input(
        "Timeout (seconds)",
        min_value=5,
        max_value=120,
        value=int(st.session_state.get("wp_api_orders_timeout_seconds", 30)),
        step=5,
        key="wp_api_orders_timeout_seconds",
    )

    check_user_clicked = False
    if auth_mode == AUTH_MODE_APPLICATION_PASSWORD:
        action_1, action_2, action_3, action_4, action_5 = st.columns(5)
        discover_clicked = action_1.button(
            "Discover endpoints",
            width="stretch",
            key="wp_api_orders_discover_button",
        )
        check_user_clicked = action_2.button(
            "Check WP user access",
            width="stretch",
            key="wp_api_orders_check_user_button",
        )
        verify_clicked = action_3.button(
            "Verify selected endpoint",
            width="stretch",
            key="wp_api_orders_verify_button",
        )
        fetch_clicked = action_4.button(
            "Fetch dashboard data",
            type="primary",
            width="stretch",
            key="wp_api_orders_fetch_button",
        )
        clear_clicked = action_5.button(
            "Clear",
            width="stretch",
            key="wp_api_orders_clear_button",
        )
    else:
        action_1, action_2, action_3, action_4 = st.columns(4)
        discover_clicked = action_1.button(
            "Discover endpoints",
            width="stretch",
            key="wp_api_orders_discover_button",
        )
        verify_clicked = action_2.button(
            "Verify selected endpoint",
            width="stretch",
            key="wp_api_orders_verify_button",
        )
        fetch_clicked = action_3.button(
            "Fetch dashboard data",
            type="primary",
            width="stretch",
            key="wp_api_orders_fetch_button",
        )
        clear_clicked = action_4.button(
            "Clear",
            width="stretch",
            key="wp_api_orders_clear_button",
        )

    if clear_clicked:
        _clear_wp_api_orders_state()
        st.rerun()

    if discover_clicked:
        try:
            with st.spinner("Inspecting the REST index..."):
                discovery = discover_wp_orders_endpoints(
                    site_url, timeout_seconds=int(timeout_seconds)
                )
            st.session_state[WP_API_ORDERS_DISCOVERY_KEY] = discovery
            st.success("REST routes discovered successfully.")
        except Exception as exc:
            log_error(exc, context="WP API Orders Discovery")
            st.error(f"Endpoint discovery error: {exc}")

    if check_user_clicked:
        if not site_url.strip():
            st.error("Site URL is required.")
        elif not username.strip() or not app_password.strip():
            st.error(
                "Username and application password are required to check WP user access."
            )
        else:
            try:
                with st.spinner("Checking WordPress user access..."):
                    user_access = verify_wp_application_user_access(
                        site_url=site_url,
                        endpoint_path=endpoint_path,
                        username=username,
                        app_password=app_password,
                        timeout_seconds=int(timeout_seconds),
                    )
                st.session_state[WP_API_ORDERS_USER_ACCESS_KEY] = user_access
            except Exception as exc:
                log_error(exc, context="WP API Orders User Access")
                st.error(f"WP user access check error: {exc}")

    if verify_clicked:
        if not site_url.strip():
            st.error("Site URL is required.")
        elif not endpoint_path.strip():
            st.error("API endpoint path is required.")
        elif auth_mode == AUTH_MODE_APPLICATION_PASSWORD and (
            not username.strip() or not app_password.strip()
        ):
            st.error(
                "Username and application password are required for application-password verification."
            )
        elif auth_mode == AUTH_MODE_WOO_KEYS and (
            not consumer_key.strip() or not consumer_secret.strip()
        ):
            st.error(
                "Consumer key and consumer secret are required for WooCommerce key verification."
            )
        else:
            try:
                with st.spinner("Verifying the selected endpoint..."):
                    verification = verify_wp_orders_endpoint_access(
                        site_url=site_url,
                        endpoint_path=endpoint_path,
                        auth_mode=auth_mode,
                        username=username,
                        app_password=app_password,
                        consumer_key=consumer_key,
                        consumer_secret=consumer_secret,
                        timeout_seconds=int(timeout_seconds),
                    )
                st.session_state[WP_API_ORDERS_DIAGNOSTICS_KEY] = verification
            except Exception as exc:
                log_error(exc, context="WP API Orders Verify")
                st.error(f"Endpoint verification error: {exc}")

    if fetch_clicked:
        if not site_url.strip():
            st.error("Site URL is required.")
        elif not endpoint_path.strip():
            st.error("API endpoint path is required.")
        elif auth_mode == AUTH_MODE_APPLICATION_PASSWORD and (
            not username.strip() or not app_password.strip()
        ):
            st.error("Username and application password are required.")
        elif auth_mode == AUTH_MODE_WOO_KEYS and (
            not consumer_key.strip() or not consumer_secret.strip()
        ):
            st.error("Consumer key and consumer secret are required.")
        elif start_date > end_date:
            st.error("Start date cannot be after end date.")
        else:
            try:
                if auth_mode == AUTH_MODE_APPLICATION_PASSWORD:
                    user_access = verify_wp_application_user_access(
                        site_url=site_url,
                        endpoint_path=endpoint_path,
                        username=username,
                        app_password=app_password,
                        timeout_seconds=int(timeout_seconds),
                    )
                    st.session_state[WP_API_ORDERS_USER_ACCESS_KEY] = user_access
                    if not user_access.get("orders_access"):
                        raise RuntimeError(
                            user_access.get(
                                "hint",
                                "The WordPress user cannot read WooCommerce orders.",
                            )
                        )
                with st.spinner("Fetching orders from the WooCommerce API..."):
                    df, meta = fetch_wp_api_orders(
                        site_url=site_url,
                        endpoint_path=endpoint_path,
                        auth_mode=auth_mode,
                        username=username,
                        app_password=app_password,
                        consumer_key=consumer_key,
                        consumer_secret=consumer_secret,
                        start_date=start_date,
                        end_date=end_date,
                        per_page=int(per_page),
                        timeout_seconds=int(timeout_seconds),
                    )
                st.session_state[WP_API_ORDERS_RESULT_KEY] = df
                st.session_state[WP_API_ORDERS_META_KEY] = meta
                st.session_state[WP_API_ORDERS_DIAGNOSTICS_KEY] = {
                    "endpoint": meta["endpoint"],
                    "endpoint_path": meta["endpoint_path"],
                    "auth_mode": meta["auth_mode"],
                    "status_code": 200,
                    "code": "",
                    "message": f"Fetched {len(df):,} orders successfully.",
                    "detail": "",
                    "hint": "Endpoint verified and data fetched successfully.",
                    "verified_at": meta["fetched_at"],
                }
                if df.empty:
                    st.warning(
                        "The API request succeeded, but no orders matched the selected date range."
                    )
                else:
                    st.success(f"Fetched {len(df):,} orders from the API.")
            except Exception as exc:
                log_error(exc, context="WP API Orders Report")
                st.error(f"WooCommerce orders error: {exc}")

    discovery = st.session_state.get(WP_API_ORDERS_DISCOVERY_KEY)
    if discovery:
        _render_discovery_results(discovery)

    user_access = st.session_state.get(WP_API_ORDERS_USER_ACCESS_KEY)
    if user_access:
        _render_user_access_result(user_access)

    diagnostics = st.session_state.get(WP_API_ORDERS_DIAGNOSTICS_KEY)
    if diagnostics:
        _render_verification_result(diagnostics)

    result_df = st.session_state.get(WP_API_ORDERS_RESULT_KEY)
    result_meta = st.session_state.get(WP_API_ORDERS_META_KEY, {})

    if result_df is None:
        st.caption(
            "Start with `Discover endpoints` if you are unsure about the route, then verify the selected endpoint and click `Fetch dashboard data`."
        )
        return

    if result_df.empty:
        st.info(
            "The request completed, but there are no orders to chart for this date range."
        )
        return

    _render_wp_orders_dashboard(result_df, result_meta)
