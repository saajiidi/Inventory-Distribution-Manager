# DEEN Commerce BI - AI Agent Coder Guide

> **Purpose**: Comprehensive blueprint for AI agents to understand, navigate, and extend this codebase.
> **Last Updated**: 2026-04-20
> **Project**: DEEN Commerce Business Intelligence Dashboard
> **Auto-Update**: This file is automatically updated via pre-commit hooks and GitHub Actions

---

## 1. Project Overview

**DEEN-BI** is a Streamlit-based Business Intelligence dashboard for DEEN Commerce that:
- Synchronizes with WooCommerce API for sales/order data
- Tracks returns, partials, and exchanges via Google Sheets integration
- Provides real-time KPIs, analytics, and ML-powered insights
- Features fast-loading skeleton UI with background data loading

### Core Capabilities
1. **Sales Analytics**: Revenue, orders, customer insights, geographic intelligence
2. **Returns Tracking**: Classification, item-level tracking, financial impact
3. **Inventory Health**: Stock levels, reorder forecasting
4. **Customer Intelligence**: RFM scoring, lifetime value, segmentation
5. **Operational Monitoring**: Real-time sync status, data freshness

---

## 2. Project Structure

```
DEEN-BI/
├── app.py                          # Main entry point, Streamlit page config, sidebar navigation
├── requirements.txt                # Python dependencies
├── pyproject.toml                  # Project metadata, tools config
│
├── BackEnd/                        # Backend services & data processing
│   ├── core/                       # Core utilities
│   │   ├── categories.py           # Product category mapping
│   │   ├── geo.py                  # Geographic/district code resolution
│   │   ├── logging_config.py       # Centralized logging
│   │   ├── ml_engine.py            # ML/forecasting engine
│   │   ├── paths.py                # System paths
│   │   ├── zones.py                # Zone/Area resolution
│   │   └── woocommerce_service.py  # Core WooCommerce API service
│   │
│   ├── services/                   # Business logic services
│   │   ├── returns_tracker.py      # Returns data loading, classification
│   │   ├── hybrid_data_loader.py   # WooCommerce data with caching & background refresh
│   │   ├── customer_manager.py     # Customer insights, deduplication
│   │   ├── customer_insights.py    # RFM scoring and segmentation
│   │   ├── strategic_intelligence.py # Executive narratives
│   │   ├── powerbi_export.py       # PowerBI integration
│   │   ├── processor.py            # Orders dataframe processing
│   │   └── woocommerce_client/      # Advanced WooCommerce API clients
│   │       ├── api_client.py       # Base client with retry logic
│   │       ├── fetch_orders.py     # Order fetching logic
│   │       └── fetch_customers.py  # Customer profile fetching
│   │
│   ├── commerce_ops/               # Operational tools
│   │   ├── pathao_tab.py           # Pathao shipping integration
│   │   ├── fuzzy_parser_tab.py     # Unstructured data parsing
│   │   └── ui_components.py        # Shared operational UI
│   │
│   └── cache/                      # Local data cache
│       └── *.parquet               # Cached data files
│
├── FrontEnd/                       # UI components & pages
│   ├── components/                 # Reusable UI components
│   │   ├── ui.py                   # Main UI module exports
│   │   ├── metrics.py              # Metric cards, skeleton loading
│   │   ├── cards.py                # Hero cards, info boxes
│   │   ├── charts.py               # Plotly chart utilities
│   │   ├── layout.py               # Theme, sidebar, page structure
│   │   ├── animation.py            # Lottie animations
│   │   ├── ai_chatbot.py           # Floating AI assistant
│   │   └── customer_insight/       # Customer-specific UI components
│   │       ├── customer_report.py  # Detailed customer reports
│   │       └── customer_filters.py # Advanced filtering logic
│
│
├── tests/                          # Test suite
│   ├── test_app.py
│   ├── test_customer_history.py
│   └── conftest.py
│
├── scripts/                        # Utility scripts
│   ├── build_customer_mapping.py
│   └── generate_snapshot.py
│
└── .github/workflows/              # CI/CD
    └── ci.yml
```

---

## 3. Architecture Patterns

### 3.1 Staged Loading (Fast UI Render)

**Pattern**: Render skeleton UI immediately while data loads in background.

```python
# dashboard.py - Main dashboard uses staged loading
if cache_empty and not data_ready:
    # Phase 1: Render skeleton immediately
    ui.skeleton_row(count=6)
    st.rerun()
    
# Phase 2: Load data
# Phase 3: Replace skeletons with real metrics
```

**Key Files**:
- `@FrontEnd/components/metrics.py` - `skeleton_metric()`, `skeleton_row()`
- `@FrontEnd/pages/dashboard.py` - Skeleton rendering logic

### 3.2 Background Data Loading

**Pattern**: Use threading for non-blocking API calls.

```python
# Returns data loads in background thread
def _load_returns_async(window: str, sales_df: pd.DataFrame):
    try:
        st.session_state["returns_loading"] = True
        returns_df = load_returns_data(sync_window=window, sales_df=sales_df)
        st.session_state.returns_data = returns_df
        st.session_state["returns_loading"] = False
        st.session_state["returns_load_complete"] = True
    except Exception as e:
        log_error(e, context="Returns Background Load")

# Start background thread
if needs_load and not st.session_state.get("returns_loading", False):
    thread = Thread(target=_load_returns_async, args=(sync_window, df_exec), daemon=True)
    thread.start()
```

**Important**: Streamlit's `st.session_state` is thread-safe for basic operations, but UI updates only happen on rerun.

### 3.3 Defensive Column Access

**Pattern**: Always check if columns exist before accessing.

```python
# CORRECT - With guards
if "date" in df.columns and not df.empty:
    valid_dates = df["date"].notna()
    date_mask = (df["date"].dt.date >= start_dt) & (df["date"].dt.date <= end_dt)
    
# WRONG - Direct access causes KeyError
mask = (df["date"].dt.date >= start_dt)  # Crashes if 'date' missing
```

**Files with Defensive Checks**:
- `@FrontEnd/pages/dashboard_lib/returns_tracker.py` - All functions check for `date`, `issue_type` columns
- `@BackEnd/services/returns_tracker.py` - Ensures columns exist in data cleaning

### 3.4 Cache-First Data Loading

**Pattern**: Check cache before hitting APIs.

```python
# hybrid_data_loader.py
def load_hybrid_data(start_date, end_date, woocommerce_mode="live"):
    # 1. Check local cache first
    cached = load_cached_woocommerce_history()
    if not cached.empty and is_fresh(cached):
        return cached
    
    # 2. Background refresh if stale
    if woocommerce_mode == "live":
        start_orders_background_refresh(start_date, end_date)
    
    # 3. Return what we have (cache or API)
    return cached if not cached.empty else fetch_from_api()
```

---

## 4. Key Data Structures

### 4.1 Returns DataFrame Schema

```python
returns_df = pd.DataFrame({
    "order_id": str,           # Normalized order ID (numeric part)
    "order_id_raw": str,       # Original order ID string
    "date": datetime,          # Issue date (pandas datetime)
    "issue_type": str,         # "Paid Return" | "Non Paid Return" | "Partial" | "Exchange"
    "return_reason": str,      # Classified reason
    "product_details": str,    # Raw product description from sheet
    "returned_items": list,    # Normalized items: [{"name": "...", "sku": "...", "qty": 1, ...}]
    "partial_amount": float,   # For partials: amount charged
    "is_exchange": bool,
    "is_return": bool,
    "is_partial": bool,
    "customer_reason": str,
    "courier_reason": str,
    # ... additional metadata
})
```

### 4.2 Sales DataFrame Schema

```python
sales_df = pd.DataFrame({
    "order_id": str,
    "order_date": datetime,
    "item_name": str,
    "sku": str,
    "quantity": int,
    "item_revenue": float,
    "order_status": str,       # "completed", "shipped", "cancelled", etc.
    "customer_key": str,       # "reg_{id}" or "guest_{email}"
    "city": str,
    "state": str,
    "Category": str,           # Product category (mapped)
})
```

### 4.3 Session State Keys

```python
# Critical session state variables
st.session_state["time_window"] = "Last Month"  # Current selected window
st.session_state["returns_data"] = pd.DataFrame()  # Returns dataframe
st.session_state["returns_loading"] = bool  # Background load in progress
st.session_state["returns_load_complete"] = bool  # Fresh data ready
st.session_state["last_returns_sync"] = str  # Last sync window identifier
st.session_state["active_section"] = "💎 Sales Overview"  # Current tab
```

---

## 5. Critical Code Locations

### 5.1 Date Range Calculation

**File**: `@FrontEnd/pages/dashboard.py:95-143`

```python
# Key: start_dt and end_dt must be calculated for ALL window types
if window == "Custom Date Range":
    start_dt = st.session_state.get("wc_sync_start_date", today)
    end_dt = st.session_state.get("wc_sync_end_date", today)
else:
    # Calculate proper date range for ALL window types
    end_dt = today
    start_dt = today - timedelta(days=days_back)
```

### 5.2 Returns Data Loading

**File**: `@BackEnd/services/returns_tracker.py:50-180`

```python
def load_returns_data(url=None, sync_window="", sales_df=None):
    # 1. Load from Google Sheets CSV
    # 2. Clean & normalize columns
    # 3. Classify issue types
    # 4. Cross-reference with WooCommerce (if sales_df provided)
    # 5. Return enhanced DataFrame
```

### 5.3 Cross-Referencing Orders

**File**: `@BackEnd/services/returns_tracker.py:280-400`

```python
def cross_reference_return_items(returns_df, sales_df=None):
    # 1. Try to match from cached sales data
    # 2. Bulk fetch missing orders from WooCommerce (limit 50)
    # 3. Fallback: fetch individual orders via fetch_woocommerce_order_by_id()
    # 4. Enhance returned_items with SKU, price, revenue_impact
```

### 5.4 UI Component Usage

**File**: `@FrontEnd/components/ui.py`

Available components:
- `ui.skeleton_metric(icon)` - Loading placeholder
- `ui.skeleton_row(count)` - Row of skeletons
- `ui.icon_metric(label, value, icon, delta, delta_val, loading)` - Metric card
- `ui.metric_highlight(...)` - Premium KPI card
- `ui.operational_card(...)` - Multi-line operational card

---

## 6. Common Tasks for Agents

### 6.1 Add a New Tab

1. Add tab name to nav_map in `@app.py:102-110`
2. Add elif block in `@FrontEnd/pages/dashboard.py:547-616`
3. Create render function in appropriate dashboard_lib file

### 6.2 Add Column Guard

```python
# Before accessing any DataFrame column:
if "column_name" not in df.columns:
    st.info("📊 Data is loading... column_name not yet available.")
    return  # or provide default value
```

### 6.3 Add Background Loading

```python
from threading import Thread
import time

def _load_data_async(key: str, loader_func):
    try:
        st.session_state[f"{key}_loading"] = True
        st.session_state[f"{key}_load_started"] = time.time()
        data = loader_func()
        st.session_state[f"{key}_data"] = data
        st.session_state[f"{key}_loading"] = False
        st.session_state[f"{key}_complete"] = True
    except Exception as e:
        st.session_state[f"{key}_loading"] = False
        log_error(e, context=f"{key} Background Load")

# Trigger load
if needs_load and not st.session_state.get(f"{key}_loading", False):
    thread = Thread(target=_load_data_async, args=(key, loader_func), daemon=True)
    thread.start()
```

### 6.4 Handle Renamed Features

```python
# Migration pattern (in @app.py)
if st.session_state.active_section == "🔄 Old Tab Name":
    st.session_state.active_section = "🔄 New Tab Name"
```

---

## 7. Data Sources & Integration

### 7.1 WooCommerce API
- **URL**: Configured via environment variables
- **Authentication**: Consumer key + secret
- **Rate Limiting**: Background refresh with 360min TTL
- **Key Endpoints**: Orders, Products, Customers

### 7.2 Google Sheets (Returns Data)
- **URL**: `DEFAULT_SHEET_URL` in returns_tracker.py
- **Format**: Published CSV
- **Columns**: Order ID, Date, Product Details, Issue Type, etc.
- **Sync**: Real-time on page load with caching

### 7.3 Local Cache
- **Location**: `BackEnd/cache/`
- **Format**: Parquet files
- **TTL**: Varies by data type (20min for stock, 360min for orders)

---

## 8. Error Handling Patterns

### 8.1 Silent Failures (Background Threads)
```python
try:
    data = load_data()
    st.session_state.data = data
except Exception as e:
    logger.error(f"Failed to load: {e}")
    st.session_state.data = pd.DataFrame()  # Empty but valid
```

### 8.2 User-Friendly Messages (UI)
```python
if "date" not in df.columns:
    st.info("📊 Return data is loading... Date information not yet available.")
    return
```

### 8.3 Error Logging
```python
from BackEnd.core.logging_config import get_logger
logger = get_logger("module_name")

# Log with context
try:
    risky_operation()
except Exception as e:
    logger.exception("Operation failed")  # Includes traceback
    log_error(e, context="Specific Context")
```

---

## 9. Testing & Verification

### 9.1 Run Tests
```bash
pytest tests/ -v
```

### 9.2 Manual Verification Checklist
- [ ] Skeleton loading shows immediately on first load
- [ ] Metrics populate within 5 seconds (from cache)
- [ ] Returns data loads in background (spinner visible)
- [ ] Date range matches selected window (sidebar vs debug info)
- [ ] All tabs render without KeyError
- [ ] Financial chart appears when returns data ready

---

## 10. Recent Changes (Agent Context)

### 2026-04-20: Fast Loading Implementation
- Added skeleton loading components (`skeleton_metric`, `skeleton_row`)
- Implemented staged loading in main dashboard
- Added background thread loading for returns data
- Added defensive column checks throughout returns tracker

### 2026-04-20: Tab Rename
- Renamed "Returns & Net Sales" → "Returns Insights"
- Added session state migration for old tab names
- Updated all references across frontend and backend

### 2026-04-20: Date Range Fix
- Fixed bug where `start_dt`/`end_dt` weren't calculated for non-Custom windows
- Now properly calculates date range for all window types (MTD, YTD, Last X Days)

### 2026-04-20: Column Safety
- Added guards for `date`, `issue_type`, `returned_items` column access
- Returns tracker functions now gracefully handle missing columns
- Financial chart wrapped in `returns_ready` guard

---

## 11. Extension Guidelines

### 11.1 Adding New Data Sources
1. Create client in `BackEnd/core/` (e.g., `new_api_client.py`)
2. Add service in `BackEnd/services/` for business logic
3. Use caching pattern from `hybrid_data_loader.py`
4. Add loading indicator in UI

### 11.2 Adding New Visualizations
1. Create render function in appropriate `dashboard_lib/` file
2. Use Plotly for charts (see `charts.py` for theme utilities)
3. Add guards for required columns
4. Include in tab navigation

### 11.3 Adding New Metrics
1. Calculate in appropriate service function
2. Return in metrics dictionary
3. Display using `ui.icon_metric()` or `ui.metric_highlight()`
4. Add skeleton state for loading

---

## 12. Troubleshooting Quick Reference

| Issue | Location | Fix |
|-------|----------|-----|
| `'date' KeyError` | returns_tracker.py | Add `if "date" in df.columns:` guard |
| `'issue_type' KeyError` | returns_tracker.py | Add column existence check |
| Slow initial load | dashboard.py | Skeleton loading implemented |
| Date range mismatch | dashboard.py:130-143 | Ensure `start_dt`/`end_dt` calculated |
| Tab not rendering | app.py:102-110 | Check nav_map and selection elif |
| Background thread crash | Any thread | Wrap in try/except, log errors |
| Session state error | Any | Check key exists before access |

---

## 13. Environment & Configuration

### 13.1 Required Environment Variables
```bash
WOOCOMMERCE_URL=https://...
WOOCOMMERCE_CONSUMER_KEY=...
WOOCOMMERCE_CONSUMER_SECRET=...
```

### 13.2 Feature Flags (config.py)
```python
USE_STATIC_SNAPSHOT = False  # Use snapshot for slow connections
SNAPSHOT_DATE = "2026-04-15"
MAP_FORCE_SNAPSHOT = False   # Map always uses snapshot
```

---

## 14. CHANGELOG (Version History)

> **Note**: This section contains the project version history from CHANGELOG.md

### Version 2.1.0 (2025-08-15)

#### Features
- **Customer Insight Module**: Dynamic customer filtering with real-time data
  - Filter by products purchased, amount range, order count, date range
  - Individual customer deep-dive analysis
  - Order history with spending trend visualization
  - 1-hour caching for performance
- **Live API Integration**: WooCommerce REST API with `/wp-json/wc/v3/` endpoints
  - Customers, Orders, Products endpoints
  - Retry logic with exponential backoff
  - Error handling with user-friendly messages

#### Technical
- Added `src/` directory structure for customer insight
- Base API client with inheritance pattern
- Component-based architecture for customer filters

### Version 2.0.0 (2025-07-20)

#### Features
- **WooCommerce-First BI Refactor**: Complete migration to WooCommerce-centric architecture
- **Hybrid Data Loading**: Cache-first with background refresh
- **AutoML Forecasting**: Smart Model Router with 4-tier approach
- **Market Basket Analysis**: Product affinity and bundle detection
- **Inventory Intelligence**: Bundle-aware stock management

#### Architecture
- Local-first analytics with thin Streamlit shell
- Service-oriented backend
- Registry-based navigation system
- Page registry keeps `app.py` small

### Version 1.5.0 (2025-06-01)

#### Features
- **Returns & Net Sales Tracking**: Delivery issue intelligence
- **Net Sales Calculation**: True revenue after returns/partials/exchanges
- **Financial Integrity Charts**: Gross vs Net settled visualization

### Version 1.0.0 (2025-04-01)

#### Initial Release
- **DEEN OPS Terminal**: Operational Command Center
- **Sales Analytics**: Revenue, orders, customer insights
- **Geographic Intelligence**: District code resolution
- **ML Insights**: Forecasting and anomaly detection

---

## 15. Development Guidelines (from DEVELOPMENT.md)

### 15.1 Adding New Customer Insight Filters

When extending the Customer Insight module with new filter criteria:

**Location**: `FrontEnd/pages/dashboard_lib/customer_insight_page.py`

**Steps**:
1. Add UI control (slider, multiselect, etc.) in the filter section
2. Store value in `st.session_state["filter_key"]`
3. Apply filter to customer DataFrame before display

**Example Pattern**:
```python
# Add to filter section
col1, col2 = st.columns(2)
with col1:
    min_orders = st.number_input(
        "Min Orders", 
        min_value=1, 
        value=st.session_state.get("min_orders", 1),
        key="min_orders"
    )

# Apply filter
if len(df_customers) > 0:
    mask = df_customers["order_count"] >= st.session_state.get("min_orders", 1)
    df_customers = df_customers[mask]
```

### 15.2 Creating New API Clients

**Location**: `src/inheritance/base_api_client.py` (base) or new service

**Pattern**:
```python
from src.inheritance.base_api_client import BaseAPIClient

class NewAPIClient(BaseAPIClient):
    def __init__(self, base_url, consumer_key, consumer_secret):
        super().__init__(base_url, consumer_key, consumer_secret)
    
    def fetch_resource(self, resource_id):
        endpoint = f"/wp-json/wc/v3/resource/{resource_id}"
        return self._make_request("GET", endpoint)
```

**Key Principles**:
- Inherit from `BaseAPIClient` for retry logic
- Use `_make_request()` wrapper for automatic retries
- Handle pagination with `per_page=100`
- Cache results with TTL

### 15.3 Common Development Patterns

**Session State Management**:
```python
# Initialize with default
if "key" not in st.session_state:
    st.session_state.key = default_value

# Read safely
current_value = st.session_state.get("key", default_value)
```

**Data Caching Pattern**:
```python
import hashlib
from datetime import datetime, timedelta

def cache_key(*args):
    """Generate cache key from arguments."""
    content = "|".join(str(a) for a in args)
    return hashlib.md5(content.encode()).hexdigest()

def is_cache_valid(cache_file, ttl_hours=1):
    """Check if cache file is still fresh."""
    if not cache_file.exists():
        return False
    modified = datetime.fromtimestamp(cache_file.stat().st_mtime)
    return datetime.now() - modified < timedelta(hours=ttl_hours)
```

**Column Guards (Defensive Programming)****:
```python
# Always check before accessing DataFrame columns
if "date" not in df.columns:
    st.info("📊 Data is loading... date not yet available.")
    return

# Safe access with default
value = row.get("column_name", default_value)
```

### 15.4 Testing Guidelines

**Unit Testing**:
- Tests in `tests/` directory
- Name format: `test_<module>.py`
- Use pytest fixtures for common setup

**Manual Testing Checklist**:
1. First load shows skeleton UI (< 2 seconds)
2. Metrics populate from cache (< 5 seconds)
3. Date range matches selected window
4. All tabs render without KeyError
5. Background loading shows indicators
6. Financial charts appear when data ready

### 15.5 Deployment Notes

**Pre-Deployment**:
- Run `python .ai/update_agents_md.py --force` to update docs
- Verify `.pre-commit-config.yaml` hooks pass
- Check `agents.md` is current

**Environment Variables** (Required):
```bash
WOOCOMMERCE_URL=https://your-store.com
WOOCOMMERCE_CONSUMER_KEY=ck_...
WOOCOMMERCE_CONSUMER_SECRET=cs_...
```

**Performance Optimization**:
- Use `USE_STATIC_SNAPSHOT=True` for slow connections
- Enable background refresh for freshness
- Set appropriate cache TTLs

---

## 16. Auto-Update System

This `agents.md` file is **automatically maintained** by an update system that keeps it synchronized with the codebase.

### 16.1 How It Works

**Three mechanisms ensure this file stays current:**

1. **Pre-commit Hook** (Local)
   - Runs automatically before every git commit
   - Updates "Last Updated" timestamp
   - Extracts recent changes from git history
   - File: `.pre-commit-config.yaml`

2. **GitHub Actions** (CI/CD)
   - Runs on every push to main/master branch
   - Runs daily via scheduled cron job
   - Auto-commits updates back to repository
   - Workflow: `.github/workflows/update-agents-md.yml`

3. **Manual Update** (On-demand)
   ```bash
   python .ai/update_agents_md.py --force
   ```

### 16.2 What Gets Updated

- ✅ "Last Updated" timestamp
- ✅ Recent changes section (from git log)
- ✅ Project structure (scanned from codebase)
- ✅ File counts and module lists

### 16.3 For AI Agents

> **Note to AI Agents**: You don't need to manually update this file. The system handles it automatically. However, if you make significant architectural changes, you may want to:
> 
> 1. Add a new entry to Section 14 (CHANGELOG) or Section 10 (Recent Changes)
2. Update Section 2 (Project Structure) if files moved
3. Add new patterns to Section 3 (Architecture Patterns) or Section 15 (Development Guidelines)

---

**END OF AI AGENT CODER GUIDE**

> **Remember**: When in doubt, add a column guard. When adding features, follow the staged loading pattern. When renaming, add migration logic. Never let the user see a raw Python exception.
