from datetime import date

APP_TITLE = "DEEN Business Intelligence"
APP_VERSION = "v10.0"
APP_DATA_START_DATE = date(2022, 8, 1)

MORE_TOOLS = [
    "System Logs",
    "Dev Lab",
]

INVENTORY_LOCATIONS = ["Ecom", "Mirpur", "Wari", "Cumilla", "Sylhet"]

STATUS_COLORS = {
    "success": "#15803d",
    "warning": "#b45309",
    "error": "#b91c1c",
    "info": "#1d4ed8",
}

# Performance & Memory Optimization
# Global flag (Manual or Slow Connection fallback)
USE_STATIC_SNAPSHOT = False 

# Components that ALWAYS use snapshot to save memory (like complex maps)
MAP_FORCE_SNAPSHOT = True

SNAPSHOT_DATE = "2026-04-08"
SNAPSHOT_LABEL = "April 2026 Operational Snapshot"
