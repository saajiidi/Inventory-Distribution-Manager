from datetime import date

APP_TITLE = "DEEN Commerce BI"
APP_VERSION = "v2.5.0"
APP_DATA_START_DATE = date(2022, 8, 1)

PRIMARY_NAV = (
    "Business Intelligence",
    "Customer Intelligence",
    "Commerce Hub",
    "Business Cycles",
    "ShopAI CRM",
    "System Health",
)

PRIMARY_PAGE_CONFIG = (
    {
        "key": "business_intelligence",
        "label": "Business Intelligence",
        "description": "Executive KPIs, sales analysis, customer behavior, inventory, and forecasts.",
    },

    {
        "key": "customer_intelligence",
        "label": "Customer Intelligence",
        "description": "Lifetime customer metrics, RFM segmentation, and retention context.",
    },
    {
        "key": "commerce_hub",
        "label": "Commerce Hub",
        "description": "WooCommerce order sync, inventory fetch, and operational previews.",
    },
    {
        "key": "business_cycles",
        "label": "Business Cycles",
        "description": "Order performance tracking based on 5 PM operational cutoffs.",
    },
    {
        "key": "shop_ai_crm",
        "label": "ShopAI CRM",
        "description": "CRM analytics for support conversations, customer routing, and agent workflow testing.",
    },
    {
        "key": "system_health",
        "label": "System Health",
        "description": "Diagnostics, logs, and operational confidence checks.",
    },
)

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
