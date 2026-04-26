"""Frontend Components Module

Reusable UI components for the Streamlit application.
"""

from .ui import *

# Explicitly expose everything available in `ui` just in case legacy code 
# still tries `from FrontEnd.components import card`
from .layout import setup_theme, sidebar_branding, page_header, page_footer
from .cards import card, hero, commentary, info_box
from .metrics import metric_highlight, badge, date_context
from .charts import build_discrete_color_map, apply_plotly_theme, donut_chart, bar_chart
from .data_display import file_summary, export_to_excel, show_last_updated, _safe_datetime_series
from .interactive import floating_action_bar, dialog_confirm

__all__ = [
    "setup_theme",
    "sidebar_branding",
    "page_header",
    "card",
    "hero",
    "commentary",
    "badge",
    "info_box",
    "metric_highlight",
    "build_discrete_color_map",
    "apply_plotly_theme",
    "donut_chart",
    "bar_chart",
    "_safe_datetime_series",
    "date_context",
    "page_footer",
    "file_summary",
    "floating_action_bar",
    "dialog_confirm",
    "export_to_excel",
    "show_last_updated",
    "ui"
]
