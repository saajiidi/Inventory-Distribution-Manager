"""DEEN Commerce UI Elements Dashboard Abstraction.
Use `from FrontEnd.components import ui` and access components like `ui.hero(...)`.
"""

from .layout import setup_theme, sidebar_branding, page_header, page_footer
from .cards import card, hero, commentary, info_box
from .metrics import metric_highlight, icon_metric, badge, date_context, operational_card, skeleton_metric, skeleton_row
from .charts import build_discrete_color_map, apply_plotly_theme, donut_chart, bar_chart
from .data_display import file_summary, export_to_excel, show_last_updated, _safe_datetime_series
from .interactive import floating_action_bar, dialog_confirm
from .animation import animation_bike
from .mui_components import mui_stat_card, render_mui_dashboard_sync

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
    "icon_metric",
    "skeleton_metric",
    "skeleton_row",
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
    "animation_bike",
    "mui_stat_card",
    "operational_card",
    "render_mui_dashboard_sync",
]
