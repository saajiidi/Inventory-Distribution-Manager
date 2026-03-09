import streamlit as st
import pandas as pd
import plotly.express as px
import os
import json
import streamlit.components.v1 as components
from datetime import datetime
from io import BytesIO
from email.utils import parsedate_to_datetime
from urllib.request import Request, urlopen
from urllib.parse import parse_qs, urlparse
from app_modules.ui_components import section_card, render_action_bar


def _hex_to_rgb(hex_color):
    if not hex_color:
        return None
    h = hex_color.strip().lstrip("#")
    if len(h) == 3:
        h = "".join(ch * 2 for ch in h)
    if len(h) != 6:
        return None
    try:
        return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))
    except ValueError:
        return None


def _is_dark_theme():
    base = st.get_option("theme.base")
    if base == "dark":
        return True
    if base == "light":
        return False

    # Prefer text color when available because it reflects actual theme contrast.
    text_rgb = _hex_to_rgb(st.get_option("theme.textColor"))
    if text_rgb:
        luminance = (0.2126 * text_rgb[0] + 0.7152 * text_rgb[1] + 0.0722 * text_rgb[2]) / 255
        return luminance > 0.6

    bg = _hex_to_rgb(st.get_option("theme.backgroundColor"))
    if bg:
        luminance = (0.2126 * bg[0] + 0.7152 * bg[1] + 0.0722 * bg[2]) / 255
        return luminance < 0.45

    return False


def _chart_theme():
    dark = _is_dark_theme()
    return {
        "template": "plotly_dark" if dark else "plotly_white",
        "legend_bg": "rgba(2,6,23,0.88)" if dark else "rgba(255,255,255,0.88)",
        "legend_border": "#334155" if dark else "#cbd5e1",
        "font_color": "#e5e7eb" if dark else "#0f172a",
    }


def render_snapshot_button(marker_id="snapshot-target"):
    """Capture and download dashboard area snapshot as PNG."""
    html_code = """
    <div style="text-align:right; margin:6px 0 2px 0;">
      <button onclick="captureDashboard()" style="
          background:#1d4ed8; color:#fff; border:none; border-radius:8px;
          padding:7px 12px; font-size:13px; font-weight:600; cursor:pointer;">
          Save Snapshot
      </button>
    </div>
    <script>
    function captureDashboard() {
      const marker = window.parent.document.getElementById('__MARKER__');
      let target = null;
      if (marker) {
        target = marker.closest('[data-testid="stVerticalBlock"]');
      }
      if (!target) {
        target = window.parent.document.querySelector('[data-testid="stAppViewContainer"]');
      }
      if (!target) return;

      const doCapture = () => {
        const originalPadding = target.style.padding;
        const originalBackground = target.style.backgroundColor;
        const originalBorderRadius = target.style.borderRadius;
        const appBg = window.parent.getComputedStyle(window.parent.document.body).backgroundColor || '#0f172a';

        // Add breathing room so left edge does not feel cramped in snapshot.
        target.style.padding = '18px 22px 18px 30px';
        target.style.backgroundColor = appBg;
        target.style.borderRadius = '12px';

        window.parent.html2canvas(target, {useCORS: true, scale: 2, backgroundColor: appBg})
          .then((canvas) => {
            target.style.padding = originalPadding;
            target.style.backgroundColor = originalBackground;
            target.style.borderRadius = originalBorderRadius;
            const a = window.parent.document.createElement('a');
            a.download = 'dashboard_snapshot.png';
            a.href = canvas.toDataURL('image/png');
            a.click();
          })
          .catch(() => {
            target.style.padding = originalPadding;
            target.style.backgroundColor = originalBackground;
            target.style.borderRadius = originalBorderRadius;
          });
      };

      if (!window.parent.html2canvas) {
        const script = window.parent.document.createElement('script');
        script.src = 'https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js';
        script.onload = doCapture;
        window.parent.document.head.appendChild(script);
      } else {
        doCapture();
      }
    }
    </script>
    """.replace("__MARKER__", marker_id)
    components.html(html_code, height=44)

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
FEEDBACK_DIR = os.path.join(DATA_DIR, "feedback")
INCOMING_DIR = os.path.join(DATA_DIR, "incoming")
DEFAULT_GSHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTBDukmkRJGgHjCRIAAwGmlWaiPwESXSp9UBXm3_sbs37bk2HxavPc62aobmL1cGWUfAKE4Zd6yJySO/pubhtml"
os.makedirs(FEEDBACK_DIR, exist_ok=True)
os.makedirs(INCOMING_DIR, exist_ok=True)

def log_system_event(event_type, details):
    """Logs errors or system events to a JSON file for further analysis."""
    log_file = os.path.join(FEEDBACK_DIR, "system_logs.json")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_entry = {
        "timestamp": timestamp,
        "type": event_type,
        "details": details
    }

    try:
        logs = []
        if os.path.exists(log_file):
            with open(log_file, "r", encoding="utf-8") as f:
                logs = json.load(f)

        logs.append(log_entry)
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=4)
    except Exception as e:
        print(f"Logging failed: {e}")


def save_user_feedback(comment):
    """Saves user comments to a feedback file."""
    feedback_file = os.path.join(FEEDBACK_DIR, "user_feedback.json")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    feedback_entry = {
        "timestamp": timestamp,
        "comment": comment
    }

    try:
        data = []
        if os.path.exists(feedback_file):
            with open(feedback_file, "r", encoding="utf-8") as f:
                data = json.load(f)

        data.append(feedback_entry)
        with open(feedback_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        return True
    except Exception:
        return False


def get_setting(key, default=None):
    """Reads setting from Streamlit secrets first, then env var."""
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)


def get_gcp_service_account_info():
    """Returns service account info from st.secrets or env JSON."""
    try:
        if "gcp_service_account" in st.secrets:
            return dict(st.secrets["gcp_service_account"])
    except Exception:
        pass

    raw = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if raw:
        try:
            return json.loads(raw)
        except Exception as e:
            raise ValueError(f"Invalid GCP_SERVICE_ACCOUNT_JSON: {e}")

    return None


def get_category(name):
    """Categorizes products based on keywords in their names."""
    name_str = str(name).lower()

    def has_any(keywords, text):
        return any(kw.lower() in text for kw in keywords)

    specific_cats = {
        'Boxer': ['boxer'],
        'Jeans': ['jeans'],
        'Denim': ['denim'],
        'Flannel': ['flannel'],
        'Polo Shirt': ['polo'],
        'Panjabi': ['panjabi', 'punjabi'],
        'Trousers': ['trousers', 'pant', 'cargo', 'trouser', 'joggers', 'track pant', 'jogger'],
        'Twill Chino': ['twill chino'],
        'Mask': ['mask'],
        'Bag': ['bag', 'backpack'],
        'Water Bottle': ['water bottle'],
        'Contrast': ['contrast'],
        'Turtleneck': ['turtleneck', 'mock neck'],
        'Drop Shoulder': ['drop', 'shoulder'],
        'Wallet': ['wallet'],
        'Kaftan': ['kaftan'],
        'Active Wear': ['active wear'],
        'Jersy': ['jersy'],
        'Sweatshirt': ['sweatshirt', 'hoodie', 'pullover'],
        'Jacket': ['jacket', 'outerwear', 'coat'],
        'Belt': ['belt'],
        'Sweater': ['sweater', 'cardigan', 'knitwear'],
        'Passport Holder': ['passport holder'],
        'Cap': ['cap'],
    }

    for cat, keywords in specific_cats.items():
        if has_any(keywords, name_str):
            return cat

    fs_keywords = ['full sleeve', 'long sleeve', 'fs', 'l/s']
    if has_any(['t-shirt', 't shirt', 'tee'], name_str):
        return 'FS T-Shirt' if has_any(fs_keywords, name_str) else 'T-Shirt'

    if has_any(['shirt'], name_str):
        return 'FS Shirt' if has_any(fs_keywords, name_str) else 'HS Shirt'

    return 'Others'


@st.cache_data(show_spinner=False)
def find_columns(df):
    """Detects primary columns using exact and then partial matching."""
    mapping = {
        'name': ['item name', 'product name', 'product', 'item', 'title', 'description', 'name'],
        'cost': ['item cost', 'price', 'unit price', 'cost', 'rate', 'mrp', 'selling price'],
        'qty': ['quantity', 'qty', 'units', 'sold', 'count', 'total quantity'],
        'date': ['date', 'order date', 'month', 'time', 'created at'],
        'order_id': ['order id', 'order #', 'invoice number', 'invoice #', 'order number', 'transaction id', 'id'],
        'phone': ['phone', 'contact', 'mobile', 'cell', 'phone number', 'customer phone']
    }

    found = {}
    actual_cols = [c.strip() for c in df.columns]
    lower_cols = [c.lower() for c in actual_cols]

    for key, aliases in mapping.items():
        for alias in aliases:
            if alias in lower_cols:
                idx = lower_cols.index(alias)
                found[key] = actual_cols[idx]
                break

    for key, aliases in mapping.items():
        if key not in found:
            for col, l_col in zip(actual_cols, lower_cols):
                if any(alias in l_col for alias in aliases):
                    found[key] = col
                    break

    return found


@st.cache_data(show_spinner=False)
def process_data(df, selected_cols):
    """Processed data using validated user-selected or auto-detected columns."""
    try:
        df = df.copy()
        df['Internal_Name'] = df[selected_cols['name']].fillna('Unknown Product').astype(str)
        df = df[~df['Internal_Name'].str.contains('Choose Any', case=False, na=False)]

        df['Internal_Cost'] = pd.to_numeric(df[selected_cols['cost']], errors='coerce').fillna(0)
        df['Internal_Qty'] = pd.to_numeric(df[selected_cols['qty']], errors='coerce').fillna(0)

        timeframe_suffix = ""
        if 'date' in selected_cols and selected_cols['date'] in df.columns:
            try:
                dates = pd.to_datetime(df[selected_cols['date']], errors='coerce').dropna()
                if not dates.empty:
                    if dates.dt.to_period('M').nunique() == 1:
                        timeframe_suffix = dates.iloc[0].strftime("%B_%Y")
                    else:
                        timeframe_suffix = f"{dates.min().strftime('%d%b')}_to_{dates.max().strftime('%d%b_%y')}"
            except Exception:
                non_null = df[selected_cols['date']].dropna()
                val = str(non_null.iloc[0]) if not non_null.empty else ""
                timeframe_suffix = val.replace("/", "-").replace(" ", "_")[:20]

        if (df['Internal_Qty'] < 0).any():
            log_system_event("DATA_ISSUE", "Found negative quantities, converted to 0.")
            df.loc[df['Internal_Qty'] < 0, 'Internal_Qty'] = 0

        df['Category'] = df['Internal_Name'].apply(get_category)
        df['Total Amount'] = df['Internal_Cost'] * df['Internal_Qty']

        others = df[df['Category'] == 'Others']
        if len(others) > 0:
            log_system_event("OTHERS_LOG", {"count": len(others), "samples": others['Internal_Name'].head(10).tolist()})

        summary = df.groupby('Category').agg({'Internal_Qty': 'sum', 'Total Amount': 'sum'}).reset_index()
        summary.columns = ['Category', 'Total Qty', 'Total Amount']

        total_rev = summary['Total Amount'].sum()
        total_qty = summary['Total Qty'].sum()
        if total_rev > 0:
            summary['Revenue Share (%)'] = (summary['Total Amount'] / total_rev * 100).round(2)
        if total_qty > 0:
            summary['Quantity Share (%)'] = (summary['Total Qty'] / total_qty * 100).round(2)

        drilldown = df.groupby(['Category', 'Internal_Cost']).agg({'Internal_Qty': 'sum', 'Total Amount': 'sum'}).reset_index()
        drilldown.columns = ['Category', 'Price (TK)', 'Total Qty', 'Total Amount']

        top_items = df.groupby('Internal_Name').agg({'Internal_Qty': 'sum', 'Total Amount': 'sum', 'Category': 'first'}).reset_index()
        top_items.columns = ['Product Name', 'Total Qty', 'Total Amount', 'Category']
        top_items = top_items.sort_values('Total Amount', ascending=False)

        basket_metrics = {"avg_basket_qty": 0, "avg_basket_value": 0, "total_orders": 0}
        group_cols = []
        if 'order_id' in selected_cols and selected_cols['order_id'] in df.columns:
            group_cols.append(selected_cols['order_id'])
        if 'phone' in selected_cols and selected_cols['phone'] in df.columns:
            group_cols.append(selected_cols['phone'])

        if group_cols:
            order_groups = df.groupby(group_cols).agg({'Internal_Qty': 'sum', 'Total Amount': 'sum'})
            basket_metrics["avg_basket_qty"] = order_groups['Internal_Qty'].mean()
            basket_metrics["avg_basket_value"] = order_groups['Total Amount'].mean()
            basket_metrics["total_orders"] = len(order_groups)

        return drilldown, summary, top_items, timeframe_suffix, basket_metrics
    except Exception as e:
        log_system_event("CRASH", str(e))
        st.error(f"Error in calculation: {e}")
        return None, None, None, "", {}


def get_latest_incoming_file(folder_path):
    """Returns the newest XLSX/CSV file path from incoming folder."""
    allowed_ext = (".xlsx", ".csv")
    files = []
    try:
        for name in os.listdir(folder_path):
            full_path = os.path.join(folder_path, name)
            if os.path.isfile(full_path) and name.lower().endswith(allowed_ext):
                files.append(full_path)
    except Exception as e:
        log_system_event("INCOMING_SCAN_ERROR", str(e))
        return None

    if not files:
        return None

    return max(files, key=os.path.getmtime)


@st.cache_data(show_spinner=False)
def read_sales_file(file_obj, file_name):
    """Reads CSV/XLSX from uploader, file path, or bytes buffer."""
    if str(file_name).lower().endswith('.csv'):
        return pd.read_csv(file_obj)
    return pd.read_excel(file_obj)


def normalize_gsheet_url_to_csv(sheet_url):
    """Converts Google Sheet URLs to CSV export URLs."""
    if not sheet_url:
        return None

    url = sheet_url.strip()
    if "output=csv" in url:
        return url

    parsed = urlparse(url)
    path = parsed.path or ""
    query = parse_qs(parsed.query)

    if "/spreadsheets/d/e/" in path:
        base = f"{parsed.scheme}://{parsed.netloc}{path.split('/pubhtml')[0].split('/pub')[0]}"
        gid = query.get("gid", ["0"])[0]
        return f"{base}/pub?output=csv&gid={gid}"

    if "/spreadsheets/d/" in path:
        parts = [p for p in path.split("/") if p]
        if "d" in parts:
            idx = parts.index("d")
            if idx + 1 < len(parts):
                sheet_id = parts[idx + 1]
                gid = query.get("gid", ["0"])[0]
                return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

    return url


def _read_csv_with_last_modified(csv_url):
    """Read CSV and capture server-provided Last-Modified time when available."""
    req = Request(csv_url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req) as resp:
        raw = resp.read()
        last_modified = resp.headers.get("Last-Modified")

    df = pd.read_csv(BytesIO(raw))
    if last_modified:
        try:
            dt = parsedate_to_datetime(last_modified)
            if dt.tzinfo:
                dt = dt.astimezone()
            last_modified = dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    else:
        last_modified = "Google Sheet snapshot"
    return df, last_modified


@st.cache_data(show_spinner=False)
def _read_local_sales_file(file_path, modified_ts):
    # modified_ts participates in cache key; when file updates cache invalidates.
    return read_sales_file(file_path, file_path)


def load_latest_from_incoming():
    """Loads latest file from local incoming folder."""
    latest_file = get_latest_incoming_file(INCOMING_DIR)
    if latest_file is None:
        raise FileNotFoundError("No XLSX/CSV found in incoming folder.")

    modified_ts = os.path.getmtime(latest_file)
    df_live = _read_local_sales_file(latest_file, modified_ts)
    modified_at = datetime.fromtimestamp(modified_ts).strftime("%Y-%m-%d %H:%M:%S")
    return df_live, os.path.basename(latest_file), modified_at


@st.cache_data(ttl=45, show_spinner=False)
def load_from_google_sheet():
    """Loads live data from a Google Sheet worksheet (CSV export)."""
    sheet_url = get_setting("GSHEET_URL", DEFAULT_GSHEET_URL)
    if sheet_url:
        csv_url = normalize_gsheet_url_to_csv(sheet_url)
        df_live, modified_at = _read_csv_with_last_modified(csv_url)
        return df_live, "google_sheet_live.csv", modified_at

    sheet_id = get_setting("GSHEET_ID")
    gid = str(get_setting("GSHEET_GID", "0"))
    if not sheet_id:
        raise ValueError("Missing GSHEET_URL (or GSHEET_ID) in secrets or environment.")

    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    df_live, modified_at = _read_csv_with_last_modified(csv_url)
    source_name = f"gsheet_{sheet_id}_{gid}.csv"
    return df_live, source_name, modified_at


@st.cache_data(ttl=45, show_spinner=False)
def load_latest_from_gdrive_folder():
    """Loads the latest CSV/XLSX file from a Google Drive folder."""
    folder_id = get_setting("GDRIVE_FOLDER_ID")
    if not folder_id:
        raise ValueError("Missing GDRIVE_FOLDER_ID in secrets or environment.")

    sa_info = get_gcp_service_account_info()
    if not sa_info:
        raise ValueError("Missing gcp_service_account in secrets or GCP_SERVICE_ACCOUNT_JSON env.")

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload
    except Exception as e:
        raise ImportError("Google Drive client libs are missing. Install google-api-python-client and google-auth.") from e

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    query = (
        f"'{folder_id}' in parents and trashed = false and "
        "(mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='text/csv')"
    )

    response = service.files().list(
        q=query,
        orderBy="modifiedTime desc",
        pageSize=1,
        fields="files(id,name,mimeType,modifiedTime)"
    ).execute()

    files = response.get("files", [])
    if not files:
        raise FileNotFoundError("No CSV/XLSX found in the configured Google Drive folder.")

    latest = files[0]
    request = service.files().get_media(fileId=latest["id"])
    file_bytes = BytesIO()
    downloader = MediaIoBaseDownload(file_bytes, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    file_bytes.seek(0)
    file_name = latest.get("name", "gdrive_file")
    df_live = read_sales_file(file_bytes, file_name)
    modified_at = latest.get("modifiedTime", "")

    return df_live, file_name, modified_at


def load_live_source(source_mode):
    """Routes loading by selected source mode."""
    if source_mode == "Incoming Folder":
        return load_latest_from_incoming()
    if source_mode == "Google Sheet":
        return load_from_google_sheet()
    if source_mode == "Google Drive Folder":
        return load_latest_from_gdrive_folder()
    raise ValueError(f"Unsupported source mode: {source_mode}")


def _render_welcome_popup_content(summ, basket, last_updated="N/A", focus="all"):
    t_qty = summ["Total Qty"].sum()
    t_rev = summ["Total Amount"].sum()
    chart_style = _chart_theme()
    with st.container():
        st.markdown('<div id="snapshot-target-popup"></div>', unsafe_allow_html=True)
        st.caption(f"Last update: {last_updated}")
        if focus != "all":
            st.info(f"Focused view: {focus.replace('_', ' ').title()}")

        if focus in ("all", "core_metrics"):
            st.subheader("Core Metrics")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Items Sold", f"{t_qty:,.0f}")
            total_orders = basket.get("total_orders", 0)
            m2.metric("Total Orders", f"{total_orders:,.0f}" if total_orders else "-")
            m3.metric("Revenue", f"TK {t_rev:,.2f}")
            m4.metric("Avg Price", f"TK {(t_rev / t_qty if t_qty > 0 else 0):,.2f}")

        if focus in ("all", "basket_analysis"):
            st.subheader("Basket Analysis")
            m5, m6 = st.columns(2)
            if basket.get("avg_basket_qty", 0) > 0:
                m5.metric("Avg Basket (Qty)", f"{basket['avg_basket_qty']:.2f} items")
                m6.metric("Avg Basket (TK)", f"TK {basket['avg_basket_value']:,.2f}")
            else:
                m5.metric("Avg Basket (Qty)", "-")
                m6.metric("Avg Basket (TK)", "-")

        if focus in ("all", "visual_analytics"):
            st.subheader("Visual Analytics")
            v1, v2 = st.columns(2)
            with v1:
                fig_pie = px.pie(
                    summ,
                    values='Total Amount',
                    names='Category',
                    hole=0.6,
                    title='Revenue Share',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig_pie.update_layout(
                    template=chart_style["template"],
                    margin=dict(l=12, r=170, t=50, b=12),
                    uniformtext_minsize=10,
                    uniformtext_mode="hide",
                    font=dict(color=chart_style["font_color"]),
                    title_font=dict(color=chart_style["font_color"]),
                    legend=dict(
                        orientation="v",
                        yanchor="top",
                        y=1,
                        xanchor="left",
                        x=1.02,
                        bgcolor=chart_style["legend_bg"],
                        bordercolor=chart_style["legend_border"],
                        borderwidth=1,
                        font=dict(size=11),
                    ),
                )
                fig_pie.update_traces(
                    textposition="outside",
                    textinfo="label+percent",
                    textfont=dict(size=12, color=chart_style["font_color"]),
                    pull=0.01,
                )
                st.plotly_chart(fig_pie, use_container_width=True)
                
            with v2:
                fig_bar = px.bar(
                    summ.sort_values('Total Qty', ascending=False),
                    x='Category',
                    y='Total Qty',
                    color='Category',
                    title='Volume by Category',
                    text_auto='.0f',
                    color_discrete_sequence=px.colors.qualitative.Bold
                )
                fig_bar.update_layout(
                    template=chart_style["template"],
                    margin=dict(l=12, r=12, t=50, b=12),
                    xaxis_title="",
                    yaxis_title="Quantity Sold",
                    showlegend=True,
                    font=dict(color=chart_style["font_color"]),
                    title_font=dict(color=chart_style["font_color"]),
                    xaxis=dict(tickfont=dict(color=chart_style["font_color"])),
                    yaxis=dict(
                        tickfont=dict(color=chart_style["font_color"]),
                        title=dict(font=dict(color=chart_style["font_color"])),
                    ),
                legend=dict(
                        orientation="v",
                        yanchor="top",
                        y=1,
                        xanchor="left",
                        x=1.02,
                        bgcolor=chart_style["legend_bg"],
                        bordercolor=chart_style["legend_border"],
                        borderwidth=1,
                        font=dict(color=chart_style["font_color"]),
                    ),
                )
                fig_bar.update_traces(textfont=dict(color=chart_style["font_color"]))
                st.plotly_chart(fig_bar, use_container_width=True)

    render_snapshot_button("snapshot-target-popup")

    if st.button("Close & Continue to Dashboard", use_container_width=True, key=f"close_popup_{focus}"):
        st.rerun()


popup_datetime = datetime.now().strftime("%B %d, %Y %I:%M %p")
if hasattr(st, "dialog"):
    @st.dialog(f"Welcome! Today's Actionable Insights ({popup_datetime})", width="large")
    def show_welcome_popup(summ, basket, last_updated="N/A", focus="all"):
        st.session_state.has_seen_dashboard_popup = True
        _render_welcome_popup_content(summ, basket, last_updated, focus)
else:
    def show_welcome_popup(summ, basket, last_updated="N/A", focus="all"):
        st.session_state.has_seen_dashboard_popup = True
        st.info("Quick summary view (dialog not supported by this Streamlit version).")
        _render_welcome_popup_content(summ, basket, last_updated, focus)

def render_dashboard_output(drill, summ, top, timeframe, basket, source_name, last_updated="N/A"):
    """Renders common dashboard widgets/charts/tables/export."""
    chart_style = _chart_theme()
    today_key = datetime.now().strftime("%Y-%m-%d")
    source_key = os.path.basename(str(source_name))
    popup_key = f"popup_seen::{today_key}::{source_key}"
    if not st.session_state.get(popup_key, False):
        show_welcome_popup(summ, basket, last_updated)
        st.session_state[popup_key] = True

    reopen_popup, _ = render_action_bar("Open quick summary popup", "open_summary_popup")
    if reopen_popup:
        show_welcome_popup(summ, basket, last_updated, "all")

    st.caption("Focused task popups")
    p1, p2, p3, p4 = st.columns(4)
    if p1.button("Core Metrics", key=f"focus_core_{source_key}", use_container_width=True):
        show_welcome_popup(summ, basket, last_updated, "core_metrics")
    if p2.button("Basket Analysis", key=f"focus_basket_{source_key}", use_container_width=True):
        show_welcome_popup(summ, basket, last_updated, "basket_analysis")
    if p3.button("Visual Analytics", key=f"focus_visual_{source_key}", use_container_width=True):
        show_welcome_popup(summ, basket, last_updated, "visual_analytics")
    if p4.button("Full Summary", key=f"focus_all_{source_key}", use_container_width=True):
        show_welcome_popup(summ, basket, last_updated, "all")

    t_qty = summ['Total Qty'].sum()
    t_rev = summ['Total Amount'].sum()

    with st.container():
        st.markdown('<div id="snapshot-target-main"></div>', unsafe_allow_html=True)
        st.subheader("Core Metrics")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Items Sold", f"{t_qty:,.0f}")
        total_orders = basket.get("total_orders", 0)
        m2.metric("Total Orders", f"{total_orders:,.0f}" if total_orders else "-")
        m3.metric("Revenue", f"TK {t_rev:,.2f}")
        m4.metric("Avg Price", f"TK {(t_rev / t_qty if t_qty > 0 else 0):,.2f}")

        st.subheader("Basket Analysis")
        m5, m6 = st.columns(2)
        if basket.get("avg_basket_qty", 0) > 0:
            m5.metric("Avg Basket (Qty)", f"{basket['avg_basket_qty']:.2f} items")
            m6.metric("Avg Basket (TK)", f"TK {basket['avg_basket_value']:,.2f}")
        else:
            m5.metric("Avg Basket (Qty)", "-")
            m6.metric("Avg Basket (TK)", "-")

        st.divider()

        st.subheader("Visual Analytics")
        v1, v2 = st.columns(2)
        with v1:
            fig_pie = px.pie(
                summ,
                values='Total Amount',
                names='Category',
                hole=0.6,
                title='Revenue Share',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_pie.update_layout(
                template=chart_style["template"],
                margin=dict(l=12, r=170, t=50, b=12),
                uniformtext_minsize=10,
                uniformtext_mode="hide",
                font=dict(color=chart_style["font_color"]),
                title_font=dict(color=chart_style["font_color"]),
                legend=dict(
                    orientation="v",
                    yanchor="top",
                    y=1,
                    xanchor="left",
                    x=1.02,
                    bgcolor=chart_style["legend_bg"],
                    bordercolor=chart_style["legend_border"],
                    borderwidth=1,
                    font=dict(size=11),
                ),
            )
            fig_pie.update_traces(
                textposition="outside",
                textinfo="label+percent",
                textfont=dict(size=12, color=chart_style["font_color"]),
                pull=0.01,
            )
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with v2:
            fig_bar = px.bar(
                summ.sort_values('Total Qty', ascending=False),
                x='Category',
                y='Total Qty',
                color='Category',
                title='Volume by Category',
                text_auto='.0f',
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            fig_bar.update_layout(
                template=chart_style["template"],
                margin=dict(l=12, r=12, t=50, b=12),
                xaxis_title="",
                yaxis_title="Quantity Sold",
                showlegend=True,
                font=dict(color=chart_style["font_color"]),
                title_font=dict(color=chart_style["font_color"]),
                xaxis=dict(tickfont=dict(color=chart_style["font_color"])),
                yaxis=dict(
                    tickfont=dict(color=chart_style["font_color"]),
                    title=dict(font=dict(color=chart_style["font_color"])),
                ),
                legend=dict(
                    orientation="v",
                    yanchor="top",
                    y=1,
                    xanchor="left",
                    x=1.02,
                    bgcolor=chart_style["legend_bg"],
                    bordercolor=chart_style["legend_border"],
                    borderwidth=1,
                    font=dict(color=chart_style["font_color"]),
                ),
            )
            fig_bar.update_traces(textfont=dict(color=chart_style["font_color"]))
            st.plotly_chart(fig_bar, use_container_width=True)

    render_snapshot_button("snapshot-target-main")
    st.divider()

    st.subheader("Top Products Spotlight")
    spotlight = top.head(10).sort_values("Total Amount", ascending=True)
    fig_top = px.bar(
        spotlight,
        x="Total Amount",
        y="Product Name",
        orientation="h",
        color="Category",
        title="Top 10 products by revenue",
        text_auto=".2s",
    )
    fig_top.update_layout(
        template=chart_style["template"],
        margin=dict(l=12, r=12, t=50, b=12),
        yaxis_title="",
        xaxis_title="Revenue (TK)",
        legend_title="Category",
        font=dict(color=chart_style["font_color"]),
        title_font=dict(color=chart_style["font_color"]),
        xaxis=dict(
            tickfont=dict(color=chart_style["font_color"]),
            title=dict(font=dict(color=chart_style["font_color"])),
        ),
        yaxis=dict(tickfont=dict(color=chart_style["font_color"])),
    )
    fig_top.update_traces(textfont=dict(color=chart_style["font_color"]))
    st.plotly_chart(fig_top, use_container_width=True)
    
    st.subheader("Deep Dive Data")

    tabs = st.tabs(["Summary", "Rankings", "Drilldown"])
    with tabs[0]:
        st.dataframe(summ.sort_values('Total Amount', ascending=False), use_container_width=True, hide_index=True)
    with tabs[1]:
        st.dataframe(top.head(20), use_container_width=True, hide_index=True)
    with tabs[2]:
        st.dataframe(drill.sort_values(['Category', 'Price (TK)']), use_container_width=True, hide_index=True)

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as wr:
        summ.to_excel(wr, sheet_name='Summary', index=False)
        top.to_excel(wr, sheet_name='Rankings', index=False)
        drill.to_excel(wr, sheet_name='Details', index=False)

    base_name = os.path.splitext(os.path.basename(source_name))[0]
    file_suffix = f"_{timeframe}" if timeframe else ""
    final_filename = f"Report_{base_name}{file_suffix}.xlsx"
    st.download_button("Export Report", data=buf.getvalue(), file_name=final_filename)


def render_manual_tab():
    """Existing manual upload flow."""
    section_card("Manual Upload Dashboard", "Upload sales data, confirm column mapping, then generate charts and exports.")
    uploaded_file = st.file_uploader("Upload Sales Data (Excel or CSV)", type=['xlsx', 'csv'])

    if uploaded_file is None:
        return

    try:
        df = read_sales_file(uploaded_file, uploaded_file.name)
        st.caption(f"File uploaded: {uploaded_file.name}")

        auto_cols = find_columns(df)
        all_cols = list(df.columns)

        section_card("Column Mapping", "Detected columns are prefilled. Verify before generating dashboard output.")

        def get_col_idx(key):
            if key in auto_cols and auto_cols[key] in all_cols:
                return all_cols.index(auto_cols[key])
            return 0

        mapped_name = st.selectbox("Product Name", all_cols, index=get_col_idx('name'), key="manual_name")
        mapped_cost = st.selectbox("Price/Cost", all_cols, index=get_col_idx('cost'), key="manual_cost")
        mapped_qty = st.selectbox("Quantity", all_cols, index=get_col_idx('qty'), key="manual_qty")
        mapped_date = st.selectbox("Date (Optional)", ["None"] + all_cols, index=get_col_idx('date') + 1 if 'date' in auto_cols else 0, key="manual_date")
        mapped_order = st.selectbox("Order ID (Optional)", ["None"] + all_cols, index=get_col_idx('order_id') + 1 if 'order_id' in auto_cols else 0, key="manual_order")
        mapped_phone = st.selectbox("Phone (Optional)", ["None"] + all_cols, index=get_col_idx('phone') + 1 if 'phone' in auto_cols else 0, key="manual_phone")

        final_mapping = {
            'name': mapped_name,
            'cost': mapped_cost,
            'qty': mapped_qty,
            'date': mapped_date if mapped_date != "None" else None,
            'order_id': mapped_order if mapped_order != "None" else None,
            'phone': mapped_phone if mapped_phone != "None" else None
        }

        with st.expander("Search Raw Data"):
            search = st.text_input("Product search...", key="manual_search")
            if search:
                st.dataframe(df[df[mapped_name].astype(str).str.contains(search, case=False, na=False)], use_container_width=True)
            else:
                st.dataframe(df.head(10), use_container_width=True)

        generate_clicked, _ = render_action_bar("Generate dashboard", "manual_generate")
        if generate_clicked:
            drill, summ, top, timeframe, basket = process_data(df, final_mapping)
            if drill is not None:
                manual_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                render_dashboard_output(drill, summ, top, timeframe, basket, uploaded_file.name, manual_updated)

    except Exception as e:
        log_system_event("FILE_ERROR", str(e))
        st.error(f"File error: {e}")


def render_live_tab():
    """Always running dashboard from selected source."""
    section_card("Live Dashboard", "Reads from configured live source and auto-maps required columns.")
    
    # -----------------------------------------
    # âš™ï¸ CONFIGURATION EXPANDER (Hidden by default)
    # -----------------------------------------
    with st.expander("Live Data Configuration", expanded=False):
        source_options = ["Incoming Folder", "Google Sheet", "Google Drive Folder"]
        default_idx = 0
        if get_setting("GSHEET_URL", DEFAULT_GSHEET_URL):
            default_idx = 1
        elif get_setting("GSHEET_ID"):
            default_idx = 1
        elif get_setting("GDRIVE_FOLDER_ID") and get_gcp_service_account_info():
            default_idx = 2

        source_mode = st.selectbox("Live source", source_options, index=default_idx, key="live_source_mode")

        if source_mode == "Incoming Folder":
            st.caption(f"Incoming folder: {os.path.abspath(INCOMING_DIR)}")
        elif source_mode == "Google Sheet":
            st.caption("Uses GSHEET_URL (recommended) or GSHEET_ID + GSHEET_GID from st.secrets/environment.")
        elif source_mode == "Google Drive Folder":
            st.caption("Uses GDRIVE_FOLDER_ID and gcp_service_account credentials from st.secrets.")

        refresh_sec = st.slider("Auto refresh every (seconds)", min_value=15, max_value=300, value=30, step=5, key="live_refresh_sec")
        if hasattr(st, "autorefresh"):
            st.autorefresh(interval=refresh_sec * 1000, key="live_autorefresh")
        else:
            st.info("Auto-refresh is not available in this Streamlit version. Click below to reload.")
            st.button("Refresh Now", key="live_refresh_now")

        with st.expander("Cloud setup keys"):
            st.code(
                "# .streamlit/secrets.toml\nGSHEET_URL = \"https://docs.google.com/spreadsheets/d/e/.../pubhtml\"\n# Optional fallback if not using GSHEET_URL:\nGSHEET_ID = \"<google_sheet_id>\"\nGSHEET_GID = \"0\"\n\nGDRIVE_FOLDER_ID = \"<drive_folder_id>\"\n\n[gcp_service_account]\ntype = \"service_account\"\nproject_id = \"...\"\nprivate_key_id = \"...\"\nprivate_key = \"-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n\"\nclient_email = \"...\"\nclient_id = \"...\"\nauth_uri = \"https://accounts.google.com/o/oauth2/auth\"\ntoken_uri = \"https://oauth2.googleapis.com/token\"\nauth_provider_x509_cert_url = \"https://www.googleapis.com/oauth2/v1/certs\"\nclient_x509_cert_url = \"...\""
            )

    try:
        df_live, source_name, modified_at = load_live_source(source_mode)
        if modified_at:
            st.caption(f"Last updated: {modified_at}")

        auto_cols = find_columns(df_live)
        missing_required = [k for k in ['name', 'cost', 'qty'] if k not in auto_cols]
        if missing_required:
            st.error(f"Cannot auto-map required columns: {', '.join(missing_required)}")
            st.dataframe(df_live.head(20), use_container_width=True)
            return

        live_mapping = {
            'name': auto_cols.get('name'),
            'cost': auto_cols.get('cost'),
            'qty': auto_cols.get('qty'),
            'date': auto_cols.get('date'),
            'order_id': auto_cols.get('order_id'),
            'phone': auto_cols.get('phone')
        }

        drill, summ, top, timeframe, basket = process_data(df_live, live_mapping)
        if drill is not None:
            render_dashboard_output(drill, summ, top, timeframe, basket, source_name, modified_at)

    except Exception as e:
        log_system_event("LIVE_FILE_ERROR", str(e))
        st.error(f"Live source error: {e}")




