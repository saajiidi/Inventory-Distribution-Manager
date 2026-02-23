import streamlit as st
import pandas as pd
import io
import core
import importlib

# Ensure core is always fresh during development
try:
    importlib.reload(core)
except Exception:
    pass

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Inventory Manager",
    page_icon="�",
    layout="wide",
)

# --- CLEAN UI STYLING ---
st.markdown("""
    <style>
    /* Logo Animation */
    @keyframes float {
        0% { transform: translateY(0px); }
        50% { transform: translateY(-5px); }
        100% { transform: translateY(0px); }
    }
    .logo-container {
        display: flex;
        align-items: center;
        gap: 15px;
        margin-bottom: 20px;
    }
    .animated-logo {
        width: 50px;
        height: 50px;
        animation: float 3s ease-in-out infinite;
    }
    
    /* Mobile Responsive Adjustments */
    @media (max-width: 768px) {
        .logo-container {
            flex-direction: column;
            text-align: center;
        }
        .main-header {
            font-size: 1.5rem !important;
        }
        [data-testid="column"] {
            width: 100% !important;
            flex-basis: 100% !important;
            min-width: 100% !important;
            margin-bottom: 1rem;
        }
        .stMetric {
            margin-bottom: 0.5rem;
        }
        /* Mobile File Uploader Fix */
        [data-testid="stFileUploader"] section {
            padding: 0.5rem 1rem !important;
            min-height: 70px !important;
        }
        [data-testid="stFileUploader"] [data-testid="stMarkdownContainer"] p {
            font-size: 0.7rem !important;
            line-height: 1.1 !important;
            margin: 0 !important;
            display: block !important;
        }
        /* Hide the 'Drag and drop' part on tiny screens, keep just 'Browse' */
        [data-testid="stFileUploader"] section > div > div > span:first-child {
            font-size: 0.75rem !important;
        }
    }
    
    /* Clean Professional Styling */
    .stApp {
        background-color: #f8f9fa;
    }
    .main-header {
        color: #1e3a8a;
        font-weight: 700;
        margin-bottom: 0rem;
    }
    .section-card {
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        border: 1px solid #e5e7eb;
        margin-bottom: 1rem;
    }
    .stMetric {
        background: white;
        padding: 10px;
        border-radius: 8px;
        border: 1px solid #e5e7eb;
    }
    .status-badge {
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 600;
        display: inline-block;
        margin-bottom: 4px;
    }
    .status-sync { background-color: #d1fae5; color: #065f46; border: 1px solid #10b981; }
    .status-pending { background-color: #f3f4f6; color: #4b5563; border: 1px solid #d1d5db; }
    
    /* Simplified Buttons */
    .stButton > button {
        border-radius: 6px;
        font-weight: 500;
    }
    </style>
""", unsafe_allow_html=True)

# --- HEADER ---
st.markdown('''
    <div class="logo-container">
        <svg class="animated-logo" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M21 8L12 3L3 8V16L12 21L21 16V8Z" stroke="#1e3a8a" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M12 21V12" stroke="#1e3a8a" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M12 12L21 8" stroke="#1e3a8a" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M12 12L3 8" stroke="#1e3a8a" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M7 5.5V11.5" stroke="#1e3a8a" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        <h1 class="main-header" style="margin:0;">Inventory Stock Mapper</h1>
    </div>
''', unsafe_allow_html=True)
st.write("Professional distribution and matching tool")
st.divider()

# --- MAIN LAYOUT ---
col_main, col_sidebar = st.columns([3, 1], gap="large")

with col_main:
    # 1. Product List Section
    st.markdown("### 1. Upload Product List")
    with st.container():
        product_file = st.file_uploader(
            "Upload the main order list or product file (XLSX/CSV)", 
            type=["xlsx", "csv"]
        )

    st.write(" ")
    
    # 2. Inventory Locations Grid
    st.markdown("### 2. Inventory Locations")
    locations = ["Ecom", "Mirpur", "Wari", "Cumilla", "Sylhet"]
    loc_files = {}
    
    cols = st.columns(len(locations))
    for i, loc in enumerate(locations):
        with cols[i]:
            # Status badge
            is_loaded = st.session_state.get(f"loaded_{loc}", False)
            if is_loaded:
                st.markdown(f'<span class="status-badge status-sync">✓ {loc} Synced</span>', unsafe_allow_html=True)
            else:
                st.markdown(f'<span class="status-badge status-pending">{loc} Pending</span>', unsafe_allow_html=True)
            
            f = st.file_uploader(f"Upload {loc}", type=["xlsx", "csv"], key=loc, label_visibility="collapsed")
            if f:
                loc_files[loc] = f
                st.session_state[f"loaded_{loc}"] = True
            else:
                st.session_state[f"loaded_{loc}"] = False

# --- SIDEBAR & SETTINGS ---
with col_sidebar:
    st.markdown("### 🔍 Filters")
    search_query = st.text_input("Search Product/SKU", placeholder="Type to search...", help="Search by name or SKU")
    
    status_options = ["✅ Available", "⚠️ Partial", "❌ OOS", "❌ No Match"]
    selected_statuses = st.multiselect("Fulfillment Status", options=status_options, default=status_options)
    
    stock_threshold = st.number_input("Total Stock Less Than", min_value=0, value=None, help="Filter items where total stock across all locations is below this number.")

    st.divider()
    st.markdown("### ⚙️ Settings")
    
    separator = st.radio(
        "Report Grouping Style",
        ["Colored groups", "Blank lines"],
        index=0
    )
    
    debug_mode = st.checkbox("Show technical details", value=False)
    
    st.divider()
    
    if product_file:
        process_btn = st.button("Generate Stock Map", type="primary", use_container_width=True)
    else:
        st.info("Upload files to start processing")
        process_btn = False

# --- PERSISTENCE & FILTERING ---
# We store the processed results to allow instant filtering without re-running the heavy logic
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
if 'active_locs' not in st.session_state:
    st.session_state.active_locs = []

if process_btn and product_file:
    with st.spinner("Processing data..."):
        try:
            # 1. Read Product List
            product_file.seek(0)
            main_df = pd.read_csv(product_file) if product_file.name.endswith(".csv") else pd.read_excel(product_file)

            # 2. Load Inventory Mapping
            inventory, load_warnings, enriched_dfs, sku_to_title_size = core.load_inventory_from_uploads(loc_files)
            
            # 3. Match Stock
            _size_col, _qty_col, title_col, sku_col = core.identify_columns(main_df)
            if not title_col:
                st.error("Could not find a Title/Item Name column.")
                st.stop()

            st.session_state.active_locs = list(loc_files.keys())
            main_df, match_count = core.add_stock_columns_from_inventory(
                product_df=main_df,
                item_name_col=title_col,
                inventory=inventory,
                locations=st.session_state.active_locs,
                sku_col=sku_col,
                sku_to_title_size=sku_to_title_size,
            )
            
            st.session_state.processed_data = main_df
            st.session_state.title_col = title_col
            st.session_state.sku_col = sku_col
            st.success("Analysis complete!")

        except Exception as e:
            st.error(f"Error during processing: {str(e)}")

# --- DISPLAY LOGIC (Apply Sidebar Filters) ---
if st.session_state.processed_data is not None:
    df_to_show = st.session_state.processed_data.copy()
    active_locs = st.session_state.active_locs
    title_col = st.session_state.title_col
    sku_col = st.session_state.sku_col

    # 1. Search Filter
    if search_query:
        q = search_query.lower()
        search_mask = df_to_show[title_col].astype(str).str.lower().str.contains(q)
        if sku_col and sku_col in df_to_show.columns:
            search_mask |= df_to_show[sku_col].astype(str).str.lower().str.contains(q)
        df_to_show = df_to_show[search_mask]

    # 2. Status Filter
    if selected_statuses:
        def status_match(val):
            val_str = str(val)
            for s in selected_statuses:
                clean_s = s.replace("✅ ", "").replace("⚠️ ", "").replace("❌ ", "")
                if clean_s in val_str: return True
            return False
        df_to_show = df_to_show[df_to_show['Fulfillment'].apply(status_match)]

    # 3. Threshold Filter
    if stock_threshold is not None:
        total_stock = df_to_show[active_locs].sum(axis=1)
        df_to_show = df_to_show[total_stock < stock_threshold]

    # 4. Results Dashboard
    st.divider()
    m1, m2, m3 = st.columns(3)
    m1.metric("Items Processed", len(st.session_state.processed_data))
    m2.metric("Filtered View", len(df_to_show))
    has_match = ~df_to_show['Match Status'].str.contains("No Match", na=False)
    m3.metric("Matches in View", has_match.sum())

    # 5. Export Preparation
    cols = list(df_to_show.columns)
    reordered = []
    inserted = False
    # Move priority columns near the item name
    priority_cols = ["Fulfillment", "Dispatch Suggestion"]
    status_col = "Match Status"
    
    other_cols = [c for c in cols if c not in priority_cols and c not in active_locs and c != status_col]
    
    for c in other_cols:
        reordered.append(c)
        if c == title_col and not inserted:
            reordered.extend(priority_cols)
            reordered.extend(active_locs)
            inserted = True
    
    if not inserted:
        reordered = priority_cols + reordered + active_locs
    
    # Always append status at the absolute end
    if status_col in cols:
        reordered.append(status_col)
    
    final_df = df_to_show[reordered].copy()

    group_col = core.get_group_by_column(final_df)
    if group_col:
        final_df = final_df.sort_values(group_col, na_position="last").reset_index(drop=True)
        seen = {}
        gids = []
        for val in final_df[group_col]:
            if pd.isna(val) or str(val).strip() == "": gids.append(-1)
            else:
                v = str(val).strip()
                if v not in seen: seen[v] = len(seen)
                gids.append(seen[v])
        final_df["_gid"] = gids
    else:
        final_df["_gid"] = -1

    # 6. Display & Download
    st.subheader("Results Preview")
    export_df = final_df.drop(columns=["_gid"], errors="ignore")
    
    def color_stock_cells(row):
        styles = [""] * len(row)
        for loc in active_locs:
            if loc in row:
                idx = row.index.get_loc(loc)
                val = row[loc]
                try:
                    num = float(val)
                    if num == 0: styles[idx] = "color: #dc2626; font-weight: bold;"
                    elif num > 0: styles[idx] = "color: #16a34a;"
                except: pass
        if "Fulfillment" in row:
            f_idx = row.index.get_loc("Fulfillment")
            f_val = str(row["Fulfillment"])
            if "Available" in f_val: styles[f_idx] = "background-color: #dcfce7; color: #166534; font-weight: bold;"
            elif "OOS" in f_val or "No Match" in f_val: styles[f_idx] = "background-color: #fee2e2; color: #991b1b;"
            elif "Partial" in f_val: styles[f_idx] = "background-color: #fef9c3; color: #854d0e;"
        if group_col and separator == "Colored groups" and "_gid" in final_df.columns:
            gid = final_df["_gid"].get(row.name, -1)
            if gid != -1:
                g_clrs = ["#eff6ff55", "#f0fdf455", "#fffbeb55", "#fef2f255", "#faf5ff55"]
                base_bg = g_clrs[int(gid) % len(g_clrs)]
                styles = [s + f"background-color: {base_bg};" if "background-color" not in s else s for s in styles]
        return styles

    st.dataframe(export_df.head(100).style.apply(color_stock_cells, axis=1), use_container_width=True)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        export_df.to_excel(writer, index=False, sheet_name="Stock Report")
        workbook = writer.book
        worksheet = writer.sheets["Stock Report"]
        fmt_red = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        fmt_green = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        fmt_yellow = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})

        for i, col_name in enumerate(export_df.columns):
            if col_name in active_locs:
                worksheet.conditional_format(1, i, len(export_df), i, {
                    'type': 'cell', 'criteria': 'equal to', 'value': 0, 'format': fmt_red
                })
                worksheet.conditional_format(1, i, len(export_df), i, {
                    'type': 'cell', 'criteria': 'greater than', 'value': 0, 'format': fmt_green
                })
            if col_name == "Fulfillment":
                worksheet.conditional_format(1, i, len(export_df), i, {
                    'type': 'text', 'criteria': 'containing', 'value': 'Available', 'format': fmt_green
                })
                worksheet.conditional_format(1, i, len(export_df), i, {
                    'type': 'text', 'criteria': 'containing', 'value': 'OOS', 'format': fmt_red
                })
                worksheet.conditional_format(1, i, len(export_df), i, {
                    'type': 'text', 'criteria': 'containing', 'value': 'Partial', 'format': fmt_yellow
                })
        for i, col in enumerate(export_df.columns):
            worksheet.set_column(i, i, 15)

        # --- GROUP COLORING IN EXCEL ---
        if group_col and separator == "Colored groups" and "_gid" in final_df.columns:
            # Subtle pastel colors for groups
            excel_g_clrs = [
                '#F0F7FF', # Blue-ish
                '#F0FFF4', # Green-ish
                '#FFFBEB', # Yellow-ish
                '#FFF5F5', # Red-ish
                '#F5F3FF'  # Purple-ish
            ]
            group_formats = [workbook.add_format({'bg_color': c}) for c in excel_g_clrs]
            
            for row_idx, gid in enumerate(final_df["_gid"]):
                if gid != -1:
                    fmt = group_formats[int(gid) % len(group_formats)]
                    # Apply format to the entire data row (row_idx + 1 because of header)
                    worksheet.set_row(row_idx + 1, None, fmt)


    st.download_button(
        "Download Filtered Excel Report",
        buffer.getvalue(),
        "Stock_Report_Filtered.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True
    )


