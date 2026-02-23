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
    st.markdown("### Settings")
    
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

# --- PROCESSING ---
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

            active_locs = list(loc_files.keys())
            main_df, match_count = core.add_stock_columns_from_inventory(
                product_df=main_df,
                item_name_col=title_col,
                inventory=inventory,
                locations=active_locs,
                sku_col=sku_col,
                sku_to_title_size=sku_to_title_size,
            )

            # 4. Results Dashboard
            st.divider()
            m1, m2 = st.columns(2)
            m1.metric("Items Processed", len(main_df))
            m2.metric("Successful Matches", match_count)

            # 5. Export Preparation
            cols = list(main_df.columns)
            reordered = []
            inserted = False
            for c in cols:
                if c in active_locs: continue
                reordered.append(c)
                if c == title_col and not inserted:
                    reordered.extend(active_locs)
                    inserted = True
            if not inserted: reordered.extend(active_locs)
            
            final_df = main_df[reordered].copy()

            # Grouping
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

            # 6. Display & Download
            st.subheader("Results Preview")
            export_df = final_df.drop(columns=["_gid"], errors="ignore")
            
            if group_col and separator == "Colored groups" and "_gid" in final_df.columns:
                clrs = ["#eff6ff", "#f0fdf4", "#fffbeb", "#fef2f2", "#faf5ff"]
                def color_rows(row):
                    gid = final_df["_gid"].get(row.name, -1)
                    if gid == -1: return [""] * len(row)
                    return [f"background-color: {clrs[int(gid) % len(clrs)]}"] * len(row)
                st.dataframe(export_df.head(100).style.apply(color_rows, axis=1), use_container_width=True)
            else:
                st.dataframe(export_df.head(100), use_container_width=True)

            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                export_df.to_excel(writer, index=False, sheet_name="Stock Report")
            
            st.download_button(
                "Download Excel Report",
                buffer.getvalue(),
                "Stock_Report.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True
            )

        except Exception as e:
            st.error(f"Error: {str(e)}")

