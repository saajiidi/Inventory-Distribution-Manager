import streamlit as st
import pandas as pd
import io
import xlsxwriter
import math
import core


def normalize_key(val):
    """Normalize values from Excel/CSV so keys match reliably (e.g., 123.0 -> '123')."""
    if pd.isna(val):
        return ""
    if isinstance(val, (int,)):
        return str(int(val))
    if isinstance(val, (float,)):
        if math.isfinite(val) and float(val).is_integer():
            return str(int(val))
        return str(val).strip()
    s = str(val).strip()
    if s.endswith(".0") and s[:-2].replace(".", "", 1).isdigit():
        s = s[:-2]
    return s


def normalize_size(val):
    if pd.isna(val) or val == "":
        return "NO_SIZE"
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def item_name_to_title_size(item_name: str):
    """
    Convert product list 'Item Name' into (title, size).
    Expected common format: "Title - Size" (split on last ' - ').
    If size can't be parsed, returns ("<item_name>", "NO_SIZE").
    """
    if item_name is None or (isinstance(item_name, float) and pd.isna(item_name)):
        return "", "NO_SIZE"
    s = normalize_key(item_name)
    if not s:
        return "", "NO_SIZE"

    if " - " in s:
        left, right = s.rsplit(" - ", 1)
        title = left.strip()
        size = normalize_size(right.strip())
        if title and size and size != "NO_SIZE":
            return title, size

    return s.strip(), "NO_SIZE"


def build_title_size_key(title: str, size: str):
    title_norm = normalize_key(title).strip()
    size_norm = normalize_size(size)
    if not title_norm:
        return ""
    if size_norm and size_norm != "NO_SIZE":
        return f"{title_norm} - {size_norm}".lower()
    return title_norm.lower()


def identify_columns(df: pd.DataFrame):
    """Auto-identify relevant columns based on headers."""
    cols = [str(c) for c in df.columns]
    cols_map = {c.lower().strip(): c for c in cols}

    size_col = None
    qty_col = None
    title_col = None

    for c_lower, c_orig in cols_map.items():
        if "size" in c_lower:
            size_col = c_orig
        if "quantity" in c_lower or "qty" in c_lower or "stock" in c_lower:
            qty_col = c_orig
        if "title" in c_lower or "item name" in c_lower or "product" in c_lower or "name" in c_lower:
            title_col = c_orig

    if not qty_col and "Quantity" in df.columns:
        qty_col = "Quantity"

    return size_col, qty_col, title_col


def load_inventory_from_uploads(uploaded_files):
    """
    Process uploaded inventory files.
    Keys:
      - "{Title} - {Size}" (lower-cased)
    """
    inventory = {}
    all_locations = list(uploaded_files.keys())
    warnings = []
    # Optional: keep enriched dataframes for debugging/preview
    enriched_dfs = {}

    for loc_name, file_obj in uploaded_files.items():
        if file_obj is None:
            continue
        try:
            file_obj.seek(0)
            if file_obj.name.endswith(".csv"):
                df = pd.read_csv(file_obj)
            else:
                df = pd.read_excel(file_obj)

            size_col, qty_col, title_col = identify_columns(df)

            if not title_col:
                warnings.append(f"⚠️ {loc_name}: Missing 'Title/Item Name' column. Skipped.")
                continue

            if not qty_col:
                warnings.append(f"⚠️ {loc_name}: Missing 'Quantity' column. Assuming 0 stock.")

            # Add helper column: Title + " - " + Size
            # This is computed for every uploaded inventory file and can be used for matching/QA.
            def _joined_title_size(r):
                title = normalize_key(r.get(title_col, ""))
                size = "NO_SIZE"
                if size_col and size_col in df.columns:
                    size = normalize_size(r.get(size_col, ""))
                if title and size and size != "NO_SIZE":
                    return f"{title} - {size}"
                return title

            df["Title - Size"] = df.apply(_joined_title_size, axis=1)
            enriched_dfs[loc_name] = df

            for _, row in df.iterrows():
                qty = 0
                if qty_col and qty_col in df.columns:
                    try:
                        val = row[qty_col]
                        if pd.notna(val):
                            if isinstance(val, str):
                                val = val.replace(",", "").strip()
                                if val == "":
                                    val = 0
                            qty = int(float(val))
                    except Exception:
                        qty = 0

                keys_to_add = []

                if title_col:
                    # Use the joined column (Title - Size) for keying
                    joined = normalize_key(row.get("Title - Size", ""))
                    if joined:
                        keys_to_add.append(joined.lower())

                for k in keys_to_add:
                    if not k:
                        continue
                    if k not in inventory:
                        inventory[k] = {loc: 0 for loc in all_locations}
                    inventory[k][loc_name] += qty

        except Exception as e:
            warnings.append(f"❌ Error in {loc_name}: {e}")

    return inventory, warnings, enriched_dfs


st.set_page_config(page_title="Inventory Manager", layout="wide")

st.title("Inventory Distribution Manager")
st.markdown(
    "**Matching Logic:** Match by `Item Name` → `Title - Size`. "
    "Uses SKU as a secondary confirmation. "
    "Flags mismatches (e.g. SKU matches but Name doesn't)."
)

col1, col2 = st.columns([2, 2], gap="large")

with col1:
    st.subheader("1. Product List")
    product_file = st.file_uploader("Upload Product List", type=["xlsx", "csv"])

with col2:
    st.subheader("2. Inventory Locations")
    loc_files = {}
    locations = ["Ecom", "Mirpur", "Wari", "Cumilla", "Sylhet"]
    tabs = st.tabs(locations)
    for i, loc in enumerate(locations):
        with tabs[i]:
            f = st.file_uploader(loc, type=["xlsx", "csv"], key=loc)
            if f:
                loc_files[loc] = f
                st.success(f"{loc} loaded")

st.sidebar.divider()
debug_mode = st.sidebar.checkbox("Enable Debug Mode", value=False)

st.divider()

if product_file:
    process_btn = st.button("Process Distribution", type="primary")

    if process_btn:
        with st.spinner("Processing..."):
            try:
                product_file.seek(0)
                if product_file.name.endswith(".csv"):
                    main_df = pd.read_csv(product_file)
                else:
                    main_df = pd.read_excel(product_file)

                _size_col, _qty_col, title_col, sku_col = core.identify_columns(main_df)
                if not title_col:
                    st.error("No 'Item Name'/'Title' column found in product list.")
                    st.stop()

                inventory, load_warnings, enriched_dfs, sku_to_title_size = core.load_inventory_from_uploads(loc_files)
                if load_warnings:
                    with st.expander("Loading warnings"):
                        for w in load_warnings:
                            st.write(w)

                if debug_mode:
                    st.sidebar.write("Sample inventory keys:")
                    st.sidebar.write(list(inventory.keys())[:10])
                    with st.expander("Inventory files preview (with Title - Size)"):
                        for loc_name, df_inv in enriched_dfs.items():
                            st.write(f"**{loc_name}**")
                            st.dataframe(df_inv.head(20), use_container_width=True)

                active_locations = list(loc_files.keys())
                main_df, match_count = core.add_stock_columns_from_inventory(
                    product_df=main_df,
                    item_name_col=title_col,
                    inventory=inventory,
                    locations=active_locations,
                    sku_col=sku_col,
                    sku_to_title_size=sku_to_title_size,
                )
                total = len(main_df)
                perc = (match_count / total) * 100 if total else 0
                st.metric("Rows processed", total)
                st.metric("Matched rows", f"{match_count} ({perc:.1f}%)")

                # Put location columns right after the title column
                cols = list(main_df.columns)
                final_cols = []
                inserted = False
                for c in cols:
                    if c in active_locations:
                        continue
                    final_cols.append(c)
                    if c == title_col and not inserted:
                        final_cols.extend(active_locations)
                        inserted = True
                if not inserted:
                    final_cols.extend(active_locations)

                out_df = main_df[final_cols].copy()

                # Group by order number or phone: sort and optionally add blank row / colored rows
                group_col = core.get_group_by_column(out_df)
                separator = st.sidebar.radio(
                    "Separator between orders",
                    ["Colored rows (alternating)", "Blank row between orders"],
                    index=0,
                ) if group_col else None

                if group_col:
                    out_df = out_df.sort_values(group_col, na_position="last").reset_index(drop=True)
                    # Assign group index (0,1,2,...) by first appearance for alternating colors
                    seen = {}
                    group_ids = []
                    for val in out_df[group_col]:
                        if pd.isna(val) or str(val).strip() == "":
                            group_ids.append(-1)
                        else:
                            key = str(val).strip()
                            if key not in seen:
                                seen[key] = len(seen)
                            group_ids.append(seen[key])
                    out_df["_group_id_"] = group_ids

                    if separator == "Blank row between orders":
                        parts = []
                        for _, grp in out_df.groupby("_group_id_", sort=False):
                            parts.append(grp)
                            if grp["_group_id_"].iloc[0] != -1:
                                blank = pd.DataFrame([{c: "" if c != "_group_id_" else -1 for c in out_df.columns}])
                                parts.append(blank)
                        # Drop the trailing blank row (added after last group)
                        out_display = pd.concat(parts[:-1], ignore_index=True) if len(parts) > 1 else out_df
                    else:
                        out_display = out_df
                else:
                    out_display = out_df

                # Drop helper for display/export
                export_df = out_display.drop(columns=["_group_id_"], errors="ignore")
                preview_df = export_df.head(50)

                st.subheader("Result preview")
                if group_col and "_group_id_" in out_display.columns:
                    # Color rows by order group in preview
                    group_ids_preview = out_display["_group_id_"].head(50)
                    colors = ["#e3f2fd", "#fff8e1", "#f3e5f5", "#e8f5e9", "#fce4ec"]

                    def row_style(row):
                        g = group_ids_preview.loc[row.name] if row.name in group_ids_preview.index else -1
                        if g == -1 or (isinstance(g, float) and pd.isna(g)):
                            return [""] * len(row)
                        c = colors[int(g) % len(colors)]
                        return [f"background-color: {c}"] * len(row)

                    st.dataframe(preview_df.style.apply(row_style, axis=1), use_container_width=True)
                else:
                    st.dataframe(preview_df, use_container_width=True)

                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                    export_df.to_excel(writer, index=False, sheet_name="Stock")
                    ws = writer.sheets["Stock"]
                    for i, c in enumerate(export_df.columns):
                        width = 10
                        try:
                            width = max(export_df[c].astype(str).map(len).max(), len(str(c))) + 2
                        except Exception:
                            pass
                        ws.set_column(i, i, min(width, 50))

                    if group_col and "_group_id_" in out_display.columns:
                        # Alternating row fill by order group
                        light_fills = [
                            "#dae8fc", "#fff2cc", "#e2efda", "#f8cecc", "#e4dfec",
                        ]
                        formats = [writer.book.add_format({"bg_color": f}) for f in light_fills]
                        for row_idx in range(len(out_display)):
                            g = out_display["_group_id_"].iloc[row_idx]
                            if g >= 0:
                                ws.set_row(row_idx + 1, None, formats[g % len(formats)])

                buffer.seek(0)
                st.download_button(
                    "Download Excel report",
                    buffer,
                    "Inventory_Report.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                )

            except Exception as e:
                st.error(f"Error: {e}")

