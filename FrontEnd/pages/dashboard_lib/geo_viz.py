import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import json

@st.cache_resource
def load_bangladesh_geojson():
    """Fetch and cache the Bangladesh 64-district GeoJSON."""
    url = "https://raw.githubusercontent.com/ahnaf-tahmid-chowdhury/Choropleth-Bangladesh/master/bangladesh_geojson_adm2_64_districts_zillas.json"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Failed to load map data: {str(e)}")
    return None

def render_district_map(df_sales: pd.DataFrame):
    """Render a choropleth map of Bangladesh showing sales density."""
    st.markdown("### 🗺️ Market Hotspots: Regional Density")
    
    geojson = load_bangladesh_geojson()
    if not geojson:
        st.warning("Map data is currently unavailable. Please check your connection.")
        return

    # 1. Aggregate Data by Parent District (for Map Matching)
    from BackEnd.core.geo import get_parent_district, get_region_display, normalize_city_name
    
    df_map = df_sales.copy()
    
    # Standardize 'state' as parent district (No BD-** codes)
    df_map["District_Parent"] = df_map.apply(lambda x: get_parent_district(x.get("state", x.get("city", "Unknown"))), axis=1)
    df_map["District_Parent"] = df_map["District_Parent"].apply(normalize_city_name)
    
    # Store refined label for deep intelligence
    df_map["Display_Region"] = df_map.apply(lambda x: get_region_display(x.get("city", ""), x.get("state", "")), axis=1)
    
    # --- Ensure all 64 Districts are present for a "Full Map" look ---
    all_districts = [f['properties']['ADM2_EN'] for f in geojson['features']]
    base_df = pd.DataFrame({"District": all_districts, "Value": 0.0})
    
    map_metric = st.segmented_control(
        "Map focus",
        options=["Revenue", "Orders"],
        default="Revenue",
        key="geo_map_metric_toggle",
        label_visibility="collapsed"
    )

    if map_metric == "Revenue":
        agg_raw = df_map.groupby("District_Parent")["order_total"].sum().reset_index()
        spot_raw = df_map.groupby("Display_Region")["order_total"].sum().reset_index()
        agg_raw.columns = ["District", "Value"]
        spot_raw.columns = ["Region", "Value"]
        color_scale = "Tealgrn"
        labels = {"Value": "Revenue (৳)"}
    else:
        agg_raw = df_map.groupby("District_Parent")["order_id"].nunique().reset_index()
        spot_raw = df_map.groupby("Display_Region")["order_id"].nunique().reset_index()
        agg_raw.columns = ["District", "Value"]
        spot_raw.columns = ["Region", "Value"]
        color_scale = "Purp"
        labels = {"Value": "Orders"}

    # Merge real data onto the base 64-district set
    agg_df = base_df.merge(agg_raw, on="District", how="left", suffixes=('_base', ''))
    agg_df["Value"] = agg_df["Value"].fillna(0) + agg_df["Value_base"]
    agg_df = agg_df.drop(columns=["Value_base"])

    gv1, gv2 = st.columns([2, 1])
    with gv1:
        # 2. Rendering Map
        fig = px.choropleth(
            agg_df,
            geojson=geojson,
            locations="District",
            featureidkey="properties.ADM2_EN",
            color="Value",
            color_continuous_scale=color_scale,
            range_color=(0, agg_df["Value"].max() if agg_df["Value"].max() > 0 else 100),
            labels=labels,
            template="plotly_dark"
        )

    fig.update_geos(
        projection_type="mercator",
        visible=False,
        bgcolor="rgba(0,0,0,0)"
    )
    
    fig.update_layout(
        height=600, # Increased height for "Full Map" feel
        margin={"r":0,"t":40,"l":0,"b":0},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        coloraxis_colorbar=dict(
            thicknessmode="pixels", thickness=15,
            lenmode="fraction", len=0.6,
            yanchor="middle", y=0.5,
            title=None
        )
    )

    st.plotly_chart(fig, width="stretch")
    
    with gv2:
        st.markdown(f"#### 🔥 Top 20 Hotspots")
        st.caption(f"Refined Areas ({map_metric})")
        
        # 3. Neighborhood Leaderboard (Graph Chart)
        spot_df = spot_raw.sort_values("Value", ascending=True).tail(20)
        fig_spot = px.bar(
            spot_df, x="Value", y="Region",
            orientation='h', color="Value",
            color_continuous_scale=color_scale,
            labels={"Value": map_metric, "Region": "Refined Area"},
            template="plotly_dark"
        )
        
        fig_spot.update_layout(
            height=600, margin=dict(l=0, r=0, t=0, b=0),
            coloraxis_showscale=False,
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=False, categoryorder="total ascending")
        )
        st.plotly_chart(fig_spot, width="stretch")
    
    # 📝 Summary Insight
    if not spot_raw.empty:
        top_spot = spot_raw.sort_values("Value", ascending=False).iloc[0]
        st.caption(f"📍 **Dominant Hub:** {top_spot['Region']} is your highest density zone for {map_metric.lower()}.")
