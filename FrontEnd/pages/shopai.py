"""CRM Analytics page."""

from __future__ import annotations

import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

try:
    import anthropic
except Exception:  # pragma: no cover - optional dependency at runtime
    anthropic = None

from FrontEnd.components import ui
MOCK_CONVERSATIONS = [
    {
        "id": "c001",
        "customer": "Arif Rahman",
        "customer_id": "+8801711234567",
        "platform": "whatsapp",
        "status": "open",
        "preview": "Where is my order? I placed it 3 days ago.",
        "updated": datetime.now() - timedelta(minutes=4),
        "response_minutes": 2.8,
        "topic": "Order tracking",
        "tools": ["get_order_status"],
        "messages": [
            {"role": "customer", "content": "Hi, I placed order #2041 three days ago. Where is it?", "time": "10:02 AM"},
            {"role": "assistant", "content": "I checked order #2041. It shipped yesterday by Sundarban Courier and should arrive tomorrow by 6 PM.", "time": "10:02 AM"},
            {"role": "customer", "content": "Thanks. What if it does not arrive tomorrow?", "time": "10:05 AM"},
            {"role": "assistant", "content": "If it misses tomorrow evening, I will escalate it to logistics and offer a reship or refund.", "time": "10:05 AM"},
        ],
    },
    {
        "id": "c002",
        "customer": "Nadia Islam",
        "customer_id": "nadia.islam",
        "platform": "instagram",
        "status": "escalated",
        "preview": "I received a damaged product and want a refund.",
        "updated": datetime.now() - timedelta(minutes=22),
        "response_minutes": 6.4,
        "topic": "Refund",
        "tools": ["create_refund", "escalate_to_human"],
        "messages": [
            {"role": "customer", "content": "My kurta arrived torn at the sleeve. I want a full refund.", "time": "9:41 AM"},
            {"role": "assistant", "content": "I am sorry about that. I have escalated this to a human agent for a full-refund workflow.", "time": "9:44 AM"},
        ],
    },
    {
        "id": "c003",
        "customer": "Karim Uddin",
        "customer_id": "karim.uddin.88",
        "platform": "messenger",
        "status": "resolved",
        "preview": "Do you have this panjabi in red?",
        "updated": datetime.now() - timedelta(hours=1),
        "response_minutes": 1.9,
        "topic": "Product discovery",
        "tools": ["search_products", "get_coupon"],
        "messages": [
            {"role": "customer", "content": "Do you have the cotton panjabi in red?", "time": "8:15 AM"},
            {"role": "assistant", "content": "Yes. The red variant is available in M, L, and XL for Tk 850.", "time": "8:15 AM"},
            {"role": "customer", "content": "Can I get a discount?", "time": "8:17 AM"},
            {"role": "assistant", "content": "You can use SAVE10 today for 10 percent off.", "time": "8:17 AM"},
        ],
    },
    {
        "id": "c004",
        "customer": "Sumi Akter",
        "customer_id": "+8801822345678",
        "platform": "whatsapp",
        "status": "open",
        "preview": "What are your delivery charges to Chittagong?",
        "updated": datetime.now() - timedelta(minutes=2),
        "response_minutes": 1.2,
        "topic": "Delivery policy",
        "tools": ["delivery_policy"],
        "messages": [
            {"role": "customer", "content": "What are your delivery charges to Chittagong?", "time": "10:18 AM"},
            {"role": "assistant", "content": "Standard delivery is Tk 120 and express delivery is Tk 200. Orders above Tk 2000 ship free.", "time": "10:18 AM"},
        ],
    },
]

MOCK_PRODUCTS = [
    {"id": 101, "name": "Blue Denim Jacket", "price": "Tk 2,200", "stock": 14, "category": "Outerwear"},
    {"id": 102, "name": "Cotton Panjabi - Red", "price": "Tk 850", "stock": 8, "category": "Traditional"},
    {"id": 103, "name": "Printed Kurti Set", "price": "Tk 1,450", "stock": 0, "category": "Women"},
    {"id": 104, "name": "Casual Joggers", "price": "Tk 680", "stock": 23, "category": "Men"},
    {"id": 105, "name": "Embroidered Saree", "price": "Tk 3,800", "stock": 5, "category": "Women"},
]

MOCK_ORDERS_TODAY = [
    {"id": "#2048", "customer": "Rahim Ali", "total": "Tk 2,200", "status": "Processing"},
    {"id": "#2047", "customer": "Fatema B.", "total": "Tk 1,450", "status": "Shipped"},
    {"id": "#2046", "customer": "Jabir H.", "total": "Tk 680", "status": "Delivered"},
]


def _normalize_name(value: str) -> str:
    return " ".join(str(value).strip().lower().split()) if value else ""


def _normalize_phone(value: str) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) == 13 and digits.startswith("880"):
        return "0" + digits[3:]
    if len(digits) == 10 and digits.startswith("1"):
        return "0" + digits
    return digits


def _customer_lookup(customers_df: pd.DataFrame) -> tuple[dict[str, dict], dict[str, dict]]:
    if customers_df is None or customers_df.empty:
        return {}, {}

    customers = customers_df.copy()
    if "primary_name" not in customers.columns:
        customers["primary_name"] = customers.get("customer_name", "")
    name_lookup: dict[str, dict] = {}
    phone_lookup: dict[str, dict] = {}

    for record in customers.to_dict("records"):
        normalized_name = _normalize_name(record.get("primary_name", ""))
        if normalized_name and normalized_name not in name_lookup:
            name_lookup[normalized_name] = record

        for phone in str(record.get("all_phones", "")).split(","):
            normalized_phone = _normalize_phone(phone)
            if normalized_phone and normalized_phone not in phone_lookup:
                phone_lookup[normalized_phone] = record

    return name_lookup, phone_lookup


def build_shopai_conversation_frame(
    conversations: list[dict] | None = None,
    customers_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    conversations = conversations or MOCK_CONVERSATIONS
    name_lookup, phone_lookup = _customer_lookup(customers_df if isinstance(customers_df, pd.DataFrame) else pd.DataFrame())

    rows = []
    for convo in conversations:
        customer_record = phone_lookup.get(_normalize_phone(convo.get("customer_id", ""))) or name_lookup.get(
            _normalize_name(convo.get("customer", ""))
        )
        total_orders = pd.to_numeric((customer_record or {}).get("total_orders", 0), errors="coerce")
        total_orders = 0 if pd.isna(total_orders) else int(total_orders)
        total_revenue = pd.to_numeric((customer_record or {}).get("total_revenue", 0), errors="coerce")
        total_revenue = 0.0 if pd.isna(total_revenue) else float(total_revenue)
        rows.append(
            {
                "conversation_id": convo["id"],
                "customer": convo["customer"],
                "customer_id": convo["customer_id"],
                "platform": convo["platform"].title(),
                "status": convo["status"].title(),
                "preview": convo["preview"],
                "updated": convo["updated"],
                "response_minutes": float(convo.get("response_minutes", 0)),
                "topic": convo.get("topic", "General"),
                "tools": convo.get("tools", []),
                "segment": (customer_record or {}).get("segment", "Unmapped"),
                "total_orders": total_orders,
                "total_revenue": total_revenue,
                "favorite_product": (customer_record or {}).get("favorite_product", ""),
                "recency_days": pd.to_numeric((customer_record or {}).get("recency_days", pd.NA), errors="coerce"),
            }
        )
    return pd.DataFrame(rows)


def build_shopai_crm_summary(
    conversations: list[dict] | None = None,
    customers_df: pd.DataFrame | None = None,
) -> dict[str, object]:
    frame = build_shopai_conversation_frame(conversations=conversations, customers_df=customers_df)
    if frame.empty:
        return {
            "conversations": frame,
            "status_mix": pd.DataFrame(),
            "platform_mix": pd.DataFrame(),
            "segment_mix": pd.DataFrame(),
            "tool_usage": pd.DataFrame(),
            "queue": pd.DataFrame(),
            "recommendations": ["No ShopAI conversations are available yet."],
            "kpis": {
                "conversations": 0,
                "resolution_rate": 0.0,
                "avg_response_minutes": 0.0,
                "needs_attention": 0,
                "linked_customers": 0,
            },
        }

    status_mix = frame["status"].value_counts().rename_axis("Status").reset_index(name="Conversations")
    platform_mix = frame["platform"].value_counts().rename_axis("Platform").reset_index(name="Conversations")
    segment_mix = (
        frame["segment"].fillna("Unmapped").replace("", "Unmapped").value_counts().rename_axis("Segment").reset_index(name="Conversations")
    )
    tool_usage = (
        frame.explode("tools")["tools"]
        .fillna("no_tool")
        .value_counts()
        .rename_axis("Tool")
        .reset_index(name="Calls")
    )

    resolution_rate = float((frame["status"].eq("Resolved").sum() / max(len(frame), 1)) * 100)
    needs_attention = int(frame["status"].isin(["Open", "Escalated"]).sum())
    linked_customers = int(frame["segment"].ne("Unmapped").sum())

    queue = frame.copy()
    queue["priority_score"] = (
        queue["status"].map({"Escalated": 3, "Open": 2, "Resolved": 1}).fillna(0)
        + queue["segment"].map({"VIP": 2, "At Risk": 2, "Potential Loyalist": 1}).fillna(0)
    )
    queue = queue.sort_values(["priority_score", "updated"], ascending=[False, False]).reset_index(drop=True)

    recommendations: list[str] = []
    escalated_vips = frame[(frame["status"] == "Escalated") & (frame["segment"] == "VIP")]
    if not escalated_vips.empty:
        recommendations.append(f"{len(escalated_vips)} VIP conversations are escalated and should be handled first.")
    at_risk_open = frame[(frame["status"].isin(["Open", "Escalated"])) & (frame["segment"] == "At Risk")]
    if not at_risk_open.empty:
        recommendations.append(f"{len(at_risk_open)} at-risk customers are waiting inside the support queue.")
    delivery_share = (frame["topic"] == "Order tracking").mean()
    if delivery_share >= 0.25:
        recommendations.append("Order-tracking demand is high. A stronger delivery-status macro would reduce repetitive support load.")
    if linked_customers < len(frame):
        recommendations.append("Some conversations are not matched to customer intelligence yet. Add phone or account linking for stronger CRM routing.")
    if not recommendations:
        recommendations.append("ShopAI conversation load is balanced. Keep automations focused on fast order-tracking and refund resolution.")

    return {
        "conversations": frame,
        "status_mix": status_mix,
        "platform_mix": platform_mix,
        "segment_mix": segment_mix,
        "tool_usage": tool_usage,
        "queue": queue,
        "recommendations": recommendations,
        "kpis": {
            "conversations": int(len(frame)),
            "resolution_rate": resolution_rate,
            "avg_response_minutes": float(frame["response_minutes"].mean()),
            "needs_attention": needs_attention,
            "linked_customers": linked_customers,
        },
    }


def _format_currency(value: float) -> str:
    return f"Tk {value:,.0f}"


def _customer_source_frame() -> pd.DataFrame:
    if "customer_insights_df" in st.session_state:
        return st.session_state["customer_insights_df"]
    if "dashboard_data" in st.session_state:
        return st.session_state["dashboard_data"].get("customers", pd.DataFrame())
    return pd.DataFrame()


def render_shopai_crm_snapshot(customers_df: pd.DataFrame | None = None):
    customer_frame = customers_df if isinstance(customers_df, pd.DataFrame) else _customer_source_frame()
    crm = build_shopai_crm_summary(customers_df=customer_frame)
    kpis = crm["kpis"]

    st.markdown("#### CRM Analytics Snapshot")
    metric_cols = st.columns(4)
    metric_cols[0].metric("Conversations", f"{kpis['conversations']:,}")
    metric_cols[1].metric("Resolution Rate", f"{kpis['resolution_rate']:.0f}%")
    metric_cols[2].metric("Avg Response", f"{kpis['avg_response_minutes']:.1f} min")
    metric_cols[3].metric("Needs Attention", f"{kpis['needs_attention']:,}")
    ui.commentary("CRM signals", crm["recommendations"][:3])

    chart_cols = st.columns(2)
    with chart_cols[0]:
        status_chart = ui.donut_chart(
            crm["status_mix"],
            values="Conversations",
            names="Status",
            title="Conversation Status Mix",
        )
        st.plotly_chart(status_chart, use_container_width=True)
    with chart_cols[1]:
        platform_chart = ui.bar_chart(
            crm["platform_mix"].sort_values("Conversations"),
            x="Conversations",
            y="Platform",
            title="Conversation Volume by Platform",
            color="Conversations",
            color_scale="Blues",
            orientation="h",
            text_auto=".0f",
        )
        st.plotly_chart(platform_chart, use_container_width=True)


def _ensure_shopai_state():
    st.session_state.setdefault("shopai_test_messages", [])
    st.session_state.setdefault("shopai_selected_conversation", MOCK_CONVERSATIONS[0]["id"])
    st.session_state.setdefault("shopai_anthropic_key", "")


def _render_conversation_detail(conversation: dict, crm_frame: pd.DataFrame):
    matched = crm_frame[crm_frame["conversation_id"] == conversation["id"]]
    if not matched.empty:
        row = matched.iloc[0]
        ui.info_box(
            "Customer context",
            (
                f"Segment: {row['segment']} | Lifetime revenue: {_format_currency(row['total_revenue'])} | "
                f"Orders: {int(row['total_orders'])} | Favorite product: {row['favorite_product'] or 'Unknown'}"
            ),
        )

    st.markdown(f"**{conversation['customer']}**")
    st.caption(
        f"{conversation['platform'].title()} | {conversation['status'].title()} | "
        f"Updated {conversation['updated'].strftime('%Y-%m-%d %H:%M')}"
    )
    for message in conversation["messages"]:
        role = "assistant" if message["role"] == "assistant" else "user"
        with st.chat_message(role):
            st.write(message["content"])
            st.caption(message["time"])


def _render_agent_lab():
    config_col, chat_col = st.columns([1, 1.5])
    with config_col:
        system_prompt = st.text_area(
            "System Prompt",
            value=(
                "You are ShopAI, a concise customer-support agent for a Bangladeshi commerce brand. "
                "Prioritize delivery clarity, refund confidence, and sales conversion."
            ),
            height=140,
        )
        st.session_state["shopai_anthropic_key"] = st.text_input(
            "Anthropic API Key",
            type="password",
            value=st.session_state["shopai_anthropic_key"],
            placeholder="sk-ant-...",
        )
        if st.button("Clear Agent Lab", use_container_width=True):
            st.session_state["shopai_test_messages"] = []
            st.rerun()

    with chat_col:
        for message in st.session_state["shopai_test_messages"]:
            with st.chat_message(message["role"]):
                st.write(message["content"])

        prompt = st.chat_input("Test ShopAI with a customer message...")
        if not prompt:
            return

        st.session_state["shopai_test_messages"].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

        api_key = st.session_state["shopai_anthropic_key"]
        if not api_key or anthropic is None:
            mock_replies = {
                "order": "I checked the order. It is in transit and scheduled for delivery tomorrow.",
                "refund": "I have marked this for a refund workflow and escalated it to the human queue.",
                "discount": "You can use SAVE10 today for a 10 percent discount.",
            }
            reply = next((text for key, text in mock_replies.items() if key in prompt.lower()), "ShopAI would answer here. Add an Anthropic key to test live responses.")
            time.sleep(0.4)
        else:
            try:
                client = anthropic.Anthropic(api_key=api_key)
                response = client.messages.create(
                    model="claude-3-haiku-20240307",
                    max_tokens=350,
                    system=system_prompt,
                    messages=[{"role": msg["role"], "content": msg["content"]} for msg in st.session_state["shopai_test_messages"]],
                )
                reply = next((block.text for block in response.content if hasattr(block, "text")), "No response returned.")
            except Exception as exc:  # pragma: no cover - runtime API failures
                reply = f"API error: {exc}"

        st.session_state["shopai_test_messages"].append({"role": "assistant", "content": reply})
        with st.chat_message("assistant"):
            st.write(reply)


def render_shopai_tab():
    _ensure_shopai_state()
    customer_frame = _customer_source_frame()
    crm = build_shopai_crm_summary(customers_df=customer_frame)
    crm_frame = crm["conversations"]
    kpis = crm["kpis"]

    ui.hero(
        "ShopAI CRM",
        (
            "Support operations are surfaced as CRM analytics: conversation load, linked customer value, resolution pressure, "
            "and the highest-priority follow-ups for the team."
        ),
        chips=[
            "Conversation analytics",
            "Customer-linked routing",
            "Agent lab",
            "Commerce context",
        ],
    )

    linked_share = (kpis["linked_customers"] / max(kpis["conversations"], 1)) * 100
    ui.metric_highlight(
        "CRM pressure",
        f"{kpis['needs_attention']} conversations need action",
        f"{linked_share:.0f}% of the visible queue is matched to customer intelligence.",
    )
    ui.commentary("CRM narrative", crm["recommendations"])

    metric_cols = st.columns(4)
    metric_cols[0].metric("Conversations", f"{kpis['conversations']:,}")
    metric_cols[1].metric("Resolution Rate", f"{kpis['resolution_rate']:.0f}%")
    metric_cols[2].metric("Avg Response", f"{kpis['avg_response_minutes']:.1f} min")
    metric_cols[3].metric("Linked Customers", f"{kpis['linked_customers']:,}")
    ui.badge("Customer linking uses phone first, then normalized name matching against customer intelligence.")

    tabs = st.tabs(["CRM Command", "Queue", "Agent Lab", "Commerce Context"])

    with tabs[0]:
        top_left, top_right = st.columns(2)
        with top_left:
            status_chart = ui.donut_chart(
                crm["status_mix"],
                values="Conversations",
                names="Status",
                title="Conversation Status Mix",
            )
            st.plotly_chart(status_chart, use_container_width=True)
        with top_right:
            platform_chart = ui.bar_chart(
                crm["platform_mix"].sort_values("Conversations"),
                x="Conversations",
                y="Platform",
                title="Platform Load",
                color="Conversations",
                color_scale="Blues",
                text_auto=".0f",
            )
            st.plotly_chart(platform_chart, use_container_width=True)

        bottom_left, bottom_right = st.columns(2)
        with bottom_left:
            segment_chart = ui.donut_chart(
                crm["segment_mix"],
                values="Conversations",
                names="Segment",
                title="CRM Segment Exposure",
                color_scale="Tealgrn",
            )
            st.plotly_chart(segment_chart, use_container_width=True)
        with bottom_right:
            tool_chart = ui.bar_chart(
                crm["tool_usage"].sort_values("Calls"),
                x="Calls",
                y="Tool",
                title="Tool Usage in Queue",
                color="Calls",
                color_scale="Oranges",
                text_auto=".0f",
            )
            st.plotly_chart(tool_chart, use_container_width=True)

        st.markdown("#### Priority Queue")
        st.dataframe(
            crm["queue"][
                [
                    "customer",
                    "platform",
                    "status",
                    "topic",
                    "segment",
                    "response_minutes",
                    "preview",
                ]
            ].rename(
                columns={
                    "customer": "Customer",
                    "platform": "Platform",
                    "status": "Status",
                    "topic": "Topic",
                    "segment": "Segment",
                    "response_minutes": "Response (min)",
                    "preview": "Latest Message",
                }
            ),
            use_container_width=True,
            hide_index=True,
            height=320,
        )

    with tabs[1]:
        filter_col_1, filter_col_2, filter_col_3 = st.columns([1, 1, 1.2])
        with filter_col_1:
            status_filter = st.selectbox("Status", ["All"] + sorted(crm_frame["status"].unique().tolist()))
        with filter_col_2:
            platform_filter = st.selectbox("Platform", ["All"] + sorted(crm_frame["platform"].unique().tolist()))
        with filter_col_3:
            segment_filter = st.selectbox("Customer Segment", ["All"] + sorted(crm_frame["segment"].unique().tolist()))

        queue_view = crm_frame.copy()
        if status_filter != "All":
            queue_view = queue_view[queue_view["status"] == status_filter]
        if platform_filter != "All":
            queue_view = queue_view[queue_view["platform"] == platform_filter]
        if segment_filter != "All":
            queue_view = queue_view[queue_view["segment"] == segment_filter]

        queue_col, detail_col = st.columns([1, 1.5])
        with queue_col:
            st.markdown(f"#### {len(queue_view)} visible conversations")
            for convo_id in queue_view["conversation_id"].tolist():
                row = queue_view[queue_view["conversation_id"] == convo_id].iloc[0]
                label = f"{row['customer']} | {row['status']} | {row['platform']}"
                if st.button(label, key=f"shopai_queue_{convo_id}", use_container_width=True):
                    st.session_state["shopai_selected_conversation"] = convo_id

        with detail_col:
            selected = next(
                (convo for convo in MOCK_CONVERSATIONS if convo["id"] == st.session_state["shopai_selected_conversation"]),
                None,
            )
            if not selected:
                st.info("Select a conversation to inspect the CRM context.")
            else:
                _render_conversation_detail(selected, crm_frame)

    with tabs[2]:
        _render_agent_lab()

    with tabs[3]:
        commerce_left, commerce_right = st.columns([1.2, 1])
        with commerce_left:
            product_df = pd.DataFrame(MOCK_PRODUCTS)
            search = st.text_input("Search product catalog", "")
            if search:
                product_df = product_df[product_df["name"].str.contains(search, case=False, na=False)]
            stock_chart = px.bar(
                product_df.sort_values("stock"),
                x="stock",
                y="name",
                orientation="h",
                color="stock",
                title="Product Stock Context",
                color_continuous_scale="Tealgrn",
                text_auto=".0f",
            )
            st.plotly_chart(ui.apply_plotly_theme(stock_chart, height=380), use_container_width=True)
            st.dataframe(product_df.rename(columns={"name": "Product", "price": "Price", "stock": "Stock", "category": "Category"}), use_container_width=True, hide_index=True)

        with commerce_right:
            st.markdown("#### Orders touched today")
            for order in MOCK_ORDERS_TODAY:
                st.markdown(
                    f"""
                    <div class="hub-ui.card" style="padding:0.9rem 1rem;">
                        <div style="font-weight:700; color:#102132;">{order['id']} · {order['customer']}</div>
                        <div style="color:#5f7183; margin-top:0.25rem;">{order['total']} · {order['status']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            ui.info_box(
                "Why this matters",
                "Keep ShopAI close to order and stock context so support can resolve tracking, refund, and conversion questions without jumping between tools.",
            )
