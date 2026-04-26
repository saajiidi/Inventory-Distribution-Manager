
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go





















def build_discrete_color_map(labels: list[str], scale_name: str = "Plasma") -> dict[str, str]:
    cleaned = [str(label) for label in labels if str(label).strip()]
    if not cleaned:
        return {}

    color_map: dict[str, str] = {}
    for index, label in enumerate(cleaned):
        value = (index / max(1, len(cleaned) - 1)) * 0.85 if len(cleaned) > 1 else 0.0
        color_map[label] = px.colors.sample_colorscale(scale_name, [value])[0]
    return color_map


def apply_plotly_theme(
    fig,
    *,
    height: int = 380,
    margin: dict | None = None,
    showlegend: bool = True,
):
    chart_margin = margin or dict(l=12, r=12, t=56, b=12)
    fig.update_layout(
        height=height,
        margin=chart_margin,
        showlegend=showlegend,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        title=dict(font=dict(size=16)),
        hoverlabel=dict(
            bgcolor="rgba(15, 23, 42, 0.95)",
            font=dict(family="Inter, sans-serif", color="white", size=12),
            bordercolor="rgba(255, 255, 255, 0.1)",
            namelength=-1,
        ),
        legend=dict(
            bgcolor="rgba(255,255,255,0.0)",
            borderwidth=0,
            font=dict(size=11),
        ),
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="rgba(128, 128, 128, 0.15)", zeroline=False)
    return fig


def donut_chart(
    df: pd.DataFrame,
    *,
    values: str,
    names: str,
    title: str,
    color_scale: str = "Plasma",
):
    if df is None or df.empty or values not in df.columns or names not in df.columns:
        return go.Figure()

    labels = df[names].astype(str).tolist()
    color_map = build_discrete_color_map(labels, scale_name=color_scale)
    fig = px.pie(
        df,
        values=values,
        names=names,
        color=names,
        hole=0.6,
        title=title,
        color_discrete_map=color_map,
    )

    if fig.data and getattr(fig.data[0], "values", None) is not None:
        total_value = sum(fig.data[0].values) or 1
        text_positions = [
            "inside" if (value / total_value) >= 0.02 else "none"
            for value in fig.data[0].values
        ]
    else:
        text_positions = "inside"

    fig.update_traces(
        textposition=text_positions,
        textinfo="label+percent",
        textfont_size=11,
        pull=0.01,
        rotation=270,
        direction="clockwise",
    )
    fig.update_layout(
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.05,
        ),
        uniformtext_minsize=10,
        uniformtext_mode="hide",
    )
    return apply_plotly_theme(fig, height=380, margin=dict(l=80, r=160, t=44, b=24))


def bar_chart(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    color: str | None = None,
    color_scale: str = "Tealgrn",
    orientation: str = "h",
    text_auto: str | bool | None = None,
):
    if df is None or df.empty or x not in df.columns or y not in df.columns:
        return go.Figure()

    chart_color = color or x
    fig = px.bar(
        df,
        x=x,
        y=y,
        color=chart_color,
        orientation=orientation,
        title=title,
        text_auto=text_auto if text_auto is not None else False,
        color_continuous_scale=color_scale if chart_color in df.columns else None,
    )
    if orientation == "h":
        fig.update_layout(yaxis_title="", xaxis_title="")
    else:
        fig.update_layout(xaxis_title="", yaxis_title="")
    return apply_plotly_theme(fig, height=400)
