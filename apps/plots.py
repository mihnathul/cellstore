"""Plot helpers for the celljar viewer.

Pure-function plot constructors (no Streamlit calls). The viewer renders
them via `st.plotly_chart`.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def build_overlay_figure(
    loaded: dict[str, pd.DataFrame],
    align: bool = True,
    max_pts: int = 20_000,
) -> go.Figure:
    """V / I / T overlay across selected tests, decimated for browser performance."""
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05)
    palette = px.colors.qualitative.Plotly
    non_empty = [tid for tid, df in loaded.items() if not df.empty]
    colors = {tid: palette[i % len(palette)] for i, tid in enumerate(non_empty)}

    for test_id in non_empty:
        ts = loaded[test_id]
        t = ts["timestamp_s"].values.astype(float)
        if align:
            t = t - t[0]

        # Decimate to keep the browser responsive on large tests.
        if len(t) > max_pts:
            step = max(1, len(t) // max_pts)
            idx = np.arange(0, len(t), step)
            t_dec = t[idx]
            i_dec = ts["current_A"].values[idx]
            v_dec = ts["voltage_V"].values[idx]
            temp_dec = ts["temperature_C"].values[idx]
        else:
            t_dec = t
            i_dec = ts["current_A"].values
            v_dec = ts["voltage_V"].values
            temp_dec = ts["temperature_C"].values

        common = dict(
            mode="lines", legendgroup=test_id, name=test_id,
            line=dict(color=colors[test_id]),
        )
        fig.add_trace(go.Scatter(x=t_dec, y=i_dec, showlegend=True, **common),
                      row=1, col=1)
        fig.add_trace(go.Scatter(x=t_dec, y=v_dec, showlegend=False, **common),
                      row=2, col=1)
        fig.add_trace(go.Scatter(x=t_dec, y=temp_dec, showlegend=False, **common),
                      row=3, col=1)

    fig.update_yaxes(title_text="Current (A)", row=1, col=1)
    fig.update_yaxes(title_text="Voltage (V)", row=2, col=1)
    fig.update_yaxes(title_text="Temperature (°C)", row=3, col=1)
    fig.update_xaxes(title_text="time (s)", row=3, col=1)
    fig.update_layout(
        height=600, margin=dict(t=20), dragmode="zoom",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(14,17,23,1)",
    )
    return fig


def build_soh_figure(aging: pd.DataFrame) -> go.Figure:
    """SOH vs cycle_count_at_test, one trace per cell."""
    fig = go.Figure()
    for cid, grp in aging.groupby("cell_id"):
        grp = grp.sort_values("cycle_count_at_test")
        fig.add_trace(go.Scatter(
            x=grp["cycle_count_at_test"], y=grp["soh_pct"],
            mode="lines+markers", name=cid,
        ))
    fig.update_layout(
        height=400, xaxis_title="Cycles elapsed",
        yaxis_title="SOH (%)", yaxis=dict(range=[50, 105]),
    )
    return fig
