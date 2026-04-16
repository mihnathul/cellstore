"""cellstore viewer — minimal Streamlit app for the harmonized data.

    streamlit run apps/viewer.py

Expects `data/harmonized/` populated by `examples/demo_end_to_end.py`.
"""

import io
import json
import zipfile
from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots


DATA = Path(__file__).parent.parent / "data" / "harmonized"

st.set_page_config(page_title="cellstore", layout="wide")
st.title("cellstore — harmonized public battery test data")
st.caption(
    "Browse and inspect the canonical schema in action. Source: "
    "[github.com/mihnathul/cellstore](https://github.com/mihnathul/cellstore)"
)


@st.cache_data
def load_cells():
    return pd.DataFrame([json.load(open(p)) for p in (DATA / "cells").glob("*.json")])


@st.cache_data
def load_tests():
    return pd.DataFrame([json.load(open(p)) for p in (DATA / "tests").glob("*.json")])


@st.cache_data
def load_timeseries(test_id: str) -> pd.DataFrame:
    return duckdb.sql(
        f"SELECT * FROM '{DATA}/timeseries.parquet' WHERE test_id = '{test_id}' "
        "ORDER BY timestamp_s"
    ).df()


cells = load_cells()
tests = load_tests()

if cells.empty or tests.empty:
    st.warning("No harmonized data. Run `python examples/demo_end_to_end.py` first.")
    st.stop()

# --- Sidebar filters ---
st.sidebar.header("Filters")
src = st.sidebar.multiselect(
    "Source", sorted(cells["source"].dropna().unique()),
    default=sorted(cells["source"].dropna().unique()),
)
ttype = st.sidebar.multiselect(
    "Test type", sorted(tests["test_type"].dropna().unique()),
    default=sorted(tests["test_type"].dropna().unique()),
)

cells_f = cells[cells["source"].isin(src)]
tests_f = tests[tests["cell_id"].isin(cells_f["cell_id"]) & tests["test_type"].isin(ttype)]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Cells", len(cells_f))
# Unique cell models (dedupe by manufacturer + model_number) — distinguishes
# cell instances from distinct hardware designs (e.g. 8 cells of the same
# Kokam pouch model would count as 8 instances but 1 unique model).
unique_models = (
    cells_f[["manufacturer", "model_number"]]
    .dropna(how="all")
    .drop_duplicates()
    .shape[0]
)
c2.metric("Unique cell models", unique_models)
c3.metric("Tests", len(tests_f))
c4.metric("Samples", f"{int(tests_f['n_samples'].sum()):,}" if not tests_f.empty else "0")
# Exclude "mixed" — the enum's generic / undisclosed-composition bucket.
named_chems = cells_f["chemistry"][cells_f["chemistry"] != "mixed"].nunique()
c5.metric("Chemistries", named_chems,
          help="Count of named chemistry families (LFP/NMC/NCA/LCO/LMO/LTO). "
               "Excludes the generic 'mixed' bucket used when a publisher "
               "doesn't disclose exact composition.")

def _build_bundle_zip(test_ids: list[str]) -> bytes:
    """Build a self-contained cellstore export ZIP for the given test_ids.

    Layout (mirrors data/harmonized/):
        cells/<cell_id>.json        — one per unique cell referenced by the tests
        tests/<test_id>.json        — one per selected test
        timeseries.parquet          — only the selected tests' rows
    """
    selected_tests = tests_f[tests_f["test_id"].isin(test_ids)]
    selected_cell_ids = set(selected_tests["cell_id"].dropna())
    selected_cells = cells_f[cells_f["cell_id"].isin(selected_cell_ids)]

    ts_df = pd.concat([load_timeseries(tid) for tid in test_ids], ignore_index=True)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for _, cell in selected_cells.iterrows():
            zf.writestr(f"cells/{cell['cell_id']}.json",
                        cell.dropna().to_json(indent=2))
        for _, test in selected_tests.iterrows():
            zf.writestr(f"tests/{test['test_id']}.json",
                        test.dropna().to_json(indent=2))
        parquet_buf = io.BytesIO()
        ts_df.to_parquet(parquet_buf, index=False)
        zf.writestr("timeseries.parquet", parquet_buf.getvalue())
    return buf.getvalue()


# --- Tables ---
st.subheader("Cells")
st.dataframe(cells_f, use_container_width=True, hide_index=True)

st.subheader("Tests")
st.dataframe(tests_f, use_container_width=True, hide_index=True)

# --- Download ---
st.subheader("Download selected tests")
st.caption(
    "Pick one or more tests. The ZIP bundle mirrors cellstore's canonical "
    "layout — `cells/*.json`, `tests/*.json`, `timeseries.parquet` — so you "
    "unpack it and have everything needed (metadata, license, citation, and "
    "measurements) for those tests, self-contained."
)
if tests_f.empty:
    st.info("No tests available — adjust the sidebar filters.")
else:
    dl_selected = st.multiselect(
        "Tests",
        sorted(tests_f["test_id"].tolist()),
        key="dl_specific",
    )
    if dl_selected:
        bundle = _build_bundle_zip(dl_selected)
        st.download_button(
            f"Download {len(dl_selected)} test(s) — cellstore bundle (ZIP)",
            data=bundle,
            file_name=f"cellstore_bundle_{len(dl_selected)}tests.zip",
            mime="application/zip",
            key="dl_bundle",
            help=f"~{len(bundle)/1e6:.2f} MB — includes cell metadata, test "
                 "metadata, and timeseries parquet.",
        )
    else:
        st.download_button(
            "Download 0 test(s) — cellstore bundle (ZIP)",
            data=b"", file_name="empty.zip",
            mime="application/zip", disabled=True,
            key="dl_bundle_disabled",
        )

# --- Aging ---
if {"soh_pct", "cycle_count_at_test"}.issubset(tests_f.columns):
    aging = tests_f[
        tests_f["soh_pct"].notna()
        & tests_f["cycle_count_at_test"].notna()
        & (tests_f["cycle_count_at_test"] > 0)
    ]
else:
    aging = tests_f.iloc[0:0]

if not aging.empty:
    st.subheader("SOH vs. cycle count")
    fig = go.Figure()
    for cid, grp in aging.groupby("cell_id"):
        grp = grp.sort_values("cycle_count_at_test")
        fig.add_trace(go.Scatter(x=grp["cycle_count_at_test"], y=grp["soh_pct"],
                                 mode="lines+markers", name=cid))
    fig.update_layout(height=400, xaxis_title="Cycles elapsed",
                      yaxis_title="SOH (%)", yaxis=dict(range=[50, 105]))
    st.plotly_chart(fig, use_container_width=True)

# --- Overlay ---
st.subheader("Inspect tests")
if not tests_f.empty:
    selected = st.multiselect(
        "Tests to overlay",
        sorted(tests_f["test_id"].tolist()),
        default=[sorted(tests_f["test_id"].tolist())[0]],
    )
    align = st.checkbox("Align at t = 0", value=True)

    if selected:
        # Load once per test, reuse for both the plot and the download.
        loaded = {tid: load_timeseries(tid) for tid in selected}

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05)
        for test_id, ts in loaded.items():
            if ts.empty:
                continue
            t = ts["timestamp_s"].values
            if align:
                t = t - t[0]
            common = dict(mode="lines", legendgroup=test_id, name=test_id)
            fig.add_trace(go.Scatter(x=t, y=ts["current_A"], showlegend=True, **common),
                          row=1, col=1)
            fig.add_trace(go.Scatter(x=t, y=ts["voltage_V"], showlegend=False, **common),
                          row=2, col=1)
            fig.add_trace(go.Scatter(x=t, y=ts["temperature_C"], showlegend=False, **common),
                          row=3, col=1)
        fig.update_yaxes(title_text="Current (A)", row=1, col=1)
        fig.update_yaxes(title_text="Voltage (V)", row=2, col=1)
        fig.update_yaxes(title_text="Temperature (°C)", row=3, col=1)
        fig.update_xaxes(title_text="time (s)", row=3, col=1)
        fig.update_layout(height=600, margin=dict(t=20))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("To download the harmonized data for any of these tests, "
                   "use the 'Download selected tests' section above.")
