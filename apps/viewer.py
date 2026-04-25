"""celljar viewer — Streamlit UI for the harmonized data.

    streamlit run apps/viewer.py

Default: fetches metadata from HuggingFace; queries timeseries.parquet over
HTTPS via DuckDB. Override env vars:
    CELLJAR_LOCAL=1               read from data/harmonized/
    CELLJAR_HF_REVISION=v0.2.0    pin to a specific HF tag

Data access lives in apps/data.py; aging-plot helpers in apps/aging.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Sibling-module imports (apps/data.py, apps/aging.py, apps/plots.py).
sys.path.insert(0, str(Path(__file__).parent))
from data import (
    HF_REPO, HF_REVISION, USE_LOCAL,
    data_mtime, ensure_metadata,
    load_cells, load_tests, load_timeseries, load_cycle_summary_for_tests,
)
from aging import build_aging_figure, resolve_per_test_axis
from bundle import build_bundle_zip
from plots import build_overlay_figure, build_soh_figure


# --- Page setup ---
st.set_page_config(page_title="celljar", layout="wide")
st.title("celljar — harmonized public battery test data")
_source_caption = (
    f"data: HuggingFace `{HF_REPO}` @ `{HF_REVISION}`"
    if not USE_LOCAL else
    "data: local `data/harmonized/` (CELLJAR_LOCAL=1)"
)
st.caption(
    f"{_source_caption} · "
    "[github.com/mihnathul/celljar](https://github.com/mihnathul/celljar)"
)

_have_metadata, _hf_error = ensure_metadata()
if not _have_metadata:
    st.error(_hf_error or "No data available.")
    st.stop()


# --- Load data (cached) ---
_MTIME = data_mtime()
cells = load_cells(_mtime=_MTIME)
tests = load_tests(_mtime=_MTIME)

if cells.empty or tests.empty:
    st.warning(
        "No harmonized data. Run `python examples/demo_end_to_end.py` first, "
        "or check HuggingFace connectivity."
    )
    st.stop()


# --- Sidebar filters ---
st.sidebar.header("Filters")

src = st.sidebar.multiselect(
    "Source", sorted(cells["source"].dropna().unique()),
    help="Pick one or more datasets (e.g. HNEI, NASA_PCOE) to filter on.",
)

# Test-type options narrow to types actually present in the selected source(s).
# Without this, the dropdown shows all corpus test types even when the source
# doesn't contain any (e.g. BILLS only has cycle_aging / drive_cycle / capacity_check).
if src:
    cells_for_src = cells[cells["source"].isin(src)]
    available_types = (
        tests[tests["cell_id"].isin(cells_for_src["cell_id"])]
        ["test_type"].dropna().unique()
    )
else:
    available_types = tests["test_type"].dropna().unique()

ttype = st.sidebar.multiselect(
    "Test type", sorted(available_types),
    help="Filter by characterization protocol (hppc, cycle_aging, drive_cycle, etc.).",
)

# Empty-state gate: avoid rendering an empty-looking app on first load.
if not src or not ttype:
    st.info(
        "**Select one or more sources and test types in the sidebar to begin.**\n\n"
        f"celljar harmonizes {cells['source'].nunique()} sources and "
        f"{len(tests):,} tests across "
        f"{cells['chemistry'][cells['chemistry'] != 'mixed'].nunique()} named chemistries. "
        "Pick a source (e.g. HNEI for HPPC, MATR for cycling-aging) and a test type to "
        "inspect cells, overlay timeseries, and download bundles."
    )
    st.stop()

# Apply filters.
cells_f = cells[cells["source"].isin(src)]
tests_f = tests[
    tests["cell_id"].isin(cells_f["cell_id"]) & tests["test_type"].isin(ttype)
]


# --- Metrics row ---
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Cells", len(cells_f))
# Unique cell models — distinguishes cell instances from distinct hardware designs.
unique_models = (
    cells_f[["manufacturer", "model_number"]]
    .dropna(how="all").drop_duplicates().shape[0]
)
c2.metric("Unique cell models", unique_models)
c3.metric("Tests", len(tests_f))
c4.metric("Samples", f"{int(tests_f['n_samples'].sum()):,}" if not tests_f.empty else "0")
named_chems = cells_f["chemistry"][cells_f["chemistry"] != "mixed"].nunique()
c5.metric("Chemistries", named_chems,
          help="Count of named chemistry families (LFP/NMC/NCA/LCO/LMO/LTO). "
               "Excludes the generic 'mixed' bucket.")


# --- Bundle export ---
# Closure captures filtered DataFrames + the cached loader; bundle.py stays
# Streamlit-free.
def _build_bundle_zip(test_ids: list[str]) -> bytes:
    return build_bundle_zip(
        test_ids, cells_f, tests_f,
        load_timeseries=lambda tid: load_timeseries(tid, _mtime=_MTIME),
    )


# --- Tables ---
st.subheader("Cells")
st.dataframe(cells_f, use_container_width=True, hide_index=True)

st.subheader("Tests")
st.dataframe(tests_f, use_container_width=True, hide_index=True)


# --- Download ---
st.subheader("Download selected tests")
st.caption(
    "Pick one or more tests. The ZIP bundle mirrors celljar's canonical "
    "layout — `cells/*.json`, `tests/*.json`, `timeseries.parquet` — so you "
    "unpack it and have everything (metadata, license, citation, measurements) "
    "for those tests, self-contained."
)
if tests_f.empty:
    st.info("No tests available — adjust the sidebar filters.")
else:
    dl_selected = st.multiselect(
        "Tests", sorted(tests_f["test_id"].tolist()), key="dl_specific",
    )
    if dl_selected:
        bundle = _build_bundle_zip(dl_selected)
        st.download_button(
            f"Download {len(dl_selected)} test(s) — celljar bundle (ZIP)",
            data=bundle,
            file_name=f"celljar_bundle_{len(dl_selected)}tests.zip",
            mime="application/zip", key="dl_bundle",
            help=f"~{len(bundle)/1e6:.2f} MB — includes cell metadata, test "
                 "metadata, and timeseries parquet.",
        )
    else:
        st.download_button(
            "Download 0 test(s) — celljar bundle (ZIP)",
            data=b"", file_name="empty.zip", mime="application/zip",
            disabled=True, key="dl_bundle_disabled",
        )


# --- SOH vs cycle_count plot ---
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
    st.plotly_chart(build_soh_figure(aging), use_container_width=True)


# --- Inspect / overlay (V/I/T timeseries) ---
st.subheader("Inspect tests")
if tests_f.empty:
    st.stop()

selected = st.multiselect(
    "Tests to overlay",
    sorted(tests_f["test_id"].tolist()),
    default=[sorted(tests_f["test_id"].tolist())[0]],
)
align = st.checkbox("Align at t = 0", value=True)

if selected:
    loaded = {tid: load_timeseries(tid, _mtime=_MTIME) for tid in selected}

    if all(df.empty for df in loaded.values()):
        st.caption(
            "Selected tests have no raw V/I/T timeseries (likely summary-only "
            "sources like Naumann). See the 'Aging trajectory' panel below for "
            "cycle_summary data instead."
        )
    else:
        st.plotly_chart(
            build_overlay_figure(loaded, align=align),
            use_container_width=True,
        )


# --- Aging trajectory (cycle_summary) ---
if selected:
    csum_df = load_cycle_summary_for_tests(tuple(selected), _mtime=_MTIME)
    if not csum_df.empty:
        st.subheader("Aging trajectory")
        per_test_axis = resolve_per_test_axis(csum_df, selected)

        if not per_test_axis:
            st.caption(
                "Selected tests have cycle_summary rows but no usable aging axis "
                "(elapsed_time_s / equivalent_full_cycles / cycle_number all null)."
            )
        else:
            kinds = {kind for (_, _, kind) in per_test_axis.values()}
            if len(kinds) > 1:
                st.warning(
                    "Selected tests use incompatible aging axes — pick all "
                    "calendar OR all cycling tests."
                )
            else:
                fig_age, _, _, _ = build_aging_figure(csum_df, selected, per_test_axis)
                st.plotly_chart(
                    fig_age, use_container_width=True,
                    config={
                        "modeBarButtonsToAdd": ["drawline", "drawrect", "eraseshape"],
                        "displayModeBar": True,
                    },
                )
                st.caption(
                    "Aging trajectory derived from `cycle_summary.parquet` — "
                    "per-cycle aggregates (capacity retention, DC resistance) "
                    "emitted by sources like Naumann that publish check-up "
                    "summaries rather than raw V/I/T."
                )
