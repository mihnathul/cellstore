"""ZIP-bundle export for the celljar viewer.

Builds a self-contained ZIP that mirrors data/harmonized/ for a chosen set
of tests — cells/*.json + tests/*.json + a slim timeseries.parquet with
only the selected tests. Used by the viewer's "Download selected tests"
button.

Pure helper — no Streamlit calls. The viewer passes the filtered cells/tests
DataFrames + a timeseries-loader callback so this module stays decoupled
from Streamlit caching specifics.
"""

from __future__ import annotations

import io
import zipfile
from typing import Callable

import pandas as pd


def build_bundle_zip(
    test_ids: list[str],
    cells_df: pd.DataFrame,
    tests_df: pd.DataFrame,
    load_timeseries: Callable[[str], pd.DataFrame],
) -> bytes:
    """Return a ZIP byte-string mirroring data/harmonized/ for the given test_ids.

    Layout:
        cells/<cell_id>.json        — one per unique cell referenced by the tests
        tests/<test_id>.json        — one per selected test
        timeseries.parquet          — only the selected tests' rows
    """
    selected_tests = tests_df[tests_df["test_id"].isin(test_ids)]
    selected_cell_ids = set(selected_tests["cell_id"].dropna())
    selected_cells = cells_df[cells_df["cell_id"].isin(selected_cell_ids)]
    ts_df = pd.concat(
        [load_timeseries(tid) for tid in test_ids],
        ignore_index=True,
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for _, cell in selected_cells.iterrows():
            zf.writestr(f"cells/{cell['cell_id']}.json",
                        cell.dropna().to_json(indent=2))
        for _, test in selected_tests.iterrows():
            zf.writestr(f"tests/{test['test_id']}.json",
                        test.dropna().to_json(indent=2))
        parquet_buf = io.BytesIO()
        ts_df.to_parquet(parquet_buf, index=False, compression="zstd")
        zf.writestr("timeseries.parquet", parquet_buf.getvalue())
    return buf.getvalue()
