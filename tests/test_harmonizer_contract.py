"""Contract test: every harmonize_<source>.harmonize() returns HarmonizerOutput shape.

This is the layer-2 golden-path test (the layer-1 ingest test is per-source
and skipped if raw data is missing; the layer-3 end-to-end test is
test_schema_roundtrip.py). Together they form one regression-catching test
per architectural layer.
"""

from __future__ import annotations

import importlib

import polars as pl
import pytest


HARMONIZER_MODULES = [
    "celljar.harmonize.harmonize_ornl_leaf",
    "celljar.harmonize.harmonize_hnei",
    "celljar.harmonize.harmonize_matr",
    "celljar.harmonize.harmonize_clo",
    "celljar.harmonize.harmonize_bills",
    "celljar.harmonize.harmonize_mohtat",
    "celljar.harmonize.harmonize_nasa_pcoe",
    "celljar.harmonize.harmonize_snl_preger",
    "celljar.harmonize.harmonize_naumann",
    "celljar.harmonize.harmonize_ecker_2015",
]

REQUIRED_KEYS = {"cell_metadata", "cells_metadata", "test_metadata", "timeseries"}
OPTIONAL_KEYS = {"cycle_summary", "eis"}  # eis kept for future


@pytest.mark.parametrize("module_path", HARMONIZER_MODULES)
def test_harmonizer_module_imports_and_exposes_harmonize(module_path: str) -> None:
    """Every harmonizer module is importable and exports a `harmonize` callable."""
    mod = importlib.import_module(module_path)
    assert hasattr(mod, "harmonize"), f"{module_path}.harmonize not found"
    assert callable(mod.harmonize), f"{module_path}.harmonize is not callable"


def test_harmonizer_output_shape_via_ornl():
    """End-to-end: ORNL ingest → harmonize → assert HarmonizerOutput shape.

    ORNL ships in the repo so this test always runs (no skip). Other sources
    require local downloads and are exercised via test_schema_roundtrip when
    their raw data is present.
    """
    from celljar.ingest import ornl_leaf as ornl_ingest_mod
    from celljar.harmonize import harmonize_ornl_leaf as ornl_harmonize_mod

    ingested = ornl_ingest_mod.ingest("data/raw/ornl_leaf")
    out = ornl_harmonize_mod.harmonize(ingested)

    # Required keys present
    missing = REQUIRED_KEYS - out.keys()
    assert not missing, f"harmonizer output missing required keys: {missing}"

    # Type contracts
    assert isinstance(out["cell_metadata"], dict)
    assert isinstance(out["cells_metadata"], list)
    assert all(isinstance(c, dict) for c in out["cells_metadata"])
    assert isinstance(out["test_metadata"], list)
    assert all(isinstance(t, dict) for t in out["test_metadata"])
    assert isinstance(out["timeseries"], dict)
    assert all(isinstance(df, pl.DataFrame) for df in out["timeseries"].values())

    # cycle_summary, if present, is list[dict]
    if "cycle_summary" in out:
        assert isinstance(out["cycle_summary"], list)
        assert all(isinstance(r, dict) for r in out["cycle_summary"])

    # FK consistency: every test's cell_id appears in cells_metadata
    cell_ids = {c["cell_id"] for c in out["cells_metadata"]}
    for t in out["test_metadata"]:
        assert t["cell_id"] in cell_ids, (
            f"test {t['test_id']} references unknown cell_id {t['cell_id']}"
        )

    # FK consistency: every timeseries key matches a test_id
    test_ids = {t["test_id"] for t in out["test_metadata"]}
    for tid in out["timeseries"]:
        assert tid in test_ids, f"timeseries key {tid} has no matching test"
