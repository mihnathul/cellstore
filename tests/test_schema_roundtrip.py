"""Smoke test — runs the ORNL pipeline end-to-end and verifies that
every harmonized output conforms to the published schema.

This is the contract test for celljar: it proves the pipeline produces
data that matches the schema declarations. If this test ever fails, either
the pipeline drifted or the schema drifted — and they should be brought
back into sync before any release.

ORNL is used because its raw data is bundled in the repo (tests pass on a
fresh clone). HNEI requires a download and is not exercised here.
"""

from pathlib import Path

import polars as pl

from celljar.ingest.ornl_leaf import ingest
from celljar.harmonize.harmonize_ornl_leaf import harmonize
from celljar.harmonize.harmonize_schema import (
    CellMetadataSchema, TestMetadataSchema, TimeseriesSchema,
)


REPO_ROOT = Path(__file__).parent.parent
ORNL_RAW = REPO_ROOT / "data" / "raw" / "ornl_leaf"


def test_ornl_pipeline_produces_schema_valid_output():
    """End-to-end: ingest → harmonize → validate against published schemas.

    Asserts that:
      1. The pipeline runs without errors on bundled ORNL Leaf data.
      2. cell_metadata, test_metadata, and timeseries all satisfy their
         declared Pandera schemas (which are the source-of-truth for the
         JSON Schema and Frictionless artifacts).
      3. The output shape matches expectations (3 temperatures, 3 tests,
         non-trivial sample count).
    """
    # Stage 2 + 3: ingest → harmonize
    raw = ingest(str(ORNL_RAW))
    harmonized = harmonize(raw, capacity_Ah=33.1)  # AESC pouch nominal (Zenodo)

    # Schema validation — these raise SchemaError if the data drifts from
    # the declared schema. That's the contract.
    cell_meta_df = pl.DataFrame([harmonized["cell_metadata"]])
    CellMetadataSchema.validate(cell_meta_df)

    for test_dict in harmonized["test_metadata"]:
        TestMetadataSchema.validate(pl.DataFrame([test_dict]))

    ts_df = pl.concat(list(harmonized["timeseries"].values()), how="vertical_relaxed")
    TimeseriesSchema.validate(ts_df)

    # Shape sanity — catches accidental data loss or misconfiguration
    assert len(harmonized["timeseries"]) == 3, \
        f"Expected 3 ORNL HPPC temperatures, got {len(harmonized['timeseries'])}"
    assert len(harmonized["test_metadata"]) == 3, \
        f"Expected 3 test_metadata records, got {len(harmonized['test_metadata'])}"
    assert len(ts_df) > 1000, \
        f"Expected substantial timeseries (>1000 rows), got {len(ts_df)}"
    assert harmonized["cell_metadata"]["cell_id"] == "ORNL_LEAF_2013"
