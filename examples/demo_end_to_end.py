"""
cellstore End-to-End Demo
=========================
Ingest → harmonize → write canonical outputs across one or more sources.

v0.1 sources:
  - ORNL Leaf (HPPC, mixed chemistry pouch, automotive)
  - HNEI Panasonic 18650PF (HPPC, NCA cylindrical 18650, BMS-canonical)
  - MATR / Severson 2019 (cycling, LFP 18650, fast-charge aging)

Each source is processed only if its raw data is present at
data/raw/{subdir}/; see each README for download instructions.

Downstream tools (PyBOP, equiv-circ-model, ess_system_sim, custom fitters)
read the harmonized parquet directly — they don't need to know anything
about cellstore's internals.
"""

import json
import math
from pathlib import Path

import pandas as pd

from cellstore.ingest import ornl_leaf as ornl_ingest_mod
from cellstore.ingest import hnei as hnei_ingest_mod
from cellstore.ingest import matr as matr_ingest_mod
from cellstore.harmonize import harmonize_ornl_leaf as ornl_harm_mod
from cellstore.harmonize import harmonize_hnei as hnei_harm_mod
from cellstore.harmonize import harmonize_matr as matr_harm_mod
from cellstore.harmonize.harmonize_schema import TimeseriesSchema, TestMetadataSchema


def _nan_to_none(obj):
    """Recursively convert NaN floats to None so json.dump emits spec-valid null."""
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if isinstance(obj, dict):
        return {k: _nan_to_none(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_nan_to_none(v) for v in obj]
    return obj


ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"

# Source registry — each source is a thin adapter onto the canonical pipeline
SOURCES = [
    {
        "name": "ORNL_LEAF",
        "raw_subdir": "ornl_leaf",
        "ingest": ornl_ingest_mod.ingest,
        "harmonize": ornl_harm_mod.harmonize,
        "capacity_Ah": 30.6,
    },
    {
        "name": "HNEI",
        "raw_subdir": "hnei",
        "ingest": hnei_ingest_mod.ingest,
        "harmonize": hnei_harm_mod.harmonize,
        "capacity_Ah": 2.9,
    },
    {
        "name": "MATR",
        "raw_subdir": "matr",
        "ingest": matr_ingest_mod.ingest,
        "harmonize": matr_harm_mod.harmonize,
        "capacity_Ah": 1.1,
    },
]


def process_source(src: dict):
    """Ingest + harmonize one source. Returns harmonized dict, or None if
    the source's raw data is missing (ingester raises FileNotFoundError)."""
    raw_dir = DATA / "raw" / src["raw_subdir"]
    print(f"\n[{src['name']}]")
    try:
        raw = src["ingest"](str(raw_dir))
    except FileNotFoundError as e:
        print(f"  Skipped — {e}")
        return None
    harmonized = src["harmonize"](raw, capacity_Ah=src["capacity_Ah"])
    cells = harmonized.get("cells_metadata") or [harmonized["cell_metadata"]]
    n_tests = len(harmonized["test_metadata"])
    n_ts = sum(len(df) for df in harmonized["timeseries"].values())
    print(f"  {len(cells)} cell(s), {n_tests} test(s), {n_ts:,} timeseries rows")
    for cell in cells[:3]:
        print(f"    · {cell['cell_id']} ({cell['chemistry']}, "
              f"{cell['nominal_capacity_Ah']} Ah)")
    if len(cells) > 3:
        print(f"    · ... and {len(cells) - 3} more")
    return harmonized


# ---------------------------------------------------------------------------
# Ingest + harmonize each source
# ---------------------------------------------------------------------------
print("=" * 60)
print("Ingest + Harmonize")
print("=" * 60)

harmonized_per_source = []
for src in SOURCES:
    h = process_source(src)
    if h is not None:
        harmonized_per_source.append((src, h))

if not harmonized_per_source:
    raise SystemExit("No sources produced harmonized data. Check data/raw/.")

# ---------------------------------------------------------------------------
# Write canonical harmonized outputs
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("Write canonical outputs")
print("=" * 60)

harmonized_dir = DATA / "harmonized"
cells_dir = harmonized_dir / "cells"
tests_dir = harmonized_dir / "tests"
cells_dir.mkdir(parents=True, exist_ok=True)
tests_dir.mkdir(parents=True, exist_ok=True)

all_timeseries = []
all_chemistries = set()
for src, h in harmonized_per_source:
    cells = h.get("cells_metadata") or [h["cell_metadata"]]
    # One JSON file per cell
    for cell in cells:
        with open(cells_dir / f"{cell['cell_id']}.json", "w") as f:
            json.dump(_nan_to_none(cell), f, indent=2, default=str)
        all_chemistries.add(cell["chemistry"])
    # One JSON file per test, validated individually
    for test_dict in h["test_metadata"]:
        TestMetadataSchema.validate(pd.DataFrame([test_dict]))
        with open(tests_dir / f"{test_dict['test_id']}.json", "w") as f:
            json.dump(_nan_to_none(test_dict), f, indent=2, default=str)
    # Collect timeseries rows for the combined parquet
    all_timeseries.extend(h["timeseries"].values())

# Single timeseries parquet across all sources, joinable via test_id
ts_df = pd.concat(all_timeseries, ignore_index=True)
TimeseriesSchema.validate(ts_df)
ts_df.to_parquet(harmonized_dir / "timeseries.parquet", index=False)

n_cells = len(list(cells_dir.glob("*.json")))
n_tests = len(list(tests_dir.glob("*.json")))
print(f"  Wrote {n_cells} cells/*.json, {n_tests} tests/*.json, "
      f"timeseries.parquet ({len(ts_df):,} rows)")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
chemistries = sorted(c for c in all_chemistries if c is not None)
print("\n" + "=" * 60)
print("Done")
print("=" * 60)
print(f"  Sources:     {[s['name'] for s, _ in harmonized_per_source]}")
print(f"  Cells:       {n_cells}")
print(f"  Tests:       {n_tests}")
print(f"  Chemistries: {chemistries}")
print(f"  Timeseries:  {len(ts_df):,} rows")
print()
print("Query the harmonized data with DuckDB:")
print("  duckdb -c \"SELECT * FROM 'data/harmonized/timeseries.parquet' LIMIT 5\"")
print()
print("Or load into Python:")
print("  import pandas as pd")
print("  df = pd.read_parquet('data/harmonized/timeseries.parquet')")
