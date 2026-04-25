"""
celljar End-to-End Demo
=========================
Ingest → harmonize → write canonical outputs across one or more sources.

v0.1 sources:
  - ORNL Leaf (HPPC, mixed chemistry pouch, automotive)
  - HNEI Panasonic 18650PF (HPPC, NCA cylindrical 18650, BMS-canonical)
  - MATR / Severson 2019 (cycling, LFP 18650, fast-charge aging)

v0.2 additions:
  - CLO / Attia 2020 (closed-loop optimization, LFP 18650, fast-charge aging)
  - BILLS / Bills 2023 (Sony US18650VTC6, NMC cylindrical 18650)
  - NAUMANN / Naumann 2020 (Sony US26650FTC1, LFP 26650)
  - cycle_summary entity (per-cycle aggregate metrics)

Each source is processed only if its raw data is present at
data/raw/{subdir}/; see each README for download instructions.

Downstream tools (PyBOP, equiv-circ-model, ess_system_sim, custom fitters)
read the harmonized parquet directly — they don't need to know anything
about celljar's internals.
"""

import json
from pathlib import Path

import polars as pl

from celljar.ingest import ornl_leaf as ornl_ingest_mod
from celljar.ingest import hnei as hnei_ingest_mod
from celljar.ingest import matr as matr_ingest_mod
from celljar.ingest import clo as clo_ingest_mod
from celljar.ingest import bills as bills_ingest_mod
from celljar.ingest import naumann as naumann_ingest_mod
from celljar.ingest import mohtat as mohtat_ingest_mod
from celljar.ingest import nasa_pcoe as nasa_pcoe_ingest_mod
from celljar.ingest import snl_preger as snl_preger_ingest_mod
from celljar.ingest import ecker_2015 as ecker_2015_ingest_mod
from celljar.harmonize import harmonize_ornl_leaf as ornl_harm_mod
from celljar.harmonize import harmonize_hnei as hnei_harm_mod
from celljar.harmonize import harmonize_matr as matr_harm_mod
from celljar.harmonize import harmonize_clo as clo_harm_mod
from celljar.harmonize import harmonize_bills as bills_harm_mod
from celljar.harmonize import harmonize_naumann as naumann_harm_mod
from celljar.harmonize import harmonize_mohtat as mohtat_harm_mod
from celljar.harmonize import harmonize_nasa_pcoe as nasa_pcoe_harm_mod
from celljar.harmonize import harmonize_snl_preger as snl_preger_harm_mod
from celljar.harmonize import harmonize_ecker_2015 as ecker_2015_harm_mod
from celljar.harmonize.harmonize_schema import TimeseriesSchema, TestMetadataSchema
from celljar.harmonize.harmonize_schema import CycleSummarySchema


from celljar.bundle import nan_to_none as _nan_to_none, validate_invariants


ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"

# Source registry — each source is a thin adapter onto the canonical pipeline
SOURCES = [
    {
        "name": "ORNL_LEAF",
        "raw_subdir": "ornl_leaf",
        "ingest": ornl_ingest_mod.ingest,
        "harmonize": ornl_harm_mod.harmonize,
        # Nominal per Zenodo record (AESC 33.1 Ah rated). Kept in sync with
        # CELL_METADATA["nominal_capacity_Ah"] in harmonize_ornl_leaf.py.
        "capacity_Ah": 33.1,
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
    {
        "name": "CLO",
        "raw_subdir": "clo",
        "ingest": clo_ingest_mod.ingest,
        "harmonize": clo_harm_mod.harmonize,
        "capacity_Ah": 1.1,  # A123 APR18650M1A nominal — same as MATR
    },
    {
        "name": "BILLS",
        "raw_subdir": "bills",
        "ingest": bills_ingest_mod.ingest,
        "harmonize": bills_harm_mod.harmonize,
        "capacity_Ah": 3.0,  # Sony US18650VTC6 nominal
    },
    {
        "name": "NAUMANN",
        "raw_subdir": "naumann",
        "ingest": naumann_ingest_mod.ingest,
        "harmonize": naumann_harm_mod.harmonize,
        "capacity_Ah": 3.0,  # Sony US26650FTC1 nominal
    },
    {
        "name": "MOHTAT",
        "raw_subdir": "mohtat",
        "ingest": mohtat_ingest_mod.ingest,
        "harmonize": mohtat_harm_mod.harmonize,
        "capacity_Ah": 5.0,  # UMBL custom NMC532 pouch nominal
    },
    {
        "name": "NASA_PCOE",
        "raw_subdir": "nasa_pcoe",
        "ingest": nasa_pcoe_ingest_mod.ingest,
        "harmonize": nasa_pcoe_harm_mod.harmonize,
        "capacity_Ah": 2.0,  # NASA 18650 nominal (vendor undisclosed)
    },
    {
        "name": "SNL_PREGER",
        "raw_subdir": "snl_preger",
        "ingest": snl_preger_ingest_mod.ingest,
        "harmonize": snl_preger_harm_mod.harmonize,
        # Three chemistries with different nominals (1.1/3.0/3.4 Ah); the
        # harmonizer uses each cell's chemistry-specific nominal — the
        # demo-level capacity_Ah is only informational here.
        "capacity_Ah": 1.1,  # representative (A123 LFP); per-chem nominals applied per-cell
    },
    {
        "name": "ECKER_2015",
        "raw_subdir": "ecker_2015",
        "ingest": ecker_2015_ingest_mod.ingest,
        "harmonize": ecker_2015_harm_mod.harmonize,
        "capacity_Ah": 7.5,  # Kokam SLPB75106100 nominal
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
all_cycle_summary = []
for src, h in harmonized_per_source:
    cells = h.get("cells_metadata") or [h["cell_metadata"]]
    # One JSON file per cell
    for cell in cells:
        with open(cells_dir / f"{cell['cell_id']}.json", "w") as f:
            json.dump(_nan_to_none(cell), f, indent=2, default=str)
        all_chemistries.add(cell["chemistry"])
    # One JSON file per test, validated individually.
    # _nan_to_none must run BEFORE validate: pandera.polars treats NaN as a
    # non-null Float64 value (fails range checks like ge/le); pandera.pandas
    # treated NaN as missing.
    for test_dict in h["test_metadata"]:
        test_dict = _nan_to_none(test_dict)
        TestMetadataSchema.validate(pl.DataFrame([test_dict]))
        with open(tests_dir / f"{test_dict['test_id']}.json", "w") as f:
            json.dump(test_dict, f, indent=2, default=str)
    # Collect timeseries rows for the combined parquet
    all_timeseries.extend(h["timeseries"].values())
    # Collect cycle_summary rows, if the harmonizer emits them
    if "cycle_summary" in h and h["cycle_summary"]:
        all_cycle_summary.extend(h["cycle_summary"])

# Single timeseries parquet across all sources, joinable via test_id
ts_df = pl.concat(all_timeseries, how="diagonal_relaxed")
TimeseriesSchema.validate(ts_df)
ts_df.write_parquet(harmonized_dir / "timeseries.parquet", compression="zstd")

n_cells = len(list(cells_dir.glob("*.json")))
n_tests = len(list(tests_dir.glob("*.json")))
print(f"  Wrote {n_cells} cells/*.json, {n_tests} tests/*.json, "
      f"timeseries.parquet ({len(ts_df):,} rows)")

# Optional cycle_summary parquet (only sources that emit it).
# infer_schema_length=None scans all rows so per-source type variations
# (e.g. some sources emit f64, others emit i64 for the same column) are
# resolved correctly. _nan_to_none prevents NaN-as-non-null pandera-polars
# validation failures.
n_cycle_summary = 0
if all_cycle_summary:
    all_cycle_summary = [_nan_to_none(row) for row in all_cycle_summary]
    cs_df = pl.DataFrame(all_cycle_summary, infer_schema_length=None)
    CycleSummarySchema.validate(cs_df)
    cs_df.write_parquet(harmonized_dir / "cycle_summary.parquet", compression="zstd")
    n_cycle_summary = len(cs_df)
    print(f"  Wrote cycle_summary.parquet ({n_cycle_summary:,} rows)")

# Domain invariants (cross-field) — pandera handles per-field, this catches the rest.
print("Validating domain invariants...")
all_test_metadata = []
for src, h in harmonized_per_source:
    all_test_metadata.extend(h.get("test_metadata", []))
all_test_metadata = [_nan_to_none(t) for t in all_test_metadata]
validate_invariants(all_test_metadata, all_cycle_summary if all_cycle_summary else None)
print("  Invariants OK")

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
if n_cycle_summary:
    print(f"  Cycle summary: {n_cycle_summary:,} rows")
print()
print("Query the harmonized data with DuckDB:")
print("  duckdb -c \"SELECT * FROM 'data/harmonized/timeseries.parquet' LIMIT 5\"")
print()
print("Or load into Python:")
print("  import polars as pl")
print("  df = pl.read_parquet('data/harmonized/timeseries.parquet')")
