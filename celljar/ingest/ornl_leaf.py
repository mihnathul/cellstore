"""Ingester for ORNL 2013 Nissan Leaf HPPC data.

Data source: github.com/batterysim/equiv-circ-model
Tested by: Oak Ridge National Laboratory (ORNL)
Cell specs from: National Renewable Energy Laboratory (NREL)
Cell: 2013 Nissan Leaf, NMC, 33.1 Ah rated

ORNL's CSV format is BatteryArchive-similar (single signed capacity/energy
column, embedded ``Mode`` string), so parsing is delegated to the generic
``read_batteryarchive_csv`` loader. The only source-specific logic retained
here is (a) the 10 C file ships without a header row, and (b) the
``Mode`` / ``Data`` columns are preserved raw for the harmonizer's HPPC
trimming and step-type classification.
"""

from __future__ import annotations

import io
from pathlib import Path

import polars as pl

from celljar.ingest.cyclers.batteryarchive import read_batteryarchive_csv


# Column order for the 10C file, which ships without a header row.
_EXPECTED_COLUMNS = [
    "Exclude", "Time(s)", "Cycle", "Loop", "Loop.1", "Loop.2",
    "Step", "StepTime(s)", "Current(A)", "Voltage(V)", "Power(W)",
    "Capacity(Ah)", "Energy(Wh)", "Mode", "Data", "Extra",
]

# ORNL header → celljar canonical column map. ORNL already ships a signed
# capacity/energy column (unlike the split Charge_/Discharge_ pair that the
# loader's defaults expect), so we route them through the loader's signed
# "_charge_*" intermediates with Discharge_* absent → the loader emits the
# signed value unchanged.
_ORNL_ALIASES: dict[str, str] = {
    "Time(s)": "timestamp_s",
    "Voltage(V)": "voltage_V",
    "Current(A)": "current_A",
    "Capacity(Ah)": "_charge_cap_Ah",
    "Energy(Wh)": "_charge_energy_Wh",
    "Step": "step_number",
    "Cycle": "cycle_number",
}


def _read_ornl_csv(csv_file: Path) -> pl.DataFrame:
    """Read an ORNL HPPC CSV, adding headers to the 10 C headerless variant."""
    df = pl.read_csv(csv_file)
    if "Time(s)" not in df.columns:
        df = pl.read_csv(csv_file, has_header=False, new_columns=_EXPECTED_COLUMNS)
    df = df.rename({c: c.strip() for c in df.columns})
    return df


def ingest(raw_dir: str) -> dict:
    """Load raw HPPC CSV files from the raw directory.

    Args:
        raw_dir: Path to data/raw/ornl_leaf/

    Returns:
        Dict keyed by temperature (int C) with per-test payload:
            - raw_df: DataFrame with ORNL raw column names, preserving the
              ``Mode`` and ``Data`` columns the harmonizer needs for HPPC
              trimming and step-type classification.
            - canonical_df: celljar canonical-schema DataFrame produced by
              ``read_batteryarchive_csv`` (proof the generic loader handles
              this source).
            - temperature_C, source_file: test-level metadata.
    """
    raw = Path(raw_dir)
    datasets = {}

    for csv_file in sorted(raw.glob("*.csv")):
        name = csv_file.stem
        # Only process HPPC files - skip discharge/cycling files
        if "hppc" not in name:
            continue
        # Extract temperature from filename: cell-low-current-hppc-25c.csv → 25
        temp_str = name.split("hppc-")[1].replace("c", "").replace("-", "")
        temp_c = int(temp_str)

        # Raw-named DataFrame - kept for the harmonizer, which indexes raw
        # columns (Mode, Data, Time(s), Current(A), ...) directly.
        raw_df = _read_ornl_csv(csv_file)

        # Feed the header-normalized frame to the generic loader via an
        # in-memory CSV buffer so the 10 C headerless file is handled too.
        buf = io.BytesIO()
        raw_df.write_csv(buf)
        buf.seek(0)
        canonical_df = read_batteryarchive_csv(buf, column_aliases=_ORNL_ALIASES)

        datasets[temp_c] = {
            "raw_df": raw_df,
            "canonical_df": canonical_df,
            "temperature_C": temp_c,
            "source_file": csv_file.name,
        }

    return datasets
