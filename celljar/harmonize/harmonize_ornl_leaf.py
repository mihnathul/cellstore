"""Harmonize ORNL Leaf HPPC data to celljar canonical schema.

Pipeline shape (the "small transforms" pattern other harmonizers should adopt):

    parse_record   → trim raw to HPPC region; extract test_type / temperature
    build_timeseries → canonical-column DataFrame from raw
    build_test_metadata → observed stats + protocol + provenance
    harmonize      → orchestrator: loops sources, validates, returns dict
"""

from __future__ import annotations

import numpy as np
import polars as pl

from celljar.harmonize.harmonize_schema import HarmonizerOutput


# Cell metadata for 2013 Nissan Leaf
CELL_METADATA = {
    "cell_id": "ORNL_LEAF_2013",
    "source": "ORNL",
    "source_cell_id": "2013_Nissan_Leaf",
    "manufacturer": "AESC",
    "model_number": None,
    "chemistry": "mixed",                # AESC has not disclosed exact composition
    "cathode": None,
    "anode": None,
    "electrolyte": None,
    "form_factor": "pouch",
    "nominal_capacity_Ah": 33.1,         # per Zenodo record
    "nominal_voltage_V": None,
    "max_voltage_V": None,
    "min_voltage_V": None,
}


# Per-source provenance (DOI, URL, citation, license) applied to each test_metadata record.
_SOURCE_PROVENANCE = {
    "source_doi": "10.5281/zenodo.2580327",
    "source_url": "https://zenodo.org/records/2580327",
    "source_citation": (
        "Wiggins, G., Allu, S., & Wang, H. (2019). Battery cell data from a "
        "2013 Nissan Leaf. Oak Ridge National Laboratory. "
        "https://doi.org/10.5281/zenodo.2580327"
    ),
    "source_license": "MIT",
    "source_license_url": "https://opensource.org/licenses/MIT",
}


# --- Pipeline stages -------------------------------------------------------

def parse_record(temp_c: float, raw_record: dict) -> tuple[pl.DataFrame, np.ndarray | None, str]:
    """Trim the raw frame to the HPPC region (skip initial conditioning + charge).

    Returns:
        (raw_hppc_df, flags_array, test_id)
    """
    raw = raw_record["raw_df"]
    if not isinstance(raw, pl.DataFrame):
        raw = pl.from_pandas(raw)

    cols = raw.columns
    flags = (
        raw["Data"].fill_null("").str.strip_chars().to_numpy() if "Data" in cols else None
    )
    current = raw["Current(A)"].cast(pl.Float64).to_numpy()
    mode = raw["Mode"].str.strip_chars().to_numpy() if "Mode" in cols else None

    hppc_start = 0
    large_discharge = np.where(current < -20)[0]
    if len(large_discharge) > 0 and mode is not None:
        first_pulse = large_discharge[0]
        charge_before = np.where(mode[:first_pulse] == "CHRG")[0]
        if len(charge_before) > 0:
            hppc_start = charge_before[-1] + 1

    raw_hppc = raw.slice(hppc_start)
    flags_hppc = flags[hppc_start:] if flags is not None else None
    test_id = f"{CELL_METADATA['cell_id']}_HPPC_{int(temp_c)}C"
    return raw_hppc, flags_hppc, test_id


def build_timeseries(
    raw_hppc: pl.DataFrame,
    flags: np.ndarray | None,
    test_id: str,
    temp_c: float,
) -> pl.DataFrame:
    """Map ORNL columns → celljar canonical columns. Always emits the same column set."""
    cols = raw_hppc.columns

    df = pl.DataFrame({
        "timestamp_s": raw_hppc["Time(s)"].cast(pl.Float64).to_numpy(),
        "current_A": raw_hppc["Current(A)"].cast(pl.Float64).to_numpy(),
        "voltage_V": raw_hppc["Voltage(V)"].cast(pl.Float64).to_numpy(),
    })
    df = df.with_columns([
        pl.lit(test_id).alias("test_id"),
        pl.lit(1, dtype=pl.Int64).alias("cycle_number"),
        pl.lit(float(temp_c), dtype=pl.Float64).alias("temperature_C"),
    ])

    # Optional source columns — fill with NaN if absent so the canonical
    # column set is stable across sources.
    # ORNL's "Capacity(Ah)" column is the cycler's running coulomb count.
    if "Capacity(Ah)" in cols:
        df = df.with_columns(pl.Series("coulomb_count_Ah", raw_hppc["Capacity(Ah)"].cast(pl.Float64).to_numpy()))
    else:
        df = df.with_columns(pl.lit(float("nan"), dtype=pl.Float64).alias("coulomb_count_Ah"))

    if "Energy(Wh)" in cols:
        df = df.with_columns(pl.Series("energy_Wh", raw_hppc["Energy(Wh)"].cast(pl.Float64).to_numpy()))
    else:
        df = df.with_columns(pl.lit(float("nan"), dtype=pl.Float64).alias("energy_Wh"))

    if "Step" in cols:
        df = df.with_columns(
            pl.Series("step_number", raw_hppc["Step"].cast(pl.Int64).to_numpy(), dtype=pl.Int64)
        )
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Int64).alias("step_number"))

    if "Mode" in cols:
        mode_arr = raw_hppc["Mode"].str.strip_chars().to_numpy()
        step_type = np.where(
            mode_arr == "CHRG", "charge",
            np.where(mode_arr == "DCHG", "discharge",
            np.where(mode_arr == "REST", "rest", "unknown"))
        )
        df = df.with_columns(pl.Series("step_type", step_type))

    # ORNL cycler has no laser/gauge — emit NaN.
    df = df.with_columns(pl.lit(float("nan"), dtype=pl.Float64).alias("displacement_um"))

    if flags is not None:
        df = df.with_columns(pl.Series("flags", flags))

    return df


def build_test_metadata(test_id: str, df: pl.DataFrame, temp_c: float) -> dict:
    """Test-metadata row from the canonical timeseries — observed stats + protocol."""
    sample_dt = np.diff(df["timestamp_s"].to_numpy())
    return {
        "test_id": test_id,
        "cell_id": CELL_METADATA["cell_id"],
        "test_type": "hppc",
        "temperature_C_min": float(temp_c),
        "temperature_C_max": float(temp_c),
        "soc_range_min": 0.0,
        "soc_range_max": 1.0,
        "soc_step": 0.1,
        "c_rate_charge": None,
        "c_rate_discharge": None,
        "protocol_description": "Low-current HPPC with 10% SOC steps",
        "num_cycles": 1,
        "soh_pct": 100.0,                         # BOL assumption — fresh cell
        "soh_method": "bol_assumption",
        "cycle_count_at_test": 0,
        "test_year": 2013,
        # Observed data summary
        "n_samples": int(len(df)),
        "duration_s": float(df["timestamp_s"].max() - df["timestamp_s"].min()),
        "voltage_observed_min_V": float(df["voltage_V"].min()),
        "voltage_observed_max_V": float(df["voltage_V"].max()),
        "current_observed_min_A": float(df["current_A"].min()),
        "current_observed_max_A": float(df["current_A"].max()),
        "temperature_observed_min_C": float(df["temperature_C"].min()),
        "temperature_observed_max_C": float(df["temperature_C"].max()),
        "sample_dt_min_s": float(max(0.0, np.min(sample_dt))) if len(sample_dt) else None,
        "sample_dt_median_s": float(np.median(sample_dt)) if len(sample_dt) else None,
        "sample_dt_max_s": float(np.max(sample_dt)) if len(sample_dt) else None,
        **_SOURCE_PROVENANCE,
    }


# --- Orchestrator ----------------------------------------------------------

def harmonize(ingested_data: dict, capacity_Ah: float = 33.1) -> HarmonizerOutput:
    """Harmonize raw ORNL HPPC data to canonical timeseries.

    Args:
        ingested_data: {temp_c: {raw_df, ...}} from ornl_leaf.ingest()
        capacity_Ah: Accepted for uniform pipeline signature; not used in the body
                     (CELL_METADATA["nominal_capacity_Ah"] is authoritative).

    Returns: HarmonizerOutput with cell_metadata, cells_metadata, test_metadata, timeseries.
    """
    timeseries_by_test = {}
    test_metadata = []

    for temp_c, raw_record in ingested_data.items():
        raw_hppc, flags, test_id = parse_record(temp_c, raw_record)
        df = build_timeseries(raw_hppc, flags, test_id, temp_c)
        timeseries_by_test[test_id] = df
        test_metadata.append(build_test_metadata(test_id, df, temp_c))

    return {
        "cell_metadata": CELL_METADATA,
        "cells_metadata": [CELL_METADATA],
        "test_metadata": test_metadata,
        "timeseries": timeseries_by_test,
    }
