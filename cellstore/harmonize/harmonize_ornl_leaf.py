"""Harmonize ORNL Leaf HPPC data to cellstore canonical schema."""

import numpy as np
import pandas as pd


# Cell metadata for 2013 Nissan Leaf
CELL_METADATA = {
    "cell_id": "ORNL_LEAF_2013",
    "source": "ORNL",
    "source_cell_id": "2013_Nissan_Leaf",
    "manufacturer": "AESC",              # widely reported (Nissan/NEC JV)
    "model_number": None,                # AESC has not published a part number for this cell
    "chemistry": "mixed",                # AESC has not disclosed exact composition; known to be a blend
    "cathode": None,                     # specific composition not publicly disclosed
    "anode": None,                       # not verified
    "electrolyte": None,                 # not publicly disclosed
    "form_factor": "pouch",              # well-established for 2013 Leaf
    "nominal_capacity_Ah": 33.1,         # measured, per Zenodo record
    "nominal_voltage_V": None,           # not stated in source record
    "max_voltage_V": 4.2,                # observed in test protocol
    "min_voltage_V": 3.0,                # observed in test protocol
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


def harmonize(ingested_data: dict, capacity_Ah: float = 30.6) -> dict:
    """Harmonize raw ORNL HPPC data to canonical timeseries.

    Args:
        ingested_data: Dict from ornl_leaf.ingest() — {temp: {raw_df, ...}}
        capacity_Ah: Measured cell capacity for SOC calculation

    Returns:
        Dict with:
            - cell_metadata: dict
            - test_metadata: list of dicts, one per test
            - timeseries: dict of DataFrames keyed by temperature
    """
    timeseries_by_temp = {}
    test_metadata = []

    for temp_c, data in ingested_data.items():
        raw = data["raw_df"]

        # Find HPPC start — skip initial conditioning and charge
        # Find the rest period after the last charge, just before first HPPC pulse
        flags = raw["Data"].fillna("").str.strip().values if "Data" in raw.columns else None
        current = raw["Current(A)"].values.astype(float)
        mode = raw["Mode"].str.strip().values if "Mode" in raw.columns else None
        hppc_start = 0

        large_discharge = np.where(current < -20)[0]
        if len(large_discharge) > 0 and mode is not None:
            first_pulse = large_discharge[0]
            charge_before_pulse = np.where(mode[:first_pulse] == "CHRG")[0]
            if len(charge_before_pulse) > 0:
                hppc_start = charge_before_pulse[-1] + 1

        # Trim to HPPC section only
        raw_hppc = raw.iloc[hppc_start:].reset_index(drop=True)
        flags_hppc = flags[hppc_start:] if flags is not None else None

        # Test identity — one test per (cell, protocol, temperature)
        test_id = f"{CELL_METADATA['cell_id']}_HPPC_{int(temp_c)}C"

        # Map to canonical column names
        df = pd.DataFrame({
            "test_id": test_id,
            "cycle_number": 1,  # HPPC is a single characterization sequence
            "timestamp_s": raw_hppc["Time(s)"].values.astype(float),
            "current_A": raw_hppc["Current(A)"].values.astype(float),
            "voltage_V": raw_hppc["Voltage(V)"].values.astype(float),
            "temperature_C": float(temp_c),
        })

        # Fill from raw where available, else NaN — keeps schema column set stable
        df["capacity_Ah"] = (
            raw_hppc["Capacity(Ah)"].values.astype(float)
            if "Capacity(Ah)" in raw_hppc.columns else np.nan
        )
        df["energy_Wh"] = (
            raw_hppc["Energy(Wh)"].values.astype(float)
            if "Energy(Wh)" in raw_hppc.columns else np.nan
        )
        df["step_number"] = (
            raw_hppc["Step"].values.astype(int)
            if "Step" in raw_hppc.columns else pd.NA
        )

        # Classify step types from Mode column
        if "Mode" in raw_hppc.columns:
            mode = raw_hppc["Mode"].str.strip().values
            step_type = np.where(mode == "CHRG", "charge",
                       np.where(mode == "DCHG", "discharge",
                       np.where(mode == "REST", "rest", "unknown")))
            df["step_type"] = step_type

        # Carry flags through
        if flags_hppc is not None:
            df["flags"] = flags_hppc

        timeseries_by_temp[temp_c] = df

        # One test_metadata row per (cell, protocol, temperature)
        # — including observed data summary computed from the timeseries
        sample_dt = np.diff(df["timestamp_s"].values)
        test_metadata.append({
            "test_id": test_id,
            "cell_id": CELL_METADATA["cell_id"],
            "test_type": "hppc",
            "temperature_C_min": float(temp_c),
            "temperature_C_max": float(temp_c),
            "temperature_step_C": 0.0,
            # Protocol values — HPPC sweeps full 0–100% SOC in 10% steps
            "soc_range_min": 0.0,
            "soc_range_max": 1.0,
            "soc_step": 0.1,
            "c_rate_charge": np.nan,
            "c_rate_discharge": np.nan,
            "protocol_description": "Low-current HPPC with 10% SOC steps",
            "num_cycles": 1,
            "soh_pct": 100.0,                         # BOL assumption — fresh cell
            "soh_method": "bol_assumption",
            "cycle_count_at_test": 0,
            # Observed data summary
            "n_samples": int(len(df)),
            "duration_s": float(df["timestamp_s"].max() - df["timestamp_s"].min()),
            "voltage_observed_min_V": float(df["voltage_V"].min()),
            "voltage_observed_max_V": float(df["voltage_V"].max()),
            "current_observed_min_A": float(df["current_A"].min()),
            "current_observed_max_A": float(df["current_A"].max()),
            "temperature_observed_min_C": float(df["temperature_C"].min()),
            "temperature_observed_max_C": float(df["temperature_C"].max()),
            "sample_dt_median_s": float(np.median(sample_dt)) if len(sample_dt) else np.nan,
            "sample_dt_max_s": float(np.max(sample_dt)) if len(sample_dt) else np.nan,
            **_SOURCE_PROVENANCE,
        })

    return {
        "cell_metadata": CELL_METADATA,
        "cells_metadata": [CELL_METADATA],        # uniform multi-cell interface
        "test_metadata": test_metadata,
        "timeseries": timeseries_by_temp,
    }
