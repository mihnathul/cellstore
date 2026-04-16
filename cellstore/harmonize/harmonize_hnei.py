"""Harmonize HNEI / Kollmeyer Panasonic 18650PF data to cellstore canonical schema.

Input:  dict from cellstore.ingest.hnei.ingest() — one record per .mat file
Output: canonical cell_metadata / test_metadata / timeseries
"""

import numpy as np
import pandas as pd


# Cell metadata — Panasonic NCR18650PF, from manufacturer datasheet.
# This cell is also widely cited as the BMS-research reference cell
# (Plett textbook / UCB BMS specialization curriculum).
CELL_METADATA = {
    "cell_id": "HNEI_PANASONIC_18650PF",
    "source": "HNEI",
    "source_cell_id": "Panasonic_NCR18650PF",
    "manufacturer": "Panasonic",
    "model_number": "NCR18650PF",
    "chemistry": "NCA",                   # per Panasonic datasheet
    "cathode": None,                      # proprietary NCA composition not disclosed
    "anode": "graphite",
    "electrolyte": None,                  # proprietary, not disclosed
    "form_factor": "cylindrical",
    "nominal_capacity_Ah": 2.9,
    "nominal_voltage_V": 3.6,
    "max_voltage_V": 4.2,
    "min_voltage_V": 2.5,
}


# Per-source provenance (DOI, URL, citation, license) applied to each test_metadata record.
_SOURCE_PROVENANCE = {
    "source_doi": "10.17632/wykht8y7tg.1",
    "source_url": "https://data.mendeley.com/datasets/wykht8y7tg/1",
    "source_citation": (
        "Kollmeyer, P. (2018). Panasonic 18650PF Li-ion Battery Data. "
        "Mendeley Data, v1. https://doi.org/10.17632/wykht8y7tg.1"
    ),
    "source_license": "CC-BY-4.0",
    "source_license_url": "https://creativecommons.org/licenses/by/4.0/",
}


def _derive_step_type(current_A: float, threshold_A: float = 0.01) -> str:
    """Classify a sample's step_type from its current value.

    HNEI .mat files don't carry an explicit State column, so we infer:
      - |I| < threshold → rest
      - I > 0           → charge  (Arbin convention: positive = charge)
      - I < 0           → discharge
    """
    if abs(current_A) < threshold_A:
        return "rest"
    return "charge" if current_A > 0 else "discharge"


def harmonize(ingested_data: dict, capacity_Ah: float = 2.9) -> dict:
    """Harmonize HNEI Kollmeyer HPPC .mat data to canonical timeseries.

    Args:
        ingested_data: Dict from hnei.ingest() — {temp_C: {raw_df, ...}}
        capacity_Ah: Measured / nominal cell capacity for SOC calculation

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
        test_id = f"{CELL_METADATA['cell_id']}_HPPC_{int(temp_c)}C"

        # Map Kollmeyer .mat columns → cellstore canonical columns
        n = len(raw)
        df = pd.DataFrame({
            "test_id": test_id,
            "cycle_number": 1,                       # HPPC is one characterization sequence
            "step_number": pd.array([pd.NA] * n, dtype="Int64"),  # no explicit step index in .mat
            "timestamp_s": raw["Time"].astype(float),
            "current_A": raw["Current"].astype(float),
            "voltage_V": raw["Voltage"].astype(float),
            "temperature_C": raw["Battery_Temp_degC"].astype(float),  # cell surface T
            "capacity_Ah": raw["Ah"].astype(float),  # already signed cumulative
            "energy_Wh": raw["Wh"].astype(float),    # already signed cumulative
        })

        # Derive step_type from current sign (no State column in .mat)
        df["step_type"] = df["current_A"].apply(_derive_step_type)

        # Compute SOC internally (only for test_metadata.soc_range_*).
        # NOT added to the timeseries DataFrame — SOC is consumer-derived
        # from `capacity_Ah` + cell's reference capacity + chosen initial SOC.
        soc = np.clip(1.0 + df["capacity_Ah"] / capacity_Ah, 0, 1)

        timeseries_by_temp[temp_c] = df

        sample_dt = np.diff(df["timestamp_s"].values)
        test_metadata.append({
            "test_id": test_id,
            "cell_id": CELL_METADATA["cell_id"],
            "test_type": "hppc",
            "temperature_C_min": float(temp_c),
            "temperature_C_max": float(temp_c),
            "temperature_step_C": 0.0,
            "soc_range_min": float(soc.min()),
            "soc_range_max": float(soc.max()),
            "soc_step": 0.1,                         # Kollmeyer's "5Pulse" HPPC ~ 10% SOC steps
            "c_rate_charge": np.nan,
            "c_rate_discharge": np.nan,
            "protocol_description": "5-pulse HPPC at 10% SOC steps (Kollmeyer / HNEI protocol)",
            "num_cycles": 1,
            "soh_pct": 100.0,                        # BOL assumption — fresh cell
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
        "cells_metadata": [CELL_METADATA],       # uniform multi-cell interface
        "test_metadata": test_metadata,
        "timeseries": timeseries_by_temp,
    }
