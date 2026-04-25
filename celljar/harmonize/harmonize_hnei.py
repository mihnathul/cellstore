"""Harmonize HNEI / Kollmeyer Panasonic 18650PF data to celljar canonical schema.

Input:  dict from celljar.ingest.hnei.ingest() - one record per .mat file,
        keyed by (test_type, profile, temp_C) or (test_type, profile, temp_C, idx).
Output: canonical cell_metadata / test_metadata / timeseries.

Supported test_types: hppc, drive_cycle, capacity_check, cycle_aging.

All tests share the same physical cell (Panasonic NCR18650PF), so the
cell_metadata is a single record. Each .mat becomes one test record.
"""

from __future__ import annotations

import numpy as np
import polars as pl


# Cell metadata - Panasonic NCR18650PF, from manufacturer datasheet.
# Widely cited as a BMS-research reference cell.
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


# Human-readable protocol descriptions per profile.
_PROTOCOL_DESCRIPTIONS = {
    "HPPC":     "5-pulse HPPC at 10% SOC steps (Kollmeyer / HNEI protocol)",
    "UDDS":     "Urban Dynamometer Driving Schedule (UDDS) drive cycle",
    "US06":     "US06 Supplemental Federal Test Procedure (high-acceleration) drive cycle",
    "LA92":     "LA92 (Unified / California) drive cycle",
    "NN":       "Neural Network (NN) randomized drive cycle - Kollmeyer training profile",
    "HWFET":    "Highway Fuel Economy Test (HWFET) drive cycle",
    "Cycle":    "Repeated charge/discharge under drive-cycle loading (Kollmeyer cycling)",
    "OCV_C20":  "C/20 constant-current OCV characterization (capacity check)",
    "Dis1C":    "1C constant-current discharge capacity check",
    "Dis5_10p": "5-pulse discharge at 10% SOC steps (mini-HPPC characterization)",
    "DisPulse": "Single discharge pulse characterization",
    "Charge":   "Constant-current / constant-voltage charge profile",
    "Pause":    "Rest / pause period between test segments",
}


def _derive_step_type(current_A: float, threshold_A: float = 0.01) -> str:
    """Classify a sample's step_type from its current value (scalar).

    HNEI .mat files don't carry an explicit State column, so we infer:
      - |I| < threshold → rest
      - I > 0           → charge  (Arbin convention: positive = charge)
      - I < 0           → discharge
    """
    if abs(current_A) < threshold_A:
        return "rest"
    return "charge" if current_A > 0 else "discharge"


def _step_type_expr(threshold_A: float = 0.01) -> pl.Expr:
    """Vectorized polars expression for step_type from `current_A`."""
    return (
        pl.when(pl.col("current_A").abs() < threshold_A).then(pl.lit("rest"))
        .when(pl.col("current_A") > 0).then(pl.lit("charge"))
        .otherwise(pl.lit("discharge"))
        .alias("step_type")
    )


def _normalise_key(key) -> tuple:
    """Accept the ingester's legacy int-temp key or the new tuple key.

    Returns (test_type, profile, temp_C, idx|None).
    """
    if isinstance(key, tuple):
        if len(key) == 3:
            test_type, profile, temp_c = key
            return test_type, profile, int(temp_c), None
        if len(key) == 4:
            test_type, profile, temp_c, idx = key
            try:
                idx_val = int(idx)
            except (ValueError, TypeError):
                idx_val = idx  # e.g. "Rp" for repeat
            return test_type, profile, int(temp_c), idx_val
    # Legacy: bare int temperature → treat as HPPC (backward compat for
    # any consumer still calling harmonize() with old-style dicts).
    if isinstance(key, (int, float, np.integer)):
        return "hppc", "HPPC", int(key), None
    raise ValueError(f"Unrecognised HNEI ingest key: {key!r}")


def _build_test_id(test_type: str, profile: str, temp_c: int, idx: int | None) -> str:
    """Build a deterministic test_id.

    HPPC (standard):       `HNEI_PANASONIC_18650PF_HPPC_-20C`
    HPPC (variant):        `HNEI_PANASONIC_18650PF_HPPC_DIS5_10P_25C`
    Drive cycles:          `HNEI_PANASONIC_18650PF_DRIVE_CYCLE_UDDS_25C`
    Cycling / aging:       `HNEI_PANASONIC_18650PF_CYCLE_AGING_25C_1`
    Capacity check:        `HNEI_PANASONIC_18650PF_CAP_CHECK_OCV_C20_25C`
    Checkup:               `HNEI_PANASONIC_18650PF_CHECKUP_CHARGE_25C`
    Repeated runs get an `_<idx>` suffix (e.g. HWFTa vs HWFTb → _1 / _2).
    """
    base = CELL_METADATA["cell_id"]
    temp_tag = f"{int(temp_c)}C"

    if test_type == "hppc":
        # Standard HPPC keeps backward-compat id; variant profiles get profile tag
        if profile == "HPPC":
            test_id = f"{base}_HPPC_{temp_tag}"
        else:
            test_id = f"{base}_HPPC_{profile.upper()}_{temp_tag}"
    elif test_type == "drive_cycle":
        test_id = f"{base}_DRIVE_CYCLE_{profile.upper()}_{temp_tag}"
    elif test_type == "cycle_aging":
        test_id = f"{base}_CYCLE_AGING_{temp_tag}"
    elif test_type == "capacity_check":
        test_id = f"{base}_CAP_CHECK_{profile.upper()}_{temp_tag}"
    else:
        test_id = f"{base}_{test_type.upper()}_{profile.upper()}_{temp_tag}"

    if idx is not None:
        test_id = f"{test_id}_{idx}"
    return test_id


def harmonize(ingested_data: dict, capacity_Ah: float = 2.9) -> dict:
    """Harmonize HNEI Kollmeyer .mat data (HPPC + drive cycles + capacity checks + cycling) to canonical timeseries.

    Args:
        ingested_data: Dict from hnei.ingest() - keyed by (test_type, profile, temp[, idx])
        capacity_Ah: Measured / nominal cell capacity for SOC calculation

    Returns:
        Dict with:
            - cell_metadata: dict
            - cells_metadata: [cell_metadata] (uniform multi-cell interface)
            - test_metadata: list of dicts, one per test
            - timeseries: dict of DataFrames keyed by test_id
    """
    timeseries_by_test = {}
    test_metadata = []

    for key, data in ingested_data.items():
        test_type, profile, temp_c, idx = _normalise_key(key)
        # Ingester's own view takes precedence if present.
        test_type = data.get("celljar_test_type", test_type)
        profile = data.get("profile", profile)
        temp_c = int(data.get("temperature_C", temp_c))

        test_id = _build_test_id(test_type, profile, temp_c, idx)
        raw = data["raw_df"]
        # Accept either pandas or polars raw_df.
        if not isinstance(raw, pl.DataFrame):
            raw = pl.from_pandas(raw)

        # Map Kollmeyer .mat columns → celljar canonical columns
        n = raw.height
        df = pl.DataFrame({
            "timestamp_s": raw["Time"].cast(pl.Float64),
            "current_A": raw["Current"].cast(pl.Float64),
            "voltage_V": raw["Voltage"].cast(pl.Float64),
            "temperature_C": raw["Battery_Temp_degC"].cast(pl.Float64),  # cell surface T
            "coulomb_count_Ah": raw["Ah"].cast(pl.Float64),  # signed running coulomb count
            "energy_Wh": raw["Wh"].cast(pl.Float64),     # already signed cumulative
            "displacement_um": np.full(n, np.nan),       # cycler-only, no gauge
        })
        df = df.with_columns([
            pl.lit(test_id).alias("test_id"),
            pl.lit(1, dtype=pl.Int64).alias("cycle_number"),  # single sequence
            pl.lit(None, dtype=pl.Int64).alias("step_number"),  # no explicit step
            # Derive step_type from current sign (no State column in .mat)
            _step_type_expr(),
        ])

        # Compute SOC internally (only for test_metadata.soc_range_*).
        # NOT added to the timeseries DataFrame - SOC is consumer-derived
        # from `coulomb_count_Ah` + cell's reference capacity + chosen initial SOC.
        soc = np.clip(1.0 + df["coulomb_count_Ah"].to_numpy() / capacity_Ah, 0, 1)

        timeseries_by_test[test_id] = df

        sample_dt = np.diff(df["timestamp_s"].to_numpy())

        # SOC-step + protocol description per test type
        if test_type == "hppc" and profile in ("HPPC", "Dis5_10p"):
            soc_step = 0.1
        else:
            soc_step = None
        protocol_description = _PROTOCOL_DESCRIPTIONS.get(
            profile, f"{profile} ({test_type})"
        )

        # C-rate from profile when known (capacity_check / OCV)
        # Dis1C  -> 1C discharge; OCV_C20 -> C/20 discharge (~0.05C)
        c_rate_discharge = (
            1.0 if profile == "Dis1C"
            else 0.05 if profile == "OCV_C20"
            else None
        )

        test_metadata.append({
            "test_id": test_id,
            "cell_id": CELL_METADATA["cell_id"],
            "test_type": test_type,
            "temperature_C_min": float(temp_c),
            "temperature_C_max": float(temp_c),
            "soc_range_min": float(soc.min()),
            "soc_range_max": float(soc.max()),
            "soc_step": soc_step,
            "c_rate_charge": None,
            "c_rate_discharge": c_rate_discharge,
            "protocol_description": protocol_description,
            "num_cycles": 1,
            "soh_pct": 100.0,                        # BOL assumption - fresh cell
            "soh_method": "bol_assumption",
            "cycle_count_at_test": 0,
            "test_year": 2017,
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
        })

    return {
        "cell_metadata": CELL_METADATA,
        "cells_metadata": [CELL_METADATA],       # uniform multi-cell interface
        "test_metadata": test_metadata,
        "timeseries": timeseries_by_test,
    }
