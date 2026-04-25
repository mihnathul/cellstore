"""Harmonize SNL Preger 2020 dataset to celljar canonical schema.

Input:  dict from celljar.ingest.snl_preger.ingest() — one record per cell,
        keyed by the canonical BatteryArchive cell_id.
Output: canonical cell_metadata / test_metadata / timeseries.

Each SNL Preger cell is cycled under one (T, SOC window, C-rate) condition
until end-of-life, so we emit one `cycle_aging` test per cell. Three
chemistries are present and each corresponds to a distinct physical cell
model (A123 LFP, LG NMC, Panasonic NCA).
"""

from __future__ import annotations

import numpy as np
import polars as pl


# Per-chemistry cell metadata. All three are commercial 18650s used in the
# Preger 2020 study; values are taken from vendor datasheets. Anode is
# graphite in all three.
_CELL_TEMPLATES: dict[str, dict] = {
    "LFP": {
        "manufacturer": "A123",
        "model_number": "APR18650M1A",
        "chemistry": "LFP",
        "cathode": "LFP",
        "anode": "graphite",
        "electrolyte": None,
        "form_factor": "cylindrical",
        "nominal_capacity_Ah": 1.1,
        "nominal_voltage_V": 3.3,
        "max_voltage_V": 3.6,
        "min_voltage_V": 2.0,
    },
    "NMC": {
        "manufacturer": "LG",
        "model_number": "INR18650-MJ1",
        "chemistry": "NMC",
        "cathode": "NMC",
        "anode": "graphite",
        "electrolyte": None,
        "form_factor": "cylindrical",
        "nominal_capacity_Ah": 3.5,
        "nominal_voltage_V": 3.6,
        "max_voltage_V": 4.2,
        "min_voltage_V": 2.5,
    },
    "NCA": {
        "manufacturer": "Panasonic",
        "model_number": "NCR18650B",
        "chemistry": "NCA",
        "cathode": "NCA",
        "anode": "graphite",
        "electrolyte": None,
        "form_factor": "cylindrical",
        "nominal_capacity_Ah": 3.4,
        "nominal_voltage_V": 3.6,
        "max_voltage_V": 4.2,
        "min_voltage_V": 2.5,
    },
}


# Per-source provenance (DOI, URL, citation, license) applied to each test.
_SOURCE_PROVENANCE = {
    "source_doi": "10.1149/1945-7111/abae37",
    "source_url": "https://www.batteryarchive.org",
    "source_citation": (
        "Preger, Y. et al. (2020). Degradation of Commercial Lithium-Ion Cells "
        "as a Function of Chemistry and Cycling Conditions. "
        "Journal of The Electrochemical Society, 167, 120532. "
        "https://doi.org/10.1149/1945-7111/abae37"
    ),
    "source_license": "CC-BY-4.0",
    "source_license_url": "https://creativecommons.org/licenses/by/4.0/",
}


def _build_cell_id(parsed: dict) -> str:
    """Build a deterministic celljar cell_id from parsed filename tokens.

    Avoids "/" so the id is filesystem-safe:
        SNL_PREGER_LFP_25C_0-100_0.5-1C_a
    """
    chem = parsed["chemistry"]
    temp_c = int(parsed["temperature_C"])
    soc_lo = int(parsed["soc_min_pct"])
    soc_hi = int(parsed["soc_max_pct"])
    chg = parsed["c_rate_charge"]
    dchg = parsed["c_rate_discharge"]
    rep = parsed["replicate"]

    def _fmt(c: float) -> str:
        return str(int(c)) if float(c) == int(c) else ("%g" % float(c))

    return (
        f"SNL_PREGER_{chem}_{temp_c}C_{soc_lo}-{soc_hi}_"
        f"{_fmt(chg)}-{_fmt(dchg)}C_{rep}"
    )


def _cell_metadata(parsed: dict, cell_id: str) -> dict:
    """Assemble CellMetadata for one SNL Preger cell."""
    template = _CELL_TEMPLATES[parsed["chemistry"]]
    return {
        "cell_id": cell_id,
        "source": "SNL_PREGER",
        "source_cell_id": parsed["source_cell_id"],
        **template,
    }


def _protocol_description(parsed: dict) -> str:
    chem = parsed["chemistry"]
    temp_c = int(parsed["temperature_C"])
    soc_lo = int(parsed["soc_min_pct"])
    soc_hi = int(parsed["soc_max_pct"])
    chg = parsed["c_rate_charge"]
    dchg = parsed["c_rate_discharge"]
    return (
        f"SNL Preger 2020 cycle-aging protocol on commercial {chem} 18650 cell "
        f"at T={temp_c} degC, SOC window {soc_lo}-{soc_hi}%, charge "
        f"{chg:g}C CCCV / discharge {dchg:g}C CC. Periodic C/2 capacity check "
        f"and HPPC-style DCIR interspersed per the published schedule."
    )


def harmonize(ingested_data: dict, capacity_Ah: float | None = None) -> dict:
    """Harmonize SNL Preger 2020 ingest dict to canonical celljar tables.

    Args:
        ingested_data: Dict from snl_preger.ingest() — one record per cell.
        capacity_Ah: Ignored for SNL Preger — the dataset spans three
                     chemistries (LFP 1.1 Ah, NMC 3.5 Ah, NCA 3.4 Ah) and
                     per-cell SOC bounds must use each cell's chemistry-specific
                     nominal. Accepted for signature-compatibility with other
                     celljar harmonizers that take a single scalar.

    Returns:
        Dict with cell_metadata, cells_metadata, test_metadata, timeseries.
    """
    # SNL Preger spans 3 chemistries — always use per-cell nominal.
    _ = capacity_Ah
    timeseries_by_test: dict = {}
    cells_metadata: list[dict] = []
    test_metadata: list[dict] = []

    for _, payload in ingested_data.items():
        cell_id = _build_cell_id(payload)
        test_id = f"{cell_id}_CYCLING"
        cell_meta = _cell_metadata(payload, cell_id)
        cells_metadata.append(cell_meta)

        raw = payload["raw_df"]
        # Accept either pandas or polars raw_df (snl_preger ingester delivers
        # pandas via read_batteryarchive_csv).
        if not isinstance(raw, pl.DataFrame):
            raw = pl.from_pandas(raw)
        n = raw.height
        if n == 0:
            continue

        cols = raw.columns
        # read_batteryarchive_csv already produced canonical columns; stamp
        # the test_id and ensure column order / dtypes match the schema.
        df = pl.DataFrame({
            "timestamp_s": raw["timestamp_s"].cast(pl.Float64),
            "voltage_V": raw["voltage_V"].cast(pl.Float64),
            "current_A": raw["current_A"].cast(pl.Float64),
            "temperature_C": raw["temperature_C"].cast(pl.Float64),
            "coulomb_count_Ah": raw["coulomb_count_Ah"].cast(pl.Float64),
            "energy_Wh": raw["energy_Wh"].cast(pl.Float64),
            # Displacement (Mohtat-only signal) — SNL Preger has no laser
            # sensor on the cells, so emit all-NaN to satisfy the schema.
            "displacement_um": np.full(n, np.nan),
        })

        # Stamp test_id, then add nullable cycle/step columns + step_type.
        df = df.with_columns(pl.lit(test_id).alias("test_id"))

        if "cycle_number" in cols:
            df = df.with_columns(raw["cycle_number"].cast(pl.Int64).alias("cycle_number"))
        else:
            df = df.with_columns(pl.lit(None, dtype=pl.Int64).alias("cycle_number"))

        if "step_number" in cols:
            df = df.with_columns(raw["step_number"].cast(pl.Int64).alias("step_number"))
        else:
            df = df.with_columns(pl.lit(None, dtype=pl.Int64).alias("step_number"))

        if "step_type" in cols:
            df = df.with_columns(raw["step_type"].cast(pl.Utf8).alias("step_type"))
        else:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("step_type"))

        timeseries_by_test[test_id] = df

        # soc_range_min/max come from the filename-declared SOC window
        # (e.g. "20-80"), not the timeseries-derived SOC — SNL's cycling
        # protocol targets a fixed SOC band per cell, so the filename is the
        # canonical source of truth. capacity_Ah_ref (per-cell nominal) is
        # available via cell_meta["nominal_capacity_Ah"] for downstream tools.

        cyc_max = df["cycle_number"].drop_nulls().max()
        num_cycles = int(cyc_max) if cyc_max is not None else 0

        sample_dt = np.diff(df["timestamp_s"].to_numpy())

        test_metadata.append({
            "test_id": test_id,
            "cell_id": cell_id,
            "test_type": "cycle_aging",
            "temperature_C_min": float(payload["temperature_C"]),
            "temperature_C_max": float(payload["temperature_C"]),
            "soc_range_min": float(payload["soc_min_pct"]) / 100.0,
            "soc_range_max": float(payload["soc_max_pct"]) / 100.0,
            "soc_step": None,
            "c_rate_charge": float(payload["c_rate_charge"]),
            "c_rate_discharge": float(payload["c_rate_discharge"]),
            "protocol_description": _protocol_description(payload),
            "num_cycles": num_cycles,
            "soh_pct": 100.0,                    # BOL assumption — test starts fresh
            "soh_method": "bol_assumption",
            "cycle_count_at_test": 0,
            "test_year": 2018,
            "n_samples": int(len(df)),
            "duration_s": float(df["timestamp_s"].max() - df["timestamp_s"].min()),
            "voltage_observed_min_V": float(np.nanmin(df["voltage_V"])),
            "voltage_observed_max_V": float(np.nanmax(df["voltage_V"])),
            "current_observed_min_A": float(np.nanmin(df["current_A"])),
            "current_observed_max_A": float(np.nanmax(df["current_A"])),
            "temperature_observed_min_C": float(np.nanmin(df["temperature_C"].to_numpy()))
            if df["temperature_C"].is_not_null().any() else None,
            "temperature_observed_max_C": float(np.nanmax(df["temperature_C"].to_numpy()))
            if df["temperature_C"].is_not_null().any() else None,
            "sample_dt_min_s": float(max(0.0, np.min(sample_dt))) if len(sample_dt) else None,
            "sample_dt_median_s": float(np.median(sample_dt)) if len(sample_dt) else None,
            "sample_dt_max_s": float(np.max(sample_dt)) if len(sample_dt) else None,
            **_SOURCE_PROVENANCE,
        })

    return {
        "cell_metadata": cells_metadata[0] if cells_metadata else {},
        "cells_metadata": cells_metadata,
        "test_metadata": test_metadata,
        "timeseries": timeseries_by_test,
    }
