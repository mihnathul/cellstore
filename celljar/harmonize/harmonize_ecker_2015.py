"""Harmonize Ecker 2015 Kokam SLPB75106100 NMC data to celljar canonical schema.

Follows the ORNL pipeline pattern: parse_record / build_timeseries /
build_test_metadata / harmonize. This is a STUB — the per-record column
mapping is filled in when raw data lands.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from celljar.harmonize.harmonize_schema import HarmonizerOutput


# Cell metadata for Kokam SLPB75106100 NMC pouch.
# Per Ecker et al. 2015 J. Electrochem. Soc. 162 (9): A1836.
CELL_METADATA = {
    "cell_id": "ECKER_KOKAM_SLPB75106100",
    "source": "ECKER",
    "source_cell_id": "Kokam_SLPB75106100",
    "manufacturer": "Kokam",
    "model_number": "SLPB75106100",
    "chemistry": "NMC",
    "cathode": "LiNi_{1/3}Mn_{1/3}Co_{1/3}O_2",
    "anode": "graphite",
    "electrolyte": None,                 # not specified in paper
    "form_factor": "pouch",
    "nominal_capacity_Ah": 7.5,          # per Ecker 2015 paper
    "nominal_voltage_V": 3.7,
    "max_voltage_V": 4.2,
    "min_voltage_V": 2.8,
}


_SOURCE_PROVENANCE = {
    "source_doi": "10.1149/2.0551509jes",
    "source_url": "https://iopscience.iop.org/article/10.1149/2.0551509jes",
    "source_citation": (
        "Ecker, M., Tran, T. K. D., Dechent, P., Käbitz, S., Warnecke, A., "
        "& Sauer, D. U. (2015). Parameterization of a Physico-Chemical Model "
        "of a Lithium-Ion Battery I. Determination of Parameters. J. Electrochem. "
        "Soc. 162 (9): A1836. DOI: 10.1149/2.0551509jes"
    ),
    "source_license": "research-only",   # JES copyright; redistribute via citation
    "source_license_url": "https://iopscience.iop.org/info/page/copyright",
}


# --- Pipeline stages -------------------------------------------------------

def parse_record(key: tuple, raw_record: dict) -> tuple[pl.DataFrame, str, str]:
    """Extract (raw_df, test_type, test_id) from one ingested record."""
    test_type, temp_c = key
    raw = raw_record["raw_df"]
    if not isinstance(raw, pl.DataFrame):
        raw = pl.from_pandas(raw)
    test_id = f"{CELL_METADATA['cell_id']}_{test_type.upper()}_{int(temp_c)}C"
    return raw, test_type, test_id


def build_timeseries(raw: pl.DataFrame, test_id: str, temp_c: int) -> pl.DataFrame:
    """Map Ecker columns → celljar canonical. STUB until raw column names known."""
    # Expected source columns (placeholder — refine when files are reviewed):
    #   time_s, voltage_V, current_A, ...
    # For now, accept whatever columns exist and pass through what matches.
    n = raw.height
    df = pl.DataFrame({
        "timestamp_s": raw.get_column("time_s").cast(pl.Float64) if "time_s" in raw.columns
                       else pl.Series("timestamp_s", [float("nan")] * n),
        "voltage_V": raw.get_column("voltage_V").cast(pl.Float64) if "voltage_V" in raw.columns
                     else pl.Series("voltage_V", [float("nan")] * n),
        "current_A": raw.get_column("current_A").cast(pl.Float64) if "current_A" in raw.columns
                     else pl.Series("current_A", [float("nan")] * n),
    })
    df = df.with_columns([
        pl.lit(test_id).alias("test_id"),
        pl.lit(1, dtype=pl.Int64).alias("cycle_number"),
        pl.lit(None, dtype=pl.Int64).alias("step_number"),
        pl.lit(float(temp_c), dtype=pl.Float64).alias("temperature_C"),
        pl.lit(float("nan"), dtype=pl.Float64).alias("coulomb_count_Ah"),
        pl.lit(float("nan"), dtype=pl.Float64).alias("energy_Wh"),
        pl.lit(float("nan"), dtype=pl.Float64).alias("displacement_um"),
        pl.lit("unknown").alias("step_type"),
    ])
    return df


def build_test_metadata(test_id: str, df: pl.DataFrame, test_type: str, temp_c: int) -> dict:
    """Test-metadata row from canonical timeseries."""
    sample_dt = np.diff(df["timestamp_s"].to_numpy())
    n = len(df)
    return {
        "test_id": test_id,
        "cell_id": CELL_METADATA["cell_id"],
        "test_type": test_type,
        "temperature_C_min": float(temp_c),
        "temperature_C_max": float(temp_c),
        "soc_range_min": 0.0 if test_type == "hppc" else None,
        "soc_range_max": 1.0 if test_type == "hppc" else None,
        "soc_step": 0.1 if test_type == "hppc" else None,
        "c_rate_charge": None,
        "c_rate_discharge": None,
        "protocol_description": (
            f"{test_type.upper()} characterization at {int(temp_c)} °C — "
            "Ecker 2015 DFN parameterization protocol"
        ),
        "num_cycles": 1,
        "soh_pct": 100.0,
        "soh_method": "bol_assumption",
        "cycle_count_at_test": 0,
        "test_year": 2015,
        "n_samples": n,
        "duration_s": float(df["timestamp_s"].max() - df["timestamp_s"].min()) if n else 0.0,
        "voltage_observed_min_V": float(df["voltage_V"].min()) if n else None,
        "voltage_observed_max_V": float(df["voltage_V"].max()) if n else None,
        "current_observed_min_A": float(df["current_A"].min()) if n else None,
        "current_observed_max_A": float(df["current_A"].max()) if n else None,
        "temperature_observed_min_C": float(df["temperature_C"].min()) if n else None,
        "temperature_observed_max_C": float(df["temperature_C"].max()) if n else None,
        "sample_dt_min_s": float(max(0.0, np.min(sample_dt))) if len(sample_dt) else None,
        "sample_dt_median_s": float(np.median(sample_dt)) if len(sample_dt) else None,
        "sample_dt_max_s": float(np.max(sample_dt)) if len(sample_dt) else None,
        **_SOURCE_PROVENANCE,
    }


# --- Orchestrator ----------------------------------------------------------

def harmonize(ingested_data: dict, capacity_Ah: float = 7.5) -> HarmonizerOutput:
    """Harmonize Ecker 2015 ingested dict to canonical celljar tables.

    Args:
        ingested_data: {(test_type, temp_c): {raw_df, ...}} from ecker_2015.ingest()
        capacity_Ah: Accepted for uniform pipeline signature.

    Returns: HarmonizerOutput.
    """
    timeseries_by_test = {}
    test_metadata = []

    for key, raw_record in ingested_data.items():
        raw, test_type, test_id = parse_record(key, raw_record)
        temp_c = key[1]
        df = build_timeseries(raw, test_id, temp_c)
        timeseries_by_test[test_id] = df
        test_metadata.append(build_test_metadata(test_id, df, test_type, temp_c))

    return {
        "cell_metadata": CELL_METADATA,
        "cells_metadata": [CELL_METADATA],
        "test_metadata": test_metadata,
        "timeseries": timeseries_by_test,
    }
