"""Harmonize Mohtat 2021 UMich pouch dataset to celljar canonical schema.

Input:  dict from celljar.ingest.mohtat.ingest() — one record per cell .mat
Output: canonical cell_metadata / test_metadata / timeseries

Unique among celljar sources: populates the optional `displacement_um`
column with synchronous Keyence laser expansion measurements. Cells with
missing expansion data ship NaN in that column; unrelated sources don't
need to emit the column (harmonize_schema marks it nullable).
"""

import numpy as np
import polars as pl


# Cell metadata — UMich Battery Lab (UMBL) custom pouch, NMC532 / graphite.
# Chemistry confirmed per Mohtat 2021 Deep Blue record (supersedes earlier
# Mohtat 2020 NMC111 work).
_CELL_TEMPLATE = {
    "source": "MOHTAT",
    "manufacturer": "University of Michigan Battery Lab (UMBL)",
    "model_number": None,                  # custom-built; no commercial SKU
    "chemistry": "NMC",
    "cathode": "NMC532",
    "anode": "graphite",
    "electrolyte": None,                   # not disclosed in paper
    "form_factor": "pouch",
    "nominal_capacity_Ah": 5.0,
    "nominal_voltage_V": 3.7,
    "max_voltage_V": 4.2,
    "min_voltage_V": 3.0,
}


# Per-source provenance (DOI, URL, citation, license) applied to each test.
# License: CC-BY-4.0 per Deep Blue Data record. Deep Blue HTML endpoint is
# Cloudflare-gated so programmatic re-verification is not possible, but the
# upstream record asserts CC-BY-4.0.
_SOURCE_PROVENANCE = {
    "source_doi": "10.7302/7tw1-kc35",
    "source_url": "https://deepblue.lib.umich.edu/data/concern/data_sets/5d86p0488",
    "source_citation": (
        "Mohtat, P., Lee, S., Siegel, J. B., & Stefanopoulou, A. G. (2021). "
        "UofM Pouch Cell Voltage and Expansion Cyclic Aging Dataset. "
        "University of Michigan - Deep Blue Data. "
        "https://doi.org/10.7302/7tw1-kc35"
    ),
    "source_license": "CC-BY-4.0",
    "source_license_url": "https://creativecommons.org/licenses/by/4.0/",
}


_PROTOCOL_DESCRIPTION = (
    "Cyclic aging across T in {-5, 25, 45 C} at various C-rates and DoDs, "
    "with synchronous Keyence laser displacement measurement of cell "
    "expansion on the same time base as V/I/T."
)


def _cell_id_from_tag(cell_tag: str) -> str:
    """Build canonical cell_id from the raw filename stem.

    Accepts common Mohtat filename patterns ("Cell01", "cell_1", "W8",
    etc.) and normalizes to MOHTAT_CELL{tag}.
    """
    return f"MOHTAT_CELL{cell_tag.upper().replace('CELL', '').lstrip('_')}"


def _cell_metadata(cell_tag: str) -> dict:
    """Build cell_metadata dict for one Mohtat cell."""
    cell_id = _cell_id_from_tag(cell_tag)
    return {
        "cell_id": cell_id,
        "source_cell_id": cell_tag,
        **_CELL_TEMPLATE,
    }


def _classify_step(current_A: np.ndarray, threshold_A: float = 0.01) -> np.ndarray:
    """Map per-sample current to step_type.

    Mohtat convention: positive current = charge, negative = discharge."""
    step = np.empty(current_A.shape, dtype=object)
    step[:] = "rest"
    step[current_A > threshold_A] = "charge"
    step[current_A < -threshold_A] = "discharge"
    # Samples with NaN current get "unknown"
    step[np.isnan(current_A)] = "unknown"
    return step


def harmonize(ingested_data: dict, capacity_Ah: float = 5.0) -> dict:
    """Harmonize Mohtat ingested dict to canonical celljar tables.

    Args:
        ingested_data: Dict from mohtat.ingest() — {cell_tag: {raw_df, ...}}
        capacity_Ah: Reference capacity (5.0 Ah for UMBL pouch). Not stored
                     on timeseries; cell.nominal_capacity_Ah is canonical.

    Returns:
        Dict with cell_metadata, cells_metadata, test_metadata, timeseries.
    """
    timeseries_by_test = {}
    test_metadata = []
    cells_metadata = []

    for cell_tag, payload in ingested_data.items():
        raw = payload["raw_df"]
        # Accept either pandas or polars raw_df (mohtat ingester delivers polars).
        if not isinstance(raw, pl.DataFrame):
            raw = pl.from_pandas(raw)
        cell_meta = _cell_metadata(cell_tag)
        cell_id = cell_meta["cell_id"]
        test_id = f"{cell_id}_CYCLING"
        cells_metadata.append(cell_meta)

        n = raw.height
        if n == 0:
            continue

        current_A = raw["current_A"].cast(pl.Float64).to_numpy()
        voltage_V = raw["voltage_V"].cast(pl.Float64).to_numpy()
        temperature_C = raw["temperature_C"].cast(pl.Float64).to_numpy()
        displacement_um = raw["displacement_um"].cast(pl.Float64).to_numpy()
        time_s = raw["time_s"].cast(pl.Float64).to_numpy()
        cycle_number = raw["cycle_number"].cast(pl.Int64).to_numpy()

        df = pl.DataFrame({
            "cycle_number": cycle_number,
            "step_type": _classify_step(current_A),
            "timestamp_s": time_s,
            "voltage_V": voltage_V,
            "current_A": current_A,
            "temperature_C": temperature_C,
            # coulomb count / energy not exposed per-sample; consumers can
            # integrate current_A. Nullable in schema.
            "coulomb_count_Ah": np.full(n, np.nan, dtype=float),
            "energy_Wh": np.full(n, np.nan, dtype=float),
            # The unique Mohtat column — Keyence laser swelling.
            "displacement_um": displacement_um,
        })
        df = df.with_columns([
            pl.lit(test_id).alias("test_id"),
            # Mohtat .mat files don't ship explicit step numbers — leave null.
            pl.lit(None, dtype=pl.Int64).alias("step_number"),
        ])

        timeseries_by_test[test_id] = df

        num_cycles = int(cycle_number.max()) if n else 0

        sample_dt = np.diff(time_s)
        # Robust min/max helpers that tolerate all-NaN columns.
        def _fmin(a: np.ndarray) -> float:
            return float(np.nanmin(a)) if np.any(~np.isnan(a)) else None

        def _fmax(a: np.ndarray) -> float:
            return float(np.nanmax(a)) if np.any(~np.isnan(a)) else None

        test_metadata.append({
            "test_id": test_id,
            "cell_id": cell_id,
            "test_type": "cycle_aging",
            "temperature_C_min": _fmin(temperature_C),
            "temperature_C_max": _fmax(temperature_C),
            "soc_range_min": None,
            "soc_range_max": None,
            "soc_step": None,
            "c_rate_charge": None,         # varies per cell in the aging matrix
            "c_rate_discharge": None,
            "protocol_description": _PROTOCOL_DESCRIPTION,
            "num_cycles": num_cycles,
            # Cycle-aging tests span a wide SOH range; leave scalar SOH null.
            "soh_pct": None,
            "soh_method": None,
            "cycle_count_at_test": 0,
            "test_year": 2018,
            "n_samples": int(n),
            "duration_s": float(time_s.max() - time_s.min()) if n else 0.0,
            "voltage_observed_min_V": _fmin(voltage_V),
            "voltage_observed_max_V": _fmax(voltage_V),
            "current_observed_min_A": _fmin(current_A),
            "current_observed_max_A": _fmax(current_A),
            "temperature_observed_min_C": _fmin(temperature_C),
            "temperature_observed_max_C": _fmax(temperature_C),
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
