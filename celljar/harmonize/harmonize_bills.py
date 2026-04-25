"""Harmonize Bills 2023 eVTOL dataset to celljar canonical schema.

Input:  dict from celljar.ingest.bills.ingest() - one record per VAH## cell
Output: canonical cell_metadata / test_metadata / timeseries

22 Sony-Murata US18650VTC6 cells cycled under an eVTOL mission profile
(takeoff + cruise + landing) with periodic RPTs. v0.2 treats each cell as
one `drive_cycle` test containing the full mission + RPT record, matching
MATR's "all cycles in one continuous timeseries" pattern.
"""

import numpy as np
import polars as pl


# Cell metadata - Sony-Murata US18650VTC6, per datasheet.
# Shared by all 22 cells in the dataset.
_CELL_TEMPLATE = {
    "source": "BILLS",
    "manufacturer": "Sony-Murata",
    "model_number": "US18650VTC6",
    "chemistry": "NMC",
    "cathode": "NMC",
    "anode": "graphite",
    "electrolyte": None,                   # proprietary
    "form_factor": "cylindrical",
    "nominal_capacity_Ah": 3.0,
    "nominal_voltage_V": 3.6,
    "max_voltage_V": 4.2,
    "min_voltage_V": 2.5,
}


# Per-source provenance (DOI, URL, citation, license) applied to each test_metadata record.
_SOURCE_PROVENANCE = {
    "source_doi": "10.1184/R1/14226830",
    "source_url": "https://kilthub.cmu.edu/articles/dataset/eVTOL_Battery_Dataset/14226830",
    "source_citation": (
        "Bills, A., Sripad, S., Fredericks, W. L., et al. (2023). "
        "A battery dataset for electric vertical takeoff and landing aircraft. "
        "Scientific Data 10, 344. https://doi.org/10.1038/s41597-023-02180-5"
    ),
    "source_license": "CC-BY-4.0",
    "source_license_url": "https://creativecommons.org/licenses/by/4.0/",
}


_PROTOCOL_DESCRIPTION = (
    "eVTOL mission profile (54 W takeoff 75 s + 16 W cruise 800 s + "
    "54 W landing 105 s) repeated until EOL. Interspersed RPTs every "
    "50 missions with C/5 capacity test + DCIR pulses."
)


def _cell_metadata(cell_tag: str) -> dict:
    """Build cell_metadata dict for one VAH## cell."""
    cell_id = f"BILLS_EVTOL_{cell_tag}"
    return {
        "cell_id": cell_id,
        "source_cell_id": cell_tag,
        **_CELL_TEMPLATE,
    }


def _classify_step(current_A: np.ndarray, threshold_A: float = 0.01) -> np.ndarray:
    """Map per-sample current to step_type: positive -> charge, negative ->
    discharge, |I| < threshold -> rest. Bills convention: positive I = charge."""
    step = np.empty(current_A.shape, dtype=object)
    step[:] = "rest"
    step[current_A > threshold_A] = "charge"
    step[current_A < -threshold_A] = "discharge"
    return step


def harmonize(ingested_data: dict, capacity_Ah: float = 3.0) -> dict:
    """Harmonize Bills eVTOL ingested dict to canonical celljar tables.

    Args:
        ingested_data: Dict from bills.ingest() - {cell_tag: {raw_df, ...}}
        capacity_Ah: Reference capacity (3.0 Ah for VTC6). Not stored;
                     cell.nominal_capacity_Ah is the canonical reference.

    Returns:
        Dict with cell_metadata, cells_metadata, test_metadata, timeseries.
    """
    timeseries_by_test = {}
    test_metadata = []
    cells_metadata = []

    for cell_tag, payload in ingested_data.items():
        raw = payload["raw_df"]
        # Accept either pandas or polars raw_df (bills ingester currently
        # delivers pandas via _canonical_to_bills_raw_df).
        if not isinstance(raw, pl.DataFrame):
            raw = pl.from_pandas(raw)
        cell_meta = _cell_metadata(cell_tag)
        cell_id = cell_meta["cell_id"]
        test_id = f"{cell_id}_DRIVE_CYCLE"
        cells_metadata.append(cell_meta)

        n = raw.height
        if n == 0:
            continue

        # Unit conversions: I_mA -> A, Q*_mA_h -> Ah, Energy*_W_h stays in Wh.
        current_A = raw["I_mA"].cast(pl.Float64).to_numpy() / 1000.0
        q_charge_Ah = raw["QCharge_mA_h"].cast(pl.Float64).to_numpy() / 1000.0
        q_discharge_Ah = raw["QDischarge_mA_h"].cast(pl.Float64).to_numpy() / 1000.0
        # Signed running coulomb count (charge accumulated minus discharge removed),
        # mirroring MATR's Qc - Qd convention.
        coulomb_count_Ah = q_charge_Ah - q_discharge_Ah

        energy_charge_Wh = raw["EnergyCharge_W_h"].cast(pl.Float64).to_numpy()
        energy_discharge_Wh = raw["EnergyDischarge_W_h"].cast(pl.Float64).to_numpy()
        energy_Wh_signed = energy_charge_Wh - energy_discharge_Wh

        df = pl.DataFrame({
            "cycle_number": raw["cycleNumber"].cast(pl.Int64).to_numpy(),
            "step_number": pl.Series(raw["Ns"].cast(pl.Int64).to_numpy(), dtype=pl.Int64),
            "step_type": _classify_step(current_A),
            "timestamp_s": raw["time_s"].cast(pl.Float64).to_numpy(),
            "voltage_V": raw["Ecell_V"].cast(pl.Float64).to_numpy(),
            "current_A": current_A,
            "temperature_C": raw["Temperature__C"].cast(pl.Float64).to_numpy(),
            "coulomb_count_Ah": coulomb_count_Ah,
            "energy_Wh": energy_Wh_signed,
            "displacement_um": np.full(n, np.nan),  # cycler-only, no gauge
        })
        df = df.with_columns(pl.lit(test_id).alias("test_id"))

        timeseries_by_test[test_id] = df

        num_cycles = int(df["cycle_number"].max()) if n else 0

        sample_dt = np.diff(df["timestamp_s"].to_numpy())
        test_metadata.append({
            "test_id": test_id,
            "cell_id": cell_id,
            "test_type": "drive_cycle",
            "temperature_C_min": float(np.nanmin(df["temperature_C"])),
            "temperature_C_max": float(np.nanmax(df["temperature_C"])),
            "soc_range_min": None,
            "soc_range_max": None,
            "soc_step": None,
            "c_rate_charge": 1.0,                    # 1C CC charge per default protocol
            "c_rate_discharge": None,              # power-based mission profile, not CC
            "protocol_description": _PROTOCOL_DESCRIPTION,
            "num_cycles": num_cycles,
            # Drive-cycle aging tests span a wide SOH range; a scalar soh_pct
            # is not meaningful. Consumers can derive per-cycle SOH from the
            # RPT C/5 capacity checks embedded in the timeseries.
            "soh_pct": None,
            "soh_method": None,
            "cycle_count_at_test": 0,                # test starts from BOL
            "test_year": 2020,
            "n_samples": int(len(df)),
            "duration_s": float(df["timestamp_s"].max() - df["timestamp_s"].min()),
            "voltage_observed_min_V": float(np.nanmin(df["voltage_V"])),
            "voltage_observed_max_V": float(np.nanmax(df["voltage_V"])),
            "current_observed_min_A": float(np.nanmin(df["current_A"])),
            "current_observed_max_A": float(np.nanmax(df["current_A"])),
            "temperature_observed_min_C": float(np.nanmin(df["temperature_C"])),
            "temperature_observed_max_C": float(np.nanmax(df["temperature_C"])),
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
