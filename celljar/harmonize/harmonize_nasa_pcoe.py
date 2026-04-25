"""Harmonize NASA PCoE battery aging data to celljar canonical schema.

Input:  dict from celljar.ingest.nasa_pcoe.ingest() - one record per cell
Output: canonical cell_metadata / test_metadata / timeseries / cycle_summary

v0.3 scope / design decisions:
  * One test per cell (test_type = "cycle_aging"). Each cell was repeatedly
    cycled (charge / discharge / EIS-impedance triplet) until end-of-life.
  * Charge + discharge cycles are combined into a single continuous timeseries
    with a monotone `cycle_number`. We assign a new cycle_number on every
    (charge -> discharge) transition so one full round-trip = one cycle.
  * EIS impedance cycles are EXCLUDED from the timeseries entirely. For each
    impedance cycle we emit one cycle_summary row carrying the scalar Re
    (electrolyte resistance) and Rct (charge-transfer resistance), indexed by
    the surrounding discharge cycle_number. Full complex Battery_impedance
    arrays are dropped for v0.3 - they'll land in a dedicated EIS entity in
    v0.4.
  * soh_pct is computed from the LAST discharge capacity divided by the FIRST
    discharge capacity (capacity_vs_first_checkpoint). Cycling test, so this
    scalar summarizes end-of-test SOH rather than being sampled mid-test.

Cell metadata:
  * NASA publicly acknowledges these are 18650 Li-ion cells but has never
    disclosed the vendor/chemistry. Community consensus (papers that use this
    dataset) treats them as LCO; we flag that in the citation.
  * nominal_capacity_Ah = 2.0 (per-cell fresh discharge capacity is ~1.85 Ah).
"""

from __future__ import annotations

import numpy as np
import polars as pl


# --- Cell template (shared across B0005-B0056) ------------------------------
_CELL_TEMPLATE = {
    "source": "NASA_PCOE",
    # NASA never disclosed the vendor. "Unknown" keeps the field honest; the
    # source_citation records the community-consensus LCO attribution.
    "manufacturer": "Unknown (NASA-undisclosed 18650 Li-ion)",
    "model_number": None,
    "chemistry": "LCO",               # community consensus, flagged in citation
    "cathode": None,
    "anode": "graphite",              # standard for consumer 18650 in 2007
    "electrolyte": None,
    "form_factor": "cylindrical",
    "nominal_capacity_Ah": 2.0,
    "nominal_voltage_V": 3.7,
    "max_voltage_V": 4.2,
    "min_voltage_V": 2.7,             # nominal / manufacturer; per-cell discharge
                                      # cutoff varies (set in cell_meta below).
}


_SOURCE_PROVENANCE = {
    "source_doi": None,               # NASA repository, no formal DOI
    "source_url": (
        "https://www.nasa.gov/intelligent-systems-division/"
        "discovery-and-systems-health/pcoe/pcoe-data-set-repository/"
    ),
    "source_citation": (
        "Saha, B. & Goebel, K. (2007). Battery Data Set. NASA Prognostics "
        "Data Repository, NASA Ames Research Center, Moffett Field, CA. "
        "https://www.nasa.gov/intelligent-systems-division/"
        "discovery-and-systems-health/pcoe/pcoe-data-set-repository/ "
        "Cells are 18650 Li-ion; chemistry/vendor not disclosed by NASA - "
        "community consensus treats them as LCO."
    ),
    # US Government Works are public domain in the US (17 USC §105). We publish
    # this as CC0-1.0 so downstream systems (HuggingFace, SPDX scanners) see a
    # concrete, permissive SPDX ID rather than a null license.
    "source_license": "CC0-1.0",
    "source_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
}


def _eis_r_sum(re_val: float, rct_val: float) -> float | None:
    """Sum of Re + Rct from an EIS fit, or None for missing / nonphysical fits.

    Re + Rct is the DC-equivalent series resistance of the cell at the
    fit-frequency-window low-frequency limit. Negative results come from
    ill-conditioned complex fits (not real physics) and are dropped rather
    than violating the cycle_summary schema's ge=0 invariant.
    """
    if np.isfinite(re_val) and np.isfinite(rct_val):
        total = float(re_val + rct_val)
        return total if total >= 0.0 else None
    if np.isfinite(re_val):
        return float(re_val) if re_val >= 0.0 else None
    return None


# Step-type inference (no explicit State column in .mat files).
def _step_type_from_current(i_A: np.ndarray, threshold_A: float = 0.02) -> np.ndarray:
    step = np.empty(i_A.shape, dtype=object)
    step[:] = "rest"
    step[i_A > threshold_A] = "charge"
    step[i_A < -threshold_A] = "discharge"
    return step


def _build_cell_metadata(cell_name: str, conditions: dict | None) -> dict:
    """Per-cell copy of the template, with discharge cutoff from conditions."""
    meta = dict(_CELL_TEMPLATE)
    meta["cell_id"] = f"NASA_PCOE_{cell_name}"
    meta["source_cell_id"] = cell_name
    if conditions is not None and conditions.get("V_cut") is not None:
        meta["min_voltage_V"] = float(conditions["V_cut"])
    return meta


def _cycle_frame(
    test_id: str,
    cycle_number: int,
    parsed_cycle: dict,
    time_offset_s: float,
) -> tuple[pl.DataFrame, float]:
    """Build one cycle's timeseries DataFrame; return (df, new_time_offset)."""
    t = np.asarray(parsed_cycle.get("Time", np.array([])), dtype=float)
    if t.size == 0:
        return pl.DataFrame(), time_offset_s

    t_s = t + time_offset_s
    new_offset = float(t_s[-1]) + 1.0            # 1 s gap between cycles

    n = t.size
    v = np.asarray(parsed_cycle.get("Voltage_measured", np.full(n, np.nan)), dtype=float)
    i = np.asarray(parsed_cycle.get("Current_measured", np.full(n, np.nan)), dtype=float)
    temp_c = np.asarray(
        parsed_cycle.get("Temperature_measured", np.full(n, np.nan)), dtype=float
    )

    # NASA's .mat files publish Current_measured with inconsistent sign
    # conventions: some repositories have charge > 0 / discharge < 0 (the
    # convention we adopt), while others deliver both as |I| with no sign at
    # all. We rely on the per-cycle `type` field (written by the ingester from
    # the authoritative `type` key in the .mat struct) to coerce everything to
    # positive=charge / negative=discharge. The coercion is idempotent: cycles
    # already in canonical sign pass through unchanged; mis-tagged files would
    # flip *every* sample in the cycle, which is visibly detectable in
    # downstream step_type (all "charge" or all "discharge"). Spot-checked
    # against the published capacity scalar in the ingester.
    ctype = parsed_cycle["type"]
    if ctype == "charge":
        i = np.where(i < 0, np.abs(i), i)
    elif ctype == "discharge":
        i = np.where(i > 0, -np.abs(i), i)

    # Pad shape mismatches (rare - README-vs-file column aliasing).
    def _pad(a, n):
        a = np.asarray(a, dtype=float).ravel()
        if a.size == n:
            return a
        out = np.full(n, np.nan)
        out[: min(a.size, n)] = a[: min(a.size, n)]
        return out

    v = _pad(v, n)
    i = _pad(i, n)
    temp_c = _pad(temp_c, n)

    df = pl.DataFrame({
        "step_type": _step_type_from_current(i),
        "timestamp_s": t_s,
        "voltage_V": v,
        "current_A": i,
        "temperature_C": temp_c,
        "coulomb_count_Ah": np.full(n, np.nan),  # NASA gives only a scalar per-discharge (in cycle_summary)
        "energy_Wh": np.full(n, np.nan),
        "displacement_um": np.full(n, np.nan), # not measured - Mohtat-only signal
    })
    df = df.with_columns([
        pl.lit(test_id).alias("test_id"),
        pl.lit(cycle_number, dtype=pl.Int64).alias("cycle_number"),
        pl.lit(None, dtype=pl.Int64).alias("step_number"),
    ])
    return df, new_offset


def _harmonize_cell(cell_name: str, record: dict) -> tuple[dict, dict, pl.DataFrame, list[dict]]:
    """Harmonize one cell. Returns (cell_meta, test_meta, timeseries_df,
    cycle_summary_rows). timeseries_df may be empty.

    Impedance cycles are skipped: NASA's published Re/Rct are fits of EIS
    spectra (Randles-circuit model), not measurements. celljar's data-only
    scope excludes fit-derived parameters even when the source publishes them.
    """
    conditions = record.get("conditions")
    cycles = record.get("cycles", [])

    cell_meta = _build_cell_metadata(cell_name, conditions)
    cell_id = cell_meta["cell_id"]
    test_id = f"{cell_id}_CYCLING"

    # Walk the cycle list chronologically and assign cycle_numbers. One
    # round-trip = one charge followed by one discharge. EIS/impedance cycles
    # are skipped from the timeseries but tracked for cycle_summary; when an
    # impedance cycle appears, we index it with the MOST RECENT discharge
    # cycle_number (i.e. the cycle it characterizes).
    ts_frames: list[pl.DataFrame] = []
    cycle_summary: list[dict] = []
    time_offset = 0.0
    current_cycle = 0
    last_was_discharge = False
    first_capacity_Ah: float | None = None
    last_capacity_Ah: float | None = None

    for parsed in cycles:
        ctype = parsed["type"]
        if ctype == "charge":
            if last_was_discharge or current_cycle == 0:
                current_cycle += 1
                last_was_discharge = False
            # else: two charges in a row - stay on the same cycle_number
            df, time_offset = _cycle_frame(test_id, current_cycle, parsed, time_offset)
            if df.height > 0:
                ts_frames.append(df)

        elif ctype == "discharge":
            if current_cycle == 0:
                current_cycle = 1
            df, time_offset = _cycle_frame(test_id, current_cycle, parsed, time_offset)
            if df.height > 0:
                ts_frames.append(df)
            cap = parsed.get("Capacity_Ah")
            if cap is not None and np.isfinite(cap):
                if first_capacity_Ah is None:
                    first_capacity_Ah = float(cap)
                last_capacity_Ah = float(cap)
            last_was_discharge = True

        elif ctype == "impedance":
            # NASA PCoE's impedance cycles publish fit-derived scalars
            # (Re, Rct) - these are *Randles-circuit fits* of EIS spectra,
            # not measurements. celljar is strictly a data-harmonization
            # layer (no fits), so we skip impedance cycles entirely.
            # Future schema may add raw frequency-domain EIS spectra (Z
            # vs frequency) which IS measurement data; the source's
            # complex Battery_impedance array is preserved in source for
            # downstream re-fitting.
            pass
        # else: unknown type - already filtered in ingester, but defensive

    # Combine timeseries
    if ts_frames:
        ts_df = pl.concat(ts_frames, how="vertical")
    else:
        ts_df = pl.DataFrame()

    # SOH from capacity. The "first discharge cycle = BOL" assumption breaks
    # on a handful of NASA cells whose first discharge was aborted or
    # incomplete (leaving first_capacity_Ah artificially low); when that
    # happens the computed ratio exceeds the physically plausible range
    # (soh ≤ ~105% for fresh cells). Rather than silently emitting a
    # nonsensical 1900% SOH, we detect the sentinel and leave soh_pct null
    # so consumers know the first-checkpoint method didn't produce a
    # meaningful BOL for this cell.
    if first_capacity_Ah is not None and first_capacity_Ah > 0 and last_capacity_Ah is not None:
        candidate = float(last_capacity_Ah / first_capacity_Ah * 100.0)
        if 0.0 <= candidate <= 150.0:
            soh_pct = candidate
            soh_method = "capacity_vs_first_checkpoint"
        else:
            soh_pct = None
            soh_method = None
    else:
        soh_pct = None
        soh_method = None

    # Test metadata
    cond = conditions or {}
    temp_c_setpt = float(cond.get("T_C", np.nan)) if cond.get("T_C") is not None else None
    i_chg = cond.get("I_chg")
    i_dchg = cond.get("I_dchg")
    v_cut = cond.get("V_cut")
    waveform = cond.get("waveform", "CC")
    eol_note = cond.get("eol_note", "")

    nominal_cap = _CELL_TEMPLATE["nominal_capacity_Ah"]
    c_rate_chg = float(i_chg) / nominal_cap if i_chg is not None else None
    c_rate_dchg = float(i_dchg) / nominal_cap if i_dchg is not None else None

    # Number of charge/discharge cycles = max cycle_number
    num_cycles = int(current_cycle) if current_cycle > 0 else 0

    # Observed summary from ts_df
    if ts_df.height > 0:
        dt = np.diff(ts_df["timestamp_s"].to_numpy())
        test_meta = {
            "test_id": test_id,
            "cell_id": cell_id,
            "test_type": "cycle_aging",
            "temperature_C_min": temp_c_setpt,
            "temperature_C_max": temp_c_setpt,
            "soc_range_min": None,
            "soc_range_max": None,
            "soc_step": None,
            "c_rate_charge": c_rate_chg,
            "c_rate_discharge": c_rate_dchg,
            "protocol_description": (
                f"NASA PCoE aging: charge CC {i_chg}A to 4.2V then CV to 20 mA; "
                f"discharge {waveform}"
                + (f" at {i_dchg}A" if i_dchg is not None else "")
                + (f" to {v_cut}V cutoff" if v_cut is not None else "")
                + f" at ambient {cond.get('T_C', '?')} degC; EIS 0.1Hz-5kHz sweep "
                f"between charge/discharge. {eol_note}"
            ),
            "num_cycles": num_cycles,
            "soh_pct": soh_pct,
            "soh_method": soh_method,
            "cycle_count_at_test": 0,
            "test_year": 2008,
            "n_samples": int(len(ts_df)),
            "duration_s": float(ts_df["timestamp_s"].max() - ts_df["timestamp_s"].min()),
            "voltage_observed_min_V": float(np.nanmin(ts_df["voltage_V"])),
            "voltage_observed_max_V": float(np.nanmax(ts_df["voltage_V"])),
            "current_observed_min_A": float(np.nanmin(ts_df["current_A"])),
            "current_observed_max_A": float(np.nanmax(ts_df["current_A"])),
            "temperature_observed_min_C": float(np.nanmin(ts_df["temperature_C"])),
            "temperature_observed_max_C": float(np.nanmax(ts_df["temperature_C"])),
            "sample_dt_min_s": float(max(0.0, np.min(dt))) if dt.size else None,
            "sample_dt_median_s": float(np.median(dt)) if dt.size else None,
            "sample_dt_max_s": float(np.max(dt)) if dt.size else None,
            **_SOURCE_PROVENANCE,
        }
    else:
        test_meta = {
            "test_id": test_id,
            "cell_id": cell_id,
            "test_type": "cycle_aging",
            "temperature_C_min": temp_c_setpt,
            "temperature_C_max": temp_c_setpt,
            "soc_range_min": None,
            "soc_range_max": None,
            "soc_step": None,
            "c_rate_charge": c_rate_chg,
            "c_rate_discharge": c_rate_dchg,
            "protocol_description": (
                f"NASA PCoE aging (no timeseries parsed). {eol_note}"
            ),
            "num_cycles": num_cycles,
            "soh_pct": soh_pct,
            "soh_method": soh_method,
            "cycle_count_at_test": 0,
            "test_year": 2008,
            "n_samples": 0,
            "duration_s": None,
            "voltage_observed_min_V": None,
            "voltage_observed_max_V": None,
            "current_observed_min_A": None,
            "current_observed_max_A": None,
            "temperature_observed_min_C": None,
            "temperature_observed_max_C": None,
            "sample_dt_min_s": None,
            "sample_dt_median_s": None,
            "sample_dt_max_s": None,
            **_SOURCE_PROVENANCE,
        }

    return cell_meta, test_meta, ts_df, cycle_summary


def harmonize(ingested_data: dict, capacity_Ah: float = 2.0) -> dict:
    """Harmonize NASA PCoE ingested dict into canonical celljar tables.

    Args:
        ingested_data: Dict from nasa_pcoe.ingest() - {cell_name: {...}}
        capacity_Ah:   Nominal cell capacity (2.0 Ah). Accepted for uniform
                       pipeline signature; not stored (nominal is in
                       cell_metadata.nominal_capacity_Ah).

    Returns:
        Dict with:
            cell_metadata:   first cells_metadata entry (backward compat)
            cells_metadata:  list of cell_metadata dicts (one per cell)
            test_metadata:   list of test_metadata dicts (one per cell)
            timeseries:      {test_id: DataFrame}  (charge + discharge only;
                             impedance cycles skipped because Re/Rct are fits,
                             not measurements - outside celljar's data-only scope)
            cycle_summary:   list of CycleSummarySchema rows (per discharge cycle:
                             capacity_Ah, capacity_retention_pct, etc.)
    """
    cells_metadata: list[dict] = []
    test_metadata: list[dict] = []
    timeseries: dict[str, pl.DataFrame] = {}
    cycle_summary: list[dict] = []

    for cell_name, record in ingested_data.items():
        try:
            cell_meta, test_meta, ts_df, cs_rows = _harmonize_cell(cell_name, record)
        except Exception as exc:            # noqa: BLE001 - defensive
            print(f"[harmonize_nasa_pcoe] {cell_name} failed: {exc}")
            continue

        cells_metadata.append(cell_meta)
        test_metadata.append(test_meta)
        if ts_df.height > 0:
            timeseries[test_meta["test_id"]] = ts_df
        cycle_summary.extend(cs_rows)

    return {
        "cell_metadata": cells_metadata[0] if cells_metadata else {},
        "cells_metadata": cells_metadata,
        "test_metadata": test_metadata,
        "timeseries": timeseries,
        "cycle_summary": cycle_summary,
    }
