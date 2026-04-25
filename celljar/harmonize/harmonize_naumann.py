"""Harmonize Naumann 2021 LFP calendar- and cycle-aging aggregates to
celljar canonical tables.

This is the SECOND-GENERATION implementation. Naumann's public deposits ship
only pre-aggregated summary arrays (capacity vs storage time, R_DC vs FEC,
etc.), not raw V/I/T timeseries - so this harmonizer emits `cycle_summary`
rows and leaves `timeseries` empty.

Input:  dict from celljar.ingest.naumann.ingest() - one record per
        (temperature, SOC, DoD, C-rate) test point.

Output: {
    "cell_metadata":  one representative dict (backward compat),
    "cells_metadata": list of cell_metadata dicts (one per test point),
    "test_metadata":  list of test_metadata dicts (one per test point),
    "timeseries":     {} - Naumann has no raw timeseries,
    "cycle_summary":  list of dicts matching CycleSummarySchema,
}

Cell / test ID conventions (one cell_id per test-point aggregate, since
Naumann published only means-across-replicates):
  Calendar: NAUMANN_CAL_T{temp}_SOC{soc}
  Cycle:    NAUMANN_CYC_T{temp}_SOC{soc}_D{dod}_C{c_chg}_C{c_dchg}
  Load:     NAUMANN_LOAD_{name}_T{temp}_SOC{soc}
  test_id  = cell_id + "_TEST"
"""

from __future__ import annotations

import numpy as np

from celljar.ingest.naumann import _num


# All Naumann cells are the Sony / Murata US26650FTC1 ("FTC1A").
_CELL_TEMPLATE = {
    "source": "NAUMANN",
    "manufacturer": "Sony-Murata",
    "model_number": "US26650FTC1",
    "chemistry": "LFP",
    "cathode": "LFP",
    "anode": "graphite",
    "electrolyte": None,                  # proprietary, undisclosed
    "form_factor": "cylindrical",
    "nominal_capacity_Ah": 3.0,
    "nominal_voltage_V": 3.2,
    "max_voltage_V": 3.6,
    "min_voltage_V": 2.0,
}

_SOURCE_PROVENANCE = {
    "source_doi": "10.17632/kxh42bfgtj.1",
    "source_url": "https://data.mendeley.com/datasets/kxh42bfgtj/1",
    "source_citation": (
        "Naumann, M. (2021). Data for: Analysis and modeling of calendar/cycle aging "
        "of a commercial LiFePO4/graphite cell. Mendeley Data. "
        "DOIs: 10.17632/kxh42bfgtj.1 (calendar) and 10.17632/6hgyr25h8d.1 (cycle). "
        "Companion papers: Naumann et al. JPS 2018 doi:10.1016/j.est.2018.01.019, "
        "Naumann et al. JPS 2020 doi:10.1016/j.jpowsour.2019.227666"
    ),
    "source_license": "CC-BY-4.0",
    "source_license_url": "https://creativecommons.org/licenses/by/4.0/",
}

# Default BOL DC resistance for cycle deposit rescale. The cycle .mat Y arrays
# are published as dimensionless ratios (Y[0] = 1.0). The calendar deposit
# (companion dataset, same cell model) publishes absolute mOhm and the t=0
# rows sit at ~33.3 mOhm across every (T, SOC) test point; we use that
# empirical BOL as the rescale reference. Source: the 33.3 mOhm floor is
# directly observable in the calendar xlsx rows of Mendeley kxh42bfgtj (call
# it "observed from upstream calendar deposit"), consistent with the
# manufacturer-datasheet DC-IR spec for US26650FTC1 (≈30 mOhm, 1 kHz AC).
# Changing this value would uniformly shift cycle-side resistance_dc_ohm by a
# constant factor - retention plots vs FEC are unaffected.
_CYCLE_R_BOL_OHM = 33.3e-3

# Pulse width for Naumann's R_DC metric, explicitly named "R_DC,10s" in the
# companion paper (Naumann et al. JPS 2018 §Protocol; Naumann et al. JPS 2020
# uses the same 10 s pulse).
_R_DC_PULSE_DURATION_S = 10.0

# SOC at which the R_DC pulse is applied in the cycle deposit. Not encoded
# per-file in the .mat payload; the cycle protocol (Naumann JPS 2020 §2.2)
# centres all cycling around 50% SOC with symmetric DoD windows, so the
# checkup pulse is at 50% SOC by construction.
_CYCLE_R_DC_SOC_PCT = 50.0

# 35 calendar checkpoints span ~2.4 years; cycle deposits span ~10k FEC.


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _record_cell_id(record_key: str) -> str:
    """Prepend NAUMANN_ to an ingest record_key to form a cell_id."""
    return f"NAUMANN_{record_key}"


def _retention_pct(values: np.ndarray) -> np.ndarray:
    """Capacity retention as a percentage of BOL (first finite value)."""
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return arr
    finite = arr[np.isfinite(arr)]
    if finite.size == 0:
        return np.full_like(arr, np.nan)
    bol = finite[0]
    if bol <= 0:
        return np.full_like(arr, np.nan)
    return arr / bol * 100.0


def _first_finite(arr: np.ndarray) -> float:
    """Return the first finite value in arr, or NaN if none."""
    a = np.asarray(arr, dtype=float)
    finite = a[np.isfinite(a)]
    return float(finite[0]) if finite.size else float("nan")


def _safe_max(arr) -> float:
    a = np.asarray(arr, dtype=float)
    a = a[np.isfinite(a)]
    return float(a.max()) if a.size else float("nan")


def _pad_to(length: int, arr: np.ndarray | None) -> np.ndarray:
    """Return arr padded / truncated to `length` with NaN, or all-NaN if absent."""
    if arr is None:
        return np.full(length, np.nan)
    a = np.asarray(arr, dtype=float).ravel()
    if a.size == length:
        return a
    out = np.full(length, np.nan)
    n = min(a.size, length)
    out[:n] = a[:n]
    return out


# ---------------------------------------------------------------------------
# Per-record harmonization
# ---------------------------------------------------------------------------

def _build_calendar(
    record_key: str,
    record: dict,
    capacity_Ah: float,
) -> tuple[dict, dict, list[dict]]:
    """Return (cell_meta, test_meta, cycle_summary_rows) for a calendar record."""
    cell_id = _record_cell_id(record_key)
    test_id = f"{cell_id}_TEST"

    temp_c = float(record.get("temperature_C", np.nan))
    soc_pct = float(record.get("soc_pct", np.nan))

    storage_time_h = np.asarray(record.get("storage_time_h", []), dtype=float)
    n = storage_time_h.size
    elapsed_time_s = storage_time_h * 3600.0 if n else storage_time_h

    capacity_Ah_arr = _pad_to(n, record.get("capacity_Ah"))
    resistance_mOhm = _pad_to(n, record.get("resistance_mOhm"))
    resistance_ohm = resistance_mOhm * 1e-3

    retention_pct = _retention_pct(capacity_Ah_arr)

    rows: list[dict] = []
    for i in range(n):
        rows.append({
            "test_id": test_id,
            "cell_id": cell_id,
            "cycle_number": None,
            "equivalent_full_cycles": None,
            "elapsed_time_s": float(elapsed_time_s[i])
            if np.isfinite(elapsed_time_s[i]) else None,
            "capacity_Ah": float(capacity_Ah_arr[i])
            if np.isfinite(capacity_Ah_arr[i]) else None,
            "capacity_retention_pct": float(retention_pct[i])
            if np.isfinite(retention_pct[i]) else None,
            "resistance_dc_ohm": float(resistance_ohm[i])
            if np.isfinite(resistance_ohm[i]) else None,
            "resistance_dc_pulse_duration_s": _R_DC_PULSE_DURATION_S,
            # Calendar: pulse applied at the storage SOC (test-point SOC).
            "resistance_dc_soc_pct": float(soc_pct) if np.isfinite(soc_pct) else None,
            "energy_Wh": None,
            "coulombic_efficiency": None,
            "temperature_C_mean": float(temp_c) if np.isfinite(temp_c) else None,
        })

    cell_meta = dict(_CELL_TEMPLATE)
    cell_meta["cell_id"] = cell_id
    cell_meta["source_cell_id"] = record.get("tp_label") or record_key

    soc_frac = max(0.0, min(1.0, soc_pct / 100.0)) if np.isfinite(soc_pct) else float("nan")
    test_meta = {
        "test_id": test_id,
        "cell_id": cell_id,
        "test_type": "calendar_aging",
        "temperature_C_min": float(temp_c) if np.isfinite(temp_c) else None,
        "temperature_C_max": float(temp_c) if np.isfinite(temp_c) else None,
        "soc_range_min": soc_frac,
        "soc_range_max": soc_frac,
        "soc_step": None,
        "c_rate_charge": None,
        "c_rate_discharge": None,
        "protocol_description": (
            f"Calendar aging at T={_num(temp_c)} degC, SOC={_num(soc_pct)}%. "
            f"Periodic checkups (capacity + R_DC,10s pulse) at the storage SOC. "
            f"Published as aggregated mean across 3 replicates - one logical "
            f"cell_id per (T, SOC) test point."
        ),
        "num_cycles": int(n),
        "soh_pct": None,
        "soh_method": None,
        "cycle_count_at_test": 0,
        "test_year": 2018,
        # No raw V/I/T - Naumann publishes only checkpoint aggregates.
        # Record 0 samples (as opposed to NaN) so the test_metadata pandera
        # Series[int] coercion succeeds; consumers see "no raw timeseries"
        # and can fall back to cycle_summary.
        "n_samples": 0,
        "duration_s": float(elapsed_time_s[-1]) if n and np.isfinite(elapsed_time_s[-1]) else None,
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
    return cell_meta, test_meta, rows


def _build_cycle(
    record_key: str,
    record: dict,
    capacity_Ah: float,
) -> tuple[dict, dict, list[dict]]:
    """Return (cell_meta, test_meta, cycle_summary_rows) for a cycle record."""
    cell_id = _record_cell_id(record_key)
    test_id = f"{cell_id}_TEST"

    temp_c = float(record.get("temperature_C", np.nan))
    soc_pct = float(record.get("soc_pct", np.nan))
    dod_pct = float(record.get("dod_pct", np.nan))
    c_rate_chg = float(record.get("c_rate_charge", np.nan))
    c_rate_dchg = float(record.get("c_rate_discharge", np.nan))

    # Prefer FEC for the row length; fall back to time.
    fec = record.get("fec")
    t_s = record.get("elapsed_time_s")
    ref = fec if fec is not None and np.size(fec) else t_s
    if ref is None or np.size(ref) == 0:
        # No aging axis at all - nothing to emit.
        return None, None, []

    n = int(np.size(ref))
    fec_arr = _pad_to(n, fec)
    t_arr = _pad_to(n, t_s)

    cap_ratio = _pad_to(n, record.get("capacity_ratio"))
    res_ratio = _pad_to(n, record.get("resistance_ratio"))

    capacity_Ah_arr = cap_ratio * capacity_Ah
    retention_pct = cap_ratio * 100.0
    resistance_ohm = res_ratio * _CYCLE_R_BOL_OHM

    rows: list[dict] = []
    for i in range(n):
        # Cross-field invariant: at least one aging axis must be non-null.
        # Naumann's source arrays sometimes pad with NaN at one end -
        # skip those rows so cycle_summary stays well-formed.
        if not np.isfinite(fec_arr[i]) and not np.isfinite(t_arr[i]):
            continue
        rows.append({
            "test_id": test_id,
            "cell_id": cell_id,
            "cycle_number": None,
            "equivalent_full_cycles": float(fec_arr[i])
            if np.isfinite(fec_arr[i]) else None,
            "elapsed_time_s": float(t_arr[i])
            if np.isfinite(t_arr[i]) else None,
            "capacity_Ah": float(capacity_Ah_arr[i])
            if np.isfinite(capacity_Ah_arr[i]) else None,
            "capacity_retention_pct": float(retention_pct[i])
            if np.isfinite(retention_pct[i]) else None,
            "resistance_dc_ohm": float(resistance_ohm[i])
            if np.isfinite(resistance_ohm[i]) else None,
            "resistance_dc_pulse_duration_s": _R_DC_PULSE_DURATION_S,
            # Cycle deposit: checkup pulse at 50% SOC by convention.
            "resistance_dc_soc_pct": _CYCLE_R_DC_SOC_PCT,
            "energy_Wh": None,
            "coulombic_efficiency": None,
            "temperature_C_mean": float(temp_c) if np.isfinite(temp_c) else None,
        })

    cell_meta = dict(_CELL_TEMPLATE)
    cell_meta["cell_id"] = cell_id
    cell_meta["source_cell_id"] = record.get("tp_label") or record_key

    # Represent the cycling window as a DoD range centred on `soc_pct`.
    if np.isfinite(soc_pct) and np.isfinite(dod_pct):
        centre = soc_pct / 100.0
        half = (dod_pct / 100.0) / 2.0
        soc_min = max(0.0, min(1.0, centre - half))
        soc_max = max(0.0, min(1.0, centre + half))
    else:
        soc_min = soc_max = np.nan

    profile_kind = record.get("profile_kind", "cyclization")
    if profile_kind == "loadspectrum":
        proto = (
            f"Cycle aging at T={_num(temp_c)} degC under load spectrum "
            f"{record.get('loadspectrum_name')!r} around SOC={_num(soc_pct)}%. "
            f"Checkups (capacity + R_DC,10s) embedded; values aggregated across "
            f"replicates."
        )
    else:
        proto = (
            f"Cycle aging at T={_num(temp_c)} degC, SOC centre={_num(soc_pct)}%, "
            f"DoD={_num(dod_pct)}%, charge {_num(c_rate_chg)}C / discharge "
            f"{_num(c_rate_dchg)}C, {record.get('protocol_suffix', 'CC')} profile. "
            f"Checkups (capacity + R_DC,10s at 50% SOC) embedded; values "
            f"aggregated across 3 replicates."
        )

    duration_s = float(_safe_max(t_arr)) if np.any(np.isfinite(t_arr)) else None
    num_cycles = int(_safe_max(fec_arr)) if np.any(np.isfinite(fec_arr)) else n

    test_meta = {
        "test_id": test_id,
        "cell_id": cell_id,
        "test_type": "cycle_aging",
        "temperature_C_min": float(temp_c) if np.isfinite(temp_c) else None,
        "temperature_C_max": float(temp_c) if np.isfinite(temp_c) else None,
        "soc_range_min": soc_min,
        "soc_range_max": soc_max,
        "soc_step": None,
        "c_rate_charge": c_rate_chg if np.isfinite(c_rate_chg) else None,
        "c_rate_discharge": c_rate_dchg if np.isfinite(c_rate_dchg) else None,
        "protocol_description": proto,
        "num_cycles": num_cycles,
        "soh_pct": None,
        "soh_method": None,
        "cycle_count_at_test": 0,
        "test_year": 2018,
        # No raw V/I/T - Naumann publishes only checkpoint aggregates.
        # Record 0 samples (as opposed to NaN) so the test_metadata pandera
        # Series[int] coercion succeeds; consumers see "no raw timeseries"
        # and can fall back to cycle_summary.
        "n_samples": 0,
        "duration_s": duration_s,
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
    return cell_meta, test_meta, rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def harmonize(ingested_data: dict, capacity_Ah: float = 3.0) -> dict:
    """Harmonize a Naumann ingest dict into canonical celljar tables.

    Args:
        ingested_data: dict from celljar.ingest.naumann.ingest() - records
                       keyed by CAL_* / CYC_* / LOAD_* strings.
        capacity_Ah: Nominal capacity used to rescale cycle-side normalized
                     capacity ratios to absolute Ah. Defaults to 3.0 (the
                     US26650FTC1 nominal). Not stored - consumers should
                     look up cell.nominal_capacity_Ah.

    Returns:
        {
            "cell_metadata":  first cells_metadata entry (backward compat),
            "cells_metadata": list[dict],
            "test_metadata":  list[dict],
            "timeseries":     {},   # no raw timeseries in Naumann's deposits
            "cycle_summary":  list[dict],  # CycleSummarySchema-compatible
        }
    """
    cells_metadata: list[dict] = []
    test_metadata: list[dict] = []
    cycle_summary: list[dict] = []

    for record_key, record in ingested_data.items():
        aging_mode = record.get("aging_mode")
        try:
            if aging_mode == "calendar":
                cell_meta, test_meta, rows = _build_calendar(
                    record_key, record, capacity_Ah
                )
            elif aging_mode == "cycle":
                cell_meta, test_meta, rows = _build_cycle(
                    record_key, record, capacity_Ah
                )
            else:
                print(
                    f"[harmonize_naumann] {record_key}: unknown aging_mode "
                    f"{aging_mode!r} - skipping"
                )
                continue
        except Exception as exc:              # noqa: BLE001 - defensive
            print(f"[harmonize_naumann] {record_key} failed: {exc}")
            continue

        if cell_meta is None or not rows:
            print(
                f"[harmonize_naumann] {record_key}: no checkpoints - skipping"
            )
            continue

        cells_metadata.append(cell_meta)
        test_metadata.append(test_meta)
        cycle_summary.extend(rows)

    return {
        "cell_metadata": cells_metadata[0] if cells_metadata else {},
        "cells_metadata": cells_metadata,
        "test_metadata": test_metadata,
        "timeseries": {},
        "cycle_summary": cycle_summary,
    }
