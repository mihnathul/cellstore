"""Ingester for NASA PCoE Li-ion battery aging dataset (B0005-B0056).

Data source: Saha, B. & Goebel, K. (2007). "Battery Data Set", NASA Ames
Prognostics Data Repository.
https://www.nasa.gov/intelligent-systems-division/discovery-and-systems-health/pcoe/pcoe-data-set-repository/

Dataset scope: 34 18650 Li-ion cells (vendor undisclosed, community consensus
LCO), cycled to end-of-life under charge / discharge / EIS-impedance rotation.
Each .mat holds one cell's full aging record.

File format (pre-v7.3 MATLAB .mat, scipy.io.loadmat-compatible):

    top-level struct named after the cell (e.g. 'B0005')
      .cycle  : 1-D struct array, one entry per operation in chronological order
          .type                 : 'charge' | 'discharge' | 'impedance'
          .ambient_temperature  : chamber T (deg C)
          .time                 : MATLAB date-vector (1x6: Y, M, D, h, m, s)
          .data                 : sub-struct whose fields depend on .type:
              charge:    Voltage_measured, Current_measured, Temperature_measured,
                         Current_charge, Voltage_charge, Time
              discharge: Voltage_measured, Current_measured, Temperature_measured,
                         Current_load, Voltage_load, Time, Capacity (scalar, Ah)
              impedance: Sense_current, Battery_current, Current_ratio,
                         Battery_impedance (complex array vs frequency),
                         Rectified_Impedance, Re (scalar), Rct (scalar)

Note: the shipped README_*.txt files claim discharge also uses
`Current_charge`/`Voltage_charge`, but the .mat files actually use
`Current_load`/`Voltage_load`. We read both (preferring `_load` for discharge).

Per-cell test conditions are hard-coded in _CELL_CONDITIONS (from NASA's
README_*.txt files shipped with the dataset).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
from scipy.io import loadmat


# (ambient_temperature_C, charge_current_A, discharge_current_A, discharge_cutoff_V,
#  discharge_waveform, notes)
# Values sourced from the README_*.txt files shipped with the NASA PCoE zip.
_CELL_CONDITIONS: dict[str, dict] = {
    # --- README.txt / README_25_26_27_28.txt (24 degC, 4A square-wave discharge) ---
    "B0005": {"T_C": 24, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.7,
              "waveform": "CC",     "eol_note": "Room-temp baseline; cycled to 30% fade."},
    "B0006": {"T_C": 24, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.5,
              "waveform": "CC",     "eol_note": "Room-temp baseline; cycled to 30% fade."},
    "B0007": {"T_C": 24, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.2,
              "waveform": "CC",     "eol_note": "Room-temp baseline; cycled to 30% fade."},
    "B0018": {"T_C": 24, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.5,
              "waveform": "CC",     "eol_note": "Room-temp baseline; cycled to 30% fade."},
    # --- README_25_26_27_28.txt (24 degC, 4A 0.05Hz square-wave, 50% duty) ---
    "B0025": {"T_C": 24, "I_chg": 1.5, "I_dchg": 4.0, "V_cut": 2.0,
              "waveform": "square_0.05Hz_4A_50pct",
              "eol_note": "0.05Hz square-wave discharge, 4A peak, 50% duty."},
    "B0026": {"T_C": 24, "I_chg": 1.5, "I_dchg": 4.0, "V_cut": 2.2,
              "waveform": "square_0.05Hz_4A_50pct",
              "eol_note": "0.05Hz square-wave discharge, 4A peak, 50% duty."},
    "B0027": {"T_C": 24, "I_chg": 1.5, "I_dchg": 4.0, "V_cut": 2.5,
              "waveform": "square_0.05Hz_4A_50pct",
              "eol_note": "0.05Hz square-wave discharge, 4A peak, 50% duty."},
    "B0028": {"T_C": 24, "I_chg": 1.5, "I_dchg": 4.0, "V_cut": 2.7,
              "waveform": "square_0.05Hz_4A_50pct",
              "eol_note": "0.05Hz square-wave discharge, 4A peak, 50% duty."},
    # --- README_29_30_31_32.txt (43 degC, 4A CC discharge) ---
    "B0029": {"T_C": 43, "I_chg": 1.5, "I_dchg": 4.0, "V_cut": 2.0,
              "waveform": "CC",     "eol_note": "Elevated 43 degC; CC discharge."},
    "B0030": {"T_C": 43, "I_chg": 1.5, "I_dchg": 4.0, "V_cut": 2.2,
              "waveform": "CC",     "eol_note": "Elevated 43 degC; CC discharge."},
    "B0031": {"T_C": 43, "I_chg": 1.5, "I_dchg": 4.0, "V_cut": 2.5,
              "waveform": "CC",     "eol_note": "Elevated 43 degC; CC discharge."},
    "B0032": {"T_C": 43, "I_chg": 1.5, "I_dchg": 4.0, "V_cut": 2.7,
              "waveform": "CC",     "eol_note": "Elevated 43 degC; CC discharge."},
    # --- README_33_34_36.txt (24 degC, mixed discharge) ---
    "B0033": {"T_C": 24, "I_chg": 1.5, "I_dchg": 4.0, "V_cut": 2.0,
              "waveform": "CC",     "eol_note": "Cycled to 20% fade."},
    "B0034": {"T_C": 24, "I_chg": 1.5, "I_dchg": 4.0, "V_cut": 2.2,
              "waveform": "CC",     "eol_note": "Cycled to 20% fade."},
    "B0036": {"T_C": 24, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.7,
              "waveform": "CC",     "eol_note": "2A discharge; cycled to 20% fade."},
    # --- README_38_39_40.txt (24 & 44 degC multi-temp, multiple load levels) ---
    "B0038": {"T_C": 24, "I_chg": 1.5, "I_dchg": None, "V_cut": 2.2,
              "waveform": "multi_load_1_2_4A",
              "eol_note": "Mixed 24/44 degC; multi-level discharge 1/2/4 A."},
    "B0039": {"T_C": 24, "I_chg": 1.5, "I_dchg": None, "V_cut": 2.5,
              "waveform": "multi_load_1_2_4A",
              "eol_note": "Mixed 24/44 degC; multi-level discharge 1/2/4 A."},
    "B0040": {"T_C": 24, "I_chg": 1.5, "I_dchg": None, "V_cut": 2.7,
              "waveform": "multi_load_1_2_4A",
              "eol_note": "Mixed 24/44 degC; multi-level discharge 1/2/4 A."},
    # --- README_41_42_43_44.txt (4 degC, mixed 4A/1A discharge) ---
    "B0041": {"T_C": 4, "I_chg": 1.5, "I_dchg": None, "V_cut": 2.0,
              "waveform": "multi_load_4A_1A",
              "eol_note": "4 degC; mixed 4A/1A discharge; cycled to 30% fade."},
    "B0042": {"T_C": 4, "I_chg": 1.5, "I_dchg": None, "V_cut": 2.2,
              "waveform": "multi_load_4A_1A",
              "eol_note": "4 degC; mixed 4A/1A discharge; cycled to 30% fade."},
    "B0043": {"T_C": 4, "I_chg": 1.5, "I_dchg": None, "V_cut": 2.5,
              "waveform": "multi_load_4A_1A",
              "eol_note": "4 degC; mixed 4A/1A discharge; cycled to 30% fade."},
    "B0044": {"T_C": 4, "I_chg": 1.5, "I_dchg": None, "V_cut": 2.7,
              "waveform": "multi_load_4A_1A",
              "eol_note": "4 degC; mixed 4A/1A discharge; cycled to 30% fade."},
    # --- README_45_46_47_48.txt (4 degC, 1A CC discharge) ---
    "B0045": {"T_C": 4, "I_chg": 1.5, "I_dchg": 1.0, "V_cut": 2.0,
              "waveform": "CC",     "eol_note": "4 degC; 1A CC; cycled to 30% fade."},
    "B0046": {"T_C": 4, "I_chg": 1.5, "I_dchg": 1.0, "V_cut": 2.2,
              "waveform": "CC",     "eol_note": "4 degC; 1A CC; cycled to 30% fade."},
    "B0047": {"T_C": 4, "I_chg": 1.5, "I_dchg": 1.0, "V_cut": 2.5,
              "waveform": "CC",     "eol_note": "4 degC; 1A CC; cycled to 30% fade."},
    "B0048": {"T_C": 4, "I_chg": 1.5, "I_dchg": 1.0, "V_cut": 2.7,
              "waveform": "CC",     "eol_note": "4 degC; 1A CC; cycled to 30% fade."},
    # --- README_49_50_51_52.txt (4 degC, 2A CC; run terminated by software crash) ---
    "B0049": {"T_C": 4, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.0,
              "waveform": "CC",     "eol_note": "4 degC; 2A CC; run ended by software crash."},
    "B0050": {"T_C": 4, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.2,
              "waveform": "CC",     "eol_note": "4 degC; 2A CC; run ended by software crash."},
    "B0051": {"T_C": 4, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.5,
              "waveform": "CC",     "eol_note": "4 degC; 2A CC; run ended by software crash."},
    "B0052": {"T_C": 4, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.7,
              "waveform": "CC",     "eol_note": "4 degC; 2A CC; run ended by software crash."},
    # --- README_53_54_55_56.txt (4 degC, 2A CC; cycled to 30% fade) ---
    "B0053": {"T_C": 4, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.0,
              "waveform": "CC",     "eol_note": "4 degC; 2A CC; cycled to 30% fade."},
    "B0054": {"T_C": 4, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.2,
              "waveform": "CC",     "eol_note": "4 degC; 2A CC; cycled to 30% fade."},
    "B0055": {"T_C": 4, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.5,
              "waveform": "CC",     "eol_note": "4 degC; 2A CC; cycled to 30% fade."},
    "B0056": {"T_C": 4, "I_chg": 1.5, "I_dchg": 2.0, "V_cut": 2.7,
              "waveform": "CC",     "eol_note": "4 degC; 2A CC; cycled to 30% fade."},
}


def _to_1d(x) -> np.ndarray:
    """Coerce a .mat-loaded field to a 1-D float ndarray (handles scalars)."""
    arr = np.atleast_1d(np.asarray(x))
    return arr.ravel()


def _getattr(obj, name, default=None):
    """Safe attr-or-field lookup on a scipy .mat struct_as_record=False object."""
    return getattr(obj, name, default) if hasattr(obj, name) else default


def _parse_cycle(cyc) -> dict:
    """Turn one .cycle entry into a plain dict. Returns None if unusable."""
    ctype = str(_getattr(cyc, "type", "")).strip().lower()
    if ctype not in ("charge", "discharge", "impedance"):
        return None

    try:
        amb_T = float(_getattr(cyc, "ambient_temperature", np.nan))
    except (TypeError, ValueError):
        amb_T = float("nan")

    # time: 1x6 MATLAB date-vector [Y, M, D, h, m, s]. Keep as array.
    tvec = _to_1d(_getattr(cyc, "time", np.array([])))

    data = _getattr(cyc, "data", None)
    if data is None:
        return None

    out = {
        "type": ctype,
        "ambient_temperature_C": amb_T,
        "time_vector": tvec.astype(float) if tvec.size else None,
    }

    if ctype in ("charge", "discharge"):
        # README says "Current_charge"/"Voltage_charge" for both; in practice
        # discharge entries use "Current_load"/"Voltage_load" — support both.
        cc = _getattr(data, "Current_charge", None)
        vc = _getattr(data, "Voltage_charge", None)
        if cc is None:
            cc = _getattr(data, "Current_load", None)
        if vc is None:
            vc = _getattr(data, "Voltage_load", None)

        out.update({
            "Voltage_measured":     _to_1d(_getattr(data, "Voltage_measured", np.array([]))),
            "Current_measured":     _to_1d(_getattr(data, "Current_measured", np.array([]))),
            "Temperature_measured": _to_1d(_getattr(data, "Temperature_measured", np.array([]))),
            "Current_charger":      _to_1d(cc) if cc is not None else np.array([]),
            "Voltage_charger":      _to_1d(vc) if vc is not None else np.array([]),
            "Time":                 _to_1d(_getattr(data, "Time", np.array([]))),
        })
        if ctype == "discharge":
            cap = _getattr(data, "Capacity", None)
            try:
                out["Capacity_Ah"] = float(np.asarray(cap).ravel()[0]) if cap is not None else np.nan
            except (TypeError, ValueError, IndexError):
                out["Capacity_Ah"] = float("nan")

    elif ctype == "impedance":
        re_ = _getattr(data, "Re", np.nan)
        rct = _getattr(data, "Rct", np.nan)
        try:
            re_val = float(np.asarray(re_).ravel()[0])
        except (TypeError, ValueError, IndexError):
            re_val = float("nan")
        try:
            rct_val = float(np.asarray(rct).ravel()[0])
        except (TypeError, ValueError, IndexError):
            rct_val = float("nan")

        # We keep the arrays available for v0.4 EIS support but don't emit them
        # in harmonize_nasa_pcoe v0.3.
        out.update({
            "Re_ohm":  re_val,
            "Rct_ohm": rct_val,
            # NOTE: Battery_impedance is complex; scipy loadmat returns complex dtype.
            "Battery_impedance":       _getattr(data, "Battery_impedance", None),
            "Rectified_Impedance":     _getattr(data, "Rectified_Impedance", None),
            "Sense_current":           _getattr(data, "Sense_current", None),
            "Battery_current":         _getattr(data, "Battery_current", None),
            "Current_ratio":           _getattr(data, "Current_ratio", None),
        })

    return out


def ingest(raw_dir: str) -> dict:
    """Load NASA PCoE battery aging .mat files.

    Args:
        raw_dir: Path to data/raw/nasa_pcoe/ containing B*.mat files.

    Returns:
        Dict keyed by cell name (e.g. "B0005") with:
            cycles (list[dict]):   ordered list of parsed cycle dicts
            conditions (dict):     per-cell test conditions (from NASA README
                                   tables). None if the cell isn't in our
                                   hard-coded table.
            source_file (str):     basename of the .mat file
    """
    raw = Path(raw_dir)
    if not raw.exists():
        raise FileNotFoundError(
            f"NASA PCoE data not found at {raw}. See "
            f"data/raw/nasa_pcoe/SOURCE_DATA_PROVENANCE.md for download "
            f"instructions."
        )

    mat_files = sorted(raw.glob("B*.mat"))
    if not mat_files:
        raise FileNotFoundError(
            f"No B*.mat files in {raw}. Expected at minimum B0005.mat, "
            f"B0006.mat, B0007.mat, B0018.mat."
        )

    datasets: dict = {}
    for mat_file in mat_files:
        cell_name = mat_file.stem  # "B0005"
        m = loadmat(str(mat_file), squeeze_me=True, struct_as_record=False)
        if cell_name not in m:
            # Some .mat files might name the top struct differently; pick the
            # first non-dunder key.
            candidates = [k for k in m.keys() if not k.startswith("__")]
            if not candidates:
                continue
            top = m[candidates[0]]
        else:
            top = m[cell_name]

        cycle_array = _getattr(top, "cycle", None)
        if cycle_array is None:
            continue

        cycles = []
        # scipy.io.loadmat(squeeze_me=True) gives us a 1-D ndarray of structs
        for cyc in np.atleast_1d(cycle_array):
            parsed = _parse_cycle(cyc)
            if parsed is not None:
                cycles.append(parsed)

        if not cycles:
            continue

        datasets[cell_name] = {
            "cycles": cycles,
            "conditions": _CELL_CONDITIONS.get(cell_name),
            "source_file": mat_file.name,
        }

    if not datasets:
        raise FileNotFoundError(
            f"No parseable NASA PCoE cells found in {raw}."
        )

    return datasets
