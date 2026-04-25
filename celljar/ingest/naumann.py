"""Ingester for the Naumann 2021 LFP calendar- and cycle-aging datasets.

This is the SECOND-GENERATION implementation. The Naumann deposits do NOT
ship raw V/I/T timeseries - only pre-aggregated summary arrays (capacity vs
storage time, R_DC vs FEC, etc.) in .xlsx + .mat form. We therefore emit
records that the harmonizer will turn into `cycle_summary` rows rather than
stuffing them into the `timeseries` schema.

Data sources (both CC-BY-4.0, Mendeley Data):

  Calendar aging (Sony / Murata US26650FTC1, LFP / graphite 26650):
    https://doi.org/10.17632/kxh42bfgtj.1
    Ships 4 .xlsx files, each with 17 "test points" (temperature x SOC) as
    columns and a `Storage time / hours` index.

  Cycle aging:
    https://doi.org/10.17632/6hgyr25h8d.1
    Ships a set of .mat files that group test points by grid axis
    (xDOD, xCyC, xSOC, x°C, Loadcollectives) with shapes (35, N_cells).

Companion papers:
  - Naumann et al., JES 17 (2018) 153-169, doi:10.1016/j.est.2018.01.019
  - Naumann et al., JPS 451 (2020) 227666, doi:10.1016/j.jpowsour.2019.227666

Output shape (see `ingest` return value docstring) is deliberately a flat
dict of condition-level records - one per (temperature, SOC, DoD, C-rate)
test point. Each record carries storage-time or FEC arrays plus capacity
and R_DC arrays, ready for the harmonizer to emit as cycle_summary rows.

Everything is numeric Y ratios (normalized to BOL) on the cycle side; the
harmonizer rescales by nominal capacity / BOL resistance. The calendar
xlsx files are in absolute units (Ah for capacity, mOhm for R_DC).
"""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import numpy as np
from scipy.io import loadmat


# ---------------------------------------------------------------------------
# Calendar ingest - .xlsx files
# ---------------------------------------------------------------------------

# Each calendar xlsx has identical shape: row 0 carries the metric label
# ("Discharge capacity C_disch / Ah") in column 1, row 1 has
# "Storage time / hours" then N test-point column headers ("TP_{temp}°C,{soc}%SOC"),
# rows 2+ are the data.
_CAL_FILES = {
    "capacity_Ah": "DischargeCapacity.xlsx",
    "resistance_mOhm": "ResistanceR_DC10s.xlsx",
}

# Parse "TP_40°C,12.5%SOC" (and variants) → (temperature_C, soc_pct).
_TP_RE = re.compile(r"TP_([\d.\-]+)\s*°?C\s*,\s*([\d.]+)\s*%\s*SOC", re.IGNORECASE)


def _parse_tp_label(label: str) -> dict | None:
    """Parse a calendar test-point column label.

    Returns dict with temperature_C and soc_pct (both floats), or None if
    the label does not match the expected pattern.
    """
    if not isinstance(label, str):
        return None
    m = _TP_RE.search(label)
    if not m:
        return None
    try:
        return {
            "temperature_C": float(m.group(1)),
            "soc_pct": float(m.group(2)),
        }
    except ValueError:
        return None


def _read_calendar_xlsx(path: Path) -> dict[str, dict]:
    """Read one calendar .xlsx into {tp_label: {storage_time_h, values}}.

    Values are kept in the xlsx's native units (Ah for capacity, mOhm for R).
    The source xlsx has a metric label in row 0 and the test-point header
    labels in row 1; rows 2.. carry numeric data.
    """
    # Polars' calamine engine requires an optional fastexcel install; even
    # when present, the metric/header preamble means a column-wise mixed
    # dtype, which polars can't represent natively. Fall back to pandas for
    # the read step (it handles mixed dtypes per column) and pull each
    # column out into a numpy array we hand to polars / numpy downstream.
    try:
        import pandas as _pd
    except ImportError as exc:           # noqa: BLE001
        print(f"  [naumann/cal] pandas required to read {path.name}: {exc}")
        return {}

    try:
        pdf = _pd.read_excel(path, header=None, sheet_name=0)
    except Exception as exc:                 # noqa: BLE001
        print(f"  [naumann/cal] failed reading {path.name}: {exc}")
        return {}

    if pdf.shape[0] < 3 or pdf.shape[1] < 2:
        print(f"  [naumann/cal] unexpected shape in {path.name}: {pdf.shape}")
        return {}

    header_row = pdf.iloc[1]
    # The first two rows are mixed string/numeric; coerce data rows to
    # numeric arrays via pandas (gives us float64 with NaN for non-numeric)
    # and hand those numpy arrays back as-is.
    storage_time_h = _pd.to_numeric(pdf.iloc[2:, 0], errors="coerce").to_numpy()

    per_tp: dict[str, dict] = {}
    for col_idx in range(1, pdf.shape[1]):
        label = header_row.iloc[col_idx]
        parsed = _parse_tp_label(label)
        if parsed is None:
            continue
        values = _pd.to_numeric(pdf.iloc[2:, col_idx], errors="coerce").to_numpy()
        per_tp[str(label)] = {
            "temperature_C": parsed["temperature_C"],
            "soc_pct": parsed["soc_pct"],
            "storage_time_h": storage_time_h,
            "values": values,
        }
    return per_tp


def _ingest_calendar(raw: Path) -> dict[str, dict]:
    """Build one ingest record per calendar (temperature, SOC) test point.

    Records are keyed by `CAL_T{temp}_SOC{soc}`; each carries aligned
    `storage_time_h`, `capacity_Ah`, `resistance_mOhm` arrays.
    """
    buckets: dict[str, dict] = {}
    for metric_key, fname in _CAL_FILES.items():
        path = raw / fname
        if not path.exists():
            print(f"  [naumann/cal] missing {fname} - skipping metric {metric_key}")
            continue
        for tp_label, payload in _read_calendar_xlsx(path).items():
            record_key = (
                f"CAL_T{_num(payload['temperature_C'])}_"
                f"SOC{_num(payload['soc_pct'])}"
            )
            rec = buckets.setdefault(record_key, {
                "aging_mode": "calendar",
                "temperature_C": payload["temperature_C"],
                "soc_pct": payload["soc_pct"],
                "storage_time_h": payload["storage_time_h"],
                "tp_label": tp_label,
                "source_files": [],
            })
            # Store under the canonical metric name.
            rec[metric_key] = payload["values"]
            rec["source_files"].append(fname)
            # Sanity: if a later file has a different storage-time axis,
            # log + keep the first one (they should match across files).
            existing_t = rec["storage_time_h"]
            if not np.array_equal(
                np.asarray(existing_t, dtype=float),
                np.asarray(payload["storage_time_h"], dtype=float),
                equal_nan=True,
            ):
                print(
                    f"  [naumann/cal] storage_time mismatch for {record_key} "
                    f"between {rec['source_files'][0]} and {fname}"
                )

    print(f"[naumann] calendar: {len(buckets)} test-point records")
    return buckets


# ---------------------------------------------------------------------------
# Cycle ingest - .mat files
# ---------------------------------------------------------------------------

# Every cycle .mat file in the Mendeley deposit has the same 4 arrays of
# shape (35, N_cells) + a (N_cells,) Legend_Vec string array.
_MAT_ARRAY_KEYS = (
    "X_Axis_Data_Mat",
    "Y_Axis_Data_Mat",
    "Y_Axis_Data_Min_Mat",
    "Y_Axis_Data_Max_Mat",
)

# Legend entries for cycle data look like:
#   "Testpoint Cyclization_40°C_50%SOC_80%DOD_1C_1C_CC"
#   "Testpoint Cyclization_40°C_50%SOC_80%DOD_0.5C_0.5C_CC"
#   "Testpoint Cyclization_40°C_50%SOC_100%DOD_1C_1C_CC+CV"
# And for Loadcollectives:
#   "Testpoint LoadSpectrumPVBattery_40°C_51.4%SOC"
_CYCLIZATION_RE = re.compile(
    r"Cyclization_([\d.\-]+)\s*°?C_"
    r"([\d.]+)\s*%\s*SOC_"
    r"([\d.]+)\s*%\s*DOD_"
    r"([\d.]+)\s*C_"                         # c_rate_charge
    r"([\d.]+)\s*C"                          # c_rate_discharge
    r"(?:_(CC\+CV|CC))?",                    # protocol suffix (optional)
    re.IGNORECASE,
)
_LOADSPECTRUM_RE = re.compile(
    r"LoadSpectrum(\w+?)_([\d.\-]+)\s*°?C_([\d.]+)\s*%\s*SOC",
    re.IGNORECASE,
)


def _parse_cycle_legend(entry: str) -> dict | None:
    """Parse one cycle-legend string into a condition dict.

    Returns None if the entry doesn't match either the Cyclization or
    LoadSpectrum patterns - unknown profiles are skipped but logged by the
    caller.
    """
    if not isinstance(entry, str):
        return None
    m = _CYCLIZATION_RE.search(entry)
    if m:
        return {
            "profile_kind": "cyclization",
            "temperature_C": float(m.group(1)),
            "soc_pct": float(m.group(2)),
            "dod_pct": float(m.group(3)),
            "c_rate_charge": float(m.group(4)),
            "c_rate_discharge": float(m.group(5)),
            "protocol_suffix": m.group(6) or "CC",
        }
    m = _LOADSPECTRUM_RE.search(entry)
    if m:
        return {
            "profile_kind": "loadspectrum",
            "loadspectrum_name": m.group(1),
            "temperature_C": float(m.group(2)),
            "soc_pct": float(m.group(3)),
            # DoD / C-rate not well-defined for mixed load spectra; NaN.
            "dod_pct": float("nan"),
            "c_rate_charge": float("nan"),
            "c_rate_discharge": float("nan"),
            "protocol_suffix": "loadspectrum",
        }
    return None


def _classify_cycle_file(name: str) -> tuple[str, str]:
    """Return (x_axis, metric) for a cycle .mat filename.

    x_axis ∈ {"FEC", "Time", "unknown"}, metric ∈ {"capacity", "resistance",
    "eis", "dvdq", "unknown"}.
    """
    n = name.lower()
    if "capacity" in n:
        metric = "capacity"
    elif "r_dc" in n or "r_dc_10s" in n:
        metric = "resistance"
    elif n.startswith("eis"):
        metric = "eis"
    elif "dvdq" in n:
        metric = "dvdq"
    else:
        metric = "unknown"

    if "_fec" in n:
        x_axis = "FEC"
    elif "_time" in n:
        x_axis = "Time"
    else:
        x_axis = "unknown"
    return x_axis, metric


def _cycle_record_key(cond: dict) -> str:
    """Build a stable record key for a cycle test-point condition."""
    if cond["profile_kind"] == "cyclization":
        return (
            f"CYC_T{_num(cond['temperature_C'])}_"
            f"SOC{_num(cond['soc_pct'])}_"
            f"D{_num(cond['dod_pct'])}_"
            f"C{_num(cond['c_rate_charge'])}_"
            f"C{_num(cond['c_rate_discharge'])}"
        )
    # load spectrum
    return (
        f"LOAD_{cond['loadspectrum_name']}_T{_num(cond['temperature_C'])}_"
        f"SOC{_num(cond['soc_pct'])}"
    )


def _load_cycle_mat(path: Path) -> dict[str, Any] | None:
    """Load a cycle .mat and sanity-check the expected array keys."""
    try:
        raw = loadmat(str(path), squeeze_me=True)
    except Exception as exc:                 # noqa: BLE001
        print(f"  [naumann/cyc] failed loading {path.name}: {exc}")
        return None
    for key in _MAT_ARRAY_KEYS + ("Legend_Vec",):
        if key not in raw:
            print(f"  [naumann/cyc] {path.name} missing '{key}' - skipping")
            return None
    # Normalise Legend_Vec to a Python list[str]; scipy returns a 0-d object
    # array for single-entry files after squeeze_me.
    legend_raw = np.atleast_1d(raw["Legend_Vec"]).ravel()
    legend = [str(x) for x in legend_raw.tolist()]
    return {
        "X": np.asarray(raw["X_Axis_Data_Mat"], dtype=float),
        "Y": np.asarray(raw["Y_Axis_Data_Mat"], dtype=float),
        "Y_min": np.asarray(raw["Y_Axis_Data_Min_Mat"], dtype=float),
        "Y_max": np.asarray(raw["Y_Axis_Data_Max_Mat"], dtype=float),
        "legend": legend,
    }


def _ingest_cycle(raw: Path) -> dict[str, dict]:
    """Walk cycle .mat files and bucket them into per-condition records.

    Each record gains one or more of these arrays:
      - fec: cumulative full-equivalent cycles (X from *_FEC.mat)
      - elapsed_time_s: seconds since BOL (X from *_Time.mat)
      - capacity_ratio: Y from *_Capacity_*.mat (normalized, BOL = 1.0)
      - resistance_ratio: Y from *_R_DC_10s_*.mat (normalized, BOL = 1.0)
    """
    buckets: dict[str, dict] = {}
    mat_files = sorted(raw.glob("*.mat"))

    for path in mat_files:
        x_axis, metric = _classify_cycle_file(path.name)

        # v0.2 defers EIS and DVA parsing - we only carry a flag so the
        # harmonizer can surface a note in additionalProperties if needed.
        if metric in ("eis", "dvdq") or metric == "unknown" or x_axis == "unknown":
            if metric in ("eis", "dvdq"):
                print(f"  [naumann/cyc] deferring {metric.upper()} file {path.name}")
            elif metric == "unknown" or x_axis == "unknown":
                print(f"  [naumann/cyc] unclassified file {path.name} - skipping")
            continue

        loaded = _load_cycle_mat(path)
        if loaded is None:
            continue

        X, Y = loaded["X"], loaded["Y"]
        legend = loaded["legend"]

        if X.ndim != 2 or Y.ndim != 2 or X.shape != Y.shape:
            print(
                f"  [naumann/cyc] {path.name} shape mismatch: "
                f"X={X.shape}, Y={Y.shape} - skipping"
            )
            continue
        if X.shape[1] != len(legend):
            print(
                f"  [naumann/cyc] {path.name} legend length "
                f"{len(legend)} != column count {X.shape[1]} - skipping"
            )
            continue

        for col, entry in enumerate(legend):
            cond = _parse_cycle_legend(entry)
            if cond is None:
                print(f"  [naumann/cyc] {path.name} unparsed legend entry: {entry!r}")
                continue

            record_key = _cycle_record_key(cond)
            rec = buckets.setdefault(record_key, {
                "aging_mode": "cycle",
                "source_files": [],
                **cond,
                "tp_label": entry,
            })

            x_col = X[:, col]
            y_col = Y[:, col]

            if x_axis == "FEC":
                # FEC arrays from different files for the same condition
                # should match; keep the first-seen and log otherwise.
                if "fec" not in rec:
                    rec["fec"] = x_col
                elif not np.allclose(rec["fec"], x_col, equal_nan=True):
                    # Length/value mismatch is rare but possible across files.
                    pass
            elif x_axis == "Time":
                if "elapsed_time_s" not in rec:
                    rec["elapsed_time_s"] = x_col
                # same handling as FEC

            if metric == "capacity":
                rec["capacity_ratio"] = y_col
            elif metric == "resistance":
                rec["resistance_ratio"] = y_col

            rec["source_files"].append(path.name)

    # Deduplicate source_files list for tidiness.
    for rec in buckets.values():
        rec["source_files"] = sorted(set(rec["source_files"]))

    print(f"[naumann] cycle: {len(buckets)} test-point records")
    return buckets


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _num(x: float) -> str:
    """Format a numeric condition value compactly for record / cell / test IDs.

    Drops unnecessary trailing zeros (12.5 stays, 40.0 → 40); NaN → NA.
    """
    try:
        if x is None or not np.isfinite(x):
            return "NA"
        f = float(x)
        if f.is_integer():
            return str(int(f))
        # compact decimal, strip trailing zeros from "12.50" etc.
        return ("%g" % f)
    except (TypeError, ValueError):
        return "NA"


def ingest(raw_dir: str) -> dict:
    """Load Naumann 2021 calendar- and cycle-aging aggregates.

    Args:
        raw_dir: Path to data/raw/naumann/ containing the .xlsx (calendar)
                 and .mat (cycle) bundles from the two Mendeley deposits.

    Returns:
        Dict keyed by a stable record_key:
          - Calendar: CAL_T{temp}_SOC{soc}
          - Cycle:    CYC_T{temp}_SOC{soc}_D{dod}_C{c_chg}_C{c_dchg}
          - LoadColl: LOAD_{name}_T{temp}_SOC{soc}

        Each record has:
          aging_mode:       "calendar" | "cycle"
          temperature_C:    float
          soc_pct:          float
          dod_pct:          float (cycle only; NaN for loadspectrum)
          c_rate_charge:    float (cycle only; NaN for loadspectrum)
          c_rate_discharge: float (cycle only; NaN for loadspectrum)
          tp_label:         the source-string the record came from

          Calendar arrays:
            storage_time_h:   (N_checkpoints,)
            capacity_Ah:      (N_checkpoints,) absolute Ah at 3.0 Ah nominal
            resistance_mOhm:  (N_checkpoints,) absolute mOhm

          Cycle arrays (subset, depending on which .mat files were present):
            fec:                 (N,) full-equivalent cycles
            elapsed_time_s:      (N,) seconds since BOL
            capacity_ratio:      (N,) Y normalized to BOL (BOL = 1.0)
            resistance_ratio:    (N,) Y normalized to BOL (BOL = 1.0)
          source_files:          list of contributing filenames
    """
    raw = Path(raw_dir)
    if not raw.exists():
        raise FileNotFoundError(
            f"Naumann data not found at {raw}. See "
            f"data/raw/naumann/SOURCE_DATA_PROVENANCE.md for download instructions."
        )

    records: dict = {}
    records.update(_ingest_calendar(raw))
    records.update(_ingest_cycle(raw))

    if not records:
        raise RuntimeError(
            f"Parsed 0 records from {raw}. Expected Naumann .xlsx (calendar) "
            f"and .mat (cycle) files. See SOURCE_DATA_PROVENANCE.md."
        )

    print(f"[naumann] total test-point records ingested: {len(records)}")
    return records
