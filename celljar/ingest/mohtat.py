"""Ingester for Mohtat 2021 UMich Pouch Cell V + Expansion Cyclic Aging dataset.

Data source: Mohtat, P., Lee, S., Siegel, J. B., & Stefanopoulou, A. G. (2021).
"UofM Pouch Cell Voltage and Expansion Cyclic Aging Dataset."
University of Michigan - Deep Blue Data. doi:10.7302/7tw1-kc35
URL: https://deepblue.lib.umich.edu/data/concern/data_sets/5d86p0488

Cells: 31 x UMich Battery Lab (UMBL) custom pouch, NMC532 / graphite, 5.0 Ah
nominal, 3.0 - 4.2 V. Cycle-aged across T in {-5, 25, 45 C} at various
C-rates and DoDs.

Unique feature: a Keyence laser displacement sensor records cell-surface
expansion synchronously with V/I/T, shipped in the same .mat structure.

File format: MATLAB .mat per cell (mixed v5 and v7.3). Each file holds a
per-cell struct with time-vector fields. Field naming varies across the
archive - common spellings include:

    t, time, Time          elapsed time [s]
    V, Voltage, Ecell_V    cell voltage [V]
    I, Current             current [A] (positive = charge)
    T, Temperature         cell temperature [C]
    Exp, Expansion,        displacement / swelling [um]
    Displacement, disp

The ingester is field-name-tolerant: it probes a small set of aliases and
falls back to NaN columns when a field is missing.

v0.2 scope: treats each cell's file as one cycle_aging test. Per-cycle
segmentation is deferred; downstream consumers can derive cycle boundaries
from current sign + cycle index if the source provides one.
"""

from pathlib import Path
from typing import Optional

import numpy as np
import polars as pl


# Field-name aliases in Mohtat .mat files. First match wins.
# Keyed by canonical celljar column.
_FIELD_ALIASES = {
    "time_s":          ["t", "time", "Time", "time_s", "t_s", "elapsed_time"],
    "voltage_V":       ["V", "Voltage", "voltage", "Ecell_V", "V_cell", "Vcell"],
    "current_A":       ["I", "Current", "current", "I_A", "Icell"],
    "temperature_C":   ["T", "Temperature", "temperature", "T_C", "Temp", "Tcell"],
    "displacement_um": ["Exp", "Expansion", "expansion", "Disp", "Displacement",
                        "displacement", "disp", "expansion_um", "disp_um"],
    "cycle_number":    ["cycle", "cycleNumber", "Cycle", "cycle_num", "n_cycle"],
}


def _pick_field(payload: dict, aliases: list) -> Optional[np.ndarray]:
    """Return the first alias present in `payload` as a 1-D float array, or None."""
    for name in aliases:
        if name in payload:
            arr = payload[name]
            if arr is None:
                continue
            # scipy.io.loadmat wraps scalars in (1,1) object arrays; squeeze + ravel.
            return np.asarray(arr).squeeze().ravel()
    return None


def _flatten_matv5_struct(mat: dict) -> dict:
    """Flatten a scipy.io.loadmat result into {field: ndarray} for the
    first top-level struct found. Skips __header__/__version__ etc."""
    # Strip scipy metadata keys.
    user_keys = [k for k in mat.keys() if not k.startswith("__")]
    if not user_keys:
        return {}

    # Heuristic: if exactly one user key and it is a struct, unpack it.
    # Otherwise treat the top level itself as the payload.
    if len(user_keys) == 1:
        top = mat[user_keys[0]]
        # scipy structs arrive as shape (1,1) ndarray with dtype.names set.
        if hasattr(top, "dtype") and top.dtype.names is not None:
            return {name: top[name][0, 0] for name in top.dtype.names}
        # Or a plain ndarray - no useful fields to unpack.
        return {user_keys[0]: top}
    # Multi-variable .mat: use top-level variable names directly.
    return {k: mat[k] for k in user_keys}


def _load_mat(path: Path) -> dict:
    """Load a .mat file, handling both v5 (scipy) and v7.3 (h5py) formats.
    Returns a flat {field: ndarray} dict."""
    from scipy.io import loadmat

    try:
        mat = loadmat(str(path), squeeze_me=False)
        return _flatten_matv5_struct(mat)
    except NotImplementedError:
        # v7.3 is HDF5 under the hood - scipy can't read it.
        pass

    try:
        import h5py
    except ImportError as exc:
        raise ImportError(
            f"{path.name} is MAT v7.3 (HDF5). Install h5py to read it: "
            f"`pip install h5py`."
        ) from exc

    payload = {}
    with h5py.File(str(path), "r") as f:
        # v7.3 mat files store each variable as a top-level group/dataset.
        # Struct fields become sub-groups; numeric arrays are datasets.
        def _collect(group):
            for name, obj in group.items():
                if name.startswith("#"):
                    continue  # HDF5 refs table
                if isinstance(obj, h5py.Dataset):
                    payload[name] = np.array(obj).T  # MATLAB stores column-major
                elif isinstance(obj, h5py.Group):
                    # Flatten one level of nesting (typical MATLAB struct).
                    for subname, subobj in obj.items():
                        if isinstance(subobj, h5py.Dataset):
                            payload[subname] = np.array(subobj).T
        _collect(f)
    return payload


def ingest(raw_dir: str) -> dict:
    """Load Mohtat 2021 UMich pouch .mat files.

    Args:
        raw_dir: Path to data/raw/mohtat/ containing per-cell .mat files.

    Returns:
        Dict keyed by cell tag (filename stem) with:
            raw_df (DataFrame): harmonized per-sample columns
            source_cell_id (str): filename stem
            source_file (str): filename
    """
    raw = Path(raw_dir)
    if not raw.exists():
        raise FileNotFoundError(
            f"Mohtat data not found at {raw}. See "
            f"data/raw/mohtat/SOURCE_DATA_PROVENANCE.md for download "
            f"instructions (Deep Blue DOI 10.7302/7tw1-kc35)."
        )

    mat_files = sorted(raw.glob("*.mat"))
    if not mat_files:
        raise FileNotFoundError(
            f"No Mohtat .mat files found in {raw}. Expected per-cell files "
            f"downloaded from Deep Blue record 5d86p0488. "
            f"Found: {[p.name for p in raw.iterdir()]}"
        )

    datasets = {}
    for mat_file in mat_files:
        payload = _load_mat(mat_file)
        if not payload:
            continue

        time_s = _pick_field(payload, _FIELD_ALIASES["time_s"])
        if time_s is None or len(time_s) == 0:
            # File has no recognizable time axis - skip.
            continue
        n = len(time_s)

        def _col(name: str) -> np.ndarray:
            arr = _pick_field(payload, _FIELD_ALIASES[name])
            if arr is None or len(arr) != n:
                return np.full(n, np.nan, dtype=float)
            return arr.astype(float, copy=False)

        voltage_V = _col("voltage_V")
        current_A = _col("current_A")
        temp_C = _col("temperature_C")
        disp_um = _col("displacement_um")

        cyc_arr = _pick_field(payload, _FIELD_ALIASES["cycle_number"])
        if cyc_arr is not None and len(cyc_arr) == n:
            cycle_number = cyc_arr.astype(int, copy=False)
        else:
            cycle_number = np.ones(n, dtype=int)  # one-cycle fallback

        df = pl.DataFrame({
            "time_s": time_s.astype(float),
            "voltage_V": voltage_V,
            "current_A": current_A,
            "temperature_C": temp_C,
            "displacement_um": disp_um,
            "cycle_number": cycle_number,
        })

        cell_tag = mat_file.stem  # e.g. "Cell01", preserves source naming
        datasets[cell_tag] = {
            "raw_df": df,
            "source_cell_id": cell_tag,
            "source_file": mat_file.name,
        }

    if not datasets:
        raise FileNotFoundError(
            f"Found .mat files in {raw} but none yielded a recognizable "
            f"time/V/I schema. Field aliases probed: {_FIELD_ALIASES}."
        )

    return datasets
