"""Ingester for the CLO (Closed-Loop Optimization) LFP fast-charging dataset.

Data source: P. M. Attia, A. Grover, N. Jin, K. A. Severson, T. M. Markov,
Y.-H. Liao, M. H. Chen, B. Cheong, N. Perkins, Z. Yang, P. K. Herring,
M. Aykol, S. J. Harris, R. D. Braatz, S. Ermon, W. C. Chueh,
"Closed-loop optimization of fast-charging protocols for batteries with
machine learning", Nature 578, 397-402 (2020).
DOI: 10.1038/s41586-020-1994-5
Mirror: https://data.matr.io/1/projects/5d80e633f405260001c0b60a

CLO is a successor to the Severson 2019 MATR dataset using the same
A123 APR18650M1A cells and the same file layout. It differs by using
closed-loop Bayesian-optimized fast-charge protocols instead of the
fixed 72-protocol grid. v0.1 treats CLO as "batch 4" (b4c{N}) so future
cross-source work can reuse the MATR batch/cell convention.

File format: MATLAB v7.3 (HDF5-backed) — must use h5py, not scipy.io.loadmat.
Same `batch` top-level group structure as MATR (summary, cycles,
cycle_life, policy_readable).
"""

from pathlib import Path

import h5py
import numpy as np


# CLO cell exclusions. The paper documents a handful of early-life failures;
# the community generally keeps all ~45 surviving cells. Extend this set as
# per-cell QA lands; v0.1 keeps every cell the source file exposes.
_EXCLUDE_CELLS: set[str] = set()


# Single CLO release file. CLO is one continuous campaign, not multiple batches.
_BATCH_FILE = "2019-01-24_batchdata_updated_struct_errorcorrect.mat"

# Treat CLO as "batch 4" extending the MATR b1/b2/b3 numbering.
_CLO_BATCH_NUM = 4


def _decode_policy(f: h5py.File, ref) -> str:
    """Dereference a policy_readable entry to a Python str."""
    ds = f[ref]
    arr = ds[...]
    # Stored as uint16 char codes in MATLAB v7.3 for char arrays.
    try:
        flat = np.asarray(arr).flatten()
        if flat.dtype.kind in ("u", "i"):
            return "".join(chr(int(c)) for c in flat).strip()
        if flat.dtype.kind == "S":
            return b"".join(flat.tolist()).decode("utf-8", errors="replace").strip()
        if flat.dtype.kind == "O":
            return "".join(str(x) for x in flat).strip()
    except Exception:
        pass
    return str(arr)


def _scalar(f: h5py.File, ref) -> float:
    """Dereference a (1,1) numeric ref to a Python float."""
    return float(np.asarray(f[ref][...]).flatten()[0])


def _safe_flatten(dataset) -> np.ndarray:
    """Read an HDF5 dataset and flatten; return empty array on missing/error."""
    try:
        return dataset[...].flatten()
    except Exception:
        return np.array([])


def _load_batch(path: Path, batch_num: int) -> dict:
    """Parse the CLO batch .mat file into the canonical dict shape."""
    cells = {}
    with h5py.File(str(path), "r") as f:
        batch = f["batch"]
        num_cells = batch["summary"].shape[0]
        print(f"Loading CLO batch {batch_num}: {num_cells} cells...")

        for i in range(num_cells):
            cell_key = f"b{batch_num}c{i}"
            if cell_key in _EXCLUDE_CELLS:
                continue

            # --- summary (per-cycle aggregates) ---
            summary_struct = f[batch["summary"][i, 0]]
            # IR may be missing / all-zero / NaN in CLO (paper oversight);
            # read defensively.
            summary = {
                "cycle": _safe_flatten(summary_struct["cycle"]),
                "QDischarge": _safe_flatten(summary_struct["QDischarge"]),
                "QCharge": _safe_flatten(summary_struct["QCharge"]),
                "IR": (
                    _safe_flatten(summary_struct["IR"])
                    if "IR" in summary_struct
                    else np.array([])
                ),
                "Tmax": _safe_flatten(summary_struct["Tmax"]),
                "Tavg": _safe_flatten(summary_struct["Tavg"]),
                "Tmin": _safe_flatten(summary_struct["Tmin"]),
                "chargetime": _safe_flatten(summary_struct["chargetime"]),
            }

            # --- cycles (per-cycle timeseries) ---
            cycles_struct = f[batch["cycles"][i, 0]]
            n_cycles = cycles_struct["I"].shape[0]
            cycles = {}
            # summary['cycle'] gives the cycle-number label for each index.
            cycle_labels = summary["cycle"]
            for j in range(n_cycles):
                ts = {
                    "I": f[cycles_struct["I"][j, 0]][...].flatten(),
                    "V": f[cycles_struct["V"][j, 0]][...].flatten(),
                    "Qc": f[cycles_struct["Qc"][j, 0]][...].flatten(),
                    "Qd": f[cycles_struct["Qd"][j, 0]][...].flatten(),
                    "T": f[cycles_struct["T"][j, 0]][...].flatten(),
                    "t": f[cycles_struct["t"][j, 0]][...].flatten(),
                }
                if j < cycle_labels.size:
                    label = str(int(cycle_labels[j]))
                else:
                    label = str(j + 1)
                cycles[label] = ts

            # --- scalars ---
            cycle_life = _scalar(f, batch["cycle_life"][i, 0])
            charge_policy = _decode_policy(f, batch["policy_readable"][i, 0])

            cells[cell_key] = {
                "cycle_life": cycle_life,
                "charge_policy": charge_policy,
                "summary": summary,
                "cycles": cycles,
                "source_file": path.name,
                "batch": batch_num,
            }

    return cells


def ingest(raw_dir: str) -> dict:
    """Load the CLO (Attia 2020) LFP closed-loop fast-charging dataset.

    Args:
        raw_dir: Path to data/raw/clo/ containing the single batch .mat file.

    Returns:
        Dict keyed by `b4c{idx}` with per-cell summary, cycles,
        cycle_life, charge_policy, source_file, batch.
    """
    raw = Path(raw_dir)
    if not raw.exists():
        raise FileNotFoundError(
            f"CLO data not found at {raw}. See data/raw/clo/SOURCE_DATA_PROVENANCE.md "
            f"or download from https://data.matr.io/1/projects/5d80e633f405260001c0b60a."
        )

    batch_path = raw / _BATCH_FILE
    if not batch_path.exists():
        raise FileNotFoundError(
            f"CLO batch file not found: {batch_path}. Expected: {_BATCH_FILE}. "
            f"See data/raw/clo/SOURCE_DATA_PROVENANCE.md."
        )

    cells = _load_batch(batch_path, _CLO_BATCH_NUM)

    if not cells:
        raise RuntimeError(f"No cells parsed from {batch_path}")

    return cells
