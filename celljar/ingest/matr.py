"""Ingester for the MATR LFP fast-charging dataset (Severson 2019).

Data source: K. A. Severson, P. M. Attia, N. Jin, N. Perkins, B. Jiang,
Z. Yang, M. H. Chen, M. Aykol, P. K. Herring, D. Fraggedakis, M. Z. Bazant,
S. J. Harris, W. C. Chueh, R. D. Braatz, "Data-driven prediction of
battery cycle life before capacity degradation", Nature Energy 4, 383-391 (2019).
DOI: 10.1038/s41560-019-0356-8
Mirror: https://data.matr.io/1/

Cells: 124 × A123 APR18650M1A lithium-ion (LFP/graphite, 1.1 Ah nominal)
across three batches, cycled to failure under fast-charge protocols at 30 degC.

File format: MATLAB v7.3 (HDF5-backed) - must use h5py, not scipy.io.loadmat.
Each file exposes a top-level `batch` group whose fields are arrays of HDF5
object references; each reference dereferences to a per-cell struct.

    batch
      ├── summary     (N_cells, 1) refs  → per-cycle aggregates
      │     ├── cycle, QDischarge, QCharge, IR
      │     ├── Tmax, Tavg, Tmin, chargetime    (all shape (1, N_cycles))
      ├── cycles      (N_cells, 1) refs  → per-cycle timeseries
      │     └── I, V, Qc, Qd, T, t               (each array of N_cycles refs)
      ├── cycle_life  (N_cells, 1) refs  → scalar EOL cycle count
      ├── policy_readable (N_cells, 1) refs → charge policy string
      └── ...
"""

from pathlib import Path

import h5py
import numpy as np


# Batch-2 cells that are continuations of batch-1 cells - paper and BatteryML
# exclude them from model fitting.
_BATCH2_CONTINUATIONS = {"b2c7", "b2c8", "b2c9", "b2c15", "b2c16"}

# Cells flagged noisy/invalid. v0.1 keeps only the documented continuations;
# extend this set as more per-cell QA lands.
_EXCLUDE_CELLS = _BATCH2_CONTINUATIONS


_BATCH_FILES = {
    1: "2017-05-12_batchdata_updated_struct_errorcorrect.mat",
    2: "2017-06-30_batchdata_updated_struct_errorcorrect.mat",
    3: "2018-04-12_batchdata_updated_struct_errorcorrect.mat",
}


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


def _load_batch(path: Path, batch_num: int) -> dict:
    """Parse a single MATR batch .mat file into the canonical dict shape."""
    cells = {}
    with h5py.File(str(path), "r") as f:
        batch = f["batch"]
        num_cells = batch["summary"].shape[0]
        print(f"Loading batch {batch_num}: {num_cells} cells...")

        for i in range(num_cells):
            cell_key = f"b{batch_num}c{i}"
            if cell_key in _EXCLUDE_CELLS:
                continue

            # --- summary (per-cycle aggregates) ---
            summary_struct = f[batch["summary"][i, 0]]
            summary = {
                "cycle": summary_struct["cycle"][...].flatten(),
                "QDischarge": summary_struct["QDischarge"][...].flatten(),
                "QCharge": summary_struct["QCharge"][...].flatten(),
                "IR": summary_struct["IR"][...].flatten(),
                "Tmax": summary_struct["Tmax"][...].flatten(),
                "Tavg": summary_struct["Tavg"][...].flatten(),
                "Tmin": summary_struct["Tmin"][...].flatten(),
                "chargetime": summary_struct["chargetime"][...].flatten(),
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
    """Load the MATR (Severson 2019) LFP fast-charging dataset.

    Args:
        raw_dir: Path to data/raw/matr/ containing the batch .mat files.

    Returns:
        Dict keyed by `b{batch}c{idx}` with per-cell summary, cycles,
        cycle_life, charge_policy, source_file, batch.
    """
    raw = Path(raw_dir)
    if not raw.exists():
        raise FileNotFoundError(
            f"MATR data not found at {raw}. See data/raw/matr/SOURCE_DATA_PROVENANCE.md "
            f"or download from https://data.matr.io/1/."
        )

    present = {n: raw / fname for n, fname in _BATCH_FILES.items() if (raw / fname).exists()}
    if not present:
        expected = ", ".join(_BATCH_FILES.values())
        raise FileNotFoundError(
            f"No MATR batch files found in {raw}. Expected one or more of: {expected}. "
            f"See data/raw/matr/SOURCE_DATA_PROVENANCE.md."
        )

    missing = [fname for n, fname in _BATCH_FILES.items() if n not in present]
    if missing:
        print(f"Note: missing MATR batch files (skipping): {', '.join(missing)}")

    cells: dict = {}
    for batch_num in sorted(present):
        cells.update(_load_batch(present[batch_num], batch_num))

    if not cells:
        raise RuntimeError(f"No cells parsed from {raw}")

    return cells
