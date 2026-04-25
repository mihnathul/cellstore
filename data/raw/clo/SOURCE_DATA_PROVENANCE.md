# CLO — Attia 2020 closed-loop fast-charging optimization

Download required. Place the single batch `.mat` file directly in this directory.

## Summary

| | |
|---|---|
| Cell | A123 APR18650M1A, LFP chemistry, 1.1 Ah (same as MATR / Severson 2019) |
| Form factor | cylindrical (18650) |
| Cells tested | ~45 surviving cells |
| Test types | Cycling under closed-loop Bayesian-optimized fast-charge policies, 4C CC-CV discharge |
| Raw format | MATLAB v7.3 (HDF5-backed) |
| Size | ~3 GB single file |

## Where to get the data

    https://data.matr.io/1/projects/5d80e633f405260001c0b60a

DOI: `10.1038/s41586-020-1994-5` (associated Nature paper). Direct download URLs are signed and expire; UI-driven download via the TRI project page is the canonical path.

## Expected filenames

    2019-01-24_batchdata_updated_struct_errorcorrect.mat   (~3 GB, single CLO campaign)

Format is MATLAB v7.3 (HDF5-backed). celljar parses this with `h5py` — `scipy.io.loadmat` does **not** support v7.3.

The top-level `batch` struct mirrors the MATR layout (`summary`, `cycles`, `cycle_life`, `policy_readable`), so the ingester is a near-copy of `celljar/ingest/matr.py`.

## Test protocol

Successor campaign to Severson 2019 using the same A123 APR18650M1A cells in a 30 °C forced-convection chamber. Unlike MATR's fixed 72-protocol grid, CLO selects each cell's two-step fast-charge policy (e.g. `4.8C(80%)-4.8C`) online via a Bayesian-optimization loop that jointly minimizes cycle-life uncertainty and time-to-EOL. Discharge is always 4 C CC-CV; cells are cycled toward 80% capacity retention (some cells do not reach EOL within the campaign window — this is a known artifact of the campaign budget, not a data error).

celljar treats CLO as "batch 4" (`b4c{N}`) extending the MATR b1/b2/b3 convention, so cross-source MATR-style analyses stay consistent.

## Known caveats

- `summary.IR` field may be all-zero, NaN, or missing per cell — an oversight in the published release. celljar reads it defensively and does not expose scalar resistance fields on CLO tests.
- A handful of cells fail to reach EOL within the campaign. They are kept in v0.1; downstream filters can use `num_cycles` / final `QDischarge` to separate completed vs. truncated tests.

## License / citation

**CC BY 4.0** per the data.matr.io platform index. Attribution required in any derivative work; commercial use permitted; no ShareAlike.

Note: Newer TRI datasets (2022+) are CC BY-**NC** 4.0; the Attia 2020 CLO release predates that change and remains CC BY 4.0. Verify the current terms on the TRI platform page before redistributing.

Cite as:

    Attia, P. M., Grover, A., Jin, N., et al. (2020).
    Closed-loop optimization of fast-charging protocols for batteries with machine learning.
    Nature 578, 397-402. https://doi.org/10.1038/s41586-020-1994-5

License text: https://creativecommons.org/licenses/by/4.0/

## After downloading

    python examples/demo_end_to_end.py

The demo picks up the CLO batch file automatically if present and harmonizes the cycling tests into the canonical schema alongside MATR, ORNL_LEAF, and HNEI.
