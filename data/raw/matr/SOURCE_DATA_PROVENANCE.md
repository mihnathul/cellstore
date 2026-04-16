# MATR — Severson 2019 fast-charging cycle-life

Download required. Place the three batch `.mat` files directly in this directory.

## Summary

| | |
|---|---|
| Cell | A123 APR18650M1A, LFP chemistry, 1.1 Ah |
| Form factor | cylindrical (18650) |
| Cells tested | 124 (119 after cellstore's documented exclusions) |
| Test types | Cycling under various 2-step fast-charge policies, 4C CC-CV discharge |
| Raw format | MATLAB v7.3 (HDF5-backed) |
| Size | ~4 GB total across 3 batches |

## Where to get the data

    https://data.matr.io/1/projects/5c48dd2bc625d700019f3204

DOI: `10.1038/s41560-019-0356-8` (associated Nature Energy paper). Direct download URLs are signed and expire; UI-driven download via the TRI project page is the canonical path.

## Expected filenames

    2017-05-12_batchdata_updated_struct_errorcorrect.mat   (~1.3 GB, batch 1)
    2017-06-30_batchdata_updated_struct_errorcorrect.mat   (~1.4 GB, batch 2)
    2018-04-12_batchdata_updated_struct_errorcorrect.mat   (~1.0 GB, batch 3)

Format is MATLAB v7.3 (HDF5-backed). cellstore parses these with `h5py` — `scipy.io.loadmat` does **not** support v7.3.

Partial data is fine: cellstore processes whichever of the 3 batches are present; they don't all have to be downloaded.

## Test protocol

124 cells cycled at 30 °C in a forced-convection chamber. Each cell is assigned one of 72 two-step fast-charge policies (e.g. `3.6C(80%)-3.6C` = charge at 3.6 C up to 80% SOC, then 3.6 C to full). Discharge is always 4 C CC-CV. Cells are cycled until 80% capacity retention.

## Known exclusions

cellstore skips the following batch-2 cells — they are continuations of batch-1 cells that were moved after a channel fault, per Severson's errata:

    b2c7, b2c8, b2c9, b2c15, b2c16

## License / citation

**CC BY 4.0** per the data.matr.io platform index. Attribution required in any derivative work; commercial use permitted; no ShareAlike.

Note: Newer TRI datasets (2022+) are CC BY-**NC** 4.0; the Severson 2019 data predates that change and remains CC BY 4.0. Verify the current terms on the TRI platform page before redistributing.

Cite as:

    Severson, K. A., Attia, P. M., Jin, N., et al. (2019).
    Data-driven prediction of battery cycle life before capacity degradation.
    Nature Energy 4, 383-391. https://doi.org/10.1038/s41560-019-0356-8

License text: https://creativecommons.org/licenses/by/4.0/

## After downloading

    python examples/demo_end_to_end.py

The demo picks up MATR batch files automatically if present and harmonizes the cycling tests into the canonical schema alongside ORNL_LEAF and HNEI.
