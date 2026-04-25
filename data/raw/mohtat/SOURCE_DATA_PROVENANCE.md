# Mohtat 2021 — UMich Pouch Cell V + Expansion Cyclic Aging Dataset

Download required. Place the per-cell `.mat` files directly in this directory.

## Summary

| | |
|---|---|
| Cell | UMich Battery Lab (UMBL) custom pouch, NMC532 / graphite, 5.0 Ah |
| Form factor | pouch |
| Cells tested | 31 |
| Test types | cycle_aging with synchronous cell expansion (Keyence laser) |
| Raw format | MATLAB `.mat` (mixed v5 and v7.3) |
| Voltage range | 3.0 - 4.2 V |
| Chamber temps | -5, 25, 45 C (varies per cell) |
| C-rate / DoD | multiple points across the aging matrix |

## Where to get the data

    https://deepblue.lib.umich.edu/data/concern/data_sets/5d86p0488

DOI: `10.7302/7tw1-kc35` (University of Michigan - Deep Blue Data).
Associated paper: Mohtat et al. 2021, *J. Electrochem. Soc.* 168, 110517,
`https://iopscience.iop.org/article/10.1149/1945-7111/ac2d3e`.

### Download notes

The Deep Blue landing page sits behind a Cloudflare managed challenge
that blocks non-interactive fetches (tested against the HTML page, the
`/oai` metadata endpoint, and `Accept: application/rdf+xml` — all return
the Cloudflare interstitial). Download manually via a browser.

## Expected filenames

Per-cell `.mat` files (exact naming varies with the upstream release; the
ingester is tolerant of the common conventions — `Cell01.mat`, `W8.mat`,
etc.). celljar ingests whichever `.mat` files are present in this
directory — partial downloads are fine.

## .mat structure

Each file holds a per-cell struct with time-vector fields. Field names
vary across the archive; the ingester probes these aliases:

    time_s         t | time | Time | time_s | t_s | elapsed_time
    voltage_V      V | Voltage | voltage | Ecell_V | V_cell
    current_A      I | Current | current | I_A
    temperature_C  T | Temperature | temperature | T_C | Temp
    displacement_um   Exp | Expansion | Disp | Displacement | disp
    cycle_number   cycle | cycleNumber | Cycle | n_cycle

MAT v7.3 files are HDF5 under the hood; the ingester falls back to
`h5py` when `scipy.io.loadmat` raises `NotImplementedError`.

## Why this dataset is unique

Mohtat 2021 ships **synchronous Keyence laser displacement measurements**
on the same time base as V/I/T. Mechanical expansion is a first-class
signal for electrochemo-mechanical and state-of-health modeling, and
public datasets with this channel are rare — this is the dataset's
defining contribution to PyBaMM / PyBOP and the broader battery ML
community (~250+ citations).

celljar v0.3 adds a nullable `displacement_um` column to the canonical
timeseries schema. Sources without expansion data emit null; Mohtat
populates it. No JOIN is required; downstream consumers can filter
`displacement_um IS NOT NULL` to find sources with this signal.

## v0.2 harmonization scope

celljar treats each cell's `.mat` file as one `cycle_aging` test
(`test_id = MOHTAT_CELL{tag}_CYCLING`). Per-cycle segmentation is
deferred — consumers can derive cycle boundaries from `cycle_number`
(when the source provides it) or from current-sign transitions.

## License / citation

**CC BY 4.0** per the Deep Blue Data record. Attribution required in any
derivative work; commercial use permitted; no ShareAlike.

License status: the upstream Deep Blue Data record asserts CC-BY-4.0.
At the time this note was written, the Deep Blue HTML endpoint was
gated by Cloudflare so programmatic re-verification against the live
record was not possible. The license is propagated through every
test_metadata record's `source_license` field as the in-repo
authoritative reference.

Cite as:

    Mohtat, P., Lee, S., Siegel, J. B., & Stefanopoulou, A. G. (2021).
    UofM Pouch Cell Voltage and Expansion Cyclic Aging Dataset.
    University of Michigan - Deep Blue Data.
    https://doi.org/10.7302/7tw1-kc35

Dataset DOI: `10.7302/7tw1-kc35`.
License text: https://creativecommons.org/licenses/by/4.0/

## After downloading

    python examples/demo_end_to_end.py

The demo picks up any `.mat` files present in this directory and
harmonizes them into the canonical schema alongside the other sources.
