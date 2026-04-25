# Bills 2023 - eVTOL aircraft battery dataset (CMU / Sony-Murata VTC6)

Download required. Place the per-cell `VAH##.csv` (and optional
`VAH##_impedance.csv`) files directly in this directory.

## Summary

| | |
|---|---|
| Cell | Sony-Murata US18650VTC6, NMC/graphite, 3.0 Ah |
| Form factor | cylindrical (18650) |
| Cells tested | 22 (non-contiguous IDs in range VAH01-VAH30) |
| Test types | eVTOL mission drive-cycle + periodic RPT (C/5 + DCIR) |
| Raw format | CSV (per-cell) |
| Chamber temps | 20, 30, 35 degC (varies per cell) |

## Where to get the data

    https://kilthub.cmu.edu/articles/dataset/eVTOL_Battery_Dataset/14226830

DOI: `10.1184/R1/14226830` (CMU KiltHub / Figshare).
Associated paper: `10.1038/s41597-023-02180-5` (Nature Scientific Data).

## Expected filenames

Per cell:

    VAH01.csv              - cycling / mission timeseries
    VAH01_impedance.csv    - DCIR pulse data from RPTs (optional)

Plus a top-level `README.txt` distributed with the dataset.

Cell IDs are **non-contiguous** in the range VAH01-VAH30; only 22 of the
30 indices are populated. celljar ingests whichever `VAH##.csv` files
are present - partial downloads are fine.

## CSV columns

    time_s               elapsed time, seconds
    Ecell_V              cell voltage, V
    I_mA                 current, milliamps
    EnergyCharge_W_h     signed charge energy, Wh
    QCharge_mA_h         charge capacity, mAh
    EnergyDischarge_W_h  signed discharge energy, Wh
    QDischarge_mA_h      discharge capacity, mAh
    Temperature_C        cell surface temperature, degC
    cycleNumber          cycle index (mission repeat count)
    Ns                   BioLogic step number

celljar converts `I_mA` to A and `Q*_mA_h` to Ah during harmonization.

## Test protocol

Each cell is cycled under a simulated eVTOL flight mission:

    takeoff   54 W for  75 s
    cruise    16 W for 800 s
    landing   54 W for 105 s

This mission is repeated continuously. Every 50 missions a **Reference
Performance Test (RPT)** is interspersed, consisting of a C/5 capacity
check plus a DCIR pulse sequence. Charging between missions is CC at 1 C
to the 4.2 V upper limit.

Chamber temperature varies per cell (20, 30, or 35 degC); see the
dataset `README.txt` for the per-cell assignment.

## v0.2 harmonization scope

celljar v0.2 ingests the **main `VAH##.csv` cycling files only** and
treats each cell as a single `drive_cycle` test containing the full
mission + RPT record. DCIR extraction from `VAH##_impedance.csv` is
deferred to a later release (would become a separate `test_type: "hppc"`
or `"checkup"` test per cell).

## License / citation

**CC BY 4.0** per the CMU KiltHub record. Attribution required in any
derivative work; commercial use permitted; no ShareAlike.

Cite as:

    Bills, A., Sripad, S., Fredericks, W. L., et al. (2023).
    A battery dataset for electric vertical takeoff and landing aircraft.
    Scientific Data 10, 344. https://doi.org/10.1038/s41597-023-02180-5

Dataset DOI: `10.1184/R1/14226830`.
License text: https://creativecommons.org/licenses/by/4.0/

## After downloading

    python examples/demo_end_to_end.py

The demo picks up any `VAH##.csv` files present in this directory and
harmonizes them into the canonical schema alongside the other sources.
