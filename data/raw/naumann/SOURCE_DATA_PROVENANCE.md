# Naumann 2021 — LFP calendar & cycle aging (v0.2: cycle_summary)

Aggregated summary data for calendar- and cycle-aging of the Sony / Murata
US26650FTC1 ("FTC1A") LFP / graphite 26650 cell. Both Mendeley deposits are
small (~70 KB total) and ship pre-aggregated arrays rather than raw cycler
V/I/T — celljar v0.2 therefore emits `cycle_summary` rows, not
`timeseries` rows, from these files.

## Summary

| | |
|---|---|
| Cell | Sony / Murata US26650FTC1 ("FTC1A"), LFP / graphite, 3.0 Ah nominal |
| Form factor | cylindrical (26650) |
| Voltage range | 2.0 - 3.6 V (nominal 3.2 V) |
| Test types | Calendar aging + cycle aging (checkup-only data; no raw cycler) |
| Raw formats | `.xlsx` (calendar) + `.mat` (cycle, v5/v7 — `scipy.io.loadmat`) |
| License | CC BY 4.0 (both deposits) |

## Where to get the data

Both deposits are on Mendeley Data:

Calendar aging (17 test-point conditions, mean of 3 replicates):

    https://data.mendeley.com/datasets/kxh42bfgtj/1
    DOI: 10.17632/kxh42bfgtj.1

Cycle aging (19 test-point conditions + 2 load spectra, mean of 3 replicates):

    https://data.mendeley.com/datasets/6hgyr25h8d/1
    DOI: 10.17632/6hgyr25h8d.1

Download both bundles and place the files flat in this directory.

## File layout

### Calendar — 4 `.xlsx` files

Each has one sheet with `Storage time / hours` in column 0 and 17 test-point
columns labelled `TP_{temp}°C,{soc}%SOC`:

  - `DischargeCapacity.xlsx`         → capacity_Ah vs storage_time_h (35 checkpoints)
  - `ResistanceR_DC10s.xlsx`         → R_DC,10s in mΩ vs storage_time_h
  - `ElectrochemicalImpedanceSpectroscopy.xlsx` → 14 EIS-derived scalars × 4 storage times (v0.2 defers)
  - `DifferentialVoltageAnalysis.xlsx` → DVA peak parameters × 4 storage times (v0.2 defers)

### Cycle — 21 `.mat` files

Each `.mat` carries 4 arrays `X_Axis_Data_Mat`, `Y_Axis_Data_Mat`,
`Y_Axis_Data_Min_Mat`, `Y_Axis_Data_Max_Mat` of shape `(35, N_cells)` plus a
`Legend_Vec` of `N_cells` strings. X is FEC (`*_FEC.mat`) or time in seconds
(`*_Time.mat`). Y is always **normalised to BOL** (Y[0] = 1.0); absolute
units are recovered by multiplying by nominal capacity (cycle deposits
publish capacity-retention ratio) or by the calendar-derived BOL DC
resistance (~33.3 mΩ).

Legend conventions:

  - `Testpoint Cyclization_{T}°C_{SOC}%SOC_{DoD}%DOD_{C_chg}C_{C_dchg}C_{CC|CC+CV}`
  - `Testpoint LoadSpectrum{Name}_{T}°C_{SOC}%SOC` (load-collective profiles)

File groups:

  - `xDOD_1C1C_40°C_{Capacity_CC_CV|R_DC_10s}_FEC.mat`  — DoD sweep at 40 °C, 1C/1C
  - `xSOC_20DOD_1C1C_40°C_{…}_FEC.mat`                  — SOC-centre sweep at 40 °C, 20% DoD
  - `xCyC_80DOD_40°C_{…}_FEC.mat`                       — C-rate sweep at 40 °C, 80% DoD
  - `xDOD_1C1C_x°C_{…}_FEC.mat`                         — 25 °C vs 40 °C comparison
  - `Loadcollectives_{…}_{FEC|Time}.mat`                — two dynamic profiles (PV, PV+PRL)
  - `EIS_*.mat`, `dVdQ_*.mat`                           — v0.2 defers

## Harmonization notes

- ONE `cell_id` per (T, SOC, DoD, C-rate) test point — Naumann published
  mean-across-replicates aggregates, so the "cell" here is a logical
  equivalence class of 3 replicate cells.
- `cell_id = NAUMANN_CAL_T{T}_SOC{SOC}` for calendar, `NAUMANN_CYC_T{T}_SOC{SOC}_D{DoD}_C{C_chg}_C{C_dchg}` for cycle.
- `test_id = cell_id + "_TEST"`.
- Each row in `cycle_summary` is one checkpoint; `elapsed_time_s` is the
  primary x-axis for calendar, `equivalent_full_cycles` for cycle.
- `resistance_dc_pulse_duration_s = 10.0` (Naumann uses R_DC,10s).
- `resistance_dc_soc_pct` = the storage SOC for calendar; `50%` for cycle
  deposit (Naumann's convention).

## License / citation

**CC BY 4.0** (both Mendeley deposits). Attribution required; commercial use
permitted; no ShareAlike.

Cite as:

    Naumann, M. (2021). Data for: Analysis and modeling of calendar aging of a
    commercial LiFePO4 / graphite cell. Mendeley Data, v1.
    https://doi.org/10.17632/kxh42bfgtj.1

    Naumann, M. (2021). Data for: Analysis and modeling of cycle aging of a
    commercial LiFePO4 / graphite cell. Mendeley Data, v1.
    https://doi.org/10.17632/6hgyr25h8d.1

Companion peer-reviewed papers:

    Naumann, M. et al. (2018). Analysis and modeling of calendar aging of a
    commercial LiFePO4 / graphite cell. Journal of Energy Storage 17, 153-169.
    https://doi.org/10.1016/j.est.2018.01.019

    Naumann, M. et al. (2020). Analysis and modeling of cycle aging of a
    commercial LiFePO4 / graphite cell. Journal of Power Sources 451, 227666.
    https://doi.org/10.1016/j.jpowsour.2019.227666

License text: https://creativecommons.org/licenses/by/4.0/
