# SNL Preger 2020 - Commercial 18650 LFP / NMC / NCA degradation

Download required. Place the per-cell `*_timeseries.csv` files directly in
this directory. The `metadata.csv` manifest from the BatteryArchive download
tool (see below) can also be dropped here; celljar ingests it as a cross-
reference but the ingester does not require it.

## Summary

| | |
|---|---|
| Cells | ~61 commercial 18650s across three chemistries |
| LFP | A123 APR18650M1A (1.1 Ah, 3.3 V) |
| NMC | LG INR18650-MJ1 (3.5 Ah, 3.6 V) |
| NCA | Panasonic NCR18650B (3.4 Ah, 3.6 V) |
| Anode | graphite (all three) |
| Form factor | cylindrical (18650) |
| Test type | cycle aging to ~80% capacity |
| Chamber temps | 15, 25, 35 degC |
| SOC windows | 0-100, 20-80, 40-60 % |
| C-rates | charge 0.5C CCCV; discharge 0.5C / 1C / 2C / 3C |
| Publisher | Sandia National Laboratories (Grid Energy Storage) |
| Format | BatteryArchive standardized CSV (see below) |

## Reference

    Preger, Y., Barkholtz, H.M., Fresquez, A., Campbell, D.L., Juba, B.W.,
    Romàn-Kustas, J., Ferreira, S.R., Chalamala, B. (2020).
    Degradation of Commercial Lithium-Ion Cells as a Function of Chemistry
    and Cycling Conditions.
    Journal of The Electrochemical Society, 167, 120532.
    https://doi.org/10.1149/1945-7111/abae37

## Where to get the data

Data is hosted on **BatteryArchive.org** (Sandia's public repository). The
study summary page is:

    https://www.batteryarchive.org/snl_study.html

**IMPORTANT:** At the time of this writing the `/data/*.csv` endpoint is
not publicly accessible via direct HTTP. The batteryarchive FAQ states:

> If you would like to obtain the complete CSV files for the cells in the
> study, please email info@batteryarchive.org

Two supported paths to obtain the files:

1. **Email request** - send a note to `info@batteryarchive.org` asking for
   the SNL study timeseries CSVs. They typically provide a bulk download
   link or ship a zip.
2. **Community mirror / download helper** - the BatteryArchive data-transfer
   helper (`github.com/BikingJesus/batteryarchive`, `data_transfer.py`)
   points at the canonical base URL `https://www.batteryarchive.org/data/`
   with filenames `{cell_id}_timeseries.csv` and `{cell_id}_cycle_data.csv`.
   The cell_id comes from the shipped `metadata.csv` (61 SNL rows).
   If you have access (or the endpoint becomes public), this is the bulk
   path.

A copy of `metadata.csv` (pulled from the helper repo) is already staged
in this directory so you can see the exact cell_id list before downloading.

## Filename convention

Once downloaded, timeseries CSVs are named:

    SNL_18650_{CHEM}_{TEMP}C_{SOC_LO}-{SOC_HI}_{CRATE_CHG}-{CRATE_DCHG}C_{REP}_timeseries.csv

where:

- `CHEM`  ∈ {LFP, NMC, NCA}
- `TEMP`  ∈ {15, 25, 35}  - chamber temperature, degC
- `SOC_LO-SOC_HI` - cycling SOC window in % (e.g. `0-100`, `20-80`, `40-60`)
- `CRATE_CHG-CRATE_DCHG` - C-rates (note: the download helper replaces
  the canonical BatteryArchive `/` with `-`, so `0.5/1C` → `0.5-1C`)
- `REP`   - lowercase replicate letter (`a`..`d`)

Example filenames:

    SNL_18650_LFP_25C_0-100_0.5-1C_a_timeseries.csv
    SNL_18650_LFP_25C_20-80_0.5-0.5C_b_timeseries.csv
    SNL_18650_NMC_35C_0-100_0.5-2C_a_timeseries.csv
    SNL_18650_NCA_15C_0-100_0.5-1C_a_timeseries.csv

Anything that doesn't match is silently skipped by the ingester.

## CSV columns (BatteryArchive standard)

    Date_Time                  timestamp string (optional)
    Test_Time (s)              elapsed test time
    Cycle_Index                1-based cycle number
    Step_Index                 step within cycle (optional)
    Current (A)                positive = charge
    Voltage (V)                cell voltage
    Charge_Capacity (Ah)       monotonic per-cycle charge counter
    Discharge_Capacity (Ah)    monotonic per-cycle discharge counter
    Charge_Energy (Wh)         monotonic per-cycle charge energy
    Discharge_Energy (Wh)      monotonic per-cycle discharge energy
    Cell_Temperature (C)       cell surface temperature
    Environment_Temperature (C) chamber temperature (fallback)

celljar combines `Charge_* - Discharge_*` into signed `capacity_Ah` and
`energy_Wh` following the celljar convention (positive = charge).

## Test protocol

Each cell sees **one** cycling condition - a fixed triplet of temperature,
SOC window, and charge/discharge C-rate - and is cycled until ~80% of BOL
capacity. Periodic reference capacity checks (C/2 full CCCV) and HPPC-style
DCIR pulses are interspersed per the published Preger et al. schedule.
Per-chemistry detail:

- **LFP (A123)** - charge 0.5C CCCV to 3.6 V, CV cutoff 0.05C;
  discharge CC at 0.5/1/2/3 C to 2.0 V.
- **NMC (LG MJ1)** - charge 0.5C CCCV to 4.2 V, CV cutoff 0.05C;
  discharge CC at 0.5/1/2/3 C to 2.5 V.
- **NCA (Panasonic NCR18650B)** - charge 0.5C CCCV to 4.2 V, CV cutoff 0.05C;
  discharge CC at 0.5/1/2 C to 2.5 V.

## License / citation

**CC BY 4.0** per the BatteryArchive terms. Confirm the license on the
download page / accompanying README when you pull data - if SNL ships a
different LICENSE file with a particular download bundle, that supersedes.

Cite as:

    Preger, Y. et al. (2020). Degradation of Commercial Lithium-Ion Cells
    as a Function of Chemistry and Cycling Conditions. Journal of The
    Electrochemical Society, 167, 120532.
    https://doi.org/10.1149/1945-7111/abae37

Also credit BatteryArchive as the data host:

    https://www.batteryarchive.org

License text: https://creativecommons.org/licenses/by/4.0/

## After downloading

    python examples/demo_end_to_end.py

The demo picks up any `*_timeseries.csv` files present in this directory
whose names match the SNL filename convention above, and harmonizes them
into the canonical schema alongside the other sources.

If only a subset of cells is present (partial download) the demo still
runs; celljar ingests whichever cells it finds. Disk usage is roughly
10-50 MB per `_timeseries.csv`; a full pull of all ~61 cells is on the
order of 1-3 GB.
