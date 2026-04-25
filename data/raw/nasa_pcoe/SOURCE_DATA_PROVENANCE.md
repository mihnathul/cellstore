# NASA PCoE — Li-ion Battery Aging Dataset (B0005-B0056)

Download required. Place the `.mat` files directly in this directory.

## Summary

| | |
|---|---|
| Cell | 18650 Li-ion, vendor undisclosed (community consensus: LCO chemistry), 2.0 Ah nominal |
| Form factor | cylindrical (18650) |
| Cells tested | 34 (B0005, B0006, B0007, B0018, B0025-B0056 with gaps) |
| Test types | Cycle aging: charge (CC-CV, 1.5 A → 4.2 V → 20 mA) + discharge (varied protocol per cell) + EIS (0.1 Hz – 5 kHz) triplet repeated to EOL |
| Temperatures | 4 °C (cold), 24 °C (room), 43–44 °C (hot) — per-cell assignment in ingester |
| Discharge cutoffs | 2.0 / 2.2 / 2.5 / 2.7 V — per-cell assignment |
| Raw format | MATLAB `.mat` (pre-v7.3, loadable by `scipy.io.loadmat`) |
| Size | ~200 MB zip, ~250 MB unpacked |

## Where to get the data

    https://data.nasa.gov/dataset/li-ion-battery-aging-datasets
    https://www.nasa.gov/intelligent-systems-division/discovery-and-systems-health/pcoe/pcoe-data-set-repository/

Direct download URL (single zip containing 6 sub-zips):

    https://phm-datasets.s3.amazonaws.com/NASA/5.+Battery+Data+Set.zip

## Expected filenames

celljar scans for files matching `B*.mat` directly in this directory. The full set from NASA's 6 sub-zips is:

    B0005.mat  B0006.mat  B0007.mat  B0018.mat                 # 24 degC baseline
    B0025.mat  B0026.mat  B0027.mat  B0028.mat                 # 24 degC, square-wave discharge
    B0029.mat  B0030.mat  B0031.mat  B0032.mat                 # 43 degC
    B0033.mat  B0034.mat  B0036.mat                            # 24 degC variants
    B0038.mat  B0039.mat  B0040.mat                            # mixed 24/44 degC, multi-load
    B0041.mat ... B0044.mat                                    # 4 degC, mixed loads
    B0045.mat ... B0048.mat                                    # 4 degC, 1 A
    B0049.mat ... B0052.mat                                    # 4 degC, 2 A (software crash)
    B0053.mat ... B0056.mat                                    # 4 degC, 2 A (cycled to 30% fade)

Partial data is fine — the ingester picks up whatever `B*.mat` files it finds. For the 4-most-cited subset, just `B0005/06/07/18` suffices.

## Test protocol

Each cell runs a rotating three-operation schedule until end-of-life:

1. **Charge** — 1.5 A CC to 4.2 V, then CV to 20 mA cut-off.
2. **Discharge** — per-cell current / waveform / cutoff voltage (see ingester `_CELL_CONDITIONS`).
3. **EIS impedance** — frequency sweep 0.1 Hz to 5 kHz; Re + Rct fit parameters stored per cycle.

A discharge capacity is recorded per discharge cycle. EOL criterion varies: 20 % fade, 30 % fade, or experiment-ended (software crash) depending on sub-batch.

## v0.3 harmonization scope

- **One test per cell**, `test_type = "cycle_aging"`.
- **Charge + discharge combined** into a single continuous timeseries (cycle_number monotone; `(charge → discharge)` = one cycle).
- **EIS impedance cycles dropped from timeseries**; scalar Re + Rct emitted as `cycle_summary` rows indexed at the surrounding discharge cycle_number. Full EIS spectra deferred to v0.4.
- **SOH** computed from `capacity_last / capacity_first` (`soh_method = "capacity_vs_first_checkpoint"`).

## License / citation

**US Government Work** — public domain in the United States (17 U.S.C. § 105). Non-US users should check their jurisdiction's treatment of US Government works. No formal DOI; cite the repository landing page.

Cite as:

    Saha, B. & Goebel, K. (2007). Battery Data Set. NASA Prognostics Data
    Repository, NASA Ames Research Center, Moffett Field, CA.
    https://www.nasa.gov/intelligent-systems-division/discovery-and-systems-health/pcoe/pcoe-data-set-repository/

Cells are 18650 Li-ion; chemistry/vendor not disclosed by NASA. Community consensus (per papers using this dataset) treats them as LCO — flag this assumption when using the harmonized `chemistry: "LCO"` field.

License / privacy reference: https://www.nasa.gov/about/highlights/HP_Privacy.html

## After downloading

    python examples/demo_end_to_end.py

The demo picks up NASA PCoE `B*.mat` files automatically if present and harmonizes them into the canonical schema alongside the other sources.
