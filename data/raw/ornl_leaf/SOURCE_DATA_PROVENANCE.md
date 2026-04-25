# ORNL_LEAF — 2013 Nissan Leaf cell data (bundled)

The one source whose raw files ship directly in this directory. No download required.

## Summary

| | |
|---|---|
| Cell | AESC pouch, mixed LMO/NCA blend, 33.1 Ah |
| Form factor | pouch |
| Test types | HPPC at 10, 25, 40 °C |
| Raw format | CSV |
| Size | ~5 MB |

## Where to get the data

Already here — bundled in this directory. Upstream mirror:

    https://zenodo.org/records/2580327

DOI: `10.5281/zenodo.2580327`. See also GitHub: https://github.com/batterysim/nissan-leaf-data

## Expected filenames

    cell-low-current-hppc-25c.csv
    cell-low-current-hppc-10c.csv
    cell-low-current-hppc-40c.csv
    cell-discharge-bitrode-1c.csv
    cell-discharge-bitrode-2c.csv
    cell-discharge-bitrode-3c.csv

celljar uses the three HPPC files, producing three `test_type: hppc` tests.

## Test protocol

Low-current HPPC (Hybrid Pulse Power Characterization) at three chamber temperatures. Each test sweeps SOC in 10% steps with characterization pulses at each step.

## License / citation

**MIT License** (applied to both code and data by the publishers).

Cite as:

    Wiggins, G., Allu, S., & Wang, H. (2019). Battery cell data from a
    2013 Nissan Leaf. Oak Ridge National Laboratory.
    https://doi.org/10.5281/zenodo.2580327

License text: https://opensource.org/licenses/MIT

## After first clone

No action needed. `python examples/demo_end_to_end.py` picks up these CSVs automatically and produces harmonized ORNL_LEAF output.
