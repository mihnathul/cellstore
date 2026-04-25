# HNEI - Kollmeyer Panasonic NCR18650PF HPPC

Download required. Place the `.mat` files directly in this directory.

## Summary

| | |
|---|---|
| Cell | Panasonic NCR18650PF, NCA chemistry, 2.9 Ah |
| Form factor | cylindrical (18650) |
| Test types | 5-pulse HPPC at -20, -10, 0, 10, 25 °C |
| Raw format | MATLAB `.mat` (pre-v7.3) |
| Size | ~500 KB per temperature file |

## Where to get the data

    https://data.mendeley.com/datasets/wykht8y7tg

DOI: `10.17632/wykht8y7tg.1`. The full dataset also includes UDDS, US06, LA92, and NN drive cycles plus charge/pause/cycle files - celljar's v0.1 ingester ignores anything that doesn't match the HPPC filename pattern, so leaving them in this directory is fine.

## Expected filenames

celljar scans for files matching:

    {date} {temperature}degC_(5)?[Pp]ulse_HPPC*.mat

Examples:

    03-11-17_08.47 25degC_5Pulse_HPPC_Pan18650PF.mat
    06-15-17_11.31 n20degC_5Pulse_HPPC_Pan18650PF.mat   (n = negative)

Full set: -20, -10, 0, 10, 25 °C.

## Test protocol

Kollmeyer's 5-pulse HPPC. Each test sweeps SOC in 10% steps; at each step, a sequence of charge and discharge pulses characterizes the cell's impedance at that SOC and temperature.

## License / citation

**CC BY 4.0** (per the Mendeley Data record). Attribution required in any derivative work; commercial use permitted; no ShareAlike.

Cite as:

    Kollmeyer, P. (2018). Panasonic 18650PF Li-ion Battery Data.
    Mendeley Data, v1. https://doi.org/10.17632/wykht8y7tg.1

License text: https://creativecommons.org/licenses/by/4.0/

## After downloading

    python examples/demo_end_to_end.py

The demo picks up HNEI files automatically if present and harmonizes them into the canonical schema alongside ORNL_LEAF and MATR.
