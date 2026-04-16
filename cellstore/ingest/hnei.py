"""Ingester for HNEI / Kollmeyer Panasonic NCR18650PF dataset.

Data source: Kollmeyer, P. Panasonic 18650PF Li-ion Battery Data, Mendeley
Data, V1, 2018. doi:10.17632/wykht8y7tg.1
Mirror: https://data.mendeley.com/datasets/wykht8y7tg

Cell: Panasonic NCR18650PF — NCA, 2.9 Ah nominal, cylindrical 18650.

File format: MATLAB .mat files. Each file holds a single struct named
`meas` with these fields (all length-N column vectors):

    Time              elapsed time, seconds
    Voltage           cell voltage, V
    Current           current, A (positive = charge)
    Ah                signed cumulative capacity, Ah
    Wh                signed cumulative energy, Wh
    Battery_Temp_degC cell surface temperature, °C
    Chamber_Temp_degC ambient chamber temperature, °C
    Power             V × I, W
    TimeStamp         absolute datetime objects

Filenames encode test type and temperature, e.g.:

    03-11-17_08.47 25degC_5Pulse_HPPC_Pan18650PF.mat
    06-15-17_11.31 n20degC_5Pulse_HPPC_Pan18650PF.mat   (n = negative)

v0.1 scope: HPPC files only. Drive-cycle ingestion is straightforward to
add — same struct schema, just a different filename pattern to match.
"""

import re
from pathlib import Path

import pandas as pd
from scipy.io import loadmat


# Matches files like "... 25degC_5Pulse_HPPC ..." and "... n20degC_5pulse_HPPC ..."
# Captures the temperature ("n" prefix denotes negative).
_HPPC_FILENAME_RE = re.compile(
    r"(?P<sign>n)?(?P<temp>\d+)degC_\d*[Pp]ulse_HPPC.*\.mat$",
)


def _parse_temp(match: re.Match) -> int:
    """'25' -> 25, 'n20' -> -20."""
    t = int(match.group("temp"))
    return -t if match.group("sign") == "n" else t


def ingest(raw_dir: str) -> dict:
    """Load HNEI Panasonic 18650PF HPPC .mat files.

    Args:
        raw_dir: Path to data/raw/hnei/ containing Kollmeyer .mat files.

    Returns:
        Dict keyed by temperature (int, °C) with:
            raw_df (DataFrame): one row per measurement sample
            temperature_C (int): nominal test temperature
            source_file (str): filename
    """
    raw = Path(raw_dir)
    if not raw.exists():
        raise FileNotFoundError(
            f"HNEI data not found at {raw}. See data/raw/hnei/SOURCE_DATA_PROVENANCE.md "
            f"for download instructions (Mendeley dataset: wykht8y7tg)."
        )

    datasets = {}
    for mat_file in sorted(raw.glob("*.mat")):
        match = _HPPC_FILENAME_RE.search(mat_file.name)
        if not match:
            continue  # skip non-HPPC files (drive cycles, charge/rest, etc.)

        temp_c = _parse_temp(match)

        m = loadmat(str(mat_file), squeeze_me=False)
        meas = m["meas"]
        # Each field is shape (N, 1); flatten to 1-D
        df = pd.DataFrame({
            "Time": meas["Time"][0, 0].ravel(),
            "Voltage": meas["Voltage"][0, 0].ravel(),
            "Current": meas["Current"][0, 0].ravel(),
            "Ah": meas["Ah"][0, 0].ravel(),
            "Wh": meas["Wh"][0, 0].ravel(),
            "Battery_Temp_degC": meas["Battery_Temp_degC"][0, 0].ravel(),
            "Chamber_Temp_degC": meas["Chamber_Temp_degC"][0, 0].ravel(),
        })

        datasets[temp_c] = {
            "raw_df": df,
            "temperature_C": temp_c,
            "source_file": mat_file.name,
        }

    if not datasets:
        raise FileNotFoundError(
            f"No HNEI HPPC files matched in {raw}. Expected names like "
            f"'... 25degC_5Pulse_HPPC ....mat'. "
            f"Found: {[p.name for p in raw.glob('*.mat')]}"
        )

    return datasets
