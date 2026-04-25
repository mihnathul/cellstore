"""SNL Preger 2020 dataset ingester.

Sandia National Laboratories commercial-cell degradation campaign spanning
three chemistries (LFP / NMC / NCA in 18650 format) cycled across a grid of
temperature, depth-of-discharge, and discharge C-rate. Published in the
BatteryArchive.org standardized CSV format, so we delegate the column
mapping to `read_batteryarchive_csv` and only parse the per-cell filename
to recover test conditions.

Reference:
    Preger, Y., et al. (2020). Degradation of Commercial Lithium-Ion Cells
    as a Function of Chemistry and Cycling Conditions. Journal of The
    Electrochemical Society, 167, 120532.
    doi:10.1149/1945-7111/abae37
License: CC-BY 4.0 (verify on download).

BatteryArchive cell_ids look like:

    SNL_18650_LFP_25C_0-100_0.5/1C_a

The download helper (github.com/BikingJesus/batteryarchive/data_transfer.py)
replaces "/" with "-" in filenames and appends `_timeseries.csv`, so the
on-disk filenames are:

    SNL_18650_LFP_25C_0-100_0.5-1C_a_timeseries.csv
    SNL_18650_LFP_25C_20-80_0.5/0.5C_b  ->  SNL_18650_LFP_25C_20-80_0.5-0.5C_b_timeseries.csv
    SNL_18650_NCA_35C_0-100_0.5/2C_a    ->  SNL_18650_NCA_35C_0-100_0.5-2C_a_timeseries.csv

This ingester reconstructs the five condition tokens from the filename:

    {host}_{form}_{chem}_{temp}C_{soc_lo}-{soc_hi}_{crate_chg}-{crate_dchg}C_{replicate}

and returns one record per cell, keyed by the canonical (slash-restored)
BatteryArchive cell_id.
"""

from __future__ import annotations

import re
from pathlib import Path

from celljar.ingest.cyclers.batteryarchive import read_batteryarchive_csv


# Canonical BatteryArchive naming:
#   SNL_18650_{CHEM}_{TEMP}C_{SOC_LO}-{SOC_HI}_{CRATE_CHG}-{CRATE_DCHG}C_{REP}_timeseries.csv
# CHEM ∈ {LFP, NMC, NCA}; TEMP ∈ {15, 25, 35} (integer °C, always positive);
# SOC window is two integers (e.g. 0-100, 20-80, 40-60); C-rates can be
# decimals (0.5, 1, 2, 3); replicate is a single lowercase letter.
_FILENAME_RE = re.compile(
    r"^SNL_18650_"
    r"(?P<chem>LFP|NMC|NCA)_"
    r"(?P<temp>\d+)C_"
    r"(?P<soc_lo>\d+)-(?P<soc_hi>\d+)_"
    r"(?P<crate_chg>[\d.]+)-(?P<crate_dchg>[\d.]+)C_"
    r"(?P<rep>[a-z])"
    r"_timeseries\.csv$",
    re.IGNORECASE,
)


def _parse_filename(name: str) -> dict | None:
    """Recover cell metadata from a SNL Preger timeseries CSV filename.

    Returns a dict with keys `chemistry`, `temperature_C`, `soc_min_pct`,
    `soc_max_pct`, `c_rate_charge`, `c_rate_discharge`, `replicate`,
    `source_cell_id` (slash-restored canonical BatteryArchive cell_id).
    Returns None if the filename doesn't match the expected pattern.
    """
    m = _FILENAME_RE.match(name)
    if not m:
        return None
    chem = m.group("chem").upper()
    temp_c = int(m.group("temp"))
    soc_lo = int(m.group("soc_lo"))
    soc_hi = int(m.group("soc_hi"))
    crate_chg = float(m.group("crate_chg"))
    crate_dchg = float(m.group("crate_dchg"))
    rep = m.group("rep").lower()

    # Canonical BatteryArchive cell_id uses "/" between the two C-rates.
    # Format the rates compactly: integers as "1", "2"; decimals as "0.5".
    def _fmt(c: float) -> str:
        return str(int(c)) if c == int(c) else ("%g" % c)

    source_cell_id = (
        f"SNL_18650_{chem}_{temp_c}C_{soc_lo}-{soc_hi}_"
        f"{_fmt(crate_chg)}/{_fmt(crate_dchg)}C_{rep}"
    )

    return {
        "chemistry": chem,
        "temperature_C": temp_c,
        "soc_min_pct": soc_lo,
        "soc_max_pct": soc_hi,
        "c_rate_charge": crate_chg,
        "c_rate_discharge": crate_dchg,
        "replicate": rep,
        "source_cell_id": source_cell_id,
    }


def ingest(raw_dir: str) -> dict:
    """Load all SNL Preger 2020 timeseries CSV files from `raw_dir`.

    Args:
        raw_dir: Path to data/raw/snl_preger/ containing BatteryArchive
                 `*_timeseries.csv` files.

    Returns:
        Dict keyed by the canonical BatteryArchive cell_id (slashes intact,
        e.g. "SNL_18650_LFP_25C_0-100_0.5/1C_a"). Each value has:
            raw_df (DataFrame): canonical celljar columns (via
                `read_batteryarchive_csv`).
            chemistry (str): "LFP" | "NMC" | "NCA".
            temperature_C (int): nominal chamber temperature.
            soc_min_pct / soc_max_pct (int): cycling SOC window.
            c_rate_charge / c_rate_discharge (float).
            replicate (str): single lowercase letter ("a".."d").
            source_cell_id (str): same as the dict key.
            source_file (str): filename on disk.
    """
    raw = Path(raw_dir)
    ts_files = sorted(raw.glob("*_timeseries.csv")) if raw.exists() else []
    if not ts_files:
        raise FileNotFoundError(
            f"SNL Preger 2020 data not found at {raw}. See "
            f"data/raw/snl_preger/SOURCE_DATA_PROVENANCE.md for download "
            f"instructions (BatteryArchive.org, DOI 10.1149/1945-7111/abae37)."
        )

    cells: dict = {}
    for csv_file in ts_files:
        parsed = _parse_filename(csv_file.name)
        if parsed is None:
            # Silently skip unrecognized files (e.g. *_cycle_data.csv aggregates,
            # or files from a different SNL study that got dropped in this dir).
            continue
        try:
            df = read_batteryarchive_csv(csv_file)
        except Exception:                       # pragma: no cover - defensive
            # Don't crash the whole ingest on a single malformed CSV.
            continue

        cells[parsed["source_cell_id"]] = {
            "raw_df": df,
            **parsed,
            "source_file": csv_file.name,
        }

    if not cells:
        raise FileNotFoundError(
            f"No SNL Preger *_timeseries.csv files matched in {raw}. Expected "
            f"names like 'SNL_18650_LFP_25C_0-100_0.5-1C_a_timeseries.csv'. "
            f"Found: {[p.name for p in ts_files][:5]}..."
        )

    return cells
