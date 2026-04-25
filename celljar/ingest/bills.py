"""Ingester for Bills 2023 eVTOL battery dataset (CMU / Sony-Murata VTC6).

Data source: Bills, A., Sripad, S., Fredericks, W. L., et al. (2023).
"A battery dataset for electric vertical takeoff and landing aircraft."
Scientific Data 10, 344. doi:10.1038/s41597-023-02180-5
Hosted on CMU KiltHub / Figshare: doi:10.1184/R1/14226830
Mirror: https://kilthub.cmu.edu/articles/dataset/eVTOL_Battery_Dataset/14226830

Cells: 22 x Sony-Murata US18650VTC6 (NMC/graphite, 3.0 Ah nominal, cylindrical 18650),
cycled under a simulated eVTOL mission profile (takeoff + cruise + landing) with
periodic Reference Performance Tests (RPTs) interspersed every 50 missions.

File format: per-cell CSV exported from BioLogic EC-Lab software
(VMP3/SP-300 series potentiostat). For each VAH## cell:

    VAH##.csv            -- cycling / mission timeseries
    VAH##_impedance.csv  -- DCIR pulse data from RPTs (not ingested in v0.2)

Columns in VAH##.csv (native BioLogic EC-Lab export units):

    time_s               elapsed time, seconds
    Ecell_V              cell voltage, V
    I_mA                 current, milliamps (BioLogic default unit)
    EnergyCharge_W_h     signed charge energy, Wh
    QCharge_mA_h         charge capacity, mAh
    EnergyDischarge_W_h  signed discharge energy, Wh
    QDischarge_mA_h      discharge capacity, mAh
    Temperature__C       cell surface temperature, degC (note double underscore)
    cycleNumber          cycle index (mission repeat count)
    Ns                   BioLogic step number

Cell IDs are non-contiguous in VAH01-VAH30 (22 cells present).

v0.2 scope: main cycling CSV only (VAH##.csv). Impedance CSVs are not
ingested; DCIR extraction can be added as a separate test_type later.

Implementation note:
    The per-cell CSVs are plain BioLogic EC-Lab exports, so this ingester
    delegates CSV parsing and column normalization to
    :func:`celljar.ingest.cyclers.biologic.read_biologic_csv` via the
    ``_BILLS_ALIASES`` map below. The harmonizer (harmonize_bills.py)
    still consumes a ``raw_df`` keyed on BioLogic's native column names
    / units (I_mA, QCharge_mA_h, ...), so after the canonical load we
    reconstruct that shape from the canonical frame. Routing through
    the shared loader keeps Bills on the same validated BioLogic code
    path as future EC-Lab datasets.
"""

import re
from pathlib import Path

import numpy as np
import polars as pl

from celljar.ingest.cyclers.biologic import read_biologic_csv


# Matches files like "VAH01.csv", "VAH17.csv" -- captures 2-digit cell index.
# Excludes the "_impedance" suffix so we only pick up the main cycling files.
_CELL_FILENAME_RE = re.compile(r"^VAH(?P<idx>\d{2})\.csv$")


# Bills' BioLogic export uses the compact underscore-suffixed variant of
# EC-Lab's column names. These are already covered by the generic
# BioLogic loader's default alias map, but we declare them explicitly
# here so the mapping lives next to the Bills-specific ingester and any
# future casing drift is easy to patch locally.
_BILLS_ALIASES: dict[str, str] = {
    "time_s": "timestamp_s",
    "Ecell_V": "voltage_V",
    "I_mA": "_current_mA",
    "QCharge_mA_h": "_charge_cap_mAh",
    "QDischarge_mA_h": "_discharge_cap_mAh",
    "EnergyCharge_W_h": "_charge_energy_Wh",
    "EnergyDischarge_W_h": "_discharge_energy_Wh",
    "Temperature__C": "temperature_C",
    "cycleNumber": "cycle_number",
    "Ns": "step_number",
}


def _canonical_to_bills_raw_df(canonical: pl.DataFrame, source_csv: Path) -> pl.DataFrame:
    """Reconstruct the BioLogic-native raw_df shape the harmonizer expects.

    The existing harmonize_bills.py reads ``I_mA``, ``QCharge_mA_h``, etc.
    from the ingester's ``raw_df`` payload and performs its own unit
    conversion. After routing the file through ``read_biologic_csv``
    (which converts mA -> A and mAh -> Ah), we invert those conversions
    and restore the native BioLogic column names so the harmonizer
    continues to work unchanged. The half-cycle accumulators are
    recovered from the signed canonical values by reading the raw CSV
    once more for the columns the canonical frame doesn't preserve
    individually (charge and discharge halves separately).

    Args:
        canonical: Polars DataFrame from read_biologic_csv (canonical schema).
        source_csv: Path to the source CSV; used to recover the raw
            half-cycle accumulator columns that the canonical frame
            combines into signed capacity_Ah / energy_Wh.

    Returns:
        Polars DataFrame with native Bills / BioLogic column names and units.
    """
    n = canonical.height
    # Pull the raw half-cycle accumulators directly (signed capacity /
    # energy in the canonical frame is lossy for harmonizer needs). Polars'
    # default 100-row schema sniff can miss late-appearing scientific
    # notation in the energy columns; force a full-file inference.
    raw = pl.read_csv(source_csv, infer_schema_length=None)

    def _accum(col: str) -> pl.Series:
        if col in raw.columns:
            return raw[col].cast(pl.Float64, strict=False).rename(col)
        return pl.Series(col, np.zeros(n, dtype=float))

    out = pl.DataFrame({
        "time_s": canonical["timestamp_s"].cast(pl.Float64, strict=False),
        "Ecell_V": canonical["voltage_V"].cast(pl.Float64, strict=False),
        # Convert A back to mA for the harmonizer's native-unit pipeline.
        "I_mA": canonical["current_A"].cast(pl.Float64, strict=False) * 1000.0,
        "EnergyCharge_W_h": _accum("EnergyCharge_W_h"),
        "QCharge_mA_h": _accum("QCharge_mA_h"),
        "EnergyDischarge_W_h": _accum("EnergyDischarge_W_h"),
        "QDischarge_mA_h": _accum("QDischarge_mA_h"),
        "Temperature__C": canonical["temperature_C"].cast(pl.Float64, strict=False),
        "cycleNumber": canonical["cycle_number"].cast(pl.Int64, strict=False),
        "Ns": canonical["step_number"].cast(pl.Int64, strict=False),
    })
    return out


def ingest(raw_dir: str) -> dict:
    """Load Bills eVTOL VAH##.csv cycling files via the BioLogic loader.

    Thin wrapper around :func:`read_biologic_csv`: globs for
    ``VAH##.csv`` files (excluding ``_impedance`` variants) and routes
    each through the shared BioLogic parser with ``_BILLS_ALIASES``.

    Args:
        raw_dir: Path to data/raw/bills/ containing VAH##.csv files.

    Returns:
        Dict keyed by cell tag (e.g. "VAH01") with:
            raw_df (DataFrame): rows in BioLogic-native column names and
                units (I_mA, QCharge_mA_h, ...), as the harmonizer expects
            source_cell_id (str): e.g. "VAH01"
            source_file (str): filename
    """
    raw = Path(raw_dir)
    if not raw.exists():
        raise FileNotFoundError(
            f"Bills eVTOL data not found at {raw}. See "
            f"data/raw/bills/SOURCE_DATA_PROVENANCE.md for download "
            f"instructions (CMU KiltHub DOI 10.1184/R1/14226830)."
        )

    datasets = {}
    for csv_file in sorted(raw.glob("VAH*.csv")):
        match = _CELL_FILENAME_RE.match(csv_file.name)
        if not match:
            continue  # skip VAH##_impedance.csv and other non-main files

        cell_tag = f"VAH{match.group('idx')}"

        # Route through the shared BioLogic loader: validates column
        # presence, applies mA->A / mAh->Ah conversion, derives step_type.
        canonical = read_biologic_csv(csv_file, column_aliases=_BILLS_ALIASES)

        # Reconstruct the native-unit raw_df shape harmonize_bills.py
        # currently expects (I_mA, QCharge_mA_h, Temperature__C, ...).
        raw_df = _canonical_to_bills_raw_df(canonical, csv_file)

        datasets[cell_tag] = {
            "raw_df": raw_df,
            "source_cell_id": cell_tag,
            "source_file": csv_file.name,
        }

    if not datasets:
        raise FileNotFoundError(
            f"No Bills VAH##.csv files found in {raw}. Expected names like "
            f"'VAH01.csv', 'VAH17.csv'. "
            f"Found: {[p.name for p in raw.glob('*.csv')]}"
        )

    return datasets
