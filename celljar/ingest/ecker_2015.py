"""Ingester for Ecker 2015 Kokam SLPB75106100 NMC parameterization data.

This is a STUB - implements the canonical ingest interface (`ingest(raw_dir)`
returning a nested dict) so the harmonizer pipeline + tests catch interface
regressions, but the actual file-parsing logic is intentionally minimal and
expects the user to download the data first.

When raw Ecker 2015 CSVs land at `data/raw/ecker_2015/`, expand the
`_parse_*` helpers below to populate the canonical fields.

Data scope: HPPC across temperature, GITT, half-cell OCP, EIS, capacity vs C-rate.
See `data/raw/ecker_2015/SOURCE_DATA_PROVENANCE.md` for citations.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl


def ingest(raw_dir: str) -> dict:
    """Return ingested-data dict keyed by (test_type, temp_c).

    Structure mirrors other celljar ingesters:
        {
            ("hppc", 25): {"raw_df": pl.DataFrame, ...},
            ("gitt", 25): {...},
            ...
        }

    Returns an empty dict if the raw directory has no expected files yet.
    """
    raw_path = Path(raw_dir)
    if not raw_path.exists():
        raise FileNotFoundError(
            f"Ecker 2015 raw data dir not found: {raw_path}. "
            "See data/raw/ecker_2015/SOURCE_DATA_PROVENANCE.md for download instructions."
        )

    out: dict = {}
    # Expected file pattern per the PyBaMM-vendored layout (subject to revision
    # once the actual files are reviewed):
    #   ocv_anode.csv          half-cell OCP, anode
    #   ocv_cathode.csv        half-cell OCP, cathode
    #   pulse_test_*.csv       HPPC at varied temperatures
    #   gitt_*.csv             GITT discharge / charge
    # Empty-graceful: skip what isn't there.

    for hppc_csv in sorted(raw_path.glob("pulse_test*.csv")):
        # Filename → temperature (placeholder; refine when files are present).
        # e.g. "pulse_test_25C.csv" → 25
        try:
            temp_c = _temperature_from_filename(hppc_csv.name)
        except ValueError:
            continue
        df = pl.read_csv(hppc_csv)
        out[("hppc", temp_c)] = {"raw_df": df, "source_file": str(hppc_csv)}

    for gitt_csv in sorted(raw_path.glob("gitt*.csv")):
        try:
            temp_c = _temperature_from_filename(gitt_csv.name)
        except ValueError:
            temp_c = 25  # GITT often runs at room T only
        df = pl.read_csv(gitt_csv)
        out[("gitt", temp_c)] = {"raw_df": df, "source_file": str(gitt_csv)}

    return out


def _temperature_from_filename(name: str) -> int:
    """Extract integer Celsius from filenames like 'pulse_test_25C.csv'."""
    import re
    m = re.search(r"(-?\d+)C", name)
    if not m:
        raise ValueError(f"no temperature token in {name!r}")
    return int(m.group(1))
