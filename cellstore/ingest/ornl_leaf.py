"""Ingester for ORNL 2013 Nissan Leaf HPPC data.

Data source: github.com/batterysim/equiv-circ-model
Tested by: Oak Ridge National Laboratory (ORNL)
Cell specs from: National Renewable Energy Laboratory (NREL)
Cell: 2013 Nissan Leaf, NMC, 33.1 Ah rated
"""

import pandas as pd
from pathlib import Path


def ingest(raw_dir: str) -> dict:
    """Load raw HPPC CSV files from the raw directory.

    Args:
        raw_dir: Path to data/raw/ornl_leaf/

    Returns:
        Dict with temperature as key, DataFrame as value
    """
    raw = Path(raw_dir)
    datasets = {}

    for csv_file in sorted(raw.glob("*.csv")):
        name = csv_file.stem
        # Only process HPPC files — skip discharge/cycling files
        if "hppc" not in name:
            continue
        # Extract temperature from filename: cell-low-current-hppc-25c.csv → 25
        temp_str = name.split("hppc-")[1].replace("c", "").replace("-", "")
        temp_c = int(temp_str)

        # Some files (10°C) have no header row — detect and add manually
        EXPECTED_COLUMNS = [
            "Exclude", "Time(s)", "Cycle", "Loop", "Loop.1", "Loop.2",
            "Step", "StepTime(s)", "Current(A)", "Voltage(V)", "Power(W)",
            "Capacity(Ah)", "Energy(Wh)", "Mode", "Data", "Extra",
        ]

        df = pd.read_csv(csv_file)
        if "Time(s)" not in df.columns:
            # No header — re-read with manual column names
            df = pd.read_csv(csv_file, header=None, names=EXPECTED_COLUMNS)

        df.columns = df.columns.str.strip()

        datasets[temp_c] = {
            "raw_df": df,
            "temperature_C": temp_c,
            "source_file": csv_file.name,
        }

    return datasets
