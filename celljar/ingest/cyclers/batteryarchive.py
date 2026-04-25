"""Generic loader for BatteryArchive.org CSV format.

BatteryArchive (https://www.batteryarchive.org/) is a Sandia-led standardized
publication format for battery cycler data. Multiple labs (SNL, UL, HNEI, etc.)
contribute datasets using a common CSV schema, making this the closest-to-
canonical of any cycler format in the wild.

Typical columns (whitespace and units preserved verbatim in the header):

    Date_Time                   absolute timestamp string (not required)
    Test_Time (s)               elapsed test time, s
    Cycle_Index                 1-based cycle number
    Step_Index                  step within cycle (optional)
    Current (A)                 positive = charge (BatteryArchive convention)
    Voltage (V)                 cell voltage, V
    Charge_Capacity (Ah)        monotonic per-cycle charge counter
    Discharge_Capacity (Ah)     monotonic per-cycle discharge counter
    Charge_Energy (Wh)          monotonic per-cycle charge energy
    Discharge_Energy (Wh)       monotonic per-cycle discharge energy
    Cell_Temperature (C)        cell surface temperature
    Environment_Temperature (C) chamber temperature (fallback only)

Sign convention matches celljar (positive = charge), so no current flipping
is required. Charge and discharge counters are combined into a signed running
coulomb count (celljar's per-sample charge-throughput column):

    coulomb_count_Ah = Charge_Capacity - Discharge_Capacity
    energy_Wh        = Charge_Energy  - Discharge_Energy
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from celljar.ingest.cyclers.common import CANONICAL_COLUMNS, derive_step_type


# Standard BatteryArchive header → celljar canonical column map.
# Underscore-prefixed targets are intermediates combined later.
_DEFAULT_ALIASES: dict[str, str] = {
    "Test_Time (s)": "timestamp_s",
    "Voltage (V)": "voltage_V",
    "Current (A)": "current_A",
    "Charge_Capacity (Ah)": "_charge_cap_Ah",
    "Discharge_Capacity (Ah)": "_discharge_cap_Ah",
    "Charge_Energy (Wh)": "_charge_energy_Wh",
    "Discharge_Energy (Wh)": "_discharge_energy_Wh",
    "Cell_Temperature (C)": "temperature_C",
    "Environment_Temperature (C)": "temperature_C",  # fallback if no cell temp
    "Cycle_Index": "cycle_number",
    "Step_Index": "step_number",
}


def read_batteryarchive_csv(
    path: str | Path,
    *,
    column_aliases: dict[str, str] | None = None,
    cycle_offset: int = 0,
) -> pl.DataFrame:
    """Read a BatteryArchive-format CSV into celljar canonical columns.

    Args:
        path: Path to a BatteryArchive CSV file.
        column_aliases: Optional overrides / additions to the default header
            map. Provided entries take precedence over ``_DEFAULT_ALIASES``.
        cycle_offset: Integer added to ``cycle_number`` (useful when stitching
            multiple test segments into a single cell's lifetime).

    Returns:
        Polars DataFrame with celljar canonical columns: ``timestamp_s``,
        ``voltage_V``, ``current_A``, ``temperature_C``, ``coulomb_count_Ah``,
        ``energy_Wh``, ``step_number``, ``cycle_number``, ``step_type``.
        Columns not present in the source are emitted as null.
    """
    df_raw = pl.read_csv(path)

    # Build the effective alias map: defaults first, user overrides win.
    aliases = dict(_DEFAULT_ALIASES)
    if column_aliases:
        aliases.update(column_aliases)

    # Cell_Temperature takes precedence over Environment_Temperature when both
    # are present and both map to temperature_C. Drop the environment alias
    # in that case so rename() doesn't collide.
    if (
        "Cell_Temperature (C)" in df_raw.columns
        and "Environment_Temperature (C)" in df_raw.columns
        and aliases.get("Cell_Temperature (C)") == "temperature_C"
        and aliases.get("Environment_Temperature (C)") == "temperature_C"
    ):
        aliases.pop("Environment_Temperature (C)", None)

    # Only rename headers that are actually present.
    active = {src: dst for src, dst in aliases.items() if src in df_raw.columns}
    df = df_raw.rename(active)

    n = df.height
    cols: dict[str, pl.Series] = {}

    # Direct passthroughs - null if the source didn't provide them.
    for col in ("timestamp_s", "voltage_V", "current_A", "temperature_C",
                "step_number", "cycle_number"):
        if col in df.columns:
            cols[col] = df[col]
        else:
            cols[col] = pl.Series(col, [None] * n, dtype=pl.Float64)

    # Signed coulomb count: Charge_Capacity - Discharge_Capacity (celljar convention).
    has_charge = "_charge_cap_Ah" in df.columns
    has_discharge = "_discharge_cap_Ah" in df.columns
    if has_charge or has_discharge:
        charge_cap = df["_charge_cap_Ah"] if has_charge else pl.Series(
            "_charge_cap_Ah", [0.0] * n, dtype=pl.Float64)
        discharge_cap = df["_discharge_cap_Ah"] if has_discharge else pl.Series(
            "_discharge_cap_Ah", [0.0] * n, dtype=pl.Float64)
        cols["coulomb_count_Ah"] = (charge_cap - discharge_cap).alias("coulomb_count_Ah")
    else:
        cols["coulomb_count_Ah"] = pl.Series("coulomb_count_Ah", [None] * n, dtype=pl.Float64)

    # Signed energy, same convention.
    has_charge_e = "_charge_energy_Wh" in df.columns
    has_discharge_e = "_discharge_energy_Wh" in df.columns
    if has_charge_e or has_discharge_e:
        charge_e = df["_charge_energy_Wh"] if has_charge_e else pl.Series(
            "_charge_energy_Wh", [0.0] * n, dtype=pl.Float64)
        discharge_e = df["_discharge_energy_Wh"] if has_discharge_e else pl.Series(
            "_discharge_energy_Wh", [0.0] * n, dtype=pl.Float64)
        cols["energy_Wh"] = (charge_e - discharge_e).alias("energy_Wh")
    else:
        cols["energy_Wh"] = pl.Series("energy_Wh", [None] * n, dtype=pl.Float64)

    # cycle_offset lets callers stitch segmented tests without re-writing files.
    if cycle_offset and "cycle_number" in df.columns:
        cols["cycle_number"] = (cols["cycle_number"] + cycle_offset).alias("cycle_number")

    # Classify step direction from current sign (celljar's positive=charge).
    current_series = cols["current_A"]
    if current_series.null_count() < n:
        current_arr = current_series.to_numpy()
        cols["step_type"] = pl.Series("step_type", derive_step_type(current_arr))
    else:
        cols["step_type"] = pl.Series("step_type", [None] * n, dtype=pl.Utf8)

    # Enforce canonical column order.
    out = pl.DataFrame({c: cols[c] for c in CANONICAL_COLUMNS})
    return out
