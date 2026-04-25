"""Generic BioLogic EC-Lab CSV loader.

BioLogic potentiostats (VMP3, MPG2, SP-300, BCS-8xx, etc.) are one of the
three most common cyclers in academic battery research (alongside Arbin
and Maccor). EC-Lab software exports CSV (or binary ``.mpr``) with a
distinctive column convention - slash-delimited unit suffixes, current
in milliamps by default, per-sample cycle and step indices.

This module normalizes a BioLogic CSV into a DataFrame that uses
celljar's canonical column names (see ``common.CANONICAL_COLUMNS``).
Per-source ingesters (Bills eVTOL, future BioLogic-based datasets) call
:func:`read_biologic_csv` instead of re-implementing CSV parsing and
unit conversion.

Sign convention:
    BioLogic convention matches celljar (positive = charge), so
    current is passed through unchanged (only unit-converted).

Coulomb count / energy:
    EC-Lab exports separate ``Q charge`` / ``Q discharge`` and
    ``Energy charge`` / ``Energy discharge`` columns that each
    monotonically accumulate from zero during their respective
    half-cycles. We combine them as a signed running sum:

        coulomb_count_Ah = Q charge - Q discharge     (after mAh -> Ah)
        energy_Wh        = Energy charge - Energy discharge

    matching how celljar's downstream harmonizers consume the column.

Unit quirks:
    BioLogic's default current unit is **milliamps** (``I/mA``) and its
    default capacity unit is **mAh** (``Q charge/mA.h``). Energy is
    already in Wh (``Energy charge/W.h``). We convert mA -> A and
    mAh -> Ah by default; both conversions are configurable via flags
    in case a downstream caller already works in native BioLogic units.

Header aliasing:
    EC-Lab header strings vary across export presets. Common variants:

        ``time/s``         vs  ``time_s``
        ``Ecell/V``        vs  ``Ecell_V``     (also ``<Ewe>/V``)
        ``I/mA``           vs  ``I_mA``
        ``Q charge/mA.h``  vs  ``QCharge_mA_h``
        ``Temperature/C``  vs  ``Temperature__C``  (Bills' double _)
        ``cycle number``   vs  ``cycleNumber``

    We carry a default alias map covering both casings; callers can
    layer overrides via ``column_aliases`` for dataset-specific quirks.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from .common import CANONICAL_COLUMNS, derive_step_type


# Default BioLogic header -> canonical column aliases. Targets prefixed
# with ``_`` are intermediates: raw-unit accumulators that are combined
# (and unit-converted) before the loader returns.
_DEFAULT_ALIASES: dict[str, str] = {
    # Elapsed time (seconds).
    "time/s": "timestamp_s",
    "time_s": "timestamp_s",
    # Cell / working-electrode voltage.
    "Ecell/V": "voltage_V",
    "Ecell_V": "voltage_V",
    "<Ewe>/V": "voltage_V",
    "Ewe/V": "voltage_V",
    "Ewe_V": "voltage_V",
    # Current - BioLogic default unit is milliamps.
    "I/mA": "_current_mA",
    "I_mA": "_current_mA",
    "<I>/mA": "_current_mA",
    # Occasionally an export is already in A; route to canonical directly.
    "I/A": "current_A",
    "I_A": "current_A",
    # Capacity half-cycle accumulators (mAh).
    "Q charge/mA.h": "_charge_cap_mAh",
    "QCharge_mA_h": "_charge_cap_mAh",
    "Q discharge/mA.h": "_discharge_cap_mAh",
    "QDischarge_mA_h": "_discharge_cap_mAh",
    # Energy half-cycle accumulators (Wh - no unit conversion needed).
    "Energy charge/W.h": "_charge_energy_Wh",
    "EnergyCharge_W_h": "_charge_energy_Wh",
    "Energy discharge/W.h": "_discharge_energy_Wh",
    "EnergyDischarge_W_h": "_discharge_energy_Wh",
    # Temperature (deg C). Bills uses a double-underscore variant.
    "Temperature/\u00b0C": "temperature_C",
    "Temperature/C": "temperature_C",
    "Temperature_C": "temperature_C",
    "Temperature__C": "temperature_C",
    # Cycle / step indices.
    "cycle number": "cycle_number",
    "cycleNumber": "cycle_number",
    "cycle_number": "cycle_number",
    "Ns": "step_number",
    "step number": "step_number",
}


def _numeric_or_nan(df: pl.DataFrame, col: str) -> np.ndarray:
    """Return ``df[col]`` coerced to float, or NaN-array if the column is absent."""
    if col not in df.columns:
        return np.full(df.height, np.nan, dtype=float)
    return df[col].cast(pl.Float64, strict=False).to_numpy().astype(float)


def _numeric_or_zero(df: pl.DataFrame, col: str) -> np.ndarray:
    """Return ``df[col]`` coerced to float, or a zeros array if absent.

    Used for half-cycle accumulators where a missing half (e.g. a pure
    charge-only test) should contribute zero to the signed combination.
    """
    if col not in df.columns:
        return np.zeros(df.height, dtype=float)
    return df[col].cast(pl.Float64, strict=False).fill_null(0.0).to_numpy().astype(float)


def read_biologic_csv(
    path: str | Path,
    *,
    column_aliases: dict[str, str] | None = None,
    cycle_offset: int = 0,
    convert_mA_to_A: bool = True,
    convert_mAh_to_Ah: bool = True,
) -> pl.DataFrame:
    """Read a BioLogic EC-Lab CSV and return a celljar-canonical DataFrame.

    BioLogic EC-Lab default column conventions (with or without slashes
    and unit suffixes - both forms are handled by the default alias map):

    - ``time/s`` / ``time_s``        - elapsed time (s)
    - ``Ecell/V`` / ``Ecell_V``      - cell voltage (V)
    - ``<Ewe>/V``                    - working-electrode voltage (alt)
    - ``I/mA`` / ``I_mA``            - current (**milliamps** - BioLogic default)
    - ``Q charge/mA.h`` / ``QCharge_mA_h``       - charge capacity (mAh)
    - ``Q discharge/mA.h`` / ``QDischarge_mA_h`` - discharge capacity (mAh)
    - ``Energy charge/W.h`` / ``EnergyCharge_W_h``       - charge energy (Wh)
    - ``Energy discharge/W.h`` / ``EnergyDischarge_W_h`` - discharge energy (Wh)
    - ``Temperature/C`` / ``Temperature__C``     - temperature (deg C)
    - ``cycle number`` / ``cycleNumber``         - cycle index
    - ``Ns``                                     - BioLogic step number

    Args:
        path: Path to the BioLogic EC-Lab CSV file.
        column_aliases: Extra aliases layered on top of the defaults.
            Provided entries take precedence over ``_DEFAULT_ALIASES``.
            Values should be canonical column names (see
            ``common.CANONICAL_COLUMNS``) or one of the intermediates:
            ``_current_mA``, ``_charge_cap_mAh``, ``_discharge_cap_mAh``,
            ``_charge_energy_Wh``, ``_discharge_energy_Wh``.
        cycle_offset: Integer added to every ``cycle_number`` in the
            output. Use when chaining multiple files for the same cell.
        convert_mA_to_A: If True (default), divide current values read
            as ``_current_mA`` by 1000 to emit ``current_A``. Set False
            if the downstream caller wants native BioLogic units.
        convert_mAh_to_Ah: If True (default), divide half-cycle
            capacity accumulators by 1000 before combining into the
            signed ``coulomb_count_Ah`` column.

    Returns:
        Polars DataFrame with celljar canonical columns:
        ``timestamp_s``, ``voltage_V``, ``current_A``,
        ``coulomb_count_Ah`` (signed running ∫I dt, mAh -> Ah applied),
        ``energy_Wh`` (signed = charge - discharge, already Wh),
        ``temperature_C``, ``step_number``, ``cycle_number``, ``step_type``.
        Columns not present in the source are emitted as null.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"BioLogic CSV not found: {path}")

    # BioLogic exports are commonly UTF-8 but EC-Lab presets occasionally
    # emit latin-1. utf8-lossy tolerates either without raising.
    # infer_schema_length=None scans the whole file so columns like
    # EnergyDischarge_W_h that flip from int-looking to scientific notation
    # late in the file are correctly inferred as Float64.
    df_raw = pl.read_csv(path, encoding="utf8-lossy", infer_schema_length=None)

    # Merge caller aliases on top of defaults (caller wins).
    aliases = dict(_DEFAULT_ALIASES)
    if column_aliases:
        aliases.update(column_aliases)

    # Only rename headers that actually appear in the source.
    active = {src: dst for src, dst in aliases.items() if src in df_raw.columns}
    df = df_raw.rename(active)

    n = df.height
    cols: dict[str, pl.Series] = {}

    # Direct passthroughs - emit NaN if the source didn't provide them.
    cols["timestamp_s"] = pl.Series("timestamp_s", _numeric_or_nan(df, "timestamp_s"))
    cols["voltage_V"] = pl.Series("voltage_V", _numeric_or_nan(df, "voltage_V"))
    cols["temperature_C"] = pl.Series("temperature_C", _numeric_or_nan(df, "temperature_C"))

    # Current: prefer an already-in-A column if present, otherwise derive
    # from the mA column (with optional unit conversion).
    if "current_A" in df.columns:
        current_arr = df["current_A"].cast(pl.Float64, strict=False).to_numpy().astype(float)
        cols["current_A"] = pl.Series("current_A", current_arr)
    elif "_current_mA" in df.columns:
        current_mA = df["_current_mA"].cast(pl.Float64, strict=False).to_numpy().astype(float)
        scaled = current_mA / 1000.0 if convert_mA_to_A else current_mA
        cols["current_A"] = pl.Series("current_A", scaled)
    else:
        cols["current_A"] = pl.Series("current_A", np.full(n, np.nan, dtype=float))

    # Signed coulomb count: charge - discharge. BioLogic default unit is mAh,
    # converted to Ah unless the caller opts out.
    charge_cap = _numeric_or_zero(df, "_charge_cap_mAh")
    discharge_cap = _numeric_or_zero(df, "_discharge_cap_mAh")
    cap_scale = 1.0 / 1000.0 if convert_mAh_to_Ah else 1.0
    if "_charge_cap_mAh" in df.columns or "_discharge_cap_mAh" in df.columns:
        cols["coulomb_count_Ah"] = pl.Series(
            "coulomb_count_Ah", (charge_cap - discharge_cap) * cap_scale)
    else:
        cols["coulomb_count_Ah"] = pl.Series("coulomb_count_Ah", np.full(n, np.nan, dtype=float))

    # Signed energy: already in Wh, no unit conversion.
    charge_energy = _numeric_or_zero(df, "_charge_energy_Wh")
    discharge_energy = _numeric_or_zero(df, "_discharge_energy_Wh")
    if (
        "_charge_energy_Wh" in df.columns
        or "_discharge_energy_Wh" in df.columns
    ):
        cols["energy_Wh"] = pl.Series("energy_Wh", charge_energy - discharge_energy)
    else:
        cols["energy_Wh"] = pl.Series("energy_Wh", np.full(n, np.nan, dtype=float))

    # Integer-y columns: use polars nullable Int64 so nulls survive.
    for col in ("step_number", "cycle_number"):
        if col in df.columns:
            cols[col] = df[col].cast(pl.Int64, strict=False).alias(col)
        else:
            cols[col] = pl.Series(col, [None] * n, dtype=pl.Int64)

    if cycle_offset:
        cols["cycle_number"] = (cols["cycle_number"] + cycle_offset).alias("cycle_number")

    # Classify step direction from (signed) current sign.
    current_arr = cols["current_A"].to_numpy().astype(float)
    if np.isnan(current_arr).all():
        cols["step_type"] = pl.Series("step_type", [None] * n, dtype=pl.Utf8)
    else:
        step_types = derive_step_type(current_arr).astype(object)
        # NaN-current rows can't be classified - mark as rest for a
        # categorical-clean column (matches the Arbin loader's choice).
        nan_mask = np.isnan(current_arr)
        step_types[nan_mask] = "rest"
        cols["step_type"] = pl.Series("step_type", step_types.tolist(), dtype=pl.Utf8)

    # Enforce canonical column order.
    out = pl.DataFrame({c: cols[c] for c in CANONICAL_COLUMNS})
    return out
