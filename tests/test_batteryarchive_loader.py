"""Unit tests for the generic BatteryArchive CSV loader.

Fabricates a minimal in-memory CSV matching the BatteryArchive header
schema, runs it through ``read_batteryarchive_csv``, and verifies that
the output exposes celljar's canonical columns with the correct signed
coulomb-count / energy convention (charge - discharge, positive = charge).
"""

from __future__ import annotations

import io

import pytest

from celljar.ingest.cyclers.batteryarchive import read_batteryarchive_csv
from celljar.ingest.cyclers.common import CANONICAL_COLUMNS


def _fake_csv() -> io.StringIO:
    """Build a small BatteryArchive-shaped CSV: 2 cycles, charge then discharge."""
    rows = [
        # header
        (
            "Date_Time,Test_Time (s),Cycle_Index,Step_Index,Current (A),"
            "Voltage (V),Charge_Capacity (Ah),Discharge_Capacity (Ah),"
            "Charge_Energy (Wh),Discharge_Energy (Wh),"
            "Cell_Temperature (C),Environment_Temperature (C)"
        ),
        # rest
        "2020-01-01 00:00:00,0.0,1,1,0.000,3.700,0.000,0.000,0.000,0.000,25.0,24.5",
        # charge
        "2020-01-01 00:00:10,10.0,1,2,1.500,3.800,0.010,0.000,0.038,0.000,25.1,24.5",
        "2020-01-01 00:00:20,20.0,1,2,1.500,3.900,0.020,0.000,0.078,0.000,25.2,24.5",
        # discharge
        "2020-01-01 00:00:30,30.0,2,3,-2.000,3.850,0.020,0.010,0.078,0.039,25.3,24.5",
        "2020-01-01 00:00:40,40.0,2,3,-2.000,3.700,0.020,0.020,0.078,0.077,25.4,24.5",
    ]
    return io.StringIO("\n".join(rows) + "\n")


def test_canonical_columns_present():
    df = read_batteryarchive_csv(_fake_csv())
    assert list(df.columns) == CANONICAL_COLUMNS


def test_signed_coulomb_count_convention():
    """Signed coulomb count = Charge_Capacity - Discharge_Capacity; discharge negative."""
    df = read_batteryarchive_csv(_fake_csv())
    # Row 0: both counters zero.
    assert df["coulomb_count_Ah"][0] == pytest.approx(0.0)
    # Row 2: charged 0.020 Ah, not yet discharged.
    assert df["coulomb_count_Ah"][2] == pytest.approx(0.020)
    # Row 4: 0.020 charged, 0.020 discharged -> net 0.
    assert df["coulomb_count_Ah"][4] == pytest.approx(0.0)
    # Last sample is mid-discharge, net should go negative relative to peak.
    assert df["coulomb_count_Ah"][3] == pytest.approx(0.020 - 0.010)


def test_signed_energy_convention():
    df = read_batteryarchive_csv(_fake_csv())
    assert df["energy_Wh"][0] == pytest.approx(0.0)
    assert df["energy_Wh"][2] == pytest.approx(0.078)
    # charge - discharge
    assert df["energy_Wh"][3] == pytest.approx(0.078 - 0.039)


def test_step_type_classification():
    df = read_batteryarchive_csv(_fake_csv())
    assert df["step_type"][0] == "rest"
    assert df["step_type"][1] == "charge"
    assert df["step_type"][2] == "charge"
    assert df["step_type"][3] == "discharge"
    assert df["step_type"][4] == "discharge"


def test_current_sign_unchanged():
    """BatteryArchive is already positive=charge; loader must not flip."""
    df = read_batteryarchive_csv(_fake_csv())
    assert df["current_A"][1] == pytest.approx(1.5)   # charge positive
    assert df["current_A"][3] == pytest.approx(-2.0)  # discharge negative


def test_cell_temperature_takes_precedence():
    """When both Cell_Temperature and Environment_Temperature exist, cell wins."""
    df = read_batteryarchive_csv(_fake_csv())
    # Cell_Temperature values in the fixture are 25.0..25.4
    assert df["temperature_C"][0] == pytest.approx(25.0)
    assert df["temperature_C"][4] == pytest.approx(25.4)


def test_environment_temperature_fallback():
    """If only Environment_Temperature is present, it maps to temperature_C."""
    rows = [
        "Test_Time (s),Cycle_Index,Current (A),Voltage (V),"
        "Charge_Capacity (Ah),Discharge_Capacity (Ah),Environment_Temperature (C)",
        "0.0,1,0.0,3.7,0.0,0.0,22.0",
        "1.0,1,1.0,3.8,0.001,0.0,22.1",
    ]
    df = read_batteryarchive_csv(io.StringIO("\n".join(rows) + "\n"))
    assert df["temperature_C"][0] == pytest.approx(22.0)
    assert df["temperature_C"][1] == pytest.approx(22.1)


def test_cycle_offset():
    """cycle_offset shifts cycle numbers without touching anything else."""
    df = read_batteryarchive_csv(_fake_csv(), cycle_offset=100)
    assert df["cycle_number"][0] == 101
    assert df["cycle_number"][-1] == 102


def test_column_aliases_override():
    """User-supplied aliases win over defaults - can map a custom header."""
    rows = [
        "t_s,cyc,I_A,V_V,Qc_Ah,Qd_Ah",
        "0.0,1,0.0,3.7,0.0,0.0",
        "1.0,1,1.5,3.8,0.002,0.0",
    ]
    aliases = {
        "t_s": "timestamp_s",
        "cyc": "cycle_number",
        "I_A": "current_A",
        "V_V": "voltage_V",
        "Qc_Ah": "_charge_cap_Ah",
        "Qd_Ah": "_discharge_cap_Ah",
    }
    df = read_batteryarchive_csv(io.StringIO("\n".join(rows) + "\n"),
                                 column_aliases=aliases)
    assert list(df.columns) == CANONICAL_COLUMNS
    assert df["voltage_V"][1] == pytest.approx(3.8)
    assert df["coulomb_count_Ah"][1] == pytest.approx(0.002)


def test_missing_optional_columns_become_nan():
    """Minimal CSV (no temperature, no energy) still produces canonical shape."""
    rows = [
        "Test_Time (s),Cycle_Index,Current (A),Voltage (V),"
        "Charge_Capacity (Ah),Discharge_Capacity (Ah)",
        "0.0,1,0.0,3.7,0.0,0.0",
        "1.0,1,1.0,3.8,0.001,0.0",
    ]
    df = read_batteryarchive_csv(io.StringIO("\n".join(rows) + "\n"))
    assert list(df.columns) == CANONICAL_COLUMNS
    assert df["temperature_C"].is_null().all()
    assert df["energy_Wh"].is_null().all()
    assert df["step_number"].is_null().all()
