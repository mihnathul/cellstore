"""Synthetic tests for the generic BioLogic EC-Lab CSV loader.

Builds a fake BioLogic CSV on disk (native mA / mAh units), runs it
through :func:`read_biologic_csv`, and verifies:
  - canonical column presence and order
  - mA -> A current conversion
  - mAh -> Ah coulomb-count conversion
  - signed coulomb count = charge - discharge (celljar convention)
  - signed energy  = charge - discharge (already Wh, no unit change)
  - step_type classification from current sign
  - header-alias variants (slash-delimited vs underscore-delimited)
"""

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from celljar.ingest.cyclers.biologic import read_biologic_csv
from celljar.ingest.cyclers.common import CANONICAL_COLUMNS


def _write_underscore_biologic_csv(path: Path, n_per_phase: int = 5) -> None:
    """Write a fake BioLogic CSV using Bills-style underscore headers."""
    # Charge phase: +1500 mA (~1.5 A), capacity climbs 0..7500 mAh (=7.5 Ah).
    t_chg = np.arange(n_per_phase, dtype=float)
    v_chg = np.linspace(3.0, 4.2, n_per_phase)
    i_mA_chg = np.full(n_per_phase, 1500.0)
    q_chg_mAh = np.linspace(0.0, 7500.0, n_per_phase)
    q_dis_mAh_chg = np.zeros(n_per_phase)
    e_chg_Wh = np.linspace(0.0, 30.0, n_per_phase)
    e_dis_Wh_chg = np.zeros(n_per_phase)

    # Discharge phase: -1500 mA, discharge capacity climbs 0..7500 mAh.
    t_dis = np.arange(n_per_phase, 2 * n_per_phase, dtype=float)
    v_dis = np.linspace(4.2, 3.0, n_per_phase)
    i_mA_dis = np.full(n_per_phase, -1500.0)
    q_chg_mAh_dis = np.full(n_per_phase, 7500.0)       # frozen from charge
    q_dis_mAh = np.linspace(0.0, 7500.0, n_per_phase)
    e_chg_Wh_dis = np.full(n_per_phase, 30.0)
    e_dis_Wh = np.linspace(0.0, 28.0, n_per_phase)

    # Rest phase: zero current, capacities frozen at end-of-discharge values.
    t_rest = np.arange(2 * n_per_phase, 3 * n_per_phase, dtype=float)
    v_rest = np.full(n_per_phase, 3.0)
    i_mA_rest = np.zeros(n_per_phase)
    q_chg_mAh_rest = np.full(n_per_phase, 7500.0)
    q_dis_mAh_rest = np.full(n_per_phase, 7500.0)
    e_chg_Wh_rest = np.full(n_per_phase, 30.0)
    e_dis_Wh_rest = np.full(n_per_phase, 28.0)

    df = pl.DataFrame({
        "time_s": np.concatenate([t_chg, t_dis, t_rest]),
        "Ecell_V": np.concatenate([v_chg, v_dis, v_rest]),
        "I_mA": np.concatenate([i_mA_chg, i_mA_dis, i_mA_rest]),
        "EnergyCharge_W_h": np.concatenate([e_chg_Wh, e_chg_Wh_dis, e_chg_Wh_rest]),
        "QCharge_mA_h": np.concatenate([q_chg_mAh, q_chg_mAh_dis, q_chg_mAh_rest]),
        "EnergyDischarge_W_h": np.concatenate([e_dis_Wh_chg, e_dis_Wh, e_dis_Wh_rest]),
        "QDischarge_mA_h": np.concatenate([q_dis_mAh_chg, q_dis_mAh, q_dis_mAh_rest]),
        "Temperature__C": np.full(3 * n_per_phase, 25.0),
        "cycleNumber": np.ones(3 * n_per_phase, dtype=int),
        "Ns": np.concatenate([
            np.full(n_per_phase, 0, dtype=int),
            np.full(n_per_phase, 1, dtype=int),
            np.full(n_per_phase, 2, dtype=int),
        ]),
    })
    df.write_csv(path)


def _write_slash_biologic_csv(path: Path, n: int = 4) -> None:
    """Write a fake BioLogic CSV using the EC-Lab default slash headers."""
    df = pl.DataFrame({
        "time/s": np.arange(n, dtype=float),
        "Ecell/V": np.linspace(3.2, 3.6, n),
        "I/mA": np.full(n, 500.0),
        "Q charge/mA.h": np.linspace(0.0, 2000.0, n),
        "Q discharge/mA.h": np.zeros(n),
        "Energy charge/W.h": np.linspace(0.0, 7.0, n),
        "Energy discharge/W.h": np.zeros(n),
        "cycle number": np.ones(n, dtype=int),
        "Ns": np.zeros(n, dtype=int),
    })
    df.write_csv(path)


def test_biologic_canonical_columns(tmp_path: Path) -> None:
    """Loader emits exactly the celljar canonical columns, in order."""
    csv_path = tmp_path / "underscore.csv"
    _write_underscore_biologic_csv(csv_path)

    df = read_biologic_csv(csv_path)

    assert list(df.columns) == CANONICAL_COLUMNS


def test_biologic_mA_to_A_conversion(tmp_path: Path) -> None:
    """I_mA (milliamps) is converted to current_A (amps) by default."""
    csv_path = tmp_path / "underscore.csv"
    _write_underscore_biologic_csv(csv_path, n_per_phase=5)

    df = read_biologic_csv(csv_path)

    # Charge phase samples: +1500 mA -> +1.5 A.
    assert np.allclose(df["current_A"][:5].to_numpy(), 1.5)
    # Discharge phase samples: -1500 mA -> -1.5 A.
    assert np.allclose(df["current_A"][5:10].to_numpy(), -1.5)
    # Rest phase samples: 0 mA -> 0 A.
    assert np.allclose(df["current_A"][10:15].to_numpy(), 0.0)


def test_biologic_mA_conversion_can_be_disabled(tmp_path: Path) -> None:
    """Setting convert_mA_to_A=False leaves current in native mA."""
    csv_path = tmp_path / "underscore.csv"
    _write_underscore_biologic_csv(csv_path, n_per_phase=3)

    df = read_biologic_csv(csv_path, convert_mA_to_A=False)

    assert np.allclose(df["current_A"][:3].to_numpy(), 1500.0)


def test_biologic_mAh_to_Ah_and_signed_coulomb_count(tmp_path: Path) -> None:
    """Signed coulomb count = (Q charge - Q discharge) with mAh -> Ah conversion."""
    csv_path = tmp_path / "underscore.csv"
    _write_underscore_biologic_csv(csv_path, n_per_phase=5)

    df = read_biologic_csv(csv_path)

    # End of charge phase: 7500 mAh / 1000 = 7.5 Ah; no discharge yet.
    assert df["coulomb_count_Ah"][4] == pytest.approx(7.5)
    # End of discharge phase: 7500 - 7500 = 0 Ah.
    assert df["coulomb_count_Ah"][9] == pytest.approx(0.0)
    # Midpoint of discharge: 7500 - 3750 = 3750 mAh -> 3.75 Ah.
    assert df["coulomb_count_Ah"][7] == pytest.approx(3.75)


def test_biologic_mAh_conversion_can_be_disabled(tmp_path: Path) -> None:
    """convert_mAh_to_Ah=False leaves coulomb count in native mAh."""
    csv_path = tmp_path / "underscore.csv"
    _write_underscore_biologic_csv(csv_path, n_per_phase=5)

    df = read_biologic_csv(csv_path, convert_mAh_to_Ah=False)

    # End of charge: 7500 mAh (un-converted).
    assert df["coulomb_count_Ah"][4] == pytest.approx(7500.0)


def test_biologic_signed_energy_no_unit_conversion(tmp_path: Path) -> None:
    """Signed energy = (Energy charge - Energy discharge), already in Wh."""
    csv_path = tmp_path / "underscore.csv"
    _write_underscore_biologic_csv(csv_path, n_per_phase=5)

    df = read_biologic_csv(csv_path)

    # End of charge phase: 30 Wh charged, 0 Wh discharged.
    assert df["energy_Wh"][4] == pytest.approx(30.0)
    # End of discharge phase: 30 - 28 = 2 Wh net positive (charge > discharge).
    assert df["energy_Wh"][9] == pytest.approx(2.0)


def test_biologic_step_type_from_current(tmp_path: Path) -> None:
    """step_type is derived from signed current sign per celljar convention."""
    csv_path = tmp_path / "underscore.csv"
    _write_underscore_biologic_csv(csv_path, n_per_phase=5)

    df = read_biologic_csv(csv_path)

    assert (df["step_type"][:5] == "charge").all()
    assert (df["step_type"][5:10] == "discharge").all()
    assert (df["step_type"][10:15] == "rest").all()


def test_biologic_slash_header_aliases(tmp_path: Path) -> None:
    """EC-Lab default slash-delimited headers (time/s, I/mA, ...) are recognized."""
    csv_path = tmp_path / "slash.csv"
    _write_slash_biologic_csv(csv_path, n=4)

    df = read_biologic_csv(csv_path)

    assert list(df.columns) == CANONICAL_COLUMNS
    assert np.allclose(df["current_A"].to_numpy(), 0.5)        # 500 mA -> 0.5 A
    assert df["coulomb_count_Ah"][-1] == pytest.approx(2.0)    # 2000 mAh -> 2.0 Ah
    assert df["energy_Wh"][-1] == pytest.approx(7.0)      # already Wh


def test_biologic_cycle_offset(tmp_path: Path) -> None:
    """cycle_offset is added to every cycle_number."""
    csv_path = tmp_path / "underscore.csv"
    _write_underscore_biologic_csv(csv_path, n_per_phase=3)

    df = read_biologic_csv(csv_path, cycle_offset=10)

    # Source file has cycleNumber == 1 for every row.
    assert (df["cycle_number"] == 11).all()


def test_biologic_column_aliases_override(tmp_path: Path) -> None:
    """Caller-supplied column_aliases are layered on top of defaults."""
    csv_path = tmp_path / "custom.csv"
    df_src = pl.DataFrame({
        "t": np.arange(3, dtype=float),
        "Ecell_V": np.linspace(3.0, 3.5, 3),
        "I_mA": np.full(3, 1000.0),
        "QCharge_mA_h": np.linspace(0.0, 3000.0, 3),
        "QDischarge_mA_h": np.zeros(3),
        "cycleNumber": np.ones(3, dtype=int),
        "Ns": np.zeros(3, dtype=int),
    })
    df_src.write_csv(csv_path)

    df = read_biologic_csv(csv_path, column_aliases={"t": "timestamp_s"})

    assert np.allclose(df["timestamp_s"].to_numpy(), [0.0, 1.0, 2.0])
    assert np.allclose(df["current_A"].to_numpy(), 1.0)
    assert df["coulomb_count_Ah"][-1] == pytest.approx(3.0)


def test_biologic_missing_file_raises(tmp_path: Path) -> None:
    """Loader raises FileNotFoundError for a path that doesn't exist."""
    with pytest.raises(FileNotFoundError):
        read_biologic_csv(tmp_path / "does_not_exist.csv")
