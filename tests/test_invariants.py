"""Domain invariant tests for celljar.bundle.validate_invariants.

Cross-field rules pandera doesn't enforce:
    - voltage / current / temperature observed_min <= _max
    - sample_dt min <= median <= max
    - cycle_summary needs at least one aging axis populated
    - calendar_aging cycle_summary rows need elapsed_time_s
"""

from __future__ import annotations

import pytest

from celljar.bundle import validate_invariants


def _ok_test(test_id: str = "T1", **overrides) -> dict:
    base = {
        "test_id": test_id,
        "voltage_observed_min_V": 2.5,
        "voltage_observed_max_V": 4.2,
        "current_observed_min_A": -3.0,
        "current_observed_max_A": 3.0,
        "temperature_observed_min_C": 24.0,
        "temperature_observed_max_C": 26.0,
        "sample_dt_min_s": 0.1,
        "sample_dt_median_s": 1.0,
        "sample_dt_max_s": 60.0,
        "test_type": "hppc",
    }
    base.update(overrides)
    return base


def test_clean_test_metadata_passes():
    validate_invariants([_ok_test()])  # no raise


def test_inverted_voltage_bounds_fails():
    with pytest.raises(ValueError, match="voltage_observed_min"):
        validate_invariants([_ok_test(
            voltage_observed_min_V=4.5, voltage_observed_max_V=2.5,
        )])


def test_inverted_sample_dt_fails():
    with pytest.raises(ValueError, match="sample_dt"):
        validate_invariants([_ok_test(
            sample_dt_min_s=10.0, sample_dt_median_s=1.0, sample_dt_max_s=60.0,
        )])


def test_nullable_observed_bounds_pass():
    """One side null is fine - partial observed bounds shouldn't trip the check."""
    validate_invariants([_ok_test(
        voltage_observed_min_V=None, voltage_observed_max_V=4.2,
    )])


def test_cycle_summary_needs_aging_axis():
    test_md = [_ok_test(test_type="cycle_aging")]
    bad_summary = [{
        "test_id": "T1",
        "cycle_number": None,
        "equivalent_full_cycles": None,
        "elapsed_time_s": None,
    }]
    with pytest.raises(ValueError, match="no aging axis"):
        validate_invariants(test_md, bad_summary)


def test_calendar_aging_requires_elapsed_time():
    test_md = [_ok_test(test_type="calendar_aging")]
    bad_summary = [{
        "test_id": "T1",
        "cycle_number": 1,            # has cycle_number - passes the previous check
        "equivalent_full_cycles": None,
        "elapsed_time_s": None,        # but calendar_aging needs elapsed_time_s
    }]
    with pytest.raises(ValueError, match="calendar_aging"):
        validate_invariants(test_md, bad_summary)


def test_calendar_aging_with_elapsed_time_passes():
    test_md = [_ok_test(test_type="calendar_aging")]
    summary = [{
        "test_id": "T1",
        "cycle_number": None,
        "equivalent_full_cycles": None,
        "elapsed_time_s": 86400.0,
    }]
    validate_invariants(test_md, summary)
