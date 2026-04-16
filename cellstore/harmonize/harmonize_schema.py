"""Pandera schemas for the three canonical cellstore entities.

These mirror the JSON Schemas in ../../schemas/ (which are authoritative).
Used at harmonize time to validate DataFrames before they're written out.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series


class CellMetadataSchema(pa.DataFrameModel):
    """One row per physical cell. Describes the cell's specs and origin."""

    cell_id: Series[str] = pa.Field(nullable=False, unique=True)
    source: Series[str] = pa.Field(
        nullable=False,
        isin=["CALCE", "HNEI", "MATR", "NASA", "ORNL", "SNL"],
    )
    source_cell_id: Series[str] = pa.Field(nullable=True)
    manufacturer: Series[str] = pa.Field(nullable=True)
    model_number: Series[str] = pa.Field(nullable=True)
    chemistry: Series[str] = pa.Field(
        nullable=True,
        isin=["LFP", "NMC", "NCA", "LCO", "LMO", "LTO", "mixed"],
    )
    cathode: Series[str] = pa.Field(nullable=True)
    anode: Series[str] = pa.Field(nullable=True)
    electrolyte: Series[str] = pa.Field(nullable=True)
    form_factor: Series[str] = pa.Field(
        nullable=True,
        isin=["cylindrical", "pouch", "prismatic", "coin"],
    )
    nominal_capacity_Ah: Series[float] = pa.Field(nullable=True, ge=0)
    nominal_voltage_V: Series[float] = pa.Field(nullable=True, ge=0)
    max_voltage_V: Series[float] = pa.Field(nullable=True, ge=0)
    min_voltage_V: Series[float] = pa.Field(nullable=True, ge=0)

    class Config:
        coerce = True


class TestMetadataSchema(pa.DataFrameModel):
    """One row per test/experiment on a cell.

    A single cell can have multiple tests (cycling, HPPC, GITT, etc.)
    at different temperatures and conditions.
    """
    __test__ = False  # tell pytest this is not a test class (name starts with "Test")

    test_id: Series[str] = pa.Field(nullable=False, unique=True)
    cell_id: Series[str] = pa.Field(nullable=False)
    test_type: Series[str] = pa.Field(
        nullable=False,
        isin=["cycling", "hppc", "gitt", "eis", "calendar", "abuse",
              "drive_cycle", "checkup"],
    )
    temperature_C_min: Series[float] = pa.Field(nullable=True)
    temperature_C_max: Series[float] = pa.Field(nullable=True)
    temperature_step_C: Series[float] = pa.Field(nullable=True, ge=0)
    soc_range_min: Series[float] = pa.Field(nullable=True, ge=0, le=1)
    soc_range_max: Series[float] = pa.Field(nullable=True, ge=0, le=1)
    soc_step: Series[float] = pa.Field(nullable=True, ge=0, le=1)
    c_rate_charge: Series[float] = pa.Field(nullable=True, ge=0)
    c_rate_discharge: Series[float] = pa.Field(nullable=True, ge=0)
    protocol_description: Series[str] = pa.Field(nullable=True)
    num_cycles: Series[int] = pa.Field(nullable=True, ge=0)

    # Aging context: SOH at time of test + how many cycles were applied before it.
    # Null for cycling tests that span a wide SOH range within a single test.
    soh_pct: Series[float] = pa.Field(nullable=True, ge=0, le=150)
    soh_method: Series[str] = pa.Field(
        nullable=True,
        isin=["capacity_vs_first_checkpoint", "bol_assumption", "resistance_pulse"],
    )
    cycle_count_at_test: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)

    # Source provenance + license — self-documenting for re-hosting and citation.
    # Attaches to the test (published measurement), not the cell hardware.
    source_doi: Series[str] = pa.Field(nullable=True)
    source_url: Series[str] = pa.Field(nullable=True)
    source_citation: Series[str] = pa.Field(nullable=True)
    source_license: Series[str] = pa.Field(nullable=True)          # SPDX id
    source_license_url: Series[str] = pa.Field(nullable=True)

    # Observed data summary — computed at harmonize time from the actual
    # measurements. Useful for quick quality / coverage checks without
    # loading the full timeseries.
    n_samples: Series[int] = pa.Field(nullable=True, ge=0)
    duration_s: Series[float] = pa.Field(nullable=True, ge=0)
    voltage_observed_min_V: Series[float] = pa.Field(nullable=True)
    voltage_observed_max_V: Series[float] = pa.Field(nullable=True)
    current_observed_min_A: Series[float] = pa.Field(nullable=True)
    current_observed_max_A: Series[float] = pa.Field(nullable=True)
    temperature_observed_min_C: Series[float] = pa.Field(nullable=True)
    temperature_observed_max_C: Series[float] = pa.Field(nullable=True)
    sample_dt_median_s: Series[float] = pa.Field(nullable=True, ge=0)
    sample_dt_max_s: Series[float] = pa.Field(nullable=True, ge=0)

    class Config:
        coerce = True


class TimeseriesSchema(pa.DataFrameModel):
    """Harmonized timeseries data — every measurement point.

    Note: cycle_number meaning depends on test_type in TestMetadata:
      - cycling:       full charge + discharge
      - gitt:          one pulse + rest
      - hppc:          pulses at one SOC point
      - eis:           one frequency sweep
      - calendar_aging: one checkup measurement
      - drive_cycle:   one profile repeat
    """

    test_id: Series[str] = pa.Field(nullable=False)
    cycle_number: Series[int] = pa.Field(nullable=False, ge=0)
    # Nullable int — many sources don't expose step numbers explicitly.
    # pd.Int64Dtype (capital I) preserves NA; plain int coerces NA to NaN float.
    step_number: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    step_type: Series[str] = pa.Field(
        nullable=True,
        isin=["charge", "discharge", "rest", "pulse", "ocv", "unknown"],
    )
    timestamp_s: Series[float] = pa.Field(nullable=False)
    voltage_V: Series[float] = pa.Field(nullable=True)
    current_A: Series[float] = pa.Field(nullable=True)
    temperature_C: Series[float] = pa.Field(nullable=True)
    capacity_Ah: Series[float] = pa.Field(nullable=True)
    energy_Wh: Series[float] = pa.Field(nullable=True)

    class Config:
        coerce = True


