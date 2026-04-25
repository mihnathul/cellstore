"""Pandera schemas for the canonical celljar entities.

These mirror the JSON Schemas in ../../schemas/ (which are authoritative).
Used at harmonize time to validate DataFrames before they're written out.

Plus a typed contract for harmonizer return values (`HarmonizerOutput`) so
demo_end_to_end / publish_to_huggingface / future consumers don't drift on
the dict shape.
"""

from typing import TYPE_CHECKING, TypedDict

import pandera.polars as pa

if TYPE_CHECKING:
    import polars as pl


class HarmonizerOutput(TypedDict, total=False):
    """Canonical return shape for every `harmonize_<source>.harmonize()`.

    Required keys:
        cell_metadata    - first cells_metadata entry (back-compat single-cell sources)
        cells_metadata   - list of per-cell dicts matching CellMetadataSchema
        test_metadata    - list of per-test dicts matching TestMetadataSchema
        timeseries       - {test_id: pl.DataFrame matching TimeseriesSchema}

    Optional keys:
        cycle_summary    - list of dicts matching CycleSummarySchema (sources
                           that publish per-cycle aggregates: NASA PCoE, MATR,
                           CLO, Naumann)
    """

    cell_metadata: dict
    cells_metadata: list[dict]
    test_metadata: list[dict]
    timeseries: "dict[str, pl.DataFrame]"
    cycle_summary: list[dict]


class CellMetadataSchema(pa.DataFrameModel):
    """One row per physical cell. Describes the cell's specs and origin."""

    cell_id: str = pa.Field(nullable=False, unique=True)
    source: str = pa.Field(
        nullable=False,
        isin=["BILLS", "CLO", "ECKER", "HNEI", "MATR", "MOHTAT", "NAUMANN", "NASA_PCOE", "ORNL", "SNL_PREGER"],
    )
    source_cell_id: str = pa.Field(nullable=True)
    manufacturer: str = pa.Field(nullable=True)
    model_number: str = pa.Field(nullable=True)
    chemistry: str = pa.Field(
        nullable=True,
        isin=["LFP", "NMC", "NCA", "LCO", "LMO", "LTO", "mixed"],
    )
    cathode: str = pa.Field(nullable=True)
    anode: str = pa.Field(nullable=True)
    electrolyte: str = pa.Field(nullable=True)
    form_factor: str = pa.Field(
        nullable=True,
        isin=["cylindrical", "pouch", "prismatic", "coin"],
    )
    nominal_capacity_Ah: float = pa.Field(nullable=True, ge=0)
    nominal_voltage_V: float = pa.Field(nullable=True, ge=0)
    max_voltage_V: float = pa.Field(nullable=True, ge=0)
    min_voltage_V: float = pa.Field(nullable=True, ge=0)

    class Config:
        coerce = True


class TestMetadataSchema(pa.DataFrameModel):
    """One row per test/experiment on a cell.

    A single cell can have multiple tests (cycling, HPPC, GITT, etc.)
    at different temperatures and conditions.
    """
    __test__ = False  # tell pytest this is not a test class (name starts with "Test")

    test_id: str = pa.Field(nullable=False, unique=True)
    cell_id: str = pa.Field(nullable=False)
    test_type: str = pa.Field(
        nullable=False,
        isin=["cycle_aging", "hppc", "calendar_aging",
              "drive_cycle", "capacity_check"],
    )
    temperature_C_min: float = pa.Field(nullable=True)
    temperature_C_max: float = pa.Field(nullable=True)
    soc_range_min: float = pa.Field(nullable=True, ge=0, le=1)
    soc_range_max: float = pa.Field(nullable=True, ge=0, le=1)
    soc_step: float = pa.Field(nullable=True, ge=0, le=1)
    c_rate_charge: float = pa.Field(nullable=True, ge=0)
    c_rate_discharge: float = pa.Field(nullable=True, ge=0)
    protocol_description: str = pa.Field(nullable=True)
    num_cycles: int = pa.Field(nullable=True, ge=0)

    # Aging context: SOH at time of test + how many cycles were applied before it.
    # Null for cycling tests that span a wide SOH range within a single test.
    soh_pct: float = pa.Field(nullable=True, ge=0, le=150)
    soh_method: str = pa.Field(
        nullable=True,
        isin=["capacity_vs_first_checkpoint", "bol_assumption"],
    )
    # Polars has native nullable Int64 - bare `int` annotation + nullable=True suffices.
    cycle_count_at_test: int = pa.Field(nullable=True, ge=0)
    # Year the test was actually run (not the paper publication year). Useful
    # for filtering by data recency and accounting for cell-vintage drift.
    test_year: int = pa.Field(nullable=True, ge=2000, le=2100)

    # Source provenance + license - self-documenting for re-hosting and citation.
    # Attaches to the test (published measurement), not the cell hardware.
    source_doi: str = pa.Field(nullable=True)
    source_url: str = pa.Field(nullable=True)
    source_citation: str = pa.Field(nullable=True)
    source_license: str = pa.Field(nullable=True)          # SPDX id
    source_license_url: str = pa.Field(nullable=True)

    # Observed data summary - computed at harmonize time from the actual
    # measurements. Useful for quick quality / coverage checks without
    # loading the full timeseries.
    n_samples: int = pa.Field(nullable=True, ge=0)
    duration_s: float = pa.Field(nullable=True, ge=0)
    voltage_observed_min_V: float = pa.Field(nullable=True)
    voltage_observed_max_V: float = pa.Field(nullable=True)
    current_observed_min_A: float = pa.Field(nullable=True)
    current_observed_max_A: float = pa.Field(nullable=True)
    temperature_observed_min_C: float = pa.Field(nullable=True)
    temperature_observed_max_C: float = pa.Field(nullable=True)
    sample_dt_min_s: float = pa.Field(nullable=True, ge=0)
    sample_dt_median_s: float = pa.Field(nullable=True, ge=0)
    sample_dt_max_s: float = pa.Field(nullable=True, ge=0)

    class Config:
        coerce = True


class TimeseriesSchema(pa.DataFrameModel):
    """Harmonized timeseries data - every measurement point.

    Note: cycle_number meaning depends on test_type in TestMetadata:
      - cycling:       full charge + discharge
      - gitt:          one pulse + rest
      - hppc:          pulses at one SOC point
      - eis:           one frequency sweep
      - calendar_aging: one checkup measurement
      - drive_cycle:   one profile repeat
    """

    test_id: str = pa.Field(nullable=False)
    cycle_number: int = pa.Field(nullable=False, ge=0)
    # Nullable int - many sources don't expose step numbers explicitly.
    # Polars has native nullable Int64; `int` + nullable=True is enough.
    step_number: int = pa.Field(nullable=True, ge=0)
    step_type: str = pa.Field(
        nullable=True,
        isin=["charge", "discharge", "rest", "pulse", "ocv", "unknown"],
    )
    timestamp_s: float = pa.Field(nullable=False)
    voltage_V: float = pa.Field(nullable=True)
    current_A: float = pa.Field(nullable=True)
    temperature_C: float = pa.Field(nullable=True)
    # Cycler's running signed coulomb count (∫I dt) in Ah. NOT a per-cycle
    # capacity scalar - that lives in cycle_summary.capacity_Ah.
    coulomb_count_Ah: float = pa.Field(nullable=True)
    energy_Wh: float = pa.Field(nullable=True)
    # Mechanical expansion / swelling (micrometers). Populated by sources that
    # ship synchronous displacement data (Mohtat 2021 - Keyence laser); null
    # for cycler-only sources. Nullable throughout so unrelated sources are
    # unaffected.
    displacement_um: float = pa.Field(nullable=True)

    class Config:
        coerce = True


class CycleSummarySchema(pa.DataFrameModel):
    """Harmonized per-cycle / per-checkpoint aggregated aging metrics.

    One row per (test_id, cycle-or-time-index). This entity is the home for
    aging studies that publish summary metrics (capacity vs cycle, R_DC vs
    FEC, etc.) instead of raw V/I/T timeseries - e.g. Naumann 2021 LFP,
    where the public deposits ship .xlsx / .mat with ~35 checkpoints per
    cell rather than megabytes of cycler data.

    Axis convention:
      - cycling sources with integer cycle counts  → cycle_number
      - cycling sources indexed by full-equivalent → equivalent_full_cycles
        cycles (mixed-DoD / load-collective aging)
      - calendar aging                             → elapsed_time_s
    At least one of (cycle_number, equivalent_full_cycles, elapsed_time_s)
    should be non-null in practice; not enforced here because pandera
    cross-column requirements belong in a checks stage.
    """

    test_id: str = pa.Field(nullable=False)
    # Denormalized FK - avoids a join to test_metadata for the common
    # capacity-fade / R-growth plot-by-cell filter.
    cell_id: str = pa.Field(nullable=False)

    # Aging x-axes - all nullable; source picks whichever it reports.
    cycle_number: int = pa.Field(nullable=True, ge=0)
    equivalent_full_cycles: float = pa.Field(nullable=True, ge=0)
    elapsed_time_s: float = pa.Field(nullable=True, ge=0)

    # Capacity metrics.
    capacity_Ah: float = pa.Field(nullable=True, ge=0)
    capacity_retention_pct: float = pa.Field(nullable=True, ge=0, le=150)

    # DC pulse resistance - interpret with pulse_duration_s + soc_pct.
    resistance_dc_ohm: float = pa.Field(nullable=True, ge=0)
    resistance_dc_pulse_duration_s: float = pa.Field(nullable=True, ge=0)
    resistance_dc_soc_pct: float = pa.Field(nullable=True, ge=0, le=100)

    # Other aggregated metrics.
    energy_Wh: float = pa.Field(nullable=True, ge=0)
    coulombic_efficiency: float = pa.Field(nullable=True, ge=0, le=2)
    temperature_C_mean: float = pa.Field(nullable=True)

    class Config:
        coerce = True


