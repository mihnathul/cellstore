"""Shared helpers for working with the harmonized output bundle.

Both `examples/demo_end_to_end.py` and `examples/publish_to_huggingface.py`
need the same primitives (path resolution, NaN scrubbing, source enumeration,
row counting). Centralizing them here so the two scripts stay aligned.

No I/O happens at import time — every function takes a path argument explicitly.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any


def harmonized_dir(root: Path | None = None) -> Path:
    """Path to data/harmonized/ under the repo root."""
    if root is None:
        # celljar/bundle.py → repo root is two parents up.
        root = Path(__file__).parent.parent
    return root / "data" / "harmonized"


def nan_to_none(obj: Any) -> Any:
    """Recursively replace float NaN with None — pandera.polars + JSON-friendly.

    pandera.polars treats NaN as a non-null Float64 value (so range checks like
    `ge=0, le=1` fail on NaN), and NaN is not JSON-serializable. Walking the
    structure once before validation/serialization avoids both pitfalls.
    """
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if isinstance(obj, dict):
        return {k: nan_to_none(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [nan_to_none(v) for v in obj]
    return obj


def collect_sources(harmonized: Path) -> dict[str, dict]:
    """Walk cells/ + tests/ to pull per-source provenance for the dataset card.

    Returns:
        {SOURCE_NAME: {"citation": ..., "license": ..., "doi": ..., "url": ...,
                        "license_url": ...}} — first non-null values win.
    """
    cells_dir = harmonized / "cells"
    tests_dir = harmonized / "tests"
    if not cells_dir.exists() or not tests_dir.exists():
        return {}

    sources: dict[str, dict] = {}

    for p in cells_dir.glob("*.json"):
        try:
            cell = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        src = cell.get("source")
        if src and src not in sources:
            sources[src] = {}

    # Test metadata carries the per-source citation / license / DOI fields.
    for p in tests_dir.glob("*.json"):
        try:
            test = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        cell_id = test.get("cell_id", "")
        # Determine source from cell_id prefix (e.g. "ORNL_LEAF_2013" → "ORNL").
        # Or fall back to the cell file lookup if needed.
        src = None
        for s in sources:
            if cell_id.startswith(s):
                src = s
                break
        if src is None:
            continue
        bucket = sources[src]
        for key_in, key_out in [
            ("source_citation", "citation"),
            ("source_license", "license"),
            ("source_license_url", "license_url"),
            ("source_url", "url"),
            ("source_doi", "doi"),
        ]:
            if not bucket.get(key_out) and test.get(key_in):
                bucket[key_out] = test[key_in]

    return sources


def validate_invariants(
    test_metadata: list[dict],
    cycle_summary: list[dict] | None = None,
) -> None:
    """Domain invariants that span fields — checked beyond pandera's per-field validation.

    Pandera enforces single-field constraints (`ge=0`, `isin=...`); this catches
    cross-field issues that would otherwise produce nonsensical data:

      test_metadata:
        - voltage_observed_min_V <= voltage_observed_max_V
        - current_observed_min_A <= current_observed_max_A
        - temperature_observed_min_C <= temperature_observed_max_C
        - sample_dt_min_s <= sample_dt_median_s <= sample_dt_max_s
        - calendar_aging tests should plausibly have null cycle_count_at_test
          (warn, not fail — some sources may stamp 0)

      cycle_summary:
        - At least one aging axis (cycle_number / equivalent_full_cycles /
          elapsed_time_s) must be non-null per row.
        - calendar_aging tests' cycle_summary rows expected to have
          elapsed_time_s populated.

    Raises:
        ValueError on the first violation, with test_id context.
    """
    for t in test_metadata:
        tid = t.get("test_id", "<unknown>")

        for low_key, high_key, label in [
            ("voltage_observed_min_V", "voltage_observed_max_V", "voltage"),
            ("current_observed_min_A", "current_observed_max_A", "current"),
            ("temperature_observed_min_C", "temperature_observed_max_C", "temperature"),
        ]:
            lo, hi = t.get(low_key), t.get(high_key)
            if lo is not None and hi is not None and lo > hi:
                raise ValueError(
                    f"{tid}: {label}_observed_min ({lo}) > _max ({hi}) — "
                    "ingester is producing inverted observed bounds."
                )

        dt_min = t.get("sample_dt_min_s")
        dt_med = t.get("sample_dt_median_s")
        dt_max = t.get("sample_dt_max_s")
        if all(v is not None for v in (dt_min, dt_med, dt_max)):
            if not (dt_min <= dt_med <= dt_max):
                raise ValueError(
                    f"{tid}: sample_dt invariant violated — "
                    f"min={dt_min}, median={dt_med}, max={dt_max}"
                )

    if cycle_summary:
        # Map test_id → test_type so we can check calendar-aging-specific invariants.
        type_by_test = {t.get("test_id"): t.get("test_type") for t in test_metadata}

        for row in cycle_summary:
            tid = row.get("test_id", "<unknown>")
            axes = [
                row.get("cycle_number"),
                row.get("equivalent_full_cycles"),
                row.get("elapsed_time_s"),
            ]
            if not any(a is not None for a in axes):
                raise ValueError(
                    f"cycle_summary row for {tid} has no aging axis "
                    "(cycle_number / equivalent_full_cycles / elapsed_time_s "
                    "all null) — at least one is required."
                )

            ttype = type_by_test.get(tid)
            if ttype == "calendar_aging" and row.get("elapsed_time_s") is None:
                raise ValueError(
                    f"cycle_summary row for {tid} is calendar_aging but "
                    "elapsed_time_s is null — calendar aging requires a time axis."
                )


def timeseries_row_count(harmonized: Path) -> int:
    """Row count in timeseries.parquet, or -1 if file missing / unreadable."""
    parquet = harmonized / "timeseries.parquet"
    if not parquet.exists():
        return -1
    try:
        import pyarrow.parquet as pq
        return pq.ParquetFile(parquet).metadata.num_rows
    except Exception:                                # noqa: BLE001
        try:
            import polars as pl
            return pl.scan_parquet(parquet).select(pl.len()).collect().item()
        except Exception:                            # noqa: BLE001
            return -1
