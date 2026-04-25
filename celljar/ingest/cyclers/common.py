"""Shared utilities for generic cycler-format loaders.

Defines celljar's canonical timeseries columns and the tiny helpers
(step_type classifier, current-sign convention) that every cycler loader
reuses. Kept dependency-light: numpy only.
"""

from __future__ import annotations

import numpy as np


# Canonical timeseries columns produced by cycler loaders. Per-source
# harmonizers add test_id, cell_id, and any extras on top; these are the
# physics-level columns every loader must emit (NaN where unavailable).
CANONICAL_COLUMNS = [
    "timestamp_s",
    "voltage_V",
    "current_A",
    "temperature_C",
    "coulomb_count_Ah",
    "energy_Wh",
    "step_number",
    "cycle_number",
    "step_type",
]


def derive_step_type(current_A: np.ndarray, threshold_A: float = 0.01) -> np.ndarray:
    """Classify each sample as 'charge' / 'discharge' / 'rest' from current sign.

    celljar convention: positive current = charge. Samples within
    ``threshold_A`` of zero are rest (deadband to suppress ADC noise).

    Args:
        current_A: Per-sample current in amps (celljar convention).
        threshold_A: Rest deadband in amps. Defaults to 10 mA.

    Returns:
        Object-dtype array of the same shape with values in
        ``{"charge", "discharge", "rest"}``.
    """
    current_A = np.asarray(current_A, dtype=float)
    step = np.empty(current_A.shape, dtype=object)
    step[:] = "rest"
    step = np.where(current_A > threshold_A, "charge", step)
    step = np.where(current_A < -threshold_A, "discharge", step)
    return step


def apply_sign_convention(current_A: np.ndarray, source_convention: str) -> np.ndarray:
    """Convert current to celljar's convention (positive = charge into the cell).

    Args:
        current_A: Raw current array from the source file.
        source_convention: One of:
            - "positive_charge" — already matches celljar (Arbin default); no-op.
            - "negative_charge" — source uses negative = charge (some Maccor
              configurations); flip sign.

    Returns:
        Current array in celljar convention (positive = charge, negative = discharge).

    Raises:
        ValueError: If ``source_convention`` is not recognized.
    """
    current_A = np.asarray(current_A, dtype=float)
    if source_convention == "positive_charge":
        return current_A
    if source_convention == "negative_charge":
        return -current_A
    raise ValueError(
        f"Unknown source_convention {source_convention!r}; "
        f"expected 'positive_charge' or 'negative_charge'."
    )
