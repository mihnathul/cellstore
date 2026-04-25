"""Ingester for HNEI / Kollmeyer Panasonic NCR18650PF dataset.

Data source: Kollmeyer, P. Panasonic 18650PF Li-ion Battery Data, Mendeley
Data, V1, 2018. doi:10.17632/wykht8y7tg.1
Mirror: https://data.mendeley.com/datasets/wykht8y7tg

Cell: Panasonic NCR18650PF -- NCA, 2.9 Ah nominal, cylindrical 18650.

File format: MATLAB .mat files. Each file holds a single struct named
``meas`` with these fields (all length-N column vectors):

    Time              elapsed time, seconds
    Voltage           cell voltage, V
    Current           current, A (positive = charge)
    Ah                signed cumulative capacity, Ah
    Wh                signed cumulative energy, Wh
    Battery_Temp_degC cell surface temperature, deg C
    Chamber_Temp_degC ambient chamber temperature, deg C
    Power             V * I, W
    TimeStamp         absolute datetime objects

Filenames encode test type and temperature, e.g.:

    03-11-17_08.47 25degC_5Pulse_HPPC_Pan18650PF.mat
    06-15-17_11.31 n20degC_5Pulse_HPPC_Pan18650PF.mat   (n = negative)
    03-21-17_00.29 25degC_UDDS_Pan18650PF.mat
    03-20-17_01.43 25degC_US06_Pan18650PF.mat
    03-21-17_09.38 25degC_LA92_Pan18650PF.mat
    03-21-17_16.27 25degC_NN_Pan18650PF.mat
    03-18-17_02.17 25degC_Cycle_1_Pan18650PF.mat

Some files use a numeric job-number prefix instead of temperature:

    03-11-17_10.10 3390_dis5_10p.mat
    05-20-17_10.44 3619_DisPulse.mat
    03-09-17_17.59 3349_Dis1C_1.mat

Temperature for these is inferred from the chronological test sequence
documented in the dataset readme (see ``_NUMBERED_JOB_TEMPS``).

v0.3 scope: HPPC (incl dis5_10p inter-pulse discharges, DisPulse
verification pulses), drive cycles (UDDS, US06, LA92, NN, HWFET,
Cycle_1..4), and capacity checks (C/20 OCV, 1C reference discharges).
Conditioning files (Charge, Pause, PreChg) and test station markers (TS)
are logged and skipped.
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl
from scipy.io import loadmat


# ---------------------------------------------------------------------------
# Skip patterns -- matched BEFORE test-type patterns.  These files are
# conditioning / station markers / combined duplicates.  We log them but
# do not ingest.
# ---------------------------------------------------------------------------
_SKIP_PATTERNS: list[re.Pattern] = [
    # Charge profiles: "3349_Charge1", "3390_Charge_2", "3423_Charge2a", "3349_ChargeRp"
    re.compile(r"\d+_Charge", re.IGNORECASE),
    # Rest / pause periods: "3349_Pause_1", "3406_Pause1", "3349_Pause_Rp"
    re.compile(r"\d+_Pause", re.IGNORECASE),
    # Pre-charge conditioning: "3619_PreChg"
    re.compile(r"\d+_PreChg", re.IGNORECASE),
    # Test station markers: "3349_TS002973"
    re.compile(r"\d+_TS\d+", re.IGNORECASE),
    # Combined / contiguous files -- data is duplicated in the split files
    re.compile(r"degC(?:_trise)?_US06_HWFE?T_UDDS_LA92", re.IGNORECASE),
    re.compile(r"degC(?:_trise)?_HWFE?T_UDDS_LA92", re.IGNORECASE),
    re.compile(r"degC_LA92_NN_Pan", re.IGNORECASE),
    re.compile(r"Cycle_1to4_w_pauses", re.IGNORECASE),
]


# ---------------------------------------------------------------------------
# Numbered job -> temperature mapping.  These job numbers appear on files
# that don't carry temperature in their descriptor.  Inferred from
# chronological ordering against the dataset readme.
# ---------------------------------------------------------------------------
_NUMBERED_JOB_TEMPS: dict[str, int] = {
    # Initial 1C capacity references (readme step 1-2), 25C
    "3349": 25,
    # dis5_10p immediately after 25C HPPC
    "3390": 25,
    # Drive cycle charges/pauses at 25C
    "3406": 25,
    "3415": 25,
    "3416": 25,
    # dis5_10p and charges around 10C HPPC
    "3423": 10,
    # dis5_10p around C20 OCV test sequence, 25C
    "3541": 25,
    # DisPulse + dis5_10p with 0C HPPC
    "3619": 0,
    # dis5_10p at 0C (second pass)
    "3623": 0,
    # Drive cycle charges/pauses at 0C
    "3659": 0,
    "3686": 0,
    # dis5_10p with -10C HPPC
    "3740": -10,
    # Drive cycle charges/pauses at -10C
    "3787": -10,
    # dis5_10p after -20C HPPC
    "3789": -20,
    # Drive cycle charges/pauses at -20C
    "3913": -20,
    # Trise tests starting at -20C
    "3928": -20,
    # Trise tests starting at 10C
    "3958": 10,
    # End-of-test 1C capacity references (readme step 11), 25C
    "4020": 25,
}

_JOB_NUMBER_RE = re.compile(r"(?:^|\s)(?P<job>\d{4})_")


# ---------------------------------------------------------------------------
# Test-type pattern registry.  First match wins; order matters.
_TEST_PATTERNS: list[tuple[str, re.Pattern, str]] = [
    # HPPC: "5Pulse_HPPC" or "5pulse_HPPC" etc. Must come first.
    (
        "HPPC",
        re.compile(r"(?P<sign>n)?(?P<temp>\d+)degC(?:_trise)?_\d*[Pp]ulse_HPPC", re.IGNORECASE),
        "hppc",
    ),
    # Individual drive cycles. Require boundary after the profile token so that
    # "UDDS" doesn't swallow the combined "US06_HWFET_UDDS_LA92_NN" file.
    (
        "UDDS",
        re.compile(r"(?P<sign>n)?(?P<temp>\d+)degC(?:_trise)?_UDDS_Pan", re.IGNORECASE),
        "drive_cycle",
    ),
    (
        "US06",
        re.compile(r"(?P<sign>n)?(?P<temp>\d+)degC(?:_trise)?_US06_Pan", re.IGNORECASE),
        "drive_cycle",
    ),
    (
        "LA92",
        re.compile(r"(?P<sign>n)?(?P<temp>\d+)degC(?:_trise)?_LA92_Pan", re.IGNORECASE),
        "drive_cycle",
    ),
    (
        "NN",
        re.compile(r"(?P<sign>n)?(?P<temp>\d+)degC(?:_trise)?_NN_Pan", re.IGNORECASE),
        "drive_cycle",
    ),
    (
        "HWFET",
        re.compile(r"(?P<sign>n)?(?P<temp>\d+)degC(?:_trise)?_HWFE?T[ab]?_Pan", re.IGNORECASE),
        "drive_cycle",
    ),
    # Cycling / aging: "Cycle_1_Pan..." through "Cycle_4_Pan...".
    # Also accept "Degradation" / "Aging" variants if they ever show up.
    (
        "Cycle",
        re.compile(
            r"(?P<sign>n)?(?P<temp>\d+)degC(?:_trise)?_(?:Cycle_(?P<idx>\d+)|Degradation|Aging)_Pan",
            re.IGNORECASE,
        ),
        "drive_cycle",
    ),
    # C/20 OCV test: "C20 OCV Test_C20_25dC.mat"
    (
        "OCV_C20",
        re.compile(r"C20 OCV Test_C20_(?P<temp>\d+)dC", re.IGNORECASE),
        "capacity_check",
    ),
    # 1C discharge capacity check: "3349_Dis1C_1.mat", "3349_Dis1C_Rp.mat"
    (
        "Dis1C",
        re.compile(r"(?P<job>\d{4})_Dis1C_(?P<idx>\d+|Rp)", re.IGNORECASE),
        "capacity_check",
    ),
    # Discharge segments between HPPC pulses: "3390_dis5_10p.mat"
    (
        "Dis5_10p",
        re.compile(r"(?P<job>\d{4})_dis5_10p", re.IGNORECASE),
        "hppc",
    ),
    # Pre-HPPC verification discharge pulse: "3619_DisPulse.mat"
    (
        "DisPulse",
        re.compile(r"(?P<job>\d{4})_DisPulse", re.IGNORECASE),
        "hppc",
    ),
]


def _parse_temp(match: re.Match, filename: str) -> int:
    """Extract temperature from a regex match.

    Named tests have ``(?P<temp>)`` in the filename (e.g. ``25degC``).
    Numbered tests look up their ``(?P<job>)`` in ``_NUMBERED_JOB_TEMPS``.
    Falls back to 25 C if neither is available.
    """
    groups = match.groupdict()

    # Explicit temperature group (named tests)
    if groups.get("temp") is not None:
        t = int(groups["temp"])
        sign = groups.get("sign")
        return -t if sign == "n" else t

    # Job-number lookup (numbered tests)
    if groups.get("job") is not None:
        job = groups["job"]
        if job in _NUMBERED_JOB_TEMPS:
            return _NUMBERED_JOB_TEMPS[job]

    # Fallback: extract job number from filename
    jm = _JOB_NUMBER_RE.search(filename)
    if jm and jm.group("job") in _NUMBERED_JOB_TEMPS:
        return _NUMBERED_JOB_TEMPS[jm.group("job")]

    return 25


def _should_skip(name: str) -> bool:
    """Return True if this file matches a skip pattern."""
    for pat in _SKIP_PATTERNS:
        if pat.search(name):
            return True
    return False


def _match_filename(name: str):
    """Return (profile, match, celljar_test_type) for the first test-type
    pattern that hits *name*, or None if none match.
    """
    for profile, regex, test_type in _TEST_PATTERNS:
        m = regex.search(name)
        if m:
            return profile, m, test_type
    return None


def _load_meas_df(mat_path: Path) -> pl.DataFrame:
    """Load a Kollmeyer .mat and return a canonical meas DataFrame."""
    m = loadmat(str(mat_path), squeeze_me=False)
    meas = m["meas"]
    return pl.DataFrame({
        "Time": meas["Time"][0, 0].ravel(),
        "Voltage": meas["Voltage"][0, 0].ravel(),
        "Current": meas["Current"][0, 0].ravel(),
        "Ah": meas["Ah"][0, 0].ravel(),
        "Wh": meas["Wh"][0, 0].ravel(),
        "Battery_Temp_degC": meas["Battery_Temp_degC"][0, 0].ravel(),
        "Chamber_Temp_degC": meas["Chamber_Temp_degC"][0, 0].ravel(),
    })


def ingest(raw_dir: str) -> dict:
    """Load HNEI Panasonic 18650PF .mat files.

    Ingests HPPC (including dis5_10p inter-pulse discharges and DisPulse
    verification pulses), drive cycles (UDDS, US06, LA92, NN, HWFET,
    Cycle_1..4), and capacity checks (C/20 OCV, 1C reference discharges).

    Conditioning files (Charge, Pause, PreChg) and test station markers
    (TS) are logged and skipped.  Combined/contiguous files that duplicate
    data from split files are also skipped.

    Args:
        raw_dir: Path to data/raw/hnei/ containing Kollmeyer .mat files.

    Returns:
        Dict keyed by ``(test_type, profile, temperature_C)`` tuple. If a
        profile appears multiple times at the same temperature (e.g.
        Cycle_1..Cycle_4, HWFTa vs HWFTb, Dis1C_1 vs Dis1C_Rp), the key
        is extended with a 4th element:
        ``(test_type, profile, temperature_C, idx)``.

        Each value is a dict with:
            raw_df (DataFrame): parsed meas columns
            temperature_C (int): nominal test temperature
            profile (str): test profile name
            celljar_test_type (str): schema enum value
            source_file (str): filename
            cycle_index (int | str | None): for indexed files
    """
    raw = Path(raw_dir)
    if not raw.exists():
        raise FileNotFoundError(
            f"HNEI data not found at {raw}. See data/raw/hnei/SOURCE_DATA_PROVENANCE.md "
            f"for download instructions (Mendeley dataset: wykht8y7tg)."
        )

    datasets: dict = {}
    seen_counts: dict = {}
    seen_sizes: dict = {}

    skipped_conditioning: list[str] = []
    skipped_unrecognized: list[str] = []

    for mat_file in sorted(raw.glob("*.mat")):
        name = mat_file.name

        # Skip conditioning / station markers / combined files first
        if _should_skip(name):
            skipped_conditioning.append(name)
            continue

        hit = _match_filename(name)
        if hit is None:
            skipped_unrecognized.append(name)
            continue

        profile, match, test_type = hit
        temp_c = _parse_temp(match, name)

        # Extract cycle/repeat index when present
        cycle_idx = None
        groups = match.groupdict()
        if groups.get("idx") is not None:
            idx_str = match.group("idx")
            try:
                cycle_idx = int(idx_str)
            except ValueError:
                cycle_idx = idx_str  # e.g. "Rp" for repeat

        base_key = (test_type, profile, temp_c)
        file_size = mat_file.stat().st_size
        # If we've already ingested a file of the same size for this exact
        # (test_type, profile, temp), treat it as a duplicate and skip.
        # Different-size files at the same key are legitimate repeats
        # (e.g. Cycle_1..4) and get disambiguated below.
        if file_size in seen_sizes.get(base_key, set()) and cycle_idx is None:
            continue
        seen_sizes.setdefault(base_key, set()).add(file_size)

        try:
            df = _load_meas_df(mat_file)
        except Exception:  # pragma: no cover - tolerate unreadable files
            # Don't crash the whole ingest on a single bad file.
            continue

        seen_counts[base_key] = seen_counts.get(base_key, 0) + 1
        occurrence = seen_counts[base_key]

        # HPPC + single-occurrence drive cycles use the 3-tuple key (backward
        # compat for HPPC consumers). Repeated profiles (Cycle_*, HWFTa/b,
        # duplicate drive cycles) get a 4-tuple key with an index.
        if occurrence == 1 and cycle_idx is None:
            key = base_key
        else:
            # Prefer the parsed cycle index when available; else use occurrence.
            # If the cycle_idx key already exists (e.g. two different job
            # numbers both have _1), fall back to the occurrence counter.
            idx = cycle_idx if cycle_idx is not None else occurrence
            # If the first occurrence was stored under the bare 3-tuple, move
            # it out so repeated entries stay unique and discoverable.
            if base_key in datasets and occurrence == 2:
                first = datasets.pop(base_key)
                first_idx = first.get("cycle_index") or 1
                datasets[(*base_key, first_idx)] = first
            candidate_key = (*base_key, idx)
            if candidate_key in datasets:
                # Key collision (e.g. job 3349 and 4020 both have Dis1C_1)
                key = (*base_key, occurrence)
            else:
                key = candidate_key

        datasets[key] = {
            "raw_df": df,
            "temperature_C": temp_c,
            "profile": profile,
            "celljar_test_type": test_type,
            "source_file": name,
            "cycle_index": cycle_idx,
        }

    if not datasets:
        raise FileNotFoundError(
            f"No HNEI test files matched in {raw}. Expected Kollmeyer naming "
            f"(e.g. '25degC_5Pulse_HPPC_Pan18650PF.mat', "
            f"'25degC_UDDS_Pan18650PF.mat', '25degC_Cycle_1_Pan18650PF.mat'). "
            f"Found: {[p.name for p in raw.glob('*.mat')][:5]}..."
        )

    if skipped_conditioning:
        print(f"[hnei] Skipped {len(skipped_conditioning)} conditioning/combined files")
    if skipped_unrecognized:
        print(f"[hnei] Skipped {len(skipped_unrecognized)} unrecognized .mat files:")
        for fname in skipped_unrecognized:
            print(f"  - {fname}")
    print(f"[hnei] Ingested {len(datasets)} tests, "
          f"skipped {len(skipped_conditioning)} conditioning + "
          f"{len(skipped_unrecognized)} unrecognized")

    return datasets
