"""Smoke test for the HuggingFace dataset card generator.

Catches schema-vs-card drift: if `schemas/*.schema.json` adds/renames a field,
the card section should reflect it (since it's generated from the schemas).
This was an actual P1 bug pre-v0.2 — the card hand-restated 3 entities while
the schema had 4, plus a broken f-string. Pinning a few invariants here so it
doesn't regress.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Add examples/ to path so we can import publish_to_huggingface.
sys.path.insert(0, str(Path(__file__).parent.parent / "examples"))

# Skip gracefully if huggingface_hub isn't installed (e.g. minimal CI).
huggingface_hub = pytest.importorskip("huggingface_hub")
from publish_to_huggingface import (  # noqa: E402
    build_card_body,
    build_dataset_card,
    build_frontmatter,
    build_schema_section,
    build_source_citations,
)


def test_schema_section_lists_all_4_entities():
    """The generated schema section must mention every entity in schemas/."""
    section = build_schema_section()
    for entity in ("cell_metadata", "test_metadata", "timeseries", "cycle_summary"):
        assert f"`{entity}`" in section, f"schema section missing entity: {entity}"


def test_schema_section_marks_required_fields():
    """Required fields should be marked with *."""
    section = build_schema_section()
    # cell_id is required in cell_metadata, test_metadata, timeseries, cycle_summary.
    # Marker must appear at least once.
    assert "`cell_id`*" in section, "required-field asterisk marker missing"


def test_frontmatter_is_valid_yaml():
    """The frontmatter block must start with --- and end with ---."""
    front = build_frontmatter({"ORNL": {}, "HNEI": {}})
    assert front.startswith("---")
    assert front.endswith("---")
    # Sources are listed as lowercase tags
    assert "  - ornl" in front
    assert "  - hnei" in front


def test_source_citations_lists_each_source():
    sources = {
        "ORNL": {"citation": "ORNL cite", "license": "MIT", "doi": "10.1/ornl"},
        "HNEI": {"citation": "HNEI cite", "license": "CC-BY-4.0"},
    }
    block = build_source_citations(sources)
    assert "### ORNL" in block
    assert "### HNEI" in block
    assert "ORNL cite" in block
    assert "10.1/ornl" in block


def test_source_citations_handles_empty_sources():
    block = build_source_citations({})
    assert "No sources discovered" in block


def test_card_body_no_unrendered_template_literals():
    """Catch the previous P1 bug: a literal `(if n_rows >= 0 ...)` rendered as text."""
    body = build_card_body(
        sources={"ORNL": {}}, n_cells=1, n_tests=3, n_rows=10000, per_source_block="x",
    )
    # If a Python conditional leaks into the rendered card, this string would appear.
    assert "if n_rows >= 0" not in body
    assert "if n_rows" not in body
    # Sanity: numeric stats are formatted with thousand separators.
    assert "10,000" in body


def test_full_dataset_card_assembles():
    """End-to-end smoke: build_dataset_card() runs and emits non-empty markdown."""
    card = build_dataset_card()
    assert card.startswith("---")  # frontmatter
    assert "celljar" in card.lower()
    assert "schemas" in card.lower()
    assert len(card) > 500
