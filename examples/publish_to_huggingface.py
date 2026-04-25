"""Publish harmonized celljar output to HuggingFace Datasets.

Run after `python examples/demo_end_to_end.py` to push the harmonized
bundle (cells/, tests/, timeseries.parquet) to:
    https://huggingface.co/datasets/mihnathul/celljar

Requires:
    pip install huggingface_hub
    huggingface-cli login   # one-time, requires write token
"""

from pathlib import Path
import argparse
import json
import sys

from huggingface_hub import HfApi, create_repo

ROOT = Path(__file__).parent.parent
HARMONIZED = ROOT / "data" / "harmonized"
HF_REPO = "mihnathul/celljar"
GH_REPO = "https://github.com/mihnathul/celljar"


def verify_auth() -> str:
    """Return HF username, or exit with helpful error."""
    api = HfApi()
    try:
        info = api.whoami()
        return info["name"]
    except Exception:
        sys.exit(
            "Not authenticated with HuggingFace.\n"
            "Run: huggingface-cli login\n"
            "(You'll need a write-scoped token from https://huggingface.co/settings/tokens)"
        )


# Sources whose license does not permit redistribution by celljar.
# The publisher refuses to upload the harmonized bundle if any cell from
# one of these sources is present under data/harmonized/cells/.
# (Empty for v0.3 - all current sources have permissive licenses.)
_NO_REDISTRIBUTE_SOURCES: set[str] = set()


def verify_data() -> dict:
    """Confirm harmonized output exists. Return summary stats."""
    parquet = HARMONIZED / "timeseries.parquet"
    if not parquet.exists():
        sys.exit(
            f"No timeseries.parquet found at {parquet}.\n"
            "Run examples/demo_end_to_end.py first to produce the harmonized bundle."
        )
    cells_dir = HARMONIZED / "cells"
    tests_dir = HARMONIZED / "tests"
    if not cells_dir.exists() or not tests_dir.exists():
        sys.exit(
            f"Missing cells/ or tests/ under {HARMONIZED}.\n"
            "Run examples/demo_end_to_end.py first."
        )
    cells = list(cells_dir.glob("*.json"))
    tests = list(tests_dir.glob("*.json"))
    if not cells or not tests:
        sys.exit("cells/ or tests/ is empty. Run examples/demo_end_to_end.py first.")

    # Refuse to upload sources whose license doesn't permit redistribution.
    blocked: list[str] = []
    for p in cells:
        try:
            with open(p) as f:
                c = json.load(f)
        except Exception:
            continue
        if c.get("source") in _NO_REDISTRIBUTE_SOURCES:
            blocked.append(f"{c.get('source')}: {c.get('cell_id')}")
    if blocked:
        sys.exit(
            "Refusing to publish - the harmonized bundle contains cells from "
            "sources celljar does not redistribute:\n  "
            + "\n  ".join(blocked)
            + "\nRemove these cells/tests from data/harmonized/ before publishing. "
            "See data/raw/calce/SOURCE_DATA_PROVENANCE.md."
        )

    # Total bytes across the bundle (for the summary at the end)
    total_bytes = parquet.stat().st_size
    for p in cells + tests:
        total_bytes += p.stat().st_size
    return {
        "cells": len(cells),
        "tests": len(tests),
        "parquet_bytes": parquet.stat().st_size,
        "total_bytes": total_bytes,
    }


from celljar.bundle import collect_sources as _collect_sources_impl, timeseries_row_count as _timeseries_row_count_impl   # noqa: E402


def _collect_sources() -> dict:
    return _collect_sources_impl(HARMONIZED)


def _timeseries_row_count() -> int:
    return _timeseries_row_count_impl(HARMONIZED)


def build_schema_section() -> str:
    """Generate the entity-summary block by reading schemas/*.schema.json.

    Replaces the previous hand-curated bullet list (which drifted from the real
    schema). One source of truth: schemas/*.schema.json → both the README and
    the HF dataset card render the same field set.
    """
    schemas_dir = ROOT / "schemas"
    entities = [
        ("cell_metadata", "JSON, one file per cell"),
        ("test_metadata", "JSON, one file per test"),
        ("timeseries", "Parquet, one row per measurement sample"),
        ("cycle_summary", "Parquet, one row per cycle / aging checkpoint"),
    ]
    lines = []
    for entity_name, blurb in entities:
        path = schemas_dir / f"{entity_name}.schema.json"
        if not path.exists():
            continue
        spec = json.loads(path.read_text())
        props = list(spec.get("properties", {}).keys())
        required = set(spec.get("required", []))
        # Mark required fields with a *.
        field_list = ", ".join(f"`{p}`*" if p in required else f"`{p}`" for p in props)
        lines.append(f"- **`{entity_name}`** ({blurb}) - {field_list}")
    lines.append("")
    lines.append("`*` = required field (others nullable). See "
                 f"[JSON Schemas]({GH_REPO}/tree/main/schemas) for full type info, enum values, and constraints.")
    return "\n".join(lines)


def build_frontmatter(sources: dict) -> str:
    """HF dataset card YAML frontmatter (license, tags, source list)."""
    tags = [
        "battery", "lithium-ion", "energy-storage", "timeseries",
        "electrochemistry", "bms", "hppc", "cycling",
    ]
    tag_lines = "\n".join(f"  - {t}" for t in tags)
    source_tag_lines = "\n".join(f"  - {s.lower()}" for s in sorted(sources))
    return f"""---
license: cc-by-4.0
language:
  - en
pretty_name: celljar
tags:
{tag_lines}
size_categories:
  - 10K<n<100M
task_categories:
  - time-series-forecasting
  - tabular-regression
source_datasets:
{source_tag_lines if source_tag_lines else "  - original"}
---"""


def build_source_citations(sources: dict) -> str:
    """Per-source citation markdown block (DOI, license, URL per dataset)."""
    if not sources:
        return "_No sources discovered in the harmonized bundle._"

    chunks = []
    for src in sorted(sources):
        meta = sources[src]
        citation = (meta.get("citation") or "(citation unavailable in harmonized bundle)").strip()
        lic = meta.get("license") or "see upstream"
        lic_url = meta.get("license_url")
        url = meta.get("url")
        doi = meta.get("doi")

        lines = [f"### {src}", "", f"> {citation}", ""]
        meta_bits = [f"**License:** {lic}"]
        if lic_url:
            meta_bits.append(f"[license terms]({lic_url})")
        if url:
            meta_bits.append(f"[dataset]({url})")
        if doi:
            meta_bits.append(f"DOI: `{doi}`")
        lines.append(" · ".join(meta_bits))
        chunks.append("\n".join(lines))
    return "\n\n".join(chunks)


def build_card_body(
    sources: dict,
    n_cells: int,
    n_tests: int,
    n_rows: int,
    per_source_block: str,
) -> str:
    """Main markdown body of the HF dataset card."""
    source_list_inline = ", ".join(f"`{s}`" for s in sorted(sources)) or "_(none)_"
    n_rows_str = f"**{n_rows:,} timeseries rows**" if n_rows >= 0 else "timeseries rows: see parquet"
    schema_block = build_schema_section()

    return f"""# celljar

**Public battery cell test data, harmonized and sealed in one schema (Parquet + JSON).**

celljar reads raw files from published sources and writes them into one
canonical schema across four entities: `cell_metadata` + `test_metadata`
(JSON), `timeseries` + `cycle_summary` (Parquet). Consumers read one format
instead of writing per-source loaders.

**Scope: harmonization only.** celljar focuses on measurements - unit
conversion and schema normalization. It deliberately leaves fitting and
modeling to downstream tools that specialize in those steps.

- Upstream code / issue tracker: <{GH_REPO}>
- Sources in this snapshot: {source_list_inline}
- Contents: **{n_cells} cells**, **{n_tests} tests**, {n_rows_str}

## Files

```
cells/*.json              # one file per cell (hardware metadata)
tests/*.json              # one file per test (protocol + provenance + observed stats)
timeseries.parquet        # all tests' V/I/T samples; join on test_id
cycle_summary.parquet     # per-cycle aggregates (aging studies); join on (test_id, cycle_number)
```

## Schema (overview)

Four entities; field list generated from the authoritative [JSON Schemas]({GH_REPO}/tree/main/schemas):

{schema_block}

SI units. Relative timestamps. Missing data is explicit `null` (no NaN
sentinels). Current sign convention: positive = charge (into the cell),
negative = discharge.

Join keys: `cells.cell_id = tests.cell_id`, `tests.test_id = timeseries.test_id`,
`(tests.test_id, cycle_number) = cycle_summary.(test_id, cycle_number)`.

## Download the whole bundle

```bash
# CLI - pulls everything (cells/*.json, tests/*.json, timeseries.parquet, cycle_summary.parquet)
pip install huggingface_hub
huggingface-cli download {HF_REPO} --repo-type dataset --local-dir ./celljar-bundle

# Pin a tagged release for reproducibility
huggingface-cli download {HF_REPO} --repo-type dataset --revision v0.2.1 --local-dir ./celljar-bundle
```

Or in Python:

```python
from huggingface_hub import snapshot_download
local = snapshot_download(repo_id="{HF_REPO}", repo_type="dataset", revision="v0.2.1")
print(local)  # local path containing cells/, tests/, timeseries.parquet, cycle_summary.parquet
```

## Query in place - no download needed

### DuckDB - full SQL across all entities over HTTPS

```sql
INSTALL httpfs; LOAD httpfs;
SELECT c.chemistry, c.nominal_capacity_Ah,
       t.test_id, t.test_type, t.soh_pct,
       COUNT(*) AS n_samples
FROM read_json('https://huggingface.co/datasets/{HF_REPO}/resolve/main/cells/*.json')  c
JOIN read_json('https://huggingface.co/datasets/{HF_REPO}/resolve/main/tests/*.json')  t
  ON c.cell_id = t.cell_id
JOIN 'https://huggingface.co/datasets/{HF_REPO}/resolve/main/timeseries.parquet'       ts
  ON t.test_id = ts.test_id
GROUP BY 1,2,3,4,5
ORDER BY t.test_id;
```

### pandas / Polars - predicate-pushdown read of one test

```python
import pandas as pd
df = pd.read_parquet(
    "https://huggingface.co/datasets/{HF_REPO}/resolve/main/timeseries.parquet",
    filters=[("test_id", "==", "ORNL_LEAF_2013_HPPC_25C")],
)
```

### `datasets` library - streaming

```python
from datasets import load_dataset
ds = load_dataset(
    "parquet",
    data_files="https://huggingface.co/datasets/{HF_REPO}/resolve/main/timeseries.parquet",
    split="train",
    streaming=True,
)
for row in ds.take(5):
    print(row)
```

## License & citation

The science here belongs to the original authors; celljar simply puts their
data in one place with a shared schema. Please cite their papers when you use
the data, and, if it's helpful, celljar alongside.

- **This harmonized bundle** (packaging, schema, derived test-metadata fields):
  [CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/).
- **Upstream raw data** retains each publisher's original license - listed
  per-source below. Each source's license terms apply when you use its tests.

To make attribution easy, every `tests/*.json` row carries its own
`source_doi`, `source_citation`, `source_license`, and `source_license_url`
fields, so you can pull references for any analysis with one query.

## Per-source citations

{per_source_block}

## Citing celljar

If you'd like to cite celljar:

```bibtex
@software{{celljar,
  author = {{Mihna Neerulpan}},
  title  = {{celljar: Public Battery Test Dataset Harmonization with a Canonical Schema}},
  year   = {{2026}},
  url    = {{{GH_REPO}}},
}}
```

## Links

- Code: <{GH_REPO}>
- Issues / new-source requests: <{GH_REPO}/issues>
- Canonical JSON Schemas: <{GH_REPO}/tree/main/schemas>
"""


def build_dataset_card() -> str:
    """Compose the HF dataset card README from harmonized output.

    Three pieces glued together: YAML frontmatter, markdown body, per-source
    citations. Each piece is its own function so they can be edited / tested
    independently and stay in sync with the README/schema.
    """
    sources = _collect_sources()
    n_cells = len(list((HARMONIZED / "cells").glob("*.json")))
    n_tests = len(list((HARMONIZED / "tests").glob("*.json")))
    n_rows = _timeseries_row_count()

    frontmatter = build_frontmatter(sources)
    citations = build_source_citations(sources)
    body = build_card_body(sources, n_cells, n_tests, n_rows, citations)
    return f"{frontmatter}\n\n{body}"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--message",
        default="Update harmonized bundle",
        help="Commit message for the HF dataset push.",
    )
    parser.add_argument(
        "--revision",
        default=None,
        help="Optional tag to create on the dataset after upload (e.g. v0.2.1).",
    )
    args = parser.parse_args()

    user = verify_auth()
    stats = verify_data()
    print(f"Authenticated as: {user}")
    print(
        f"Local harmonized: {stats['cells']} cells, {stats['tests']} tests, "
        f"{stats['parquet_bytes'] / 1e6:.1f} MB parquet "
        f"({stats['total_bytes'] / 1e6:.1f} MB total)"
    )

    create_repo(repo_id=HF_REPO, repo_type="dataset", exist_ok=True)

    # Write dataset card alongside the harmonized bundle so upload_folder picks it up
    card = build_dataset_card()
    card_path = HARMONIZED / "README.md"
    card_path.write_text(card)
    print(f"Wrote dataset card: {card_path} ({len(card):,} chars)")

    api = HfApi()
    print(f"Uploading to https://huggingface.co/datasets/{HF_REPO} ...")
    api.upload_folder(
        folder_path=str(HARMONIZED),
        repo_id=HF_REPO,
        repo_type="dataset",
        commit_message=args.message,
    )

    if args.revision:
        print(f"Tagging revision {args.revision} ...")
        api.create_tag(
            repo_id=HF_REPO,
            tag=args.revision,
            repo_type="dataset",
            tag_message=f"celljar {args.revision}",
            exist_ok=True,
        )

    n_rows = _timeseries_row_count()
    print()
    print("=" * 60)
    print("Published")
    print("=" * 60)
    print(f"  Cells:      {stats['cells']}")
    print(f"  Tests:      {stats['tests']}")
    print(f"  Timeseries: {n_rows:,} rows" if n_rows >= 0 else "  Timeseries: (row count unavailable)")
    print(f"  Uploaded:   {stats['total_bytes'] / 1e6:.1f} MB")
    print(f"  URL:        https://huggingface.co/datasets/{HF_REPO}")
    print()
    print("Verify the push works end-to-end with:")
    print(
        "  python -c \"import pandas as pd; "
        f"print(pd.read_parquet('https://huggingface.co/datasets/{HF_REPO}/resolve/main/timeseries.parquet', "
        "filters=[('test_id', '==', 'ORNL_LEAF_2013_HPPC_25C')]).head())\""
    )


if __name__ == "__main__":
    main()
