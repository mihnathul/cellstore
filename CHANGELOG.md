# Changelog

All notable changes to cellstore are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-04-15

Initial release.

### Schema

- Canonical three-entity schema: `cell_metadata`, `test_metadata`, `timeseries`
- Defined as JSON Schema files in `schemas/` (authoritative contract)
- Mirrored as Pandera models in `cellstore/harmonize/harmonize_schema.py` for
  runtime DataFrame validation inside the Python pipeline
- `test_metadata` carries `soh_pct`, `soh_method`, and `cycle_count_at_test`
  fields for aging context (methodology will iterate — see roadmap)

### Sources

- **ORNL_LEAF** — 2013 Nissan Leaf pouch cell, HPPC (raw + harmonized data bundled)
- **HNEI** — Panasonic NCR18650PF, 5-pulse HPPC (download required)
- **MATR** (Severson 2019) — 124 A123 LFP cells, fast-charge cycling (download required)

### Tooling

- `examples/demo_end_to_end.py` — run the pipeline across all present sources
- `apps/viewer.py` — Streamlit viewer with filters, aging plot, overlay
- GitHub Actions CI across Python 3.9–3.12
- Dependabot weekly updates
