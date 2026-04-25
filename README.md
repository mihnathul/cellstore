# celljar

**Public battery cell test data, harmonized and sealed in one schema (Parquet + JSON).**

celljar reads raw files from 9 published sources — ORNL Leaf, HNEI Kollmeyer, MATR (Severson 2019), CLO (Attia 2020), BILLS eVTOL, MOHTAT 2021, NASA PCoE, SNL Preger, Naumann — and writes them to a canonical schema with four entities: `cell_metadata` + `test_metadata` (JSON), `timeseries` + `cycle_summary` (Parquet). Query all sources via one SQL statement (DuckDB / pandas / Polars).

**Scope: harmonization only.** celljar focuses on measurements — unit conversion and schema normalization. It deliberately leaves fitting and modeling to downstream tools that specialize in those steps.

## Quick start

The full harmonized bundle lives at [huggingface.co/datasets/mihnathul/celljar](https://huggingface.co/datasets/mihnathul/celljar). Query it directly — no clone needed:

```python
import duckdb
df = duckdb.sql("""
    SELECT * FROM 'https://huggingface.co/datasets/mihnathul/celljar/resolve/main/timeseries.parquet'
    WHERE test_id = 'ORNL_LEAF_2013_HPPC_25C'
""").df()
```

Pandas and Polars work the same way against the HuggingFace URL.

**Browser viewer** — clone the repo (a PyPI release is on the roadmap):

```bash
git clone https://github.com/mihnathul/celljar.git
cd celljar
pip install -e ".[viewer]"
streamlit run apps/viewer.py    # fetches from HuggingFace by default
```

Pin a release for reproducibility: `CELLJAR_HF_REVISION=v0.2.0 streamlit run apps/viewer.py`.

**Regenerate locally** from raw sources: same setup, then `python examples/demo_end_to_end.py` and `CELLJAR_LOCAL=1 streamlit run apps/viewer.py`.

## Sources

| Source | Chemistry | Cells | Test types | Raw data |
|---|---|---|---|---|
| ORNL Leaf 2013 | mixed (LMO/NCA pouch) | 1 | HPPC × 3 temperatures | bundled |
| HNEI (Kollmeyer) | NCA (Panasonic NCR18650PF) | 1 | HPPC, drive cycle, capacity_check, cycle_aging | [download](data/raw/hnei/SOURCE_DATA_PROVENANCE.md) |
| MATR (Severson 2019) | LFP (A123 18650) | 119 | Cycling-to-failure | [download](data/raw/matr/SOURCE_DATA_PROVENANCE.md) |
| CLO (Attia 2020) | LFP (A123 18650) | 45 | Cycling, BO-optimized fast-charge | [download](data/raw/clo/SOURCE_DATA_PROVENANCE.md) |
| BILLS / eVTOL (Bills 2023) | NMC (Sony US18650VTC6) | 22 | Drive cycle (flight-duty) + RPTs | [download](data/raw/bills/SOURCE_DATA_PROVENANCE.md) |
| MOHTAT (Mohtat 2021) | NMC (UMich NMC532 pouch) | 31 | Cycle aging + synchronous expansion | [download](data/raw/mohtat/SOURCE_DATA_PROVENANCE.md) |
| NASA PCoE | LCO (vendor undisclosed, 2.0 Ah 18650) | 34 | Cycle aging | [download](data/raw/nasa_pcoe/SOURCE_DATA_PROVENANCE.md) |
| SNL Preger 2020 | LFP / NMC / NCA grid (18650) | 87 | Cycle aging across T × DoD × C-rate | [download](data/raw/snl_preger/SOURCE_DATA_PROVENANCE.md) |
| Naumann 2018/2020 | LFP / graphite | 17 calendar + 17 cycle | Calendar + cycle aging (summary-only) | [download](data/raw/naumann/SOURCE_DATA_PROVENANCE.md) |

## Schema

Four entities joined by `cell_id` and `test_id`:

```
cell_metadata.json       hardware (chemistry, capacity, form factor)
test_metadata.json       protocol, SOH, provenance, license
timeseries.parquet       V / I / T per-sample + signed running coulomb count (∫I dt)
cycle_summary.parquet    per-cycle aggregates (capacity, R_DC, …) for aging studies
```

**Conventions:** SI units. Timestamps relative. Missing data is explicit `null`. Current is positive = charge (into the cell), negative = discharge.

Authoritative field list + types in [`schemas/`](schemas/) (JSON Schema). Pandera mirrors at runtime in [`celljar/harmonize/harmonize_schema.py`](celljar/harmonize/harmonize_schema.py).

## Querying

```sql
-- Single test's timeseries
SELECT timestamp_s, voltage_V, current_A, temperature_C
FROM 'data/harmonized/timeseries.parquet'
WHERE test_id = 'ORNL_LEAF_2013_HPPC_25C'
ORDER BY timestamp_s;
```

```sql
-- Cross-source filter — same query works across all sources
SELECT cell_id, test_id, temperature_C_min
FROM 'data/harmonized/tests/*.json'
WHERE test_type = 'hppc' AND temperature_C_min = 25;
```

Same patterns from Python via `duckdb.sql(...).df()` or `pl.read_parquet(..., filters=[...])`.

## Use cases

Parameterization · modeling · aging studies · cross-source analysis.

**Out of scope:** field/fleet telemetry; ML cycling-life prediction (use [BatteryLife (KDD 2025)](https://github.com/Ruifeng-Tan/BatteryLife) — 990 cells, 18 baselines). OCV/R0 extractors, ECM/SPM/DFN fitting, ML modeling all live in separate companion repos.

## How this relates to other battery data tools

celljar tries to fit alongside, not replace, the other excellent tools in this space:

- **[Battery Data Commons](https://batterycommons.github.io/)** — registry indexing 300+ public battery datasets. Great for discovery; celljar complements it by providing a harmonized data layer for a subset of those sources.
- **[Iontech](https://github.com/shiyunliu-battery/Iontech)** (Shiyun Liu) — curated index of open-source battery monitoring & modeling datasets (RWTH home-storage, NREL failure databank, Stanford second-life, etc.) with paper links. Another good starting point for discovering datasets celljar hasn't yet harmonized.
- **[BatteryLife](https://github.com/Ruifeng-Tan/BatteryLife) / [BatteryML](https://github.com/microsoft/BatteryML)** — cycling-to-failure ML benchmark (KDD 2025). Optimized for lifetime-prediction ML; celljar keeps the full V/I/T timeseries that physics-based parameterization (ECM/SPM/DFN) needs.

## Roadmap

- More sources (CALCE, RWTH, HUST, Tongji, XJTU; Ecker 2015 + Chen 2020 for DFN parameterization)
- PyPI release (`pip install celljar`)
- SOH methodology iteration
- BDF-export converter

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Issues, ideas, and PRs welcome.

## License & citation

The science here belongs to the original authors; celljar simply puts their data in one place with a shared schema. Please cite their papers when you use the data, and, if it's helpful, celljar alongside.

- **celljar code** (this repository): MIT ([`LICENSE`](LICENSE)).
- **Harmonized bundle** (packaging, schema, derived fields): CC-BY-4.0.
- **Upstream raw data** retains each publisher's original license — see per-source provenance in `data/raw/<source>/`.

To make attribution easy, every `test_metadata` row carries its own `source_doi`, `source_citation`, `source_license`, and `source_license_url`. You can pull the references for any analysis with one query:

```python
import duckdb
duckdb.sql("""
    SELECT DISTINCT source_doi, source_citation, source_license
    FROM 'data/harmonized/tests/*.json'
    WHERE test_id IN ('ORNL_LEAF_2013_HPPC_25C', 'HNEI_NCA_HPPC_25C')
""").df()
```

If you'd like to cite celljar:

```bibtex
@software{celljar,
  author = {Mihna Neerulpan},
  title  = {celljar: Public Battery Test Dataset Harmonization with a Canonical Schema},
  year   = {2026},
  url    = {https://github.com/mihnathul/celljar},
}
```

## Acknowledgments

celljar exists because of the labs and authors who designed, ran, and openly published these experiments — work that took years of careful instrumentation and analysis. Thank you to:

Phillip Kollmeyer (HNEI) · G. Wiggins, S. Allu, H. Wang (ORNL) · K. Severson, P. Attia et al. (MATR, CLO; Stanford / MIT / TRI) · A. Bills et al. (BILLS; CMU) · P. Mohtat et al. (UMich) · B. Saha, K. Goebel (NASA PCoE) · Y. Preger et al. (Sandia) · M. Naumann et al. (TUM) · M. Ecker et al. (RWTH Aachen)
