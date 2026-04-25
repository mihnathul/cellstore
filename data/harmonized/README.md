---
license: cc-by-4.0
language:
  - en
pretty_name: celljar
tags:
  - battery
  - lithium-ion
  - energy-storage
  - timeseries
  - electrochemistry
  - bms
  - hppc
  - cycling
size_categories:
  - 10K<n<100M
task_categories:
  - time-series-forecasting
  - tabular-regression
source_datasets:
  - bills
  - clo
  - ecker
  - hnei
  - matr
  - nasa_pcoe
  - naumann
  - ornl
---

# celljar

**Public battery cell test data, harmonized and sealed in one schema (Parquet + JSON).**

celljar reads raw files from published sources and writes them into one
canonical schema across four entities: `cell_metadata` + `test_metadata`
(JSON), `timeseries` + `cycle_summary` (Parquet). Consumers read one format
instead of writing per-source loaders.

**Scope: harmonization only.** celljar focuses on measurements - unit
conversion and schema normalization. It deliberately leaves fitting and
modeling to downstream tools that specialize in those steps.

- Upstream code / issue tracker: <https://github.com/mihnathul/celljar>
- Sources in this snapshot: `BILLS`, `CLO`, `ECKER`, `HNEI`, `MATR`, `NASA_PCOE`, `NAUMANN`, `ORNL`
- Contents: **273 cells**, **348 tests**, **167,820,250 timeseries rows**

## Files

```
cells/*.json              # one file per cell (hardware metadata)
tests/*.json              # one file per test (protocol + provenance + observed stats)
timeseries.parquet        # all tests' V/I/T samples; join on test_id
cycle_summary.parquet     # per-cycle aggregates (aging studies); join on (test_id, cycle_number)
```

## Schema (overview)

Four entities; field list generated from the authoritative [JSON Schemas](https://github.com/mihnathul/celljar/tree/main/schemas):

- **`cell_metadata`** (JSON, one file per cell) - `cell_id`*, `source`*, `source_cell_id`, `manufacturer`, `model_number`, `chemistry`*, `cathode`, `anode`, `electrolyte`, `form_factor`*, `nominal_capacity_Ah`, `nominal_voltage_V`, `max_voltage_V`, `min_voltage_V`
- **`test_metadata`** (JSON, one file per test) - `test_id`*, `cell_id`*, `test_type`*, `temperature_C_min`, `temperature_C_max`, `soc_range_min`, `soc_range_max`, `soc_step`, `c_rate_charge`, `c_rate_discharge`, `protocol_description`, `num_cycles`, `soh_pct`, `soh_method`, `cycle_count_at_test`, `test_year`, `source_doi`, `source_url`, `source_citation`, `source_license`, `source_license_url`, `n_samples`, `duration_s`, `voltage_observed_min_V`, `voltage_observed_max_V`, `current_observed_min_A`, `current_observed_max_A`, `temperature_observed_min_C`, `temperature_observed_max_C`, `sample_dt_min_s`, `sample_dt_median_s`, `sample_dt_max_s`
- **`timeseries`** (Parquet, one row per measurement sample) - `test_id`*, `cycle_number`*, `step_number`, `step_type`, `timestamp_s`*, `voltage_V`, `current_A`, `temperature_C`, `coulomb_count_Ah`, `energy_Wh`, `displacement_um`
- **`cycle_summary`** (Parquet, one row per cycle / aging checkpoint) - `test_id`*, `cell_id`*, `cycle_number`, `equivalent_full_cycles`, `elapsed_time_s`, `capacity_Ah`, `capacity_retention_pct`, `resistance_dc_ohm`, `resistance_dc_pulse_duration_s`, `resistance_dc_soc_pct`, `energy_Wh`, `coulombic_efficiency`, `temperature_C_mean`

`*` = required field (others nullable). See [JSON Schemas](https://github.com/mihnathul/celljar/tree/main/schemas) for full type info, enum values, and constraints.

SI units. Relative timestamps. Missing data is explicit `null` (no NaN
sentinels). Current sign convention: positive = charge (into the cell),
negative = discharge.

Join keys: `cells.cell_id = tests.cell_id`, `tests.test_id = timeseries.test_id`,
`(tests.test_id, cycle_number) = cycle_summary.(test_id, cycle_number)`.

## Download the whole bundle

```bash
# CLI - pulls everything (cells/*.json, tests/*.json, timeseries.parquet, cycle_summary.parquet)
pip install huggingface_hub
huggingface-cli download mihnathul/celljar --repo-type dataset --local-dir ./celljar-bundle

# Pin a tagged release for reproducibility
huggingface-cli download mihnathul/celljar --repo-type dataset --revision v0.2.1 --local-dir ./celljar-bundle
```

Or in Python:

```python
from huggingface_hub import snapshot_download
local = snapshot_download(repo_id="mihnathul/celljar", repo_type="dataset", revision="v0.2.1")
print(local)  # local path containing cells/, tests/, timeseries.parquet, cycle_summary.parquet
```

## Query in place - no download needed

### DuckDB - full SQL across all entities over HTTPS

```sql
INSTALL httpfs; LOAD httpfs;
SELECT c.chemistry, c.nominal_capacity_Ah,
       t.test_id, t.test_type, t.soh_pct,
       COUNT(*) AS n_samples
FROM read_json('https://huggingface.co/datasets/mihnathul/celljar/resolve/main/cells/*.json')  c
JOIN read_json('https://huggingface.co/datasets/mihnathul/celljar/resolve/main/tests/*.json')  t
  ON c.cell_id = t.cell_id
JOIN 'https://huggingface.co/datasets/mihnathul/celljar/resolve/main/timeseries.parquet'       ts
  ON t.test_id = ts.test_id
GROUP BY 1,2,3,4,5
ORDER BY t.test_id;
```

### pandas / Polars - predicate-pushdown read of one test

```python
import pandas as pd
df = pd.read_parquet(
    "https://huggingface.co/datasets/mihnathul/celljar/resolve/main/timeseries.parquet",
    filters=[("test_id", "==", "ORNL_LEAF_2013_HPPC_25C")],
)
```

### `datasets` library - streaming

```python
from datasets import load_dataset
ds = load_dataset(
    "parquet",
    data_files="https://huggingface.co/datasets/mihnathul/celljar/resolve/main/timeseries.parquet",
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

### BILLS

> Bills, A., Sripad, S., Fredericks, W. L., et al. (2023). A battery dataset for electric vertical takeoff and landing aircraft. Scientific Data 10, 344. https://doi.org/10.1038/s41597-023-02180-5

**License:** CC-BY-4.0 · [license terms](https://creativecommons.org/licenses/by/4.0/) · [dataset](https://kilthub.cmu.edu/articles/dataset/eVTOL_Battery_Dataset/14226830) · DOI: `10.1184/R1/14226830`

### CLO

> Attia, P. M., Grover, A., Jin, N., et al. (2020). Closed-loop optimization of fast-charging protocols for batteries with machine learning. Nature 578, 397-402. https://doi.org/10.1038/s41586-020-1994-5

**License:** CC-BY-4.0 · [license terms](https://creativecommons.org/licenses/by/4.0/) · [dataset](https://data.matr.io/1/projects/5d80e633f405260001c0b60a) · DOI: `10.1038/s41586-020-1994-5`

### ECKER

> (citation unavailable in harmonized bundle)

**License:** see upstream

### HNEI

> Kollmeyer, P. (2018). Panasonic 18650PF Li-ion Battery Data. Mendeley Data, v1. https://doi.org/10.17632/wykht8y7tg.1

**License:** CC-BY-4.0 · [license terms](https://creativecommons.org/licenses/by/4.0/) · [dataset](https://data.mendeley.com/datasets/wykht8y7tg/1) · DOI: `10.17632/wykht8y7tg.1`

### MATR

> Severson, K. A., Attia, P. M., Jin, N., et al. (2019). Data-driven prediction of battery cycle life before capacity degradation. Nature Energy 4, 383-391. https://doi.org/10.1038/s41560-019-0356-8

**License:** CC-BY-4.0 · [license terms](https://creativecommons.org/licenses/by/4.0/) · [dataset](https://data.matr.io/1/projects/5c48dd2bc625d700019f3204) · DOI: `10.1038/s41560-019-0356-8`

### NASA_PCOE

> Saha, B. & Goebel, K. (2007). Battery Data Set. NASA Prognostics Data Repository, NASA Ames Research Center, Moffett Field, CA. https://www.nasa.gov/intelligent-systems-division/discovery-and-systems-health/pcoe/pcoe-data-set-repository/ Cells are 18650 Li-ion; chemistry/vendor not disclosed by NASA — community consensus treats them as LCO.

**License:** CC0-1.0 · [license terms](https://creativecommons.org/publicdomain/zero/1.0/) · [dataset](https://www.nasa.gov/intelligent-systems-division/discovery-and-systems-health/pcoe/pcoe-data-set-repository/)

### NAUMANN

> Naumann, M. (2021). Data for: Analysis and modeling of calendar/cycle aging of a commercial LiFePO4/graphite cell. Mendeley Data. DOIs: 10.17632/kxh42bfgtj.1 (calendar) and 10.17632/6hgyr25h8d.1 (cycle). Companion papers: Naumann et al. JPS 2018 doi:10.1016/j.est.2018.01.019, Naumann et al. JPS 2020 doi:10.1016/j.jpowsour.2019.227666

**License:** CC-BY-4.0 · [license terms](https://creativecommons.org/licenses/by/4.0/) · [dataset](https://data.mendeley.com/datasets/kxh42bfgtj/1) · DOI: `10.17632/kxh42bfgtj.1`

### ORNL

> Wiggins, G., Allu, S., & Wang, H. (2019). Battery cell data from a 2013 Nissan Leaf. Oak Ridge National Laboratory. https://doi.org/10.5281/zenodo.2580327

**License:** MIT · [license terms](https://opensource.org/licenses/MIT) · [dataset](https://zenodo.org/records/2580327) · DOI: `10.5281/zenodo.2580327`

## Citing celljar

If you'd like to cite celljar:

```bibtex
@software{celljar,
  author = {Mihna Neerulpan},
  title  = {celljar: Public Battery Test Dataset Harmonization with a Canonical Schema},
  year   = {2026},
  url    = {https://github.com/mihnathul/celljar},
}
```

## Links

- Code: <https://github.com/mihnathul/celljar>
- Issues / new-source requests: <https://github.com/mihnathul/celljar/issues>
- Canonical JSON Schemas: <https://github.com/mihnathul/celljar/tree/main/schemas>
