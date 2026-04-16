# cellstore

**Harmonizes battery test datasets into one canonical schema.**

Public battery data is scattered across labs and tools, each with its own format and column conventions. cellstore reads raw files from public sources like ORNL Leaf, HNEI Panasonic 18650PF, and MATR (Severson 2019) and writes them into one canonical schema: **cell metadata** (JSON), **test metadata** (JSON), **timeseries** (Parquet). Consumers read one format instead of writing per-source loaders and per-source column-name translations.

Just queryable, schema-validated data for downstream fitting, modeling, simulation, and/or analysis.

## Motivation

I wanted easily ingestible data from public battery datasets I care about, without rewriting a loader for each one - and a quick way to see what's in the dataset

## Who this is intended for

Anyone working with public battery lab data - researchers, BMS engineers, ML practitioners, or tool authors who want one consistent format across sources instead of writing a new loader for each dataset.

Lab data only. Not for field/fleet telemetry.

## Quick start

```bash
git clone https://github.com/mihnathul/cellstore.git
cd cellstore
pip install -e ".[viewer]"                # install package + viewer deps
python examples/demo_end_to_end.py        # harmonize bundled ORNL data
streamlit run apps/viewer.py              # optional: browse in the browser
```

The demo is non-empty out of the box: the ORNL Leaf HPPC data is bundled in the repo, so the first run produces a harmonized cell + 3 tests + a parquet you can query. HNEI and MATR require their own downloads (links in the Sources table below) before the demo picks them up.

## Sources

Cell counts reflect the upstream dataset. Only the bundled source ships in the repo; others require a local download before `demo_end_to_end.py` harmonizes them.

| Source | Chemistry | Cells (upstream) | Test type | Raw data |
|---|---|---|---|---|
| ORNL Leaf | mixed (LMO/NCA pouch) | 1 | HPPC | bundled |
| HNEI | NCA (Panasonic 18650PF) | 1 | HPPC | [download](data/raw/hnei/SOURCE_DATA_PROVENANCE.md) |
| MATR (Severson 2019) | LFP (A123 18650) | 119 (124 - 5 excluded) | Fast-charge cycling | [download](data/raw/matr/SOURCE_DATA_PROVENANCE.md) |

## Query the harmonized data

Each cellstore record spans three files: cell metadata (hardware), test metadata (protocol, SOH, source provenance + license), and the timeseries parquet (V/I/T samples). They join on `cell_id` and `test_id`.

**Get all artifacts for one test** - cell + test metadata + timeseries in a single query:

```sql
SELECT c.chemistry, c.nominal_capacity_Ah,
       t.soh_pct, t.source_license, t.protocol_description,
       ts.timestamp_s, ts.voltage_V, ts.current_A, ts.temperature_C
FROM read_json('data/harmonized/cells/*.json')  c
JOIN read_json('data/harmonized/tests/*.json')  t  ON c.cell_id = t.cell_id
JOIN 'data/harmonized/timeseries.parquet'       ts ON t.test_id = ts.test_id
WHERE t.test_id = 'ORNL_LEAF_2013_HPPC_25C'
ORDER BY ts.timestamp_s;
```

**Get all tests for one cell** - list every test that ran on a given cell:

```sql
SELECT test_id, test_type, soh_pct, cycle_count_at_test,
       temperature_C_min, n_samples, source_doi
FROM read_json('data/harmonized/tests/*.json')
WHERE cell_id = 'ORNL_LEAF_2013';
```

Same patterns work from Python - `pd.read_parquet(..., filters=[...])` for predicate-pushdown reads of the parquet, or `duckdb.sql(...).df()` to run the SQL above and get a DataFrame back.

## Viewer

The Streamlit app (`streamlit run apps/viewer.py`) gives you:

- Cells and tests tables with sidebar filters (source, test type)
- Self-contained ZIP-bundle download for any selected tests (cell + test metadata + timeseries together)

## Schema

Three entities, each a separate file.

### `cell_metadata` - one JSON per cell

| Field | Type | Notes |
|---|---|---|
| `cell_id` | str, unique | e.g. `HNEI_PANASONIC_18650PF`, `MATR_B1C0` |
| `source` | enum | `ORNL`, `HNEI`, `MATR`, `CALCE`, `NASA`, `SNL` |
| `source_cell_id` | str | publisher's own ID |
| `manufacturer`, `model_number` | str | nullable |
| `chemistry` | enum | `LFP`, `NMC`, `NCA`, `LCO`, `LMO`, `LTO`, `mixed` |
| `cathode`, `anode`, `electrolyte` | str | nullable |
| `form_factor` | enum | `cylindrical`, `pouch`, `prismatic`, `coin` |
| `nominal_capacity_Ah`, `nominal_voltage_V`, `max_voltage_V`, `min_voltage_V` | float | nullable |

### `test_metadata` - one JSON per test

| Field | Type | Notes |
|---|---|---|
| `test_id` | str, unique | e.g. `HNEI_PANASONIC_18650PF_HPPC_-20C` |
| `cell_id` | str | FK to cells |
| `test_type` | enum | `cycling`, `hppc`, `gitt`, `eis`, `calendar`, `abuse`, `drive_cycle`, `checkup` |
| `temperature_C_min/max`, `temperature_step_C` | float | protocol - nullable when not documented |
| `soc_range_min/max`, `soc_step` | float | protocol - nullable |
| `c_rate_charge`, `c_rate_discharge` | float | protocol - nullable |
| `protocol_description` | str | human-readable |
| `num_cycles` | int | |
| `soh_pct` | float | cell SOH at time of test, `null` if it varies within the test |
| `soh_method` | enum | how `soh_pct` was computed: `bol_assumption` (ORNL, HNEI fresh-cell characterization), `capacity_vs_first_checkpoint` (reserved for aging checkups), `resistance_pulse` (reserved for aged HPPC), or `null` |
| `cycle_count_at_test` | int | cycles elapsed before this test; `0` = BOL |
| `n_samples`, `duration_s` | | observed at harmonize time |
| `voltage_observed_min_V`/`_max_V`, `current_observed_min_A`/`_max_A`, `temperature_observed_min_C`/`_max_C`, `sample_dt_median_s`/`_max_s` | | observed |
| `source_doi`, `source_url`, `source_citation`, `source_license`, `source_license_url` | str | provenance - upstream dataset identity, how to cite it, its license |

### `timeseries` - one Parquet for all tests

| Column | Type | Notes |
|---|---|---|
| `test_id` | str | FK; filter on this |
| `cycle_number` | int | |
| `step_number` | nullable Int64 | many sources don't expose it |
| `step_type` | enum | `charge`, `discharge`, `rest`, `pulse`, `ocv`, `unknown` |
| `timestamp_s` | float | seconds elapsed from test start |
| `voltage_V`, `current_A`, `temperature_C` | float | measured |
| `capacity_Ah`, `energy_Wh` | float | cycler passthrough; nullable |
| `flags` | str | nullable per-sample annotations (e.g. `S`=suspect, `Q`=queue transition); most samples have no flag |

Conventions: current is positive = charge, negative = discharge. SI units. Timestamps are relative. Missing data is explicit `null`/`NaN`, never a sentinel. No derived fields (no SOC) - cellstore is fit-agnostic. Validation at write time: JSON Schemas in [`schemas/`](schemas/) are authoritative; Pandera models in [`cellstore/harmonize/harmonize_schema.py`](cellstore/harmonize/harmonize_schema.py) mirror them.

## Project layout

```
cellstore/
  ingest/           one reader per source
  harmonize/        one converter per source + Pandera schema
schemas/            JSON Schema (authoritative)
examples/
  demo_end_to_end.py    ingest + harmonize + write
apps/viewer.py      Streamlit viewer
tests/              pytest smoke test
data/
  raw/<source>/     user-downloaded raw files (gitignored)
  harmonized/       pipeline output (gitignored)
```

## Add a new source

1. Create `data/raw/<source>/SOURCE_DATA_PROVENANCE.md` with download instructions, license, and citation.
2. `cellstore/ingest/<source>.py` - returns nested dicts keyed by cell/test.
3. `cellstore/harmonize/harmonize_<source>.py` - returns `{cell_metadata, cells_metadata, test_metadata, timeseries}` matching the canonical schema. Populate `soh_pct` and `cycle_count_at_test` if derivable.
4. Add the source to the `SOURCES` list in `examples/demo_end_to_end.py`.
5. Add a row to the Sources table above.

## Roadmap

- CALCE, NASA, SNL ingesters
- Per-cycle summary aggregates
- HuggingFace Datasets publication
- **SOH methodology** - current approach needs more thought and iteration to appropriately tag test datasets across tests and test types.

## License

MIT ([`LICENSE`](LICENSE)). Upstream source data retains each publisher's original license - see the per-source README in `data/raw/<source>/` for the citation.

## Acknowledgments

- Phillip Kollmeyer - Panasonic NCR18650PF data (HNEI / UW Madison)
- G. Wiggins, S. Allu, H. Wang - 2013 Nissan Leaf data (ORNL)
- K. Severson, P. Attia et al. - fast-charging dataset (Stanford / MIT / TRI)

## Citation

```bibtex
@software{cellstore,

  author = {Mihna Neerulpan},
  title  = {cellstore: Public Battery Test Dataset Harmonization with a Canonical Schema},
  year   = {2026},
  url    = {https://github.com/mihnathul/cellstore},
}
```
