# Contributing to celljar

Issues, ideas, and PRs are welcome.

Please use [GitHub Issues](../../issues) with one of the labels below:

- **bug** - something is wrong or broken
- **enhancement** - feature ideas and improvements
- **dataset** - propose a new public dataset to harmonize (include license and a link)
- **question** - usage questions and open-ended discussion

Pull requests are welcome for any of the above. For larger changes, please open an issue first so we can align on scope before you invest time.

## Adding a new data source

1. Write `celljar/ingest/<source>.py` - reads raw files, returns nested dicts keyed by cell/test.
2. Write `celljar/harmonize/harmonize_<source>.py` - returns `HarmonizerOutput` (`{cell_metadata, cells_metadata, test_metadata, timeseries, cycle_summary?}`). Use `harmonize_ornl_leaf.py` as a template (parse / build_timeseries / build_test_metadata / orchestrator pipeline).
3. Add the source to the `SOURCES` list in `examples/demo_end_to_end.py`.
4. Add `data/raw/<source>/SOURCE_DATA_PROVENANCE.md` with download instructions, license, and citation.
5. Run `pytest` to verify schema compliance + invariants.

### Generic cycler-format helpers

If raw data is in a standard cycler format, delegate parsing rather than rewriting it:

- **Arbin / Maccor / Neware / Novonix / BaSyTec / Repower / Gamry / generic CSV / [BDF](https://battery-data-alliance.github.io/battery-data-format/)** → use [`ionworksdata`](https://github.com/ionworks/ionworksdata)
- **BatteryArchive standardized CSV** → `celljar.ingest.cyclers.batteryarchive.read_batteryarchive_csv`
- **BioLogic EC-Lab CSV** → `celljar.ingest.cyclers.biologic.read_biologic_csv`

## Running tests

```bash
source .venv/bin/activate
pytest
```

## Code style

- Python 3.9+ (matches `pyproject.toml` and CI matrix)
- Schema conventions (sign, units, nulls) live in [`README.md`](README.md#schema) and [`schemas/`](schemas/) - follow those when writing harmonizers.
- Every hardcoded value (chemistry, capacity, protocol parameters) needs a comment citing its source (paper, datasheet, Zenodo record).
- No formatter enforced yet - keep consistent with surrounding code.

## License

By contributing you agree your work is released under the MIT license.
