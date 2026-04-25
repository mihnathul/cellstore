# Ecker 2015 - Kokam SLPB75106100 NMC parameterization

## Citation

Ecker, M., Tran, T. K. D., Dechent, P., Käbitz, S., Warnecke, A., & Sauer, D. U. (2015).
**Parameterization of a Physico-Chemical Model of a Lithium-Ion Battery I. Determination of Parameters.**
*Journal of The Electrochemical Society* 162 (9): A1836-A1848.
DOI: [10.1149/2.0551509jes](https://doi.org/10.1149/2.0551509jes)

Companion paper:
Ecker, M., et al. (2015). **Parameterization of a Physico-Chemical Model II. Validation.**
*J. Electrochem. Soc.* 162 (9): A1849-A1857. DOI: [10.1149/2.0573509jes](https://doi.org/10.1149/2.0573509jes)

## Cell

- **Manufacturer:** Kokam
- **Model:** SLPB75106100 (NMC pouch, ~7.5 Ah nominal)
- **Chemistry:** NMC / graphite

## Why this dataset matters

One of the most-cited public reference datasets for **DFN / SPM model parameterization**.
Bundled with PyBaMM as a default Kokam parameter set. Provides:

- **HPPC** at multiple temperatures
- **GITT** for diffusion coefficient extraction (D_s,n / D_s,p)
- **Half-cell measurements** for electrode-specific OCP
- **EIS** spectra
- Capacity tests at multiple C-rates

## Download

Raw data is **not in a single canonical public download** - Ecker's data is split
across the two papers' Supporting Information PDFs (tabular form) and the
PyBaMM-vendored CSVs.

Recommended source: the [PyBaMM `Ecker2015` parameter set](https://github.com/pybamm-team/PyBaMM/tree/develop/pybamm/input/parameters/lithium_ion/Ecker2015)
which extracted the published values into structured CSVs.

```bash
# Pseudo-step until automated:
git clone https://github.com/pybamm-team/PyBaMM.git
cp -r PyBaMM/pybamm/input/parameters/lithium_ion/Ecker2015 data/raw/ecker_2015/
```

## License

Source CSVs from PyBaMM are BSD-3-Clause. The underlying data from Ecker et al.'s
JES papers is © The Electrochemical Society - reproduced under fair use for
research / parameterization. Cite both papers when using.

## Status in celljar

**v0.3 - placeholder.** Ingester and harmonizer scaffolds exist using the
canonical pipeline pattern (see `celljar/ingest/ecker_2015.py` and
`celljar/harmonize/harmonize_ecker_2015.py`); raw data has to be downloaded
locally and the format mappings finalized once Ecker's CSVs are checked in.
