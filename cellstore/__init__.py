"""cellstore - Harmonize battery test datasets into one canonical schema.

Currently ingests ORNL_LEAF, HNEI, and MATR into three canonical entities:
cell_metadata (JSON), test_metadata (JSON), and timeseries (parquet). Queryable,
schema-validated data for downstream fitting, modeling, simulation, and/or
analysis.
"""

__version__ = "0.1.0"
