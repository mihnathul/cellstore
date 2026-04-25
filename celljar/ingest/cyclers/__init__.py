# Generic cycler-format loaders. Each module reads a specific cycler/format
# (Arbin XLSX, Maccor CSV, BatteryArchive CSV) and returns a normalized
# Polars DataFrame using celljar's canonical column names. Per-source
# ingesters call these instead of writing custom XLSX/CSV parsing.
