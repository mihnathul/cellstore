#!/usr/bin/env bash
# Run the full safety net before publishing a release.
# Exits non-zero on the first failure.
#
# Usage:
#     bash scripts/release_check.sh
#
# Mirrors the CI matrix at the local Python.

set -euo pipefail

cd "$(dirname "$0")/.."

# Auto-activate local venv if present and no python is on PATH yet.
if [[ -f ".venv/bin/activate" ]] && ! command -v python >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi

echo "=== 1/5 pytest (full suite) ==="
python -m pytest -q

echo "=== 2/5 ruff (lint) ==="
ruff check celljar/ examples/ tests/ apps/

echo "=== 3/5 deptry (declared-deps audit) ==="
deptry .

echo "=== 4/5 jsonschema validation ==="
python - <<'PY'
import glob, json
from jsonschema import Draft202012Validator
for p in sorted(glob.glob("schemas/*.json")):
    Draft202012Validator.check_schema(json.load(open(p)))
    print(f"  OK: {p}")
PY

echo "=== 5/5 package build (sdist + wheel) ==="
python -m pip install --quiet --upgrade build
python -m build --quiet

echo
echo "All checks passed. Ready to publish."
