#!/usr/bin/env bash
# Publish celljar to PyPI.
#
# Pre-requisites (one-time):
#   1. Account at https://pypi.org with 2FA enabled.
#   2. API token saved at ~/.pypirc:
#        [pypi]
#          username = __token__
#          password = pypi-...
#      (or set TWINE_USERNAME=__token__ and TWINE_PASSWORD=pypi-... in env).
#   3. (Optional) account at https://test.pypi.org with a separate token to
#      dry-run via:  bash scripts/publish_pypi.sh --test
#
# Usage:
#     bash scripts/publish_pypi.sh           # publish to PyPI
#     bash scripts/publish_pypi.sh --test    # publish to TestPyPI (dry-run)
#
# Exits non-zero on any failure.

set -euo pipefail

cd "$(dirname "$0")/.."

REPO_FLAG=""
REPO_NAME="pypi"
if [[ "${1:-}" == "--test" ]]; then
    REPO_FLAG="--repository testpypi"
    REPO_NAME="testpypi"
fi

VERSION=$(python -c "import tomllib; print(tomllib.loads(open('pyproject.toml','rb').read().decode())['project']['version'])")
echo "=== Publishing celljar $VERSION to $REPO_NAME ==="
echo

echo "=== 1/4 release_check (full safety net) ==="
bash scripts/release_check.sh

echo
echo "=== 2/4 clean dist/ ==="
rm -rf dist/

echo
echo "=== 3/4 build sdist + wheel ==="
python -m pip install --quiet --upgrade build twine
python -m build

echo
echo "=== 4/4 upload via twine ==="
python -m twine check dist/*
# shellcheck disable=SC2086
python -m twine upload $REPO_FLAG dist/*

echo
echo "Published celljar $VERSION to $REPO_NAME."
if [[ "$REPO_NAME" == "pypi" ]]; then
    echo "Verify: https://pypi.org/project/celljar/$VERSION/"
    echo "Install: pip install celljar==$VERSION"
else
    echo "Verify: https://test.pypi.org/project/celljar/$VERSION/"
    echo "Install: pip install -i https://test.pypi.org/simple/ celljar==$VERSION"
fi
