#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="/Users/kirill/android_parser_service/parser_russian_version/venv/bin/python"

if [[ ! -x "$PYTHON" ]]; then
  echo "Old parser venv was not found: $PYTHON" >&2
  echo "Create a venv and install /Users/kirill/android_parser_service/parser_russian_version/requirements.txt" >&2
  exit 1
fi

SCRIPT="${1:-phone_smoke_test.py}"
if [[ "$SCRIPT" == *.py ]]; then
  shift || true
  exec "$PYTHON" "lunda_collector/$SCRIPT" "$@"
fi

exec "$PYTHON" lunda_collector/phone_smoke_test.py "$@"
