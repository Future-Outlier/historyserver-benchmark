#!/usr/bin/env bash
# Copy the harness into a KubeRay checkout. It is a Go test that imports
# KubeRay's e2e support packages, so it has to live inside that module.
set -euo pipefail

KUBERAY="${1:-}"
if [ -z "$KUBERAY" ] || [ ! -d "$KUBERAY/historyserver" ]; then
  echo "usage: $0 /path/to/kuberay" >&2
  exit 1
fi

SRC="$(cd "$(dirname "$0")" && pwd)"
DEST="$KUBERAY/historyserver/test/benchmark"
mkdir -p "$DEST"
cp "$SRC"/*.go "$SRC"/run_matrix.sh "$SRC"/aggregate.py "$SRC"/README.md "$DEST/"
chmod +x "$DEST/run_matrix.sh"

echo "installed -> $DEST"
echo "next:"
echo "  cd $DEST && ./run_matrix.sh"
