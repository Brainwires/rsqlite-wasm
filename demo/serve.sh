#!/bin/bash
# Serves the demo at http://localhost:8080/demo/
# Run from the repo root: bash demo/serve.sh
cd "$(dirname "$0")/.." || exit 1
echo "Building WASM..."
wasm-pack build --target web --out-dir pkg crates/rsqlite-wasm 2>&1
echo ""
echo "Serving at http://localhost:8080/demo/"
echo "Press Ctrl+C to stop."
python3 -m http.server 8080
