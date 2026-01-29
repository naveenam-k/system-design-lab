#!/usr/bin/env bash
set -euo pipefail

# Clean all runtime data (use when you want a fresh start)
RUNTIME_BASE="/runtime/shared/system-design-lab/top_k_user_aggregation"

echo "Stopping containers..."
docker compose down 2>/dev/null || true

echo "Removing runtime data at $RUNTIME_BASE ..."
sudo rm -rf "$RUNTIME_BASE"/*

echo "Recreating directories..."
./init-runtime-dirs.sh

echo "Done. Run 'docker compose up --build' to start fresh."
