#!/usr/bin/env bash
# Transfers CORDIS raw data to the server and triggers split generation + experiments.
# Run this from your LOCAL machine (project root).
#
# Usage:
#   bash scripts_server/push_cordis.sh

set -euo pipefail

SERVER="aker.imag.fr"
REMOTE="~/AA-CAA"

echo "=== 1. Push code changes via git ==="
git push

echo ""
echo "=== 2. Sync raw CORDIS source files ==="
rsync -avz --progress \
    data/raw/cordis/ \
    "${SERVER}:${REMOTE}/data/raw/cordis/"

echo ""
echo "=== 3. Pull code on server + build CORDIS splits ==="
ssh "$SERVER" bash -s << 'REMOTE_SCRIPT'
set -euo pipefail
cd ~/AA-CAA
git pull
source .venv/bin/activate
echo "--- Building CORDIS split folders ---"
python scripts_server/build_cordis_splits.py
echo "--- Done ---"
REMOTE_SCRIPT

echo ""
echo "=== Transfer complete. CORDIS splits ready on server. ==="
echo "Now run: bash scripts_server/run_cordis.sh"
