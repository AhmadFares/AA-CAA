#!/usr/bin/env bash
# Syncs results from server back to local machine.
# Run this from your LOCAL machine (not the server).
#
# Usage:
#   bash scripts_server/sync_results.sh

SERVER="aker.imag.fr"
REMOTE_DIR="~/AA-CAA/results/"
LOCAL_DIR="$(dirname "$0")/../results/"

echo "=== Syncing results from $SERVER ==="
rsync -avz --progress \
    "${SERVER}:${REMOTE_DIR}" \
    "${LOCAL_DIR}"
echo "=== Sync complete ==="
