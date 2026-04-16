#!/usr/bin/env bash
# Runs nopen experiment in parallel (4 processes, AM only).
# nopen = no pen==0 stopping condition (coverage >= theta only).
# The stopping-condition change is already in AttributeMatch.py.
#
# Usage (from ~/AA-CAA on the server):
#   bash scripts_server/run_nopen.sh

set -euo pipefail
cd "$(dirname "$0")/.."          # ensure we are at project root (~/AA-CAA)
source .venv/bin/activate

mkdir -p logs

BASE_ENV="JOB_TAG=nopen SPLIT_FILTER=random_20,low_penalty_20,candidates,low_penalty METHODS=AM"

echo "=== Launching nopen (4 parallel processes, AM only) ==="

# URs 19-20
nohup env $BASE_ENV \
    python3 -m SQL_Variants.scripts.run_experiments 19 20 \
    > logs/nopen_AM_19_20.log 2>&1 &
echo "  [1] AM 19-20  PID $!"

# UR 21
nohup env $BASE_ENV \
    python3 -m SQL_Variants.scripts.run_experiments 21 21 \
    > logs/nopen_AM_21.log 2>&1 &
echo "  [2] AM 21     PID $!"

# UR 22
nohup env $BASE_ENV \
    python3 -m SQL_Variants.scripts.run_experiments 22 22 \
    > logs/nopen_AM_22.log 2>&1 &
echo "  [3] AM 22     PID $!"

# UR 23
nohup env $BASE_ENV \
    python3 -m SQL_Variants.scripts.run_experiments 23 23 \
    > logs/nopen_AM_23.log 2>&1 &
echo "  [4] AM 23     PID $!"

echo ""
echo "All 4 processes running. Monitor with:"
echo "  tail -f logs/nopen_AM_19_20.log"
echo "  tail -f logs/nopen_AM_21.log"
echo "  tail -f logs/nopen_AM_22.log"
echo "  tail -f logs/nopen_AM_23.log"
echo ""
echo "Waiting for all to finish..."
wait
echo "=== nopen DONE ==="
