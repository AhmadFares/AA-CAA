#!/usr/bin/env bash
# Runs multi_ur_seeds experiment in parallel (4 processes).
# Each process handles a distinct (method, UR range) so there are no work overlaps.
# FileLock in run_experiments.py makes concurrent CSV writes safe.
#
# Usage (from ~/AA-CAA on the server):
#   bash scripts_server/run_multi_ur_seeds.sh

set -euo pipefail
cd "$(dirname "$0")/.."          # ensure we are at project root (~/AA-CAA)
source .venv/bin/activate

mkdir -p logs

BASE_ENV="JOB_TAG=multi_ur_seeds SPLIT_FILTER=random_20,low_penalty_20,candidates,low_penalty"

echo "=== Launching multi_ur_seeds (4 parallel processes) ==="

# AM — URs 19-21
nohup env $BASE_ENV METHODS=AM \
    python3 -m SQL_Variants.scripts.run_experiments 19 21 \
    > logs/multi_AM_19_21.log 2>&1 &
echo "  [1] AM 19-21  PID $!"

# AM — URs 22-23
nohup env $BASE_ENV METHODS=AM \
    python3 -m SQL_Variants.scripts.run_experiments 22 23 \
    > logs/multi_AM_22_23.log 2>&1 &
echo "  [2] AM 22-23  PID $!"

# TM — URs 19-21
nohup env $BASE_ENV METHODS=TM \
    python3 -m SQL_Variants.scripts.run_experiments 19 21 \
    > logs/multi_TM_19_21.log 2>&1 &
echo "  [3] TM 19-21  PID $!"

# TM — URs 22-23
nohup env $BASE_ENV METHODS=TM \
    python3 -m SQL_Variants.scripts.run_experiments 22 23 \
    > logs/multi_TM_22_23.log 2>&1 &
echo "  [4] TM 22-23  PID $!"

echo ""
echo "All 4 processes running. Monitor with:"
echo "  tail -f logs/multi_AM_19_21.log"
echo "  tail -f logs/multi_AM_22_23.log"
echo "  tail -f logs/multi_TM_19_21.log"
echo "  tail -f logs/multi_TM_22_23.log"
echo ""
echo "Waiting for all to finish..."
wait
echo "=== multi_ur_seeds DONE ==="
