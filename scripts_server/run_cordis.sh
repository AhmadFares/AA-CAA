#!/usr/bin/env bash
# Runs CORDIS experiments (URs 51-70), both tvd-aa and tvd-caa, AM only.
# Splits across 5 parallel processes by UR range.
#
# Usage (from ~/AA-CAA on the server):
#   bash scripts_server/run_cordis.sh

set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate
export PYTHONPATH="/home/slide/faresa/AA-CAA/.venv/lib/python3.11/site-packages:${PYTHONPATH:-}"

mkdir -p logs

BASE_ENV="JOB_TAG=cordis_all SPLIT_FILTER=candidates METHODS=AM MODES=tvd-aa,tvd-caa PYTHONUNBUFFERED=1"

echo "=== Launching CORDIS experiments (5 parallel, AM, tvd-aa + tvd-caa) ==="

# URs 51-54
nohup env $BASE_ENV \
    python3 -m SQL_Variants.scripts.run_experiments 51 54 \
    > logs/cordis_51_54.log 2>&1 &
echo "  [1] UR 51-54  PID $!"

# URs 55-58
nohup env $BASE_ENV \
    python3 -m SQL_Variants.scripts.run_experiments 55 58 \
    > logs/cordis_55_58.log 2>&1 &
echo "  [2] UR 55-58  PID $!"

# URs 59-62
nohup env $BASE_ENV \
    python3 -m SQL_Variants.scripts.run_experiments 59 62 \
    > logs/cordis_59_62.log 2>&1 &
echo "  [3] UR 59-62  PID $!"

# URs 63-66
nohup env $BASE_ENV \
    python3 -m SQL_Variants.scripts.run_experiments 63 66 \
    > logs/cordis_63_66.log 2>&1 &
echo "  [4] UR 63-66  PID $!"

# URs 67-70
nohup env $BASE_ENV \
    python3 -m SQL_Variants.scripts.run_experiments 67 70 \
    > logs/cordis_67_70.log 2>&1 &
echo "  [5] UR 67-70  PID $!"

echo ""
echo "Monitor with:"
echo "  tail -f logs/cordis_51_54.log"
echo "  tail -f logs/cordis_55_58.log"
echo "  tail -f logs/cordis_59_62.log"
echo "  tail -f logs/cordis_63_66.log"
echo "  tail -f logs/cordis_67_70.log"
echo ""
echo "Waiting for all to finish..."
wait
echo "=== CORDIS experiments DONE ==="
