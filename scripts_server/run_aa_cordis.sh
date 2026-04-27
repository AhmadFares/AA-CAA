#!/usr/bin/env bash
# TVD-AA on CORDIS (URs 51-70), thetas 0.4/0.6/0.8, variants: Random/SG/AllSource
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate
export PYTHONPATH="/home/slide/faresa/AA-CAA/.venv/lib/python3.11/site-packages:${PYTHONPATH:-}"
mkdir -p logs

BASE="JOB_TAG=aa_cordis SPLIT_FILTER=candidates METHODS=AM MODES=tvd-aa THETAS=0.4,0.6,0.8 PYTHONUNBUFFERED=1"

echo "=== TVD-AA CORDIS (5 parallel jobs) ==="
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 51 54 > logs/aa_cordis_51_54.log 2>&1 & echo "  [1] UR51-54  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 55 58 > logs/aa_cordis_55_58.log 2>&1 & echo "  [2] UR55-58  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 59 62 > logs/aa_cordis_59_62.log 2>&1 & echo "  [3] UR59-62  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 63 66 > logs/aa_cordis_63_66.log 2>&1 & echo "  [4] UR63-66  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 67 70 > logs/aa_cordis_67_70.log 2>&1 & echo "  [5] UR67-70  PID $!"
wait && echo "=== aa_cordis DONE ==="
