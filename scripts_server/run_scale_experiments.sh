#!/usr/bin/env bash
# Run scalability experiments for TVD-AA on MovieLens.
#
# Experiment 1 — Source count: same 40 URs (201-240), 4 splits
#   geo_10 (10 sources), geo (60), geo_100 (100), geo_1000 (~1000)
#
# Experiment 2 — Query complexity: existing geo (60) results are reused.
#   No new runs needed — see plot_scale_query.py.
#
# Usage:
#   bash scripts_server/run_scale_experiments.sh
#
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate
mkdir -p logs

COMMON="METHODS=AM MODES=tvd-aa THETAS=0.8 PYTHONUNBUFFERED=1"

# ── geo_10 (10 sources) ────────────────────────────────────────────────────────
echo ""
echo "=== Scale: geo_10 (URs 201-240) ==="
BASE="JOB_TAG=scale_geo10 SPLIT_FILTER=geo_10 $COMMON"
for ur in $(seq 201 240); do
    nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $ur $ur \
        > logs/scale_geo10_${ur}.log 2>&1 &
done
wait && echo "--- geo_10 DONE ---"

# ── geo (60 sources — already done, skip if results exist) ────────────────────
# Results from run_all_experiments.sh (JOB_TAG=aa_geo) are reused directly.
# Uncomment below to rerun if needed:
#
# echo ""
# echo "=== Scale: geo_60 (URs 201-240) ==="
# BASE="JOB_TAG=scale_geo60 SPLIT_FILTER=geo $COMMON"
# for ur in $(seq 201 240); do
#     nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $ur $ur \
#         > logs/scale_geo60_${ur}.log 2>&1 &
# done
# wait && echo "--- geo_60 DONE ---"

# ── geo_100 (100 sources) ──────────────────────────────────────────────────────
echo ""
echo "=== Scale: geo_100 (URs 201-240) ==="
BASE="JOB_TAG=scale_geo100 SPLIT_FILTER=geo_100 $COMMON"
for start in $(seq 201 2 240); do
    end=$((start + 1))
    nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
        > logs/scale_geo100_${start}_${end}.log 2>&1 &
done
wait && echo "--- geo_100 DONE ---"

# ── geo_1000 (~1000 sources) ───────────────────────────────────────────────────
echo ""
echo "=== Scale: geo_1000 (URs 201-240) ==="
BASE="JOB_TAG=scale_geo1000 SPLIT_FILTER=geo_1000 $COMMON"
for start in $(seq 201 2 240); do
    end=$((start + 1))
    nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
        > logs/scale_geo1000_${start}_${end}.log 2>&1 &
done
wait && echo "--- geo_1000 DONE ---"

echo ""
echo "=== SCALABILITY EXPERIMENTS COMPLETE ==="
