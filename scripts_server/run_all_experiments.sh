#!/usr/bin/env bash
# Run ALL paper experiments: TVD-AA and TVD-CAA on MovieLens, CORDIS, MIMIC-IV.
#
# Non-LLM variants (TVD, Random, All Source) run automatically.
# LLM variants (LLM Guided, LLM Adaptive) require LLM_API_KEY.
#
# Usage:
#   bash scripts_server/run_all_experiments.sh                       # non-LLM only
#   LLM_API_KEY=gsk_... bash scripts_server/run_all_experiments.sh   # all variants
#
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate
mkdir -p logs

RUN_LLM=false
if [[ -n "${LLM_API_KEY:-}" ]]; then
    RUN_LLM=true
    echo "LLM_API_KEY detected — LLM variants will run."
else
    echo "No LLM_API_KEY set — skipping LLM variants."
fi

# ══════════════════════════════════════════════════════════════════════════════
# TVD-AA  (non-LLM: TVD, Random, All Source)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "=== TVD-AA MovieLens geo (URs 201-240) ==="
BASE="JOB_TAG=aa_geo METHODS=AM MODES=tvd-aa THETAS=0.4,0.6,0.8 SPLIT_FILTER=geo PYTHONUNBUFFERED=1"
for ur in $(seq 201 240); do
    nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $ur $ur \
        > logs/aa_geo_${ur}.log 2>&1 &
done
wait && echo "--- aa_geo DONE ---"

echo ""
echo "=== TVD-AA CORDIS (URs 301-340) ==="
BASE="JOB_TAG=aa_cordis METHODS=AM MODES=tvd-aa THETAS=0.4,0.6,0.8 SPLIT_FILTER=candidates PYTHONUNBUFFERED=1"
for start in $(seq 301 2 340); do
    end=$((start + 1))
    nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
        > logs/aa_cordis_${start}_${end}.log 2>&1 &
done
wait && echo "--- aa_cordis DONE ---"

echo ""
echo "=== TVD-AA MIMIC-IV (URs 401-440) ==="
BASE="JOB_TAG=aa_mimic METHODS=AM MODES=tvd-aa THETAS=0.4,0.6,0.8 SPLIT_FILTER=admissions PYTHONUNBUFFERED=1"
for start in $(seq 401 2 440); do
    end=$((start + 1))
    nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
        > logs/aa_mimic_${start}_${end}.log 2>&1 &
done
wait && echo "--- aa_mimic DONE ---"

# ══════════════════════════════════════════════════════════════════════════════
# TVD-CAA  (non-LLM: TVD, Random, All Source)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "=== TVD-CAA MovieLens geo (URs 201-240) ==="
BASE="JOB_TAG=caa_geo METHODS=AM MODES=tvd-caa THETAS=0.8 EPSILONS=0.05 CAA_PRUNE_VERSION=v2 SPLIT_FILTER=geo PYTHONUNBUFFERED=1"
for ur in $(seq 201 240); do
    nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $ur $ur \
        > logs/caa_geo_${ur}.log 2>&1 &
done
wait && echo "--- caa_geo DONE ---"

echo ""
echo "=== TVD-CAA CORDIS (URs 301-340) ==="
BASE="JOB_TAG=caa_cordis METHODS=AM MODES=tvd-caa THETAS=0.8 EPSILONS=0.05 CAA_PRUNE_VERSION=v2 SPLIT_FILTER=candidates PYTHONUNBUFFERED=1"
for start in $(seq 301 2 340); do
    end=$((start + 1))
    nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
        > logs/caa_cordis_${start}_${end}.log 2>&1 &
done
wait && echo "--- caa_cordis DONE ---"

echo ""
echo "=== TVD-CAA MIMIC-IV (URs 401-440) ==="
BASE="JOB_TAG=caa_mimic METHODS=AM MODES=tvd-caa THETAS=0.8 EPSILONS=0.05 CAA_PRUNE_VERSION=v2 SPLIT_FILTER=admissions PYTHONUNBUFFERED=1"
for start in $(seq 401 2 440); do
    end=$((start + 1))
    nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
        > logs/caa_mimic_${start}_${end}.log 2>&1 &
done
wait && echo "--- caa_mimic DONE ---"

# ══════════════════════════════════════════════════════════════════════════════
# LLM variants (optional — requires LLM_API_KEY)
# ══════════════════════════════════════════════════════════════════════════════

if [[ "$RUN_LLM" == true ]]; then

    LLM="LLM_BACKEND=groq LLM_API_KEY=${LLM_API_KEY} PYTHONUNBUFFERED=1"

    echo ""
    echo "=== LLM TVD-AA MovieLens geo (URs 201-240) ==="
    BASE="JOB_TAG=llm_aa_geo METHODS=AM MODES=tvd-aa THETAS=0.8 SPLIT_FILTER=geo $LLM"
    for start in $(seq 201 2 240); do
        end=$((start + 1))
        nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
            > logs/llm_aa_geo_${start}_${end}.log 2>&1 &
    done
    wait && echo "--- llm_aa_geo DONE ---"

    echo ""
    echo "=== LLM TVD-AA CORDIS (URs 301-340) ==="
    BASE="JOB_TAG=llm_aa_cordis METHODS=AM MODES=tvd-aa THETAS=0.8 SPLIT_FILTER=candidates $LLM"
    for start in $(seq 301 2 340); do
        end=$((start + 1))
        nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
            > logs/llm_aa_cordis_${start}_${end}.log 2>&1 &
    done
    wait && echo "--- llm_aa_cordis DONE ---"

    echo ""
    echo "=== LLM TVD-AA MIMIC-IV (URs 401-440) ==="
    BASE="JOB_TAG=llm_aa_mimic METHODS=AM MODES=tvd-aa THETAS=0.8 SPLIT_FILTER=admissions $LLM"
    for start in $(seq 401 2 440); do
        end=$((start + 1))
        nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
            > logs/llm_aa_mimic_${start}_${end}.log 2>&1 &
    done
    wait && echo "--- llm_aa_mimic DONE ---"

    echo ""
    echo "=== LLM TVD-CAA MovieLens geo (URs 201-240) ==="
    BASE="JOB_TAG=llm_caa_geo METHODS=AM MODES=tvd-caa THETAS=0.8 EPSILONS=0.05 CAA_PRUNE_VERSION=v2 SPLIT_FILTER=geo $LLM"
    for start in $(seq 201 2 240); do
        end=$((start + 1))
        nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
            > logs/llm_caa_geo_${start}_${end}.log 2>&1 &
    done
    wait && echo "--- llm_caa_geo DONE ---"

    echo ""
    echo "=== LLM TVD-CAA CORDIS (URs 301-340) ==="
    BASE="JOB_TAG=llm_caa_cordis METHODS=AM MODES=tvd-caa THETAS=0.8 EPSILONS=0.05 CAA_PRUNE_VERSION=v2 SPLIT_FILTER=candidates $LLM"
    for start in $(seq 301 2 340); do
        end=$((start + 1))
        nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
            > logs/llm_caa_cordis_${start}_${end}.log 2>&1 &
    done
    wait && echo "--- llm_caa_cordis DONE ---"

    echo ""
    echo "=== LLM TVD-CAA MIMIC-IV (URs 401-440) ==="
    BASE="JOB_TAG=llm_caa_mimic METHODS=AM MODES=tvd-caa THETAS=0.8 EPSILONS=0.05 CAA_PRUNE_VERSION=v2 SPLIT_FILTER=admissions $LLM"
    for start in $(seq 401 2 440); do
        end=$((start + 1))
        nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments $start $end \
            > logs/llm_caa_mimic_${start}_${end}.log 2>&1 &
    done
    wait && echo "--- llm_caa_mimic DONE ---"

fi

echo ""
echo "=== ALL EXPERIMENTS COMPLETE ==="
