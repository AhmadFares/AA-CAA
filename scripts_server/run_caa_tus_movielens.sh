#!/usr/bin/env bash
# TVD-CAA on TUS + MovieLens (URs 1-40), eps 0.02/0.05/0.10, variants: Random/SG/AllSource
# Splits into 20 parallel jobs (2 URs each) for server throughput
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate
export PYTHONPATH="/home/slide/faresa/AA-CAA/.venv/lib/python3.11/site-packages:${PYTHONPATH:-}"
mkdir -p logs

BASE="JOB_TAG=caa_tus_movielens METHODS=AM MODES=tvd-caa EPSILONS=0.02,0.05,0.10 PYTHONUNBUFFERED=1"

echo "=== TVD-CAA TUS+MovieLens (20 parallel jobs) ==="
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments  1  2 > logs/caa_tml_1_2.log   2>&1 & echo "  [ 1] UR 1-2   PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments  3  4 > logs/caa_tml_3_4.log   2>&1 & echo "  [ 2] UR 3-4   PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments  5  6 > logs/caa_tml_5_6.log   2>&1 & echo "  [ 3] UR 5-6   PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments  7  8 > logs/caa_tml_7_8.log   2>&1 & echo "  [ 4] UR 7-8   PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments  9 10 > logs/caa_tml_9_10.log  2>&1 & echo "  [ 5] UR 9-10  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 11 12 > logs/caa_tml_11_12.log 2>&1 & echo "  [ 6] UR11-12  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 13 14 > logs/caa_tml_13_14.log 2>&1 & echo "  [ 7] UR13-14  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 15 16 > logs/caa_tml_15_16.log 2>&1 & echo "  [ 8] UR15-16  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 17 18 > logs/caa_tml_17_18.log 2>&1 & echo "  [ 9] UR17-18  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 19 20 > logs/caa_tml_19_20.log 2>&1 & echo "  [10] UR19-20  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 21 22 > logs/caa_tml_21_22.log 2>&1 & echo "  [11] UR21-22  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 23 24 > logs/caa_tml_23_24.log 2>&1 & echo "  [12] UR23-24  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 25 26 > logs/caa_tml_25_26.log 2>&1 & echo "  [13] UR25-26  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 27 28 > logs/caa_tml_27_28.log 2>&1 & echo "  [14] UR27-28  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 29 30 > logs/caa_tml_29_30.log 2>&1 & echo "  [15] UR29-30  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 31 32 > logs/caa_tml_31_32.log 2>&1 & echo "  [16] UR31-32  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 33 34 > logs/caa_tml_33_34.log 2>&1 & echo "  [17] UR33-34  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 35 36 > logs/caa_tml_35_36.log 2>&1 & echo "  [18] UR35-36  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 37 38 > logs/caa_tml_37_38.log 2>&1 & echo "  [19] UR37-38  PID $!"
nohup env $BASE python3 -m SQL_Variants.scripts.run_experiments 39 40 > logs/caa_tml_39_40.log 2>&1 & echo "  [20] UR39-40  PID $!"
wait && echo "=== caa_tus_movielens DONE ==="
