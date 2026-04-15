"""
Scalability experiments — independent from main run_experiments.py.

Loops over:
  - URs 101-107 (attribute series) + 111-120 (value series, 112 aliased to 103)
  - Both modes: tvd-aa, tvd-av
  - Both methods: AM, TM
  - Three variants: Random (avg over 15 seeds), Stats Guided, All Source
  - Fixed: theta=0.8, split=random_20, rewrite_sql=False

Output: data/experiment_results/scalability/summary.csv
         data/experiment_results/scalability/steps.csv
"""

import os
import sys
import time
import json
import random
from filelock import FileLock
import pandas as pd
import pyarrow.parquet as pq

from SQL_Variants.methods.AttributeMatch import Attribute_Match
from SQL_Variants.methods.TupleMatch import Tuple_Match
from SQL_Variants.core.data_loading import load_ur
from SQL_Variants.core.duckdb_connection import get_connection, register_parquet_view
from SQL_Variants.core.utils import ur_df_to_dict, compute_ecoverage, compute_ucoverage, compute_penalty
from SQL_Variants.config.test_config import GENERAL_CONFIG

# ----------------------------------------------------------------
# Config
# ----------------------------------------------------------------
SCALABILITY_DIR = os.path.join("data", "experiment_results", "scalability")
os.makedirs(SCALABILITY_DIR, exist_ok=True)
SUMMARY_PATH = os.path.join(SCALABILITY_DIR, "summary.csv")
STEPS_PATH   = os.path.join(SCALABILITY_DIR, "steps.csv")

SPLIT_PATH   = os.path.join("data", "generated_splits", "MOVIELENS", "random_20")
SPLIT_NAME   = "random_20"
DATASET_NAME = "MOVIELENS"
THETA        = 0.8
MODES        = ["tvd-aa", "tvd-av"]
METHODS      = {"AM": Attribute_Match, "TM": Tuple_Match}
SEEDS        = GENERAL_CONFIG["seeds"]   # 15 seeds
CANONICAL_SEED = SEEDS[0]

# Attribute series: 101-107
# Value series: 111-120 (112 is alias of 103, skip 112 to avoid duplicate run)
ATTR_SERIES  = list(range(101, 108))
VALUE_SERIES = [ur_id for ur_id in range(111, 121) if ur_id != 112]
ALL_UR_IDS   = ATTR_SERIES + VALUE_SERIES   # 16 unique runs

STD_FIELDS = [
    "ecoverage_final", "ucoverage_final", "penalty_final",
    "shipping_rows_total", "shipping_time_total", "processing_time_total",
    "method_time_total", "runtime_total", "rows_final", "sources_explored",
]

STEP_COLS = [
    "mode", "UR_id", "split", "n_sources", "theta", "method", "variant",
    "step", "source_selected", "sources_explored",
    "rows_current", "ecoverage_current", "ucoverage_current", "penalty_current",
    "shipping_rows_step", "shipping_time_step", "processing_time_step",
    "shipping_rows_total", "shipping_time_total", "processing_time_total",
    "method_time_total",
]

skipped = 0
executed = 0

# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------

def load_source_csv_paths(split_folder):
    return [
        os.path.join(split_folder, f)
        for f in sorted(os.listdir(split_folder))
        if f.endswith(".csv") and f.startswith("src_")
    ]


def load_source_sizes_from_parquet(parquet_paths):
    return [pq.ParquetFile(p).metadata.num_rows for p in parquet_paths]


def load_stats(split_path):
    stats_json    = os.path.join(split_path, "value_index.json")
    stats_parquet = os.path.join(split_path, "stats.parquet")
    if not (os.path.exists(stats_json) and os.path.exists(stats_parquet)):
        return None
    with open(stats_json, "r") as f:
        value_index = json.load(f)
    df = pd.read_parquet(stats_parquet)
    return {"value_index": value_index, "source_vectors": df.values}


def load_done_keys():
    if not os.path.exists(SUMMARY_PATH):
        return set()
    df = pd.read_csv(SUMMARY_PATH)
    return set(zip(df["UR_id"], df["mode"], df["method"], df["variant"]))


def is_done(done_keys, *, ur_id, mode, method, variant):
    return (ur_id, mode, method, variant) in done_keys


def mark_done(done_keys, *, ur_id, mode, method, variant):
    done_keys.add((ur_id, mode, method, variant))


def append_row(row):
    lock = FileLock(SUMMARY_PATH + ".lock")
    with lock:
        pd.DataFrame([row]).to_csv(
            SUMMARY_PATH, mode="a",
            header=not os.path.exists(SUMMARY_PATH), index=False,
        )


def write_steps(trace, meta):
    if not trace:
        return
    rows = [{**meta, **d} for d in trace]
    df = pd.DataFrame(rows)
    for c in STEP_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[STEP_COLS]
    lock = FileLock(STEPS_PATH + ".lock")
    with lock:
        df.to_csv(STEPS_PATH, mode="a",
                  header=not os.path.exists(STEPS_PATH), index=False)


def compute_final_metrics(T_res, UR):
    if T_res is None or len(T_res) == 0:
        return 0, 0.0, 0.0, 0.0
    rows_final       = len(T_res)
    ucoverage_final  = compute_ucoverage(T_res, UR)
    ecoverage_final, _ = compute_ecoverage(T_res, UR)
    penalty_final, _   = compute_penalty(T_res, UR)
    return rows_final, ecoverage_final, ucoverage_final, penalty_final


def run_one_variant(*, con, table_names, method_name, variant_name,
                    ur_id, UR, mode, n_sources, stats_obj,
                    all_source, log_steps):
    start = time.perf_counter()
    T_res, method_info = METHODS[method_name](
        con, UR, table_names, THETA,
        stats=stats_obj, mode=mode,
        all_source=all_source, rewrite_sql=False,
        trace_enabled=log_steps,
    )
    runtime_total = time.perf_counter() - start
    rows_final, ecov, ucov, pen = compute_final_metrics(T_res, UR)
    method_time_total = method_info["shipping_time_total"] + method_info["processing_time_total"]

    row = {
        "mode":                    mode,
        "UR_id":                   ur_id,
        "split":                   SPLIT_NAME,
        "n_sources":               n_sources,
        "theta":                   THETA,
        "method":                  method_name,
        "variant":                 variant_name,
        "sources_explored":        method_info["sources_explored"],
        "shipping_time_total":     method_info["shipping_time_total"],
        "shipping_rows_total":     method_info["shipping_rows_total"],
        "processing_time_total":   method_info["processing_time_total"],
        "method_time_total":       method_time_total,
        "rows_final":              rows_final,
        "ecoverage_final":         ecov,
        "ucoverage_final":         ucov,
        "penalty_final":           pen,
        "runtime_total":           runtime_total,
    }
    return row, method_info.get("trace", [])


# ----------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------

def run_scalability():
    global skipped, executed

    print("=== Starting SCALABILITY experiments ===")
    print(f"  URs:     {ALL_UR_IDS}")
    print(f"  Modes:   {MODES}")
    print(f"  Methods: {list(METHODS.keys())}")
    print(f"  Split:   {SPLIT_NAME}  |  Theta: {THETA}\n")

    # Load split once — same for all URs
    csv_paths = load_source_csv_paths(SPLIT_PATH)
    if not csv_paths:
        raise RuntimeError(f"No CSV sources found in {SPLIT_PATH}")
    parquet_paths = [p.replace(".csv", ".parquet") for p in csv_paths]
    n_sources = len(csv_paths)

    stats = load_stats(SPLIT_PATH)

    done_keys = load_done_keys()
    t0 = time.perf_counter()

    for method_name in METHODS:
        for ur_id in ALL_UR_IDS:
            UR_df = load_ur(ur_id)
            UR    = ur_df_to_dict(UR_df)

            # Open one connection per UR (register views once)
            con = get_connection()
            table_names = []
            for i, path in enumerate(parquet_paths):
                tbl = f"src{i+1}"
                register_parquet_view(con, tbl, path)
                table_names.append(tbl)

            if stats is not None:
                stats["source_sizes"] = load_source_sizes_from_parquet(parquet_paths)

            for mode in MODES:

                # 1) Random variant (avg over seeds)
                if not is_done(done_keys, ur_id=ur_id, mode=mode, method=method_name, variant="Random"):
                    executed += 1
                    seed_results = []
                    trace_to_write = None
                    for seed in SEEDS:
                        random.seed(seed)
                        row_seed, trace_seed = run_one_variant(
                            con=con, table_names=table_names,
                            method_name=method_name, variant_name="Random",
                            ur_id=ur_id, UR=UR, mode=mode, n_sources=n_sources,
                            stats_obj=None, all_source=False,
                            log_steps=(seed == CANONICAL_SEED),
                        )
                        seed_results.append(row_seed)
                        if seed == CANONICAL_SEED:
                            trace_to_write = trace_seed

                    row = seed_results[0].copy()
                    row["n_seeds"] = len(SEEDS)
                    for f in STD_FIELDS:
                        values = [r[f] for r in seed_results]
                        row[f]            = sum(values) / len(values)
                        row[f"{f}_std"]   = float(pd.Series(values).std())

                    append_row(row)
                    meta = {"mode": mode, "UR_id": ur_id, "split": SPLIT_NAME,
                            "n_sources": n_sources, "theta": THETA,
                            "method": method_name, "variant": "Random"}
                    write_steps(trace_to_write, meta)
                    mark_done(done_keys, ur_id=ur_id, mode=mode, method=method_name, variant="Random")
                    print(f"  [DONE] UR{ur_id} | {mode} | {method_name} | Random")
                else:
                    skipped += 1

                # 2) Stats Guided
                if stats is not None:
                    if not is_done(done_keys, ur_id=ur_id, mode=mode, method=method_name, variant="Stats Guided"):
                        executed += 1
                        row_sg, trace_sg = run_one_variant(
                            con=con, table_names=table_names,
                            method_name=method_name, variant_name="Stats Guided",
                            ur_id=ur_id, UR=UR, mode=mode, n_sources=n_sources,
                            stats_obj=stats, all_source=False, log_steps=True,
                        )
                        append_row(row_sg)
                        meta = {"mode": mode, "UR_id": ur_id, "split": SPLIT_NAME,
                                "n_sources": n_sources, "theta": THETA,
                                "method": method_name, "variant": "Stats Guided"}
                        write_steps(trace_sg, meta)
                        mark_done(done_keys, ur_id=ur_id, mode=mode, method=method_name, variant="Stats Guided")
                        print(f"  [DONE] UR{ur_id} | {mode} | {method_name} | Stats Guided")
                    else:
                        skipped += 1

                # 3) All Source
                if not is_done(done_keys, ur_id=ur_id, mode=mode, method=method_name, variant="All Source"):
                    executed += 1
                    row_as, trace_as = run_one_variant(
                        con=con, table_names=table_names,
                        method_name=method_name, variant_name="All Source",
                        ur_id=ur_id, UR=UR, mode=mode, n_sources=n_sources,
                        stats_obj=None, all_source=True, log_steps=True,
                    )
                    append_row(row_as)
                    meta = {"mode": mode, "UR_id": ur_id, "split": SPLIT_NAME,
                            "n_sources": n_sources, "theta": THETA,
                            "method": method_name, "variant": "All Source"}
                    write_steps(trace_as, meta)
                    mark_done(done_keys, ur_id=ur_id, mode=mode, method=method_name, variant="All Source")
                    print(f"  [DONE] UR{ur_id} | {mode} | {method_name} | All Source")
                else:
                    skipped += 1

            con.close()

    elapsed = time.perf_counter() - t0
    print(f"\n=== Finished SCALABILITY experiments ===")
    print(f"  executed={executed}  skipped={skipped}  time={elapsed:.1f}s")
    print(f"  Summary: {SUMMARY_PATH}")


if __name__ == "__main__":
    run_scalability()
