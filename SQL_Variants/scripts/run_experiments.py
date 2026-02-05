import os
import sys
import time
import json
import random
from filelock import FileLock
import pandas as pd
import pyarrow.parquet as pq

from SQL_Variants.methods.TupleMatch import Tuple_Match

random.seed(42)

from SQL_Variants.config.test_config import GENERAL_CONFIG
from SQL_Variants.core.data_loading import load_ur
from SQL_Variants.core.duckdb_connection import get_connection, register_parquet_view
from SQL_Variants.scripts.generate_splits import dataset_from_ur_id
from SQL_Variants.methods.AttributeMatch import Attribute_Match

from SQL_Variants.core.utils import (
    ur_df_to_dict,
    compute_ecoverage,
    compute_ucoverage,
    compute_penalty,
)

JOB_TAG = os.environ.get("JOB_TAG", "default")
RESULTS_DIR = os.path.join("data", "experiment_results", JOB_TAG)
os.makedirs(RESULTS_DIR, exist_ok=True)
SUMMARY_PATH = os.path.join(RESULTS_DIR, "summary.csv")
STEPS_PATH = os.path.join(RESULTS_DIR, "steps.csv")
CANONICAL_SEED = GENERAL_CONFIG["seeds"][0]  # log steps only for this seed in Random variant
skipped = 0
executed = 0



def t():
    return time.perf_counter()


def load_source_sizes_from_parquet(parquet_paths):
    return [pq.ParquetFile(p).metadata.num_rows for p in parquet_paths]

    
def load_source_csv_paths(split_folder):
    return [
        os.path.join(split_folder, f)
        for f in sorted(os.listdir(split_folder))
        if f.endswith(".csv") and f.startswith("src_")
    ]

def load_done_keys():
    if not os.path.exists(SUMMARY_PATH):
        return set()

    df = pd.read_csv(SUMMARY_PATH)
    df["rewrite_sql"] = df["rewrite_sql"].astype(str).str.lower().isin(["true", "1", "yes"])
    df["UR_id"] = df["UR_id"].astype(int)


    return set(
        zip(
            df["UR_id"],
            df["dataset"],
            df["split"],
            df["mode"],
            df["method"],
            df["variant"],
            df["theta"],
            df["rewrite_sql"],
        )
    )
    
def is_done(done_keys, *, ur_id, dataset, split, mode, method, variant, theta, rewrite_sql):
    return (
        ur_id,
        dataset,
        split,
        mode,
        method,
        variant,
        theta,
        bool(rewrite_sql),
    ) in done_keys

def mark_done(done_keys, *, ur_id, dataset, split, mode, method, variant, theta, rewrite_sql):
    done_keys.add((
        ur_id,
        dataset,
        split,
        mode,
        method,
        variant,
        theta,
        bool(rewrite_sql),
    ))

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
        df.to_csv(STEPS_PATH, mode="a", header=not os.path.exists(STEPS_PATH), index=False)


def load_stats(split_path):
    stats_json = os.path.join(split_path, "value_index.json")
    stats_parquet = os.path.join(split_path, "stats.parquet")

    if not (os.path.exists(stats_json) and os.path.exists(stats_parquet)):
        return None

    with open(stats_json, "r") as f:
        value_index = json.load(f)

    df = pd.read_parquet(stats_parquet)
    source_vectors = df.values
    return {"value_index": value_index, "source_vectors": source_vectors}


# def run_sql_method(method_func, UR, parquet_paths, theta, stats=None, mode="tvd-exi", trace_enabled=True,
#                    all_source=False, rewrite_sql=False):
#     con = get_connection()

#     table_names = []
#     for i, path in enumerate(parquet_paths):
#         tbl = f"src{i+1}"
#         register_parquet_view(con, tbl, path)
#         table_names.append(tbl)

#     T_result, method_info = method_func(
#         con, UR, table_names, theta,
#         stats=stats, mode=mode, trace_enabled=trace_enabled,
#         all_source=all_source,
#         rewrite_sql=rewrite_sql,
#     )

#     con.close()
#     return T_result, method_info

def run_sql_method(con, method_func, UR, table_names, theta, stats=None,
                   mode="tvd-exi", trace_enabled=True,
                   all_source=False, rewrite_sql=False):

    T_result, method_info = method_func(
        con, UR, table_names, theta,
        stats=stats, mode=mode, trace_enabled=trace_enabled,
        all_source=all_source,
        rewrite_sql=rewrite_sql,
    )
    return T_result, method_info


STEP_COLS = [
    "mode","UR_id","dataset","split","n_sources","theta","method","variant","rewrite_sql",
    "step","source_selected","sources_explored",
    "rows_current","ecoverage_current","ucoverage_current","penalty_current",
    "shipping_rows_step","shipping_time_step","processing_time_step",
    "shipping_rows_total","shipping_time_total","processing_time_total","cov_pen_calc_time_step","method_time_total",
]
METHODS = {"AM": Attribute_Match, "TM": Tuple_Match}
STD_FIELDS = ["ecoverage_final", "ucoverage_final", "penalty_final","shipping_rows_total", "shipping_time_total", "processing_time_total","method_time_total", "runtime_total", "rows_final", "sources_explored",]



def parse_n_sources(split_name, csv_paths):
    # MovieLens-style: something_120
    tail = split_name.split("_")[-1]
    if tail.isdigit():
        return int(tail)
    # TUS-style: candidates/high_penalty/... => infer from files
    return len(csv_paths)



# def iter_split_paths(dataset_name, ur_id):
#     roots = []

#     dataset_root = os.path.join("data", "generated_splits", dataset_name)
#     if os.path.isdir(dataset_root):
#         roots.append(dataset_root)

#     ur_root = os.path.join("data", "generated_splits", f"UR{ur_id}")
#     if os.path.isdir(ur_root):
#         roots.append(ur_root)

#     for root in roots:
#         for split_name in sorted(os.listdir(root)):
#             split_path = os.path.join(root, split_name)
#             if os.path.isdir(split_path):
#                 yield split_name, split_path

def iter_split_paths(dataset_name: str, ur_id: int):
    base = os.path.join("data", "generated_splits")
    ds = dataset_name.upper()

    allowed_prefixes = {
        "MOVIELENS": {"random", "skewed", "low_penalty", "high_penalty", "low_coverage"},
        "TUS": {"candidates", "low_penalty", "high_penalty", "low_coverage"},
    }

    if ds not in allowed_prefixes:
        return

    def is_allowed(split_folder_name: str) -> bool:
        SOURCE_NUMBERS = set(GENERAL_CONFIG.get("source_numbers", []))
        allowed = allowed_prefixes[ds]

        parts = split_folder_name.split("_")
        if parts[-1].isdigit() and int(parts[-1]) not in SOURCE_NUMBERS:
            return False

        if split_folder_name in allowed:
            return True

        if parts[0] in allowed:
            return True

        if len(parts) >= 2 and "_".join(parts[:2]) in allowed:
            return True

        return False


    roots = []

    if ds == "MOVIELENS":
        dataset_root = os.path.join(base, "MOVIELENS")
        if os.path.isdir(dataset_root):
            roots.append(dataset_root)

    ur_root = os.path.join(base, f"UR{ur_id}")
    if os.path.isdir(ur_root):
        roots.append(ur_root)

    for root in roots:
        for split_name in sorted(os.listdir(root)):
            split_path = os.path.join(root, split_name)
            if not os.path.isdir(split_path):
                continue
            if not is_allowed(split_name):
                continue
            yield split_name, split_path



def compute_final_metrics(T_res, UR):
    if T_res is None or len(T_res) == 0:
        return 0, 0.0, 0.0, 0.0
    rows_final = len(T_res)
    ucoverage_final = compute_ucoverage(T_res, UR)
    ecoverage_final, _ = compute_ecoverage(T_res, UR)
    penalty_final, _ = compute_penalty(T_res, UR)
    return rows_final, ecoverage_final, ucoverage_final, penalty_final


def append_row(row):
    lock = FileLock(SUMMARY_PATH + ".lock")
    with lock:
        pd.DataFrame([row]).to_csv(
        SUMMARY_PATH,
        mode="a",
        header=not os.path.exists(SUMMARY_PATH),
        index=False,
    )


def run_one_variant(
    *,
    con,
    table_names,
    method_name,
    variant_name,
    ur_id,
    UR,
    theta,
    mode,
    dataset_name,
    split_name,
    n_sources,
    stats_obj,
    all_source,
    rewrite_sql,
    log_steps=False,
):

    start = time.perf_counter()

    T_res, method_info = run_sql_method(
        con,
        METHODS[method_name],
        UR,
        table_names,
        theta,
        stats=stats_obj,
        mode=mode,
        all_source=all_source,
        rewrite_sql=rewrite_sql,
        trace_enabled=log_steps,
    )

    
    trace = method_info.get("trace", [])


    runtime_total = time.perf_counter() - start

    rows_final, ecoverage_final, ucoverage_final, penalty_final = compute_final_metrics(T_res, UR)
    method_time_total = method_info["shipping_time_total"] + method_info["processing_time_total"]

    row = {
        "mode": mode,
        "UR_id": ur_id,
        "dataset": dataset_name,
        "split": split_name,
        "n_sources": n_sources,
        "theta": theta,
        "method": method_name,
        "variant": variant_name,
        "rewrite_sql": bool(rewrite_sql),   
        "sources_explored": method_info["sources_explored"],
        "shipping_time_total": method_info["shipping_time_total"],
        "shipping_rows_total": method_info["shipping_rows_total"],
        "processing_time_total": method_info["processing_time_total"],
        "method_time_total": method_time_total,
        "rows_final": rows_final,
        "ecoverage_final": ecoverage_final,
        "ucoverage_final": ucoverage_final,
        "penalty_final": penalty_final,
        "runtime_total": runtime_total,
    }
    if log_steps:
        meta = {
            "mode": mode, "UR_id": ur_id, "dataset": dataset_name, "split": split_name,
            "n_sources": n_sources, "theta": theta, "method": method_name, "variant": variant_name,
            "rewrite_sql": bool(rewrite_sql),
        }

    return row, trace



 
def run_all_experiments(ur_subset=None):
    global executed, skipped
    print("=== Starting ALL experiments ===")
    SPLIT_FILTER = os.environ.get("SPLIT_FILTER")
    if SPLIT_FILTER:
        SPLIT_FILTER = set(SPLIT_FILTER.split(","))
    modes = GENERAL_CONFIG["modes"]
    modes_env = os.environ.get("MODES")
    if modes_env:
        modes = [m.strip() for m in modes_env.split(",") if m.strip()]
    methods = ["AM", "TM"]
    methods_env = os.environ.get("METHODS")
    if methods_env:
        methods = [m.strip() for m in methods_env.split(",") if m.strip()]                                  

    done_keys = load_done_keys()
    t0= time.perf_counter()
    #For each UR
    for method_name in methods: 
        urs = GENERAL_CONFIG["URs"]
        if ur_subset is not None:
            urs = ur_subset
        for ur_id in urs:
            UR_df = load_ur(ur_id)
            UR = ur_df_to_dict(UR_df)

            dataset_name = dataset_from_ur_id(ur_id)
            if dataset_name == "UNKNOWN":
                continue
            #For each mode{tvss-uni, tvd-exi}
            for mode in modes:
                #For each split in dataset-level and UR-level splits
                for split_name, split_path in iter_split_paths(dataset_name, ur_id):
                    if SPLIT_FILTER and split_name not in SPLIT_FILTER:
                        continue
                    csv_paths = load_source_csv_paths(split_path)
                    if len(csv_paths) == 0:
                        continue

                    parquet_paths = [p.replace(".csv", ".parquet") for p in csv_paths]
                    con = get_connection()
                    table_names = []
                    for i, path in enumerate(parquet_paths):
                        tbl = f"src{i+1}"
                        register_parquet_view(con, tbl, path)
                        table_names.append(tbl)

                    
                    n_sources = parse_n_sources(split_name, csv_paths)

                    stats = load_stats(split_path)
                    if stats is not None:
                        stats["source_sizes"] = load_source_sizes_from_parquet(parquet_paths)
                    #For each theta
                    for theta in GENERAL_CONFIG["thetas"]:
                        #for each rewrite_sql ∈ {False, True}
                        for rewrite_sql in (False, True):
                        
                            # 1) Classic (no stats)
                            classic_results = []
                            if not is_done(done_keys,
                                    ur_id=ur_id, dataset=dataset_name, split=split_name,
                                    mode=mode, method=method_name, variant="Random",
                                    theta=theta, rewrite_sql=rewrite_sql):
                                
                                executed += 1
                                trace_to_write = None
                                for seed in GENERAL_CONFIG["seeds"]:
                                
                                    random.seed(seed)

                                

                                    row_seed, trace_seed = run_one_variant(
                                        con=con,
                                        table_names=table_names,
                                        method_name=method_name,
                                        variant_name="Random",
                                        ur_id=ur_id,
                                        UR=UR,
                                        theta=theta,
                                        mode=mode,
                                        dataset_name=dataset_name,
                                        split_name=split_name,
                                        n_sources=n_sources,
                                        stats_obj=None,
                                        all_source=False,
                                        rewrite_sql=rewrite_sql,
                                        log_steps=(seed == CANONICAL_SEED),
                                    )

                                    classic_results.append(row_seed)
                                    if seed == CANONICAL_SEED:
                                        trace_to_write = trace_seed
                                if not classic_results:
                                     continue

                                row = classic_results[0].copy()
                                row["n_seeds"] = len(GENERAL_CONFIG["seeds"])

                                for f in STD_FIELDS:
                                    values = [r[f] for r in classic_results]

                                    row[f] = sum(values) / len(values)              # MEAN goes in original column
                                    row[f"{f}_std"] = float(pd.Series(values).std())  # STD goes in *_std column

                                append_row(row)
                                meta = {
                                    "mode": mode, "UR_id": ur_id, "dataset": dataset_name, "split": split_name,
                                    "n_sources": n_sources, "theta": theta, "method": method_name, "variant": "Random",
                                    "rewrite_sql": bool(rewrite_sql),
                                    }

                                if trace_to_write:
                                    write_steps(trace_to_write, meta)

                                mark_done(done_keys,
                                    ur_id=ur_id, dataset=dataset_name, split=split_name,
                                    mode=mode, method=method_name, variant="Random",
                                    theta=theta, rewrite_sql=rewrite_sql)
                            else:
                                skipped += 1

                            # 2) Stats-guided (only if stats exist)
                            if stats is not None:
                                if not is_done(done_keys,
                                        ur_id=ur_id, dataset=dataset_name, split=split_name,
                                        mode=mode, method=method_name, variant="Stats Guided",
                                        theta=theta, rewrite_sql=rewrite_sql):
                                    executed += 1
                                    row_stat, trace_stat = run_one_variant(
                                            con=con,
                                            table_names=table_names,
                                            method_name=method_name,
                                            variant_name="Stats Guided",
                                            ur_id=ur_id,
                                            UR=UR,
                                            theta=theta,
                                            mode=mode,
                                            dataset_name=dataset_name,
                                            split_name=split_name,
                                            n_sources=n_sources,
                                            stats_obj=stats,
                                            all_source=False,
                                            rewrite_sql=rewrite_sql,
                                            log_steps=True,
                                            )
                                        
                                    append_row(row_stat)
                                    meta = {
                                        "mode": mode, "UR_id": ur_id, "dataset": dataset_name, "split": split_name,
                                        "n_sources": n_sources, "theta": theta, "method": method_name, "variant": "Stats Guided",
                                        "rewrite_sql": bool(rewrite_sql),
                                        }
                                    write_steps(trace_stat, meta)
                                    mark_done(done_keys,
                                            ur_id=ur_id, dataset=dataset_name, split=split_name,
                                            mode=mode, method=method_name, variant="Stats Guided",
                                            theta=theta, rewrite_sql=rewrite_sql)
                                else:
                                    skipped += 1
                            # 3) AllSource baseline (you want it for both rewrite flags too)
                            if rewrite_sql is False: 
                                if not is_done(done_keys,
                                    ur_id=ur_id, dataset=dataset_name, split=split_name,
                                    mode=mode, method=method_name, variant="All Source",
                                    theta=theta, rewrite_sql=False):
                                    

                                    executed += 1
                                    row_all, trace_all = run_one_variant(
                                        con=con,
                                        table_names=table_names,
                                        method_name=method_name,
                                        variant_name="All Source",
                                        ur_id=ur_id,
                                        UR=UR,
                                        theta=theta,
                                        mode=mode,
                                        dataset_name=dataset_name,
                                        split_name=split_name,
                                        n_sources=n_sources,
                                        stats_obj=None,
                                        all_source=True,
                                        rewrite_sql=False,
                                        log_steps=True,
                                    )

                                    append_row(row_all)
                                    meta = {
                                        "mode": mode, "UR_id": ur_id, "dataset": dataset_name, "split": split_name,
                                        "n_sources": n_sources, "theta": theta, "method": method_name, "variant": "All Source",
                                        "rewrite_sql": False,
                                        }
                                    write_steps(trace_all, meta)
                                    mark_done(done_keys,
                                    ur_id=ur_id, dataset=dataset_name, split=split_name,
                                    mode=mode, method=method_name, variant="All Source",
                                    theta=theta, rewrite_sql=False)
                                else:
                                    skipped += 1
                    con.close()

    print("\n=== Finished ALL experiments ===")
    with open(os.path.join(RESULTS_DIR, "resume_stats.txt"), "w") as f:
        f.write(f"executed={executed}\n")
        f.write(f"skipped={skipped}\n")
    print(f"Total time: {time.perf_counter() - t0:.2f} seconds")
    print(f"Summary saved to: {SUMMARY_PATH}")

if __name__ == "__main__":
    if len(sys.argv) == 3:
        start = int(sys.argv[1])
        end = int(sys.argv[2])
        run_all_experiments(list(range(start, end + 1)))
    else:
        run_all_experiments()