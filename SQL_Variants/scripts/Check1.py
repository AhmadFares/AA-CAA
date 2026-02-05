import os
import time
import json
import pandas as pd
import pyarrow.parquet as pq
import random
random.seed(42)

from SQL_Variants.config.test_config import GENERAL_CONFIG
from SQL_Variants.core.data_loading import load_ur
from SQL_Variants.core.duckdb_connection import get_connection, register_parquet_view

# Choose the method you want to test:
from SQL_Variants.methods.AttributeMatch import Attribute_Match
from SQL_Variants.methods.TupleMatch import Tuple_Match

from SQL_Variants.core.utils import (
    ur_df_to_dict,
    compute_ecoverage,
    compute_ucoverage,
    compute_penalty,
)



def t():
    return time.perf_counter()

def load_source_csv_paths(split_folder):
    return [
        os.path.join(split_folder, f)
        for f in sorted(os.listdir(split_folder))
        if f.endswith(".csv")
    ]
    
import os

def resolve_split_path(
    base_path: str,        # e.g. "/Users/faresa/Desktop/TVD/data/generated_splits"
    dataset_name: str,     # "MOVIELENS" or "TUS"
    ur_id: int,
    split_name: str,       # "random", "skewed", "high_pen", "low_cov", "candidates", ...
    source_number: int,
) -> str:
    dataset = dataset_name.upper()

    # --- dataset-level splits (MovieLens only) ---
    if dataset == "MOVIELENS" and split_name in ("random", "skewed"):
        # data/generated_splits/MOVIELENS/random_10
        return os.path.join(base_path, "MOVIELENS", f"{split_name}_{source_number}")

    # --- UR-level root (all datasets) ---
    ur_root = os.path.join(base_path, f"UR{ur_id}")

    # MovieLens UR-level: high_pen_120, low_cov_5, ...
    if dataset == "MOVIELENS":
        return os.path.join(ur_root, f"{split_name}_{source_number}")

    # TUS UR-level: candidates/, high_penalty/, low_penalty/, low_coverage/
    if dataset == "TUS":
        tus_map = {
            "candidates": "candidates",
            "high_pen": "high_penalty",
            "high_penalty": "high_penalty",
            "low_pen": "low_penalty",
            "low_penalty": "low_penalty",
            "low_cov": "low_coverage",
            "low_coverage": "low_coverage",
        }
        if split_name not in tus_map:
            raise ValueError(
                f"Unknown TUS split '{split_name}'. Use one of: {sorted(tus_map)}"
            )
        return os.path.join(ur_root, tus_map[split_name])

    # fallback (UR-level with suffix)
    return os.path.join(ur_root, f"{split_name}_{source_number}")


def load_source_sizes_from_parquet(parquet_paths):
    return [pq.ParquetFile(p).metadata.num_rows for p in parquet_paths]


def load_stats(split_path):
    stats_json = os.path.join(split_path, "value_index.json")
    stats_parquet = os.path.join(split_path, "stats.parquet")

    if not (os.path.exists(stats_json) and os.path.exists(stats_parquet)):
        return None

    with open(stats_json, "r") as f:
        value_index = json.load(f)

    df = pd.read_parquet(stats_parquet)
    source_vectors = df.values  # np.ndarray [n_sources, n_values]

    return {"value_index": value_index, "source_vectors": source_vectors}

def run_sql_method_once(method_func, UR, parquet_paths, theta, stats=None, mode="tvd-exi", all_source = True, rewrite_sql = False):
    
    t0 = t()
    con = get_connection()
    t1 = t()

    table_names = []
    for i, path in enumerate(parquet_paths):
        tbl = f"src{i+1}"
        register_parquet_view(con, tbl, path)
        table_names.append(tbl)
    t2 = t()

    T_result, method_info = method_func(con, UR, table_names, theta, stats=stats, mode=mode, all_source=all_source, rewrite_sql=rewrite_sql)
    t3 = t()

    con.close()
    t4 = t()

    print(f"DB open:         {t1 - t0:0.4f}s")
    print(f"Register views:  {t2 - t1:0.4f}s")
    print(f"Method runtime:  {t3 - t2:0.4f}s")
    print(f"DB close:        {t4 - t3:0.4f}s")

    return T_result, method_info


# ------------------------------------------------------------
# SINGLE RUN (EDIT THESE)
# ------------------------------------------------------------
DATASET_NAME = "TUS"
BASE_SPLITS = "/Users/faresa/Desktop/TVD/data/generated_splits"
UR_ID = GENERAL_CONFIG["URs"][29]         
THETA = GENERAL_CONFIG["thetas"][0]      
SPLIT_NAME = GENERAL_CONFIG["source_splits"][2] 
SOURCE_NUMBER = GENERAL_CONFIG["source_numbers"][1]
USE_STATS = False      
MODE = "tvd-uni"
METHODS = {
  "AM": Attribute_Match,
  "TM": Tuple_Match,
}   


def main():
    # --- load UR ---
    UR_df = load_ur(UR_ID)
    UR = ur_df_to_dict(UR_df)

    # --- pick split folder (dataset-level) ---
    
    
    # --- pick split folder (dataset-level or UR-level) ---
    dataset_root = os.path.join("data", "generated_splits", DATASET_NAME)
    ur_root = os.path.join("data", "generated_splits", f"UR{UR_ID}")

    split_name = f"{SPLIT_NAME}_{SOURCE_NUMBER}"

    # dataset-level splits
    if SPLIT_NAME in ("random", "skewed"):
        root = dataset_root
    # UR-level splits
    else:
        root = ur_root

    split_path = resolve_split_path(
    BASE_SPLITS,
    DATASET_NAME,
    UR_ID,
    SPLIT_NAME,
    SOURCE_NUMBER,
)
    if not os.path.isdir(split_path):
        raise FileNotFoundError(f"Split folder not found: {split_path}")

    # --- load sources (.parquet paths from .csv list) ---
    csv_paths = load_source_csv_paths(split_path)
    if not csv_paths:
        raise RuntimeError(f"No CSV sources in split: {split_path}")

    parquet_paths = [p.replace(".csv", ".parquet") for p in csv_paths]
    missing = [p for p in parquet_paths if not os.path.exists(p)]
    if missing:
        raise FileNotFoundError(f"Missing parquet files (first 5 shown): {missing[:5]}")

    # --- load stats (optional) ---
    stats = load_stats(split_path) if USE_STATS else None

    if stats is not None:
        stats["source_sizes"] = load_source_sizes_from_parquet(parquet_paths)


    if USE_STATS and stats is None:
        raise FileNotFoundError(
            f"USE_STATS=True but stats files not found in split:\n  {split_path}\n"
            f"Expected: value_index.json + stats.parquet"
        )

    print("=== SINGLE RUN ===")
    print(f"UR_ID:      {UR_ID}")
    print(f"Dataset:    {DATASET_NAME}")
    print(f"Split:      {split_name}")
    print(f"Theta:      {THETA}")
    print(f"Mode:       {MODE}")
    print(f"#Sources:   {len(parquet_paths)}")
    print(f"Stats:      {'ON' if stats is not None else 'OFF'}")



    # --- run method ---
    T_res, info = run_sql_method_once(METHODS["TM"], UR, parquet_paths, THETA, stats=stats, mode=MODE, all_source=True, rewrite_sql=False)


    # --- metrics ---
    rows_final = 0 if (T_res is None) else len(T_res)
    if rows_final:
        ucov_final = compute_ucoverage(T_res, UR)
        ecov_final, _ = compute_ecoverage(T_res, UR)
        pen_final, _ = compute_penalty(T_res, UR)
    else:
        ucov_final, ecov_final, pen_final = 0, 0, 0

    print("\n=== RESULT ===")
    print(f"Mode:       {MODE}")
    print(f"rows_final:          {rows_final}")
    print(f"UCoverage:  {ucov_final:0.4f}")
    print(f"ECoverage:  {ecov_final:0.4f}")
    print(f"penalty_final:       {pen_final}")
    print(f"sources_explored:    {info.get('sources_explored')}")
    print(f"shipping_time_total: {info.get('shipping_time_total'):0.4f}s")
    print(f"processing_time_total:    {info.get('processing_time_total'):0.4f}s")
    print(f"shipping_rows_total: {info.get('shipping_rows_total')}")

    


    if rows_final:
        print("\nT_res.head():")
        print(T_res)


if __name__ == "__main__":
    main()
