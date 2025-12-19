import os
import time
import pandas as pd

from SQL_Variants.config.test_config import GENERAL_CONFIG
from SQL_Variants.core.data_loading import load_ur_and_base_table
from SQL_Variants.core.duckdb_connection import get_connection, register_parquet_view

from SQL_Variants.methods.Method1_Full_Scan import Full_Scan

from SQL_Variants.core.utils import (
    compute_overall_coverage_df,
    compute_overall_penalty_df,
)

RESULTS_DIR = os.path.join("data", "experiment_results")
os.makedirs(RESULTS_DIR, exist_ok=True)

SUMMARY_PATH = os.path.join(RESULTS_DIR, "summary.csv")


def t():
    return time.perf_counter()


def load_source_csv_paths(split_folder):
    return [
        os.path.join(split_folder, f)
        for f in sorted(os.listdir(split_folder))
        if f.endswith(".csv")
    ]


def load_stats(split_path):
    stats_json = os.path.join(split_path, "value_index.json")
    stats_parquet = os.path.join(split_path, "stats.parquet")

    if not (os.path.exists(stats_json) and os.path.exists(stats_parquet)):
        return None

    import json

    with open(stats_json, "r") as f:
        value_index = json.load(f)

    df = pd.read_parquet(stats_parquet)
    source_vectors = df.values

    return {"value_index": value_index, "source_vectors": source_vectors}


def run_sql_method(method_func, UR_df, parquet_paths, theta, stats=None):
    t0 = t()
    con = get_connection()
    t1 = t()

    table_names = []
    for i, path in enumerate(parquet_paths):
        tbl = f"src{i+1}"
        register_parquet_view(con, tbl, path)
        table_names.append(tbl)
    t2 = t()

    T_result, method_info = method_func(con, UR_df, table_names, theta, stats=stats)
    t3 = t()

    con.close()
    t4 = t()

    print(f"      DB open: {t1 - t0:0.4f}s")
    print(f"      Register views: {t2 - t1:0.4f}s")
    print(f"      Method runtime: {t3 - t2:0.4f}s")
    print(f"      DB close: {t4 - t3:0.4f}s")

    return T_result, method_info


def run_all_experiments():
    # start fresh summary
    # if os.path.exists(SUMMARY_PATH):
    #     os.remove(SUMMARY_PATH)

    for ur_id in GENERAL_CONFIG["URs"]:
        T_base, UR_df = load_ur_and_base_table(ur_id)
        dataset_name = "MATHE"

        # ------------------------------------------
        # 1. DATASET-LEVEL SPLITS
        # ------------------------------------------
        dataset_root = os.path.join("data", "generated_splits", dataset_name)

        for split_name in sorted(os.listdir(dataset_root)):
            # if ("low_penalty" not in split_name) and ("low_coverage" not in split_name):
            #     continue

            split_path = os.path.join(dataset_root, split_name)
            if not os.path.isdir(split_path):
                continue

            csv_paths = load_source_csv_paths(split_path)
            if len(csv_paths) == 0:
                continue

            parquet_paths = [p.replace(".csv", ".parquet") for p in csv_paths]

            try:
                n_sources = int(split_name.split("_")[-1])
            except Exception:
                n_sources = len(csv_paths)

            # pre-load stats for this split (may be None)
            stats = load_stats(split_path)

            for theta in GENERAL_CONFIG["thetas"]:
                # ------------------ classic Full_Scan (no stats) ------------------
                start = time.time()
                T_res, method_info = run_sql_method(
                    Full_Scan, UR_df, parquet_paths, theta, stats=None
                )
                runtime_total = time.time() - start

                rows_final = len(T_res)
                coverage_final, _ = compute_overall_coverage_df(T_res, UR_df)
                penalty_final, _ = compute_overall_penalty_df(T_res, UR_df)
                method_time_total = (
                    method_info["shipping_time_total"] + method_info["local_time_total"]
                )

                row = {
                    "UR_id": ur_id,
                    "dataset": dataset_name,
                    "split": split_name,
                    "n_sources": n_sources,
                    "theta": theta,
                    "method": "method1_full_scan",
                    "sources_explored": method_info["sources_explored"],
                    "shipping_time_total": method_info["shipping_time_total"],
                    "shipping_rows_total": method_info["shipping_rows_total"],
                    "local_time_total": method_info["local_time_total"],
                    "method_time_total": method_time_total,
                    "rows_final": rows_final,
                    "coverage_final": coverage_final,
                    "penalty_final": penalty_final,
                    "runtime_total": runtime_total,
                }

                pd.DataFrame([row]).to_csv(
                    SUMMARY_PATH,
                    mode="a",
                    header=not os.path.exists(SUMMARY_PATH),
                    index=False,
                )

                print(
                    f"[OK] (classic) UR={ur_id}, split={split_name}, theta={theta}, "
                    f"rows={rows_final}, explored={method_info['sources_explored']}"
                )

                # ------------------ stats-guided Full_Scan ------------------
                if stats is not None:
                    start = time.time()
                    T_res_stat, method_info_stat = run_sql_method(
                        Full_Scan, UR_df, parquet_paths, theta, stats=stats
                    )
                    runtime_total_stat = time.time() - start

                    if T_res_stat is None:
                        rows_final_stat = 0
                        coverage_final_stat = 0
                        penalty_final_stat = 0
                        method_time_total_stat = (
                            method_info_stat["shipping_time_total"]
                            + method_info_stat["local_time_total"]
                        )
                    else:
                        rows_final_stat = len(T_res_stat)
                        coverage_final_stat, _ = compute_overall_coverage_df(
                            T_res_stat, UR_df
                        )
                        penalty_final_stat, _ = compute_overall_penalty_df(
                            T_res_stat, UR_df
                        )
                        method_time_total_stat = (
                            method_info_stat["shipping_time_total"]
                            + method_info_stat["local_time_total"]
                        )

                    row_stat = {
                        "UR_id": ur_id,
                        "dataset": dataset_name,
                        "split": split_name,
                        "n_sources": n_sources,
                        "theta": theta,
                        "method": "method1_full_scan_stats",
                        "sources_explored": method_info_stat["sources_explored"],
                        "shipping_time_total": method_info_stat["shipping_time_total"],
                        "shipping_rows_total": method_info_stat["shipping_rows_total"],
                        "local_time_total": method_info_stat["local_time_total"],
                        "method_time_total": method_time_total_stat,
                        "rows_final": rows_final_stat,
                        "coverage_final": coverage_final_stat,
                        "penalty_final": penalty_final_stat,
                        "runtime_total": runtime_total_stat,
                    }

                    pd.DataFrame([row_stat]).to_csv(
                        SUMMARY_PATH,
                        mode="a",
                        header=False,
                        index=False,
                    )

                    print(
                        f"[OK] (stats)   UR={ur_id}, split={split_name}, theta={theta}, "
                        f"rows={rows_final_stat}, explored={method_info_stat['sources_explored']}"
                    )

        # ------------------------------------------
        # 2. UR-DEPENDENT SPLITS
        # ------------------------------------------
        ur_root = os.path.join("data", "generated_splits", f"UR{ur_id}")

        if os.path.isdir(ur_root):
            for split_name in sorted(os.listdir(ur_root)):
                # if ("low_penalty" not in split_name) and (
                #     "low_coverage" not in split_name
                # ):
                #     continue
                split_path = os.path.join(ur_root, split_name)
                if not os.path.isdir(split_path):
                    continue

                csv_paths = load_source_csv_paths(split_path)
                if len(csv_paths) == 0:
                    continue

                parquet_paths = [p.replace(".csv", ".parquet") for p in csv_paths]

                try:
                    n_sources = int(split_name.split("_")[-1])
                except Exception:
                    n_sources = len(csv_paths)

                stats = load_stats(split_path)

                for theta in GENERAL_CONFIG["thetas"]:
                    # -------- classic --------
                    start = time.time()
                    T_res, method_info = run_sql_method(
                        Full_Scan, UR_df, parquet_paths, theta, stats=None
                    )
                    runtime_total = time.time() - start

                    rows_final = len(T_res)
                    coverage_final, _ = compute_overall_coverage_df(T_res, UR_df)
                    penalty_final, _ = compute_overall_penalty_df(T_res, UR_df)
                    method_time_total = (
                        method_info["shipping_time_total"]
                        + method_info["local_time_total"]
                    )

                    row = {
                        "UR_id": ur_id,
                        "dataset": dataset_name,
                        "split": split_name,
                        "n_sources": n_sources,
                        "theta": theta,
                        "method": "method1_full_scan",
                        "sources_explored": method_info["sources_explored"],
                        "shipping_time_total": method_info["shipping_time_total"],
                        "shipping_rows_total": method_info["shipping_rows_total"],
                        "local_time_total": method_info["local_time_total"],
                        "method_time_total": method_time_total,
                        "rows_final": rows_final,
                        "coverage_final": coverage_final,
                        "penalty_final": penalty_final,
                        "runtime_total": runtime_total,
                    }

                    pd.DataFrame([row]).to_csv(
                        SUMMARY_PATH,
                        mode="a",
                        header=not os.path.exists(SUMMARY_PATH),
                        index=False,
                    )

                    print(
                        f"[OK] (classic) UR={ur_id}, split={split_name}, theta={theta}, "
                        f"rows={rows_final}, explored={method_info['sources_explored']}"
                    )

                    # -------- stats-guided --------
                    if stats is not None:
                        start = time.time()
                        T_res_stat, method_info_stat = run_sql_method(
                            Full_Scan, UR_df, parquet_paths, theta, stats=stats
                        )
                        runtime_total_stat = time.time() - start

                        if T_res_stat is None:
                            rows_final_stat = 0
                            coverage_final_stat = 0
                            penalty_final_stat = 0
                            method_time_total_stat = (
                                method_info_stat["shipping_time_total"]
                                + method_info_stat["local_time_total"]
                            )
                        else:
                            rows_final_stat = len(T_res_stat)
                            coverage_final_stat, _ = compute_overall_coverage_df(
                                T_res_stat, UR_df
                            )
                            penalty_final_stat, _ = compute_overall_penalty_df(
                                T_res_stat, UR_df
                            )
                            method_time_total_stat = (
                                method_info_stat["shipping_time_total"]
                                + method_info_stat["local_time_total"]
                            )

                        row_stat = {
                            "UR_id": ur_id,
                            "dataset": dataset_name,
                            "split": split_name,
                            "n_sources": n_sources,
                            "theta": theta,
                            "method": "method1_full_scan_stats",
                            "sources_explored": method_info_stat["sources_explored"],
                            "shipping_time_total": method_info_stat[
                                "shipping_time_total"
                            ],
                            "shipping_rows_total": method_info_stat[
                                "shipping_rows_total"
                            ],
                            "local_time_total": method_info_stat["local_time_total"],
                            "method_time_total": method_time_total_stat,
                            "rows_final": rows_final_stat,
                            "coverage_final": coverage_final_stat,
                            "penalty_final": penalty_final_stat,
                            "runtime_total": runtime_total_stat,
                        }

                        pd.DataFrame([row_stat]).to_csv(
                            SUMMARY_PATH,
                            mode="a",
                            header=False,
                            index=False,
                        )

                        print(
                            f"[OK] (stats)   UR={ur_id}, split={split_name}, theta={theta}, "
                            f"rows={rows_final_stat}, explored={method_info_stat['sources_explored']}"
                        )

    print("\n=== Finished ALL experiments ===")
    print(f"Summary saved to: {SUMMARY_PATH}")


if __name__ == "__main__":
    run_all_experiments()
