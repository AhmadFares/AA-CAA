# sql_methods/methods/method1_full_scan.py

import time
import pandas as pd
from helpers.id_utils import detect_id_column

from SQL_Variants.core.utils import (
    ur_df_to_dict,
    restrict_to_UR_columns,
    compute_overall_coverage_dict,
    compute_overall_penalty_dict,
)
from SQL_Variants.core.Algos import (
    coverage_guided_selection,
    redundancy_pruning,
)


# ------------------------------------------------------------
# OPTIONAL: choose next source using expected coverage
# ------------------------------------------------------------
def choose_next_source(remaining_sources, stats_data, already_covered):
    """
    remaining_sources: list of (index, parquet_path)
    stats_data: {
        source_vectors, value_index, UR_df
    }
    already_covered: set((col,val))
    """
    best_src = None
    best_gain = 0

    for idx, path in remaining_sources:
        gain = stats_data["expected_cov_func"](
            idx,
            stats_data["source_vectors"],
            stats_data["value_index"],
            stats_data["UR_df"],
            already_covered,
        )
        if gain > best_gain:
            best_gain = gain
            best_src = (idx, path)

    return best_src, best_gain


# ------------------------------------------------------------
# FULL SCAN (with optional stats)
# ------------------------------------------------------------
def Full_Scan(con, UR_df, table_names, theta, stats=None):

    # --- Inject required fields for stats-based selection ---
    if stats is not None:
        from SQL_Variants.core.stats import expected_delta_cov_for_source

        stats["UR_df"] = UR_df
        stats["expected_cov_func"] = expected_delta_cov_for_source

    sources_explored = 0
    shipping_time_total = 0.0
    shipping_rows_total = 0
    local_time_total = 0.0

    UR = ur_df_to_dict(UR_df)

    T = None
    id_col = None
    already_covered = set()

    # Prepare list of remaining sources
    remaining_sources = list(enumerate(table_names))

    # ------------------------------------------------------------
    # LOOP
    # ------------------------------------------------------------
    while remaining_sources:

        # ---- PICK SOURCE ----
        if stats is None:
            # Classic: FIFO
            src_idx, table_name = remaining_sources.pop(0)

        else:
            # Stats-based selection
            best, gain = choose_next_source(remaining_sources, stats, already_covered)

            if best is None or gain == 0:
                break  # no more useful sources

            src_idx, table_name = best
            # remove it from remaining
            remaining_sources = [(i, p) for (i, p) in remaining_sources if i != src_idx]

        sources_explored += 1

        # ---- SHIPPING ----
        sql_start = time.perf_counter()
        S = con.execute(f"SELECT * FROM {table_name}").fetchdf()
        sql_time = time.perf_counter() - sql_start

        shipping_time_total += sql_time
        shipping_rows_total += len(S)

        if id_col is None:
            id_col = detect_id_column(S)

        # ---- LOCAL ----
        local_start = time.perf_counter()

        S = restrict_to_UR_columns(S, UR)
        S = coverage_guided_selection(S, UR)

        if T is None:
            cols = list(UR.keys()) + ([id_col] if id_col else [])
            T = pd.DataFrame(columns=cols)

        if not S.empty:
            T = pd.concat([T, S], ignore_index=True)

        # update already_covered
        for col, vals in UR.items():
            if col not in S.columns:
                continue
            for val in vals:
                if val in S[col].values:
                    already_covered.add((col, val))

        cov, _ = compute_overall_coverage_dict(T, UR)
        pen, _ = compute_overall_penalty_dict(T, UR)

        local_time_total += time.perf_counter() - local_start

        # perfect
        if cov >= theta and pen == 0:
            T = redundancy_pruning(T, UR)
            break

    # ------------------------------------------------------------
    # FINAL pruning
    # ------------------------------------------------------------
    if T is not None and not T.empty:
        local_start = time.perf_counter()
        T = redundancy_pruning(T, UR)
        local_time_total += time.perf_counter() - local_start

    stats = {
        "sources_explored": sources_explored,
        "shipping_time_total": shipping_time_total,
        "shipping_rows_total": shipping_rows_total,
        "local_time_total": local_time_total,
    }

    return T, stats
