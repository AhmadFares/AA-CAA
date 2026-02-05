import time
import pandas as pd
import random

from helpers.id_utils import detect_id_column

from SQL_Variants.core.utils import (
    compute_ucoverage,
    restrict_to_UR_columns,
    compute_ecoverage,
    compute_penalty,
)

from SQL_Variants.core.Algos import EPrune, UPrune
from SQL_Variants.core.sql_builders import reformualte_sql, construct_TM_sql
from SQL_Variants.core.stats import EPenaltyFree, ETuples, TIndexAllLevels


# ------------------------------------------------------------
# Source selection for Tuple Match (ONLY PenaltyFree + ETuples)
# ------------------------------------------------------------
def SourceSelectionTM(UR, remaining_sources, stats):
    """
    remaining_sources: list of (src_idx, table_name)

    Pick argmax EPenaltyFree, tie-break by min ETuples.
    If best EPenaltyFree <= 0 => return None (no promising source).
    """
    if not remaining_sources:
        return None, 0

    scores = []
    for src_idx, table_name in remaining_sources:
        pf = EPenaltyFree(UR, src_idx, stats)
        scores.append(((src_idx, table_name), pf))

    best_pf = max(pf for _, pf in scores)
    if best_pf <= 0:
        return None, 0

    C = [s for (s, pf) in scores if pf == best_pf]

    # tie-break: maximize expected shipped tuples
    if len(C) > 1:
        et = [(s, ETuples(UR, s[0], stats)) for s in C]
        best_et = max(v for _, v in et)
        C = [s for (s, v) in et if v == best_et]

    return C[0], best_pf



# ------------------------------------------------------------
# Full Match Scan 
# ------------------------------------------------------------
def Tuple_Match(
    con, UR, table_names, theta,
    stats=None, mode="tvd-exi", trace_enabled=True,
    all_source=False, rewrite_sql=False,
):
    if all_source:
        assert stats is None, "All-Source must not be used with stats"

    sources_explored = 0
    shipping_time_total = 0.0
    shipping_rows_total = 0
    processing_time_total = 0.0
    pen = 0.0
    cov = 0.0

    # ---- STEP TRACE ----
    trace = []
    prev_ship_t = 0.0
    prev_ship_rows = 0
    prev_proc_t = 0.0

    T = None
    T_index = TIndexAllLevels(UR) if mode == "tvd-uni" else None
    id_col = None

    remaining_sources = list(enumerate(table_names))
    where_clause = construct_TM_sql(UR)


    # ---------------- Helper: record one step (after totals + T updated) ----------------
    def record_step(
        source_label,
        *,
        step_override=None,
        ship_rows_step_override=None,
        ship_time_step_override=None,
        proc_time_step_override=None,
    ):
        nonlocal prev_ship_t, prev_ship_rows, prev_proc_t

        step_val = sources_explored if step_override is None else step_override
        if not trace_enabled:
            return 

        rows_current = len(T) if (T is not None) else 0
        if T is not None and not T.empty:
            pen_current, _ = compute_penalty(T, UR)
            ucov_current = compute_ucoverage(T, UR)
            ecov_current, _ = compute_ecoverage(T, UR)
        else:
            pen_current = 0.0
            ucov_current = 0.0
            ecov_current = 0.0

        ship_time_step = shipping_time_total - prev_ship_t
        ship_rows_step = shipping_rows_total - prev_ship_rows
        proc_time_step = processing_time_total - prev_proc_t

        # overrides (used for prune row)
        if ship_rows_step_override is not None:
            ship_rows_step = ship_rows_step_override
        if ship_time_step_override is not None:
            ship_time_step = ship_time_step_override
        if proc_time_step_override is not None:
            proc_time_step = proc_time_step_override

        trace.append({
            "step": step_val,
            "source_selected": source_label,
            "sources_explored": sources_explored,

            "rows_current": rows_current,
            "ecoverage_current": float(ecov_current),
            "ucoverage_current": float(ucov_current),
            "penalty_current": float(pen_current),

            "shipping_rows_step": int(ship_rows_step),
            "shipping_time_step": float(ship_time_step),
            "processing_time_step": float(proc_time_step),

            "shipping_rows_total": int(shipping_rows_total),
            "shipping_time_total": float(shipping_time_total),
            "processing_time_total": float(processing_time_total),
            "method_time_total": float(shipping_time_total + processing_time_total),
        })

        prev_ship_t = shipping_time_total
        prev_ship_rows = shipping_rows_total
        prev_proc_t = processing_time_total

    # -------------------------------------------------------------------------------
    while remaining_sources:

        # ---- PICK SOURCE ----
        if all_source:
            src_idx, table_name = remaining_sources.pop(0)

        elif stats is not None:
            best, _best_pf = SourceSelectionTM(UR, remaining_sources, stats)
            if best is None:
                break
            src_idx, table_name = best
            remaining_sources = [(i, p) for (i, p) in remaining_sources if i != src_idx]

        else:
            i = random.randrange(len(remaining_sources))
            src_idx, table_name = remaining_sources.pop(i)

        sources_explored += 1

        # ---- SHIPPING (SQL) ----
        sql_start = time.perf_counter()

        if rewrite_sql and T is not None:
            where_clause = reformualte_sql(T, UR, mode, "TM") 
            if where_clause == "FALSE" and not all_source:
                # print("Reformulated SQL is FALSE; stopping.")
                break
        
        cols = ", ".join(f'"{c}"' for c in UR.keys())
        S_rows = con.execute(
            f"SELECT DISTINCT {cols} FROM {table_name} WHERE {where_clause}"
        ).fetchdf()

        sql_time = time.perf_counter() - sql_start
        shipping_time_total += sql_time
        shipping_rows_total += len(S_rows)

        if id_col is None and not S_rows.empty:
            id_col = detect_id_column(S_rows)

        # ---- LOCAL ----
        local_start = time.perf_counter()

        S_rows = restrict_to_UR_columns(S_rows, UR)

        if T is None:
            cols = list(UR.keys()) + ([id_col] if id_col else [])
            T = pd.DataFrame(columns=cols)

        if not S_rows.empty:
            T = pd.concat([T, S_rows], ignore_index=True).drop_duplicates()
            if mode == "tvd-uni" and stats is not None:
                T_index.update_from_rows(S_rows, min_matches=2)

        # stopping condition 
        if not all_source:
            if mode == "tvd-uni":
                cov = compute_ucoverage(T, UR)
            else:
                cov, _ = compute_ecoverage(T, UR)
            if cov >= theta:
                    processing_time_total += time.perf_counter() - local_start
                    record_step(table_name)   
                    break
        processing_time_total += time.perf_counter() - local_start
        record_step(table_name)

    # ---- FINAL PRUNING + TRACE ROW ----
    if T is not None and not T.empty:
        prune_start = time.perf_counter()
        if mode == "tvd-uni":
            T = UPrune(T, UR)
        else:
            T = EPrune(T, UR)
        prune_time = time.perf_counter() - prune_start
        processing_time_total += prune_time

        record_step(
            "__PRUNE__",
            step_override=sources_explored + 1,
            ship_rows_step_override=0,
            ship_time_step_override=0.0,
            proc_time_step_override=prune_time,
        )
        
    run_stats = {
        "sources_explored": sources_explored,
        "shipping_time_total": shipping_time_total,
        "shipping_rows_total": shipping_rows_total,
        "processing_time_total": processing_time_total,
        "trace": (trace if trace_enabled else []),
    }
    return T, run_stats
