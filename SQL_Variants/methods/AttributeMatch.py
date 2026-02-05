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
from SQL_Variants.core.sql_builders import reformualte_sql, construct_AM_sql
from SQL_Variants.core.stats import (
    ecoverage_after_source_stats, EPenaltyFree, ETuples,
    TIndexAllLevels, ucoverage_after_source_stats
)


# ------------------------------------------------------------
# choose next source (stats-based)
# ------------------------------------------------------------

def SourceSelectionM1(UR, T, theta,remaining_sources, stats, mode, T_index=None):
    """
    remaining_sources: list of (src_idx, table_name)
    stat: statistics object used by helper functions

    returns:
      best: (src_idx, table_name) or None
      best_gain: float (max Δcov) or 0
    """
    if not remaining_sources:
        return None, 0

    if mode == "tvd-uni":
        cov_T = compute_ucoverage(T, UR) if (T is not None and not T.empty) else 0.0
    else:
        cov_T, _ = compute_ecoverage(T, UR)

    gains = []
    
    
    for src_idx, table_name in remaining_sources:
       if cov_T < theta:
            if mode == "tvd-uni":
                cov_T_plus = ucoverage_after_source_stats(T_index, UR, src_idx, stats)
            else:
                cov_T_plus = ecoverage_after_source_stats(T, UR, src_idx, stats)
            # Coverage(UR, T ⊕ S_i) using stats for S_i
            # print("src", src_idx, "cov_T", cov_T, "cov_T_plus", cov_T_plus, "delta", cov_T_plus - cov_T)
            delta = cov_T_plus - cov_T
       else:
            delta = 0.0   # ← explicitly zero gain
        
       gains.append(((src_idx, table_name), delta))

    max_gain = max(delta for _, delta in gains)
    C = [s for (s, delta) in gains if delta == max_gain]

    # ---- 2) tie-break: maximize EPenaltyFree ----
    if len(C) > 1:
        scores = [(s, EPenaltyFree(UR, s[0], stats)) for s in C]  # s[0] is src_idx
        best_score = max(sc for _, sc in scores)
        if max_gain <=0 and best_score <= 0:
            return None, 0
        C = [s for (s, sc) in scores if sc == best_score]
        # print('Tie-break EPenaltyFree, candidates:', C)
        # print('Best score:', best_score)
    
    # ---- 3) tie-break: minimize ETuples ----
    if len(C) > 1:
        scores = [(s, ETuples(UR, s[0], stats)) for s in C]
        best_score = min(sc for _, sc in scores)
        C = [s for (s, sc) in scores if sc == best_score]
        # print('Tie-break ETuples, candidates:', C)
        # print('Best score:', best_score)
    # print('Selected source:', C[0], 'with gain:', max_gain)
    return C[0], max_gain


def Attribute_Match(
    con, UR, table_names, theta,
    stats=None, mode="tvd-exi",trace_enabled=True,
    all_source=False, rewrite_sql=False
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
    where_clause = construct_AM_sql(UR)

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
        cov_pen_calc_time = 0.0   

        step_val = sources_explored if step_override is None else step_override
        if not trace_enabled:
            return 

        rows_current = len(T) if (T is not None) else 0
        if T is not None and not T.empty:
            pentime = time.perf_counter()
            pen_current, _ = compute_penalty(T, UR)
            ucov_current = compute_ucoverage(T, UR)
            ecov_current, _ = compute_ecoverage(T, UR)  
            cov_pen_calc_time = time.perf_counter() - pentime
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
            "cov_pen_calc_time_for_step": float(cov_pen_calc_time),
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
            sel_start = time.perf_counter()
            best, gain = SourceSelectionM1(UR, T, theta, remaining_sources, stats, mode, T_index)
            sel_dt = time.perf_counter() - sel_start
            processing_time_total += sel_dt 
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
            where_clause = reformualte_sql(T, UR, mode, "AM")
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

        # stopping condition (unchanged)
        if pen == 0 and not all_source:
            pen, _ = compute_penalty(T, UR)
            if pen == 0:
                if mode == "tvd-uni":
                    cov = compute_ucoverage(T, UR)
                else:
                    cov, _ = compute_ecoverage(T, UR)
                if cov >= theta and pen == 0:
                    processing_time_total += time.perf_counter() - local_start
                    record_step(table_name)   # record BEFORE break
                    break

        processing_time_total += time.perf_counter() - local_start
        record_step(table_name)  # record at normal end of iteration

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
