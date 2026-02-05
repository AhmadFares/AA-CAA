from time import time
import pandas as pd
from SQL_Variants.core.utils import compute_ecoverage, compute_ucoverage
from helpers.id_utils import detect_id_column
from collections import defaultdict


from collections import defaultdict
import pandas as pd

# from helpers.statistics_computation import UR


def EPrune(T, UR):
    T = T.copy()

    UR_sets = {col: set(vals) for col, vals in UR.items()}
    cols = [col for col in UR_sets.keys() if col in T.columns]
    if not cols or T.empty:
        return T

    # mask[col] = rows where T[col] is a UR value
    masks = {col: T[col].isin(UR_sets[col]) for col in cols}

    # same "_matches" meaning as your old code
    T["_S_size"] = pd.DataFrame(masks).sum(axis=1)

    # counters count[(col,val)] = how many rows cover that UR value
    count = defaultdict(int)
    for col in cols:
        vc = T.loc[masks[col], col].value_counts(dropna=False)
        for v, c in vc.items():
            if pd.isna(v):
                continue
            count[(col, v)] += int(c)

    # keep your "old" sorting rule (same line)
    T_sorted = T.sort_values("_S_size", ascending=True, kind="mergesort")


    to_drop = []
    for idx in T_sorted.index:
        items = []
        for col in cols:
            if masks[col].at[idx]:
                v = T.at[idx, col]
                if not pd.isna(v):
                    items.append((col, v))

        if not items:
            to_drop.append(idx)
            continue

        if all(count[it] > 1 for it in items):
            to_drop.append(idx)
            for it in items:
                count[it] -= 1

    return T.drop(index=to_drop).drop(columns="_S_size")



# def coverage_guided_selection(S, UR):
#     rows = []
#     for _, row in S.iterrows():
#         for col in UR:
#             if col in S.columns and row[col] in UR[col]:
#                 rows.append(row)
#                 break
#     return pd.DataFrame(rows)



import itertools
import pandas as pd


import itertools
import pandas as pd


def _row_importance(row: pd.Series, UR: dict) -> int:
    # number of UR attributes satisfied by this row
    imp = 0
    for a, vals in UR.items():
        if row[a] in vals:
            imp += 1
    return imp
import numpy as np
import pandas as pd
import itertools

import numpy as np
import pandas as pd
import itertools

def UPrune(T: pd.DataFrame, UR: dict) -> pd.DataFrame:
    """
    Exact same semantics as your original UPrune:
    - same rows
    - same order
    - same tie behavior
    Just faster witness generation (no .iloc in inner loops).
    """
    if T is None or T.empty:
        return T

    attrs = list(UR.keys())
    if not attrs:
        return T

    T_work = T.reset_index(drop=True)
    n = len(T_work)

    combos = list(itertools.product(*[UR[a] for a in attrs]))
    if not combos:
        return T_work

    m = len(combos)

    # Only for matching speed; DOES NOT change T_work content/order
    A = T_work[attrs].to_numpy(dtype=object)  # (n, k)
    k = len(attrs)

    best_sets = [set() for _ in range(m)]      # W(c)
    row_to_combos = [set() for _ in range(n)]  # combos supported by each row

    # Build witness sets (vectorized)
    for j, comb in enumerate(combos):
        comb_arr = np.asarray(comb, dtype=object)  # (k,)
        matches = (A == comb_arr).sum(axis=1)      # (n,)
        best_cnt = matches.max()
        winners = np.flatnonzero(matches == best_cnt)
        wset = set(map(int, winners))
        best_sets[j] = wset
        for i in winners:
            row_to_combos[int(i)].add(j)

    # Pruning pass (identical logic, natural order)
    alive = np.ones(n, dtype=bool)
    for i in range(n):
        if not alive[i]:
            continue
        for j in row_to_combos[i]:
            if len(best_sets[j]) == 1:
                break
        else:
            alive[i] = False
            for j in row_to_combos[i]:
                best_sets[j].discard(i)

    return T_work.loc[alive].reset_index(drop=True)




# FIRST UPRUNE DID. THE STORING WAS BAD SO LONG, KEPT IT FOR REFERENCE SINCE IT WAS WORKING
# def UPrune(T: pd.DataFrame, UR: dict) -> pd.DataFrame:
#     import time
#     """
#     UPrune for cartesian/best-tuple UCoverage.
#     Removes a tuple t iff it is NOT the last best-witness for any combination c.

#     No sorting: tuples are examined in their natural order.
#     """
#     if T is None or T.empty:
#         return T

#     attrs = list(UR.keys())
#     if not attrs:
#         return T

#     T_work = T.reset_index(drop=True)
#     n = len(T_work)

#     # Cartesian combinations
    
#     combos = list(itertools.product(*[UR[a] for a in attrs]))
    
    
    
#     if not combos:
#         return T_work

#     m = len(combos)

#     best_sets = [set() for _ in range(m)]      # W(c)
#     row_to_combos = [set() for _ in range(n)]  # combos supported by each row

#     combo_time = time.perf_counter()
#     # Build witness sets
#     for j, comb in enumerate(combos):
#         best_cnt = -1
#         winners = []
#         for i in range(n):
#             row = T_work.iloc[i]
#             cnt = 0
#             for a, v in zip(attrs, comb):
#                 if row[a] == v:
#                     cnt += 1
#             if cnt > best_cnt:
#                 best_cnt = cnt
#                 winners = [i]
#             elif cnt == best_cnt:
#                 winners.append(i)

#         wset = set(winners)
#         best_sets[j] = wset
#         for i in wset:
#             row_to_combos[i].add(j)

#     combo_time = time.perf_counter() - combo_time
#     print(f"WITNESS generation time: {combo_time:0.4f}s")
    
    
    
#     time_start = time.perf_counter()
#     # No sorting: natural order
#     order = range(n)

#     alive = [True] * n

#     for i in order:
#         if not alive[i]:
#             continue

#         # removable iff not unique witness for any combination
#         for j in row_to_combos[i]:
#             if len(best_sets[j]) == 1:
#                 break
#         else:
#             alive[i] = False
#             for j in row_to_combos[i]:
#                 best_sets[j].discard(i)
#     time_end = time.perf_counter() - time_start
#     print(f"2nd loooop time: {time_end:0.4f}s")
#     return T_work[pd.Series(alive)].reset_index(drop=True)



# MADE A BIT FASTER
# def UPrune_fast(T: pd.DataFrame, UR: dict) -> pd.DataFrame:
#     """
#     Exact UPrune for cartesian/best-tuple UCoverage.
#     Same semantics as UPrune, but faster witness construction.
#     """
#     if T is None or T.empty:
#         return T

#     attrs = list(UR.keys())
#     if not attrs:
#         return T

#     T_work = T.reset_index(drop=True)
#     n = len(T_work)

#     # Cartesian combinations
#     combos = list(itertools.product(*[UR[a] for a in attrs]))
#     if not combos:
#         return T_work

#     m = len(combos)

#     # best[c] = best score for combination c
#     best = [-1] * m

#     # W[c] = set of row indices achieving best[c]
#     W = [set() for _ in range(m)]

#     # row_to_combos[i] = combinations where row i is a best witness
#     row_to_combos = [set() for _ in range(n)]

#     # ---------- Phase 1: build witnesses (row-driven) ----------
#     for i in range(n):
#         row = T_work.iloc[i]
#         for j, comb in enumerate(combos):
#             cnt = 0
#             for a, v in zip(attrs, comb):
#                 if row[a] == v:
#                     cnt += 1

#             if cnt > best[j]:
#                 # new best → replace witnesses
#                 for old_i in W[j]:
#                     row_to_combos[old_i].discard(j)
#                 best[j] = cnt
#                 W[j] = {i}
#                 row_to_combos[i].add(j)

#             elif cnt == best[j]:
#                 W[j].add(i)
#                 row_to_combos[i].add(j)

#     # ---------- Phase 2: prune ----------
#     alive = [True] * n

#     for i in range(n):
#         if not alive[i]:
#             continue

#         # removable iff NOT unique witness for any combo
#         for j in row_to_combos[i]:
#             if len(W[j]) == 1:
#                 break
#         else:
#             alive[i] = False
#             for j in row_to_combos[i]:
#                 W[j].discard(i)

#     return T_work[pd.Series(alive)].reset_index(drop=True)





def check_case(name, T, UR):
    uc0, _ = compute_ecoverage(T, UR)
    Tp = EPrune(T, UR)
    uc1, _ = compute_ecoverage(Tp, UR)
    print(f"\n=== {name} ===")
    print("rows:", len(T), "->", len(Tp))
    print("ECov:", uc0, "->", uc1)
    if abs(uc0 - uc1) > 1e-12:
        print("❌ ECov changed!")
    else:
        print("✅ ECov preserved")
    print("Pruned T:")
    print(Tp)


def main():
    UR = {"A": ["a1","a2"], "B": ["b1","b2"], "C": ["c1"]}

    T = pd.DataFrame(columns=["A","B","C"])
    check_case("Empty T", T, UR)

    T = pd.DataFrame([
        {"A":"a1","B":"b1","C":"c1"},
        {"A":"a1","B":"b2","C":"c1"},
        {"A":"a2","B":"b1","C":"c1"},
        {"A":"a2","B":"b2","C":"c1"},
    ])
    check_case("Perfect table", T, UR)

    T = pd.DataFrame([
        {"A":"a1","B":"b1","C":"c1"},
        {"A":"a1","B":"b1","C":"c1"},
        {"A":"a1","B":"b2","C":"c1"},
    ])
    check_case("Duplicates", T, UR)

    T = pd.DataFrame([
        {"A":"a1","B":"b1","C":"c1"},
        {"A":"a1","B":"b2","C":"c1"},
        {"A":"a2","B":"b1","C":"c1"},
        {"A":"a2","B":"b2","C":"x"},
        {"A":"a2","B":"y", "C":"c1"},
    ])
    check_case("Noisy rows removable", T, UR)

    T = pd.DataFrame([
        {"A":"a1","B":"b1","C":"c1"},
        {"A":"a2","B":"b2","C":"x"},
    ])
    check_case("Necessary semi-clean witness", T, UR)

    T = pd.DataFrame([
        {"A":"a1","B":"b1","C":"c1"},
        {"A":"a2","B":"b2","C":"x"},
        {"A":"a2","B":"y", "C":"c1"},
    ])
    check_case("Redundant semi-clean witnesses", T, UR)

    T = pd.DataFrame([
        {"A":"z","B":"b1","C":"x"},
        {"A":"a1","B":"y","C":"x"},
        {"A":"z","B":"y","C":"c1"},
    ])
    check_case("All noisy", T, UR)

if __name__ == "__main__":
    main()
