import numpy as np
from helpers.id_utils import detect_id_column


def compute_UR_value_frequencies_in_sources(sources_list, UR_df):
    value_index = {}
    idx = 0
    for col in UR_df.columns:
        for val in UR_df[col].dropna().unique():
            value_index[(col, val)] = idx
            idx += 1

    vector_length = len(value_index)
    source_vectors = []

    for df in sources_list:
        vector = np.zeros(vector_length, dtype=np.float32)
        n_rows = len(df)
        if n_rows > 0:
            for (col, val), i in value_index.items():
                if col in df.columns:
                    count = (df[col] == val).sum()
                    vector[i] = count / n_rows
        source_vectors.append(vector)

    return value_index, np.array(source_vectors, dtype=np.float32)

def ecoverage_after_source_stats(T, UR, src_idx, stats_data):
    """
    Compute Coverage(UR, T ⊕ S_i) without materializing T ⊕ S_i.

    Assumptions:
      - UR is a dict: {col: [v1, v2, ...]}
      - stats_data["value_index"] maps "col:value" -> int index
      - stats_data["source_vectors"][src_idx] is a dict like {"0": p0, "1": p1, ...}
      - A UR value is considered present in S_i iff p > 0
    """
    value_index = stats_data["value_index"]
    vec = stats_data["source_vectors"][src_idx] 
   

    # {"0": p0, "1": p1, ...}

    coverages = []

    for col, ur_vals_list in UR.items():
        ur_vals = set(ur_vals_list)

        # Values covered by current T in this column
        if T is not None and col in T.columns:
            covered_vals = set(T[col].dropna()) & ur_vals
        else:
            covered_vals = set()
        added = []
        # Add values known present in S_i from stats (p > 0)
        for v in ur_vals:
            if v in covered_vals:
                continue

            # try exact key first
            j = value_index.get(f"{col}:{v}")

            # fallback: numeric values stored as float in stats
            if j is None and isinstance(v, int):
                j = value_index.get(f"{col}:{float(v)}")

            if j is not None and vec[j] > 0:
                covered_vals.add(v)

    #     print(f"[src {src_idx}] col={col} covered_in_T={len(set(T[col].dropna()) & ur_vals) if (T is not None and col in T.columns) else 0}"
    #   f" addable_from_stats={covered_vals}")

        coverages.append(len(covered_vals) / len(ur_vals))

    overall = sum(coverages) / len(coverages) if coverages else 1.0
    return overall

def EPenaltyFree(UR, src_idx, stats_data):
    """
    Expected number of penalty-free tuples in source src_idx.

    Assumes:
      - UR is dict: {col: [v1, v2, ...]}
      - stats_data["value_index"] maps "col:value" -> j
      - stats_data["source_vectors"][src_idx] is a numpy row vector of p_S(col:value)
      - stats_data["source_sizes"][src_idx] is |S| (row count), injected by the runner
      - penalty-free tuple probability uses independence across columns:
          Π_col  P(UR/col), where P(UR/col) = Σ_{v in UR[col]} p_S(col:v)
    """
    value_index = stats_data["value_index"]
    vec = stats_data["source_vectors"][src_idx]
    n_rows = float(stats_data["source_sizes"][src_idx])

    p_tuple = 1.0

    for col, ur_vals in UR.items():
        p_col = 0.0
        for v in ur_vals:
            j = value_index.get(f"{col}:{v}")
            if j is not None:
                p_col += float(vec[j])

        # If a column never takes UR values, penalty-free tuples are impossible
        if p_col <= 0.0:
            return 0.0

        p_tuple *= p_col
    
    
    expected = n_rows * p_tuple
    # print(f"EPenaltyFree src_idx={src_idx}: n_rows={n_rows}, p_tuple={p_tuple}, expected={expected}")
    return expected if expected >= 1.0 else 0.0

def ETuples(UR, src_idx, stats_data):
    """
    Expected number of tuples shipped by AM  

    Assumes:
      - stats_data["value_index"]: "col:value" -> j
      - stats_data["source_vectors"][src_idx][j] = p_S(col:value)
      - stats_data["source_sizes"][src_idx] = |S|
      - Independence across UR values for OR-estimation:
          P(row matches at least one UR value) = 1 - Π_{(col,v) in UR}(1 - p_S(col:v))
    """
    value_index = stats_data["value_index"]
    vec = stats_data["source_vectors"][src_idx]
    n_rows = float(stats_data["source_sizes"][src_idx])

    prod_not = 1.0

    for col, ur_vals in UR.items():
        for v in ur_vals:
            j = value_index.get(f"{col}:{v}")
            if j is None:
                continue
            p = float(vec[j])
            # If p>0, it increases expected shipped tuples
            prod_not *= (1.0 - p)

    p_match = 1.0 - prod_not
    expected = n_rows * p_match
    # print(f"ETuples src_idx={src_idx}: n_rows={n_rows}, p_match={p_match}, expected={expected}")
    return expected









#__________________________________________________________________________UCoverage_______________________________________________________________




# import itertools

# def compute_P_levels_exact(ps):
#     """
#     Row-level probabilities for a single combination c.

#     ps: list of per-attribute probabilities p_j (one value per UR attribute)
#     Returns:
#       P_exact[ell] for ell=0..k
#       where P_exact[ell] = Prob(row matches exactly (k-ell) attributes)
#     """
#     k = len(ps)
#     P = [0.0] * (k + 1)

#     for ell in range(k + 1):
#         for J in itertools.combinations(range(k), ell):
#             prob = 1.0
#             J = set(J)
#             for i in range(k):
#                 prob *= (1 - ps[i]) if i in J else ps[i]
#             P[ell] += prob

#     return P


# def exact_to_atleast(P_exact):
#     """
#     Convert exact-match level probabilities to cumulative 'at least' probabilities.

#     P_exact[ell] = P(exactly k-ell matches)
#     P_atleast[ell] = P(at least k-ell matches) = sum_{j=0..ell} P_exact[j]
#     """
#     k = len(P_exact) - 1
#     P_atleast = [0.0] * (k + 1)
#     running = 0.0
#     for ell in range(k + 1):
#         running += P_exact[ell]
#         P_atleast[ell] = running
#     return P_atleast


# def lift_to_existence(P_levels, m):
#     """
#     Convert row-level probabilities to source-level existence probabilities.

#     If P_levels[ell] is the probability that ONE row satisfies the predicate
#     (e.g., at least k-ell matches), then existence in a source of m rows is:
#       q_ell = 1 - (1 - P_levels[ell])^m
#     """
#     # Guard for edge cases
#     if m is None or m <= 0:
#         return [0.0 for _ in P_levels]

#     q = []
#     for p in P_levels:
#         if p <= 0.0:
#             q.append(0.0)
#         elif p >= 1.0:
#             q.append(1.0)
#         else:
#             q.append(1.0 - (1.0 - p) ** m)
#     return q


# def expected_match_from_q(q_levels):
#     """
#     Expected best match score for one combination c using top-down logic.

#     q_levels[ell] = P(exists a row with at least (k-ell) matches)
#     """
#     k = len(q_levels) - 1
#     E = 0.0
#     prev_fail = 1.0

#     for ell in range(k + 1):
#         score = (k - ell) / k
#         E += score * prev_fail * q_levels[ell]
#         prev_fail *= (1.0 - q_levels[ell])

#     return E


# def ucoverage_after_source_stats(T,UR, src_idx, stats_data):
#     """
#     Expected UCoverage for a source S_i using existence probabilities 1-(1-p)^m.

#     Inputs:
#       UR: dict {col: [v1, v2, ...]}
#       stats_data["value_index"]: maps "col:value" -> int
#       stats_data["source_vectors"][src_idx]: array-like of probabilities per value_index
#       stats_data["source_sizes"][src_idx]: m (#rows in source)
#     """
#     value_index = stats_data["value_index"]
#     vec = stats_data["source_vectors"][src_idx]
#     m = stats_data["source_sizes"][src_idx]

#     cols = list(UR.keys())
#     ur_vals_lists = [UR[c] for c in cols]

#     total = 0.0
#     count = 0

#     for combo in itertools.product(*ur_vals_lists):
#         ps = []
#         for col, v in zip(cols, combo):
#             j = value_index.get(f"{col}:{v}")
#             if j is None and isinstance(v, int):
#                 j = value_index.get(f"{col}:{float(v)}")

#             p = vec[j] if j is not None else 0.0

#             # clamp to [0,1] to avoid float noise
#             if p < 0.0: p = 0.0
#             if p > 1.0: p = 1.0
#             ps.append(p)

#         # Row-level exact probabilities
#         P_exact = compute_P_levels_exact(ps)

#         # Convert to "at least" (needed for best-row logic)
#         P_atleast = exact_to_atleast(P_exact)

#         # Lift to source-level existence
#         q_levels = lift_to_existence(P_atleast, m)

#         # Expected best match score for this combination
#         total += expected_match_from_q(q_levels)
#         count += 1

#     return total / count if count else 1.0



import itertools
import pandas as pd
from collections import defaultdict

# -----------------------------
# 1) Row-level: exact match distribution
# -----------------------------
def compute_P_exact(ps):
    """
    ps: list of p_j for a combination c (one per UR attribute)
    Returns:
      P_exact[ell] for ell=0..k
      where ell = #mismatches, so (k-ell) matches
    """
    k = len(ps)
    P = [0.0] * (k + 1)

    for ell in range(k + 1):
        for J in itertools.combinations(range(k), ell):
            prob = 1.0
            J = set(J)
            for i in range(k):
                prob *= (1.0 - ps[i]) if i in J else ps[i]
            P[ell] += prob

    return P


# -----------------------------
#  Table-level: best-row semantics via tail-sum (clean + correct)
# -----------------------------
def expected_best_score_with_existence(P_exact, m):
    """
    Expected best match score for ONE combination c, under iid rows.

    Let R = max #matches among m rows. Then:
      E[R] = sum_{r=1..k} P(R >= r)
      E[score] = E[R]/k

    P_exact is over #mismatches ell, so matches = k-ell.
    """
    k = len(P_exact) - 1
    if m is None or m <= 0:
        return 0.0

    # Precompute row-level tail probs: row has at least r matches
    # P_row_atleast[r] for r=1..k
    P_row_atleast = [0.0] * (k + 1)  # index by r
    # P_row_atleast[r] = sum_{matches>=r} P_exact[ell] = sum_{ell=0..k-r} P_exact[ell]
    prefix = [0.0] * (k + 1)
    running = 0.0
    for ell in range(k + 1):
        running += P_exact[ell]
        prefix[ell] = running  # sum_{j=0..ell} P_exact[j]

    for r in range(1, k + 1):
        ell_max = k - r
        P_row_atleast[r] = prefix[ell_max] if ell_max >= 0 else 0.0

    # Lift to source-level existence: P(R >= r) = 1 - (1 - P_row_atleast[r])^m
    E_matches = 0.0
    for r in range(1, k + 1):
        p = P_row_atleast[r]
        if p <= 0.0:
            pr_ge_r = 0.0
        elif p >= 1.0:
            pr_ge_r = 1.0
        else:
            pr_ge_r = 1.0 - (1.0 - p) ** m
        E_matches += pr_ge_r

    return E_matches / k


def expected_best_score_T_plus_S(P_exact, m, r_T):
    """
    Expected score of max(T, S) for one combination:
      final matches = max(r_T, R_source)
    If T already guarantees r_T matches, then:
      E[final matches] = r_T + sum_{r=r_T+1..k} P(R_source >= r)
    """
    k = len(P_exact) - 1
    if r_T >= k:
        return 1.0
    if m is None or m <= 0:
        return r_T / k

    # row-level tails again
    prefix = [0.0] * (k + 1)
    running = 0.0
    for ell in range(k + 1):
        running += P_exact[ell]
        prefix[ell] = running

    def P_row_atleast(r):
        ell_max = k - r
        return prefix[ell_max] if ell_max >= 0 else 0.0

    E_matches = float(r_T)
    for r in range(r_T + 1, k + 1):
        p = P_row_atleast(r)
        if p <= 0.0:
            pr_ge_r = 0.0
        elif p >= 1.0:
            pr_ge_r = 1.0
        else:
            pr_ge_r = 1.0 - (1.0 - p) ** m
        E_matches += pr_ge_r

    return E_matches / k


# -----------------------------
#  Incremental T index (ALL levels)
# -----------------------------
class TIndexAllLevels:
    """
    Stores patterns from T as tuples of length k with None wildcards.
    Indexed by r = #matched UR attributes in that row.
    Supports all levels (clean, clean-1, ..., clean-k).
    """
    def __init__(self, UR):
        self.cols = list(UR.keys())
        self.k = len(self.cols)
        self.ur_sets = {c: set(vs) for c, vs in UR.items()}
        self.idx = defaultdict(set)  # r -> set(pattern tuples)
        self.r_desc = list(range(self.k, 0, -1))

    def update_from_rows(self, df, min_matches=1):
        """
        Update index using ONLY newly added rows (S_rows).
        min_matches=1 means store even 1-hit patterns; set to 2 if you want supervisor rule.
        """
        cols = self.cols
        for _, row in df[cols].iterrows():
            pat = [None] * self.k
            r = 0
            for i, c in enumerate(cols):
                v = row[c]
                if pd.isna(v):
                    continue
                if v in self.ur_sets[c]:
                    pat[i] = v
                    r += 1
            if r >= min_matches:
                self.idx[r].add(tuple(pat))

    def best_r_for_combo(self, combo):
        """
        combo: tuple length k (one UR value per attribute)
        returns best r in {0..k} already achievable in T for this combo.
        """
        k = self.k
        for r in self.r_desc:
            if not self.idx[r]:
                continue
            for keep_pos in itertools.combinations(range(k), r):
                pat = [None] * k
                for i in keep_pos:
                    pat[i] = combo[i]
                if tuple(pat) in self.idx[r]:
                    return r
        return 0


# -----------------------------
#  Expected UCov(T ⊕ S_i) using stats + TIndex
# -----------------------------
def ucoverage_after_source_stats(T_index, UR, src_idx, stats_data, min_matches_in_index=1):
    """
    Returns expected UCoverage(UR, T ⊕ S_i) WITHOUT scanning T:
      - baseline from T comes from T_index
      - source effect uses existence (1-(1-p)^m) and best-row semantics
    """
    value_index = stats_data["value_index"]
    vec = stats_data["source_vectors"][src_idx]
    m = stats_data["source_sizes"][src_idx]
    #print("T_indexxxxxxxxxxxxxxxxx:", T_index.cols, T_index.idx)

    cols = T_index.cols
    ur_vals_lists = [UR[c] for c in cols]

    total = 0.0
    count = 0

    for combo in itertools.product(*ur_vals_lists):
        r_T = T_index.best_r_for_combo(combo)

        ps = []
        for col, v in zip(cols, combo):
            j = value_index.get(f"{col}:{v}")
            if j is None and isinstance(v, int):
                j = value_index.get(f"{col}:{float(v)}")

            p = vec[j] if j is not None else 0.0
            if p < 0.0: p = 0.0
            if p > 1.0: p = 1.0
            ps.append(p)

        P_exact = compute_P_exact(ps)
        total += expected_best_score_T_plus_S(P_exact, m, r_T)
        count += 1

    return total / count if count else 1.0

