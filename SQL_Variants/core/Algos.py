import pandas as pd
from SQL_Variants.core.utils import compute_overall_coverage_dict
from helpers.id_utils import detect_id_column


def redundancy_pruning(T, UR):
    """
    Greedy redundancy pruning:
    - keep coverage (w.r.t. UR values) exactly the same
    - remove any row whose removal does not reduce coverage
    """

    id_col = detect_id_column(T)
    if id_col is None:
        print("Warning: no ID column found; cannot perform redundancy pruning.")
        return T
    if id_col not in T.columns:
        print("Warning: Identifiant column missing, cannot perform redundancy pruning.")
        return T  # cannot prune without unique row IDs

    # Work on a copy to avoid side-effects
    T = T.copy()

    # Count matches for each row
    T["_matches"] = [sum(row[col] in UR[col] for col in UR) for _, row in T.iterrows()]

    # Least useful rows first (so most useful kept until the end)
    T_sorted = T.sort_values("_matches", ascending=True)

    # Baseline coverage (value-based)
    orig_cov, _ = compute_overall_coverage_dict(T, UR)

    # Try removing rows in increasing order of usefulness
    for idx in T_sorted.index:
        T_candidate = T.drop(index=idx)
        cov, _ = compute_overall_coverage_dict(T_candidate, UR)
        if cov == orig_cov:  # safe removal
            T = T_candidate

    return T.drop(columns="_matches")


def coverage_guided_selection(S, UR):
    rows = []
    for _, row in S.iterrows():
        for col in UR:
            if col in S.columns and row[col] in UR[col]:
                rows.append(row)
                break
    return pd.DataFrame(rows)
