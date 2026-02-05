import pandas as pd
from helpers.id_utils import detect_id_column
import itertools


# Keep only columns relevant for UR (UR keys and optional ID column)
def restrict_to_UR_columns(df, UR, keep_id=True):
    cols = list(UR.keys())

    if keep_id:
        id_col = detect_id_column(df)
        if id_col in df.columns:
            cols.append(id_col)

    return df[df.columns.intersection(cols)]


# Convert Python list/iterable into SQL tuple format: ('a','b','c')
def build_value_list(values):
    values = list(values)
    return "(" + ", ".join([f"'{v}'" for v in values]) + ")"


# Convert UR DataFrame to dict-of-lists (for SQL-based methods)
def ur_df_to_dict(UR_df):
    id_col = detect_id_column(UR_df)
    return {
        col: UR_df[col].dropna().unique().tolist()
        for col in UR_df.columns
        if col != id_col
    }





# -------------------- ECOVERAGE --------------------


def compute_attr_ecoverage(T, UR, col):
    if col not in T.columns:
        return 0
    ur_vals = set(UR[col])
    if not ur_vals:
        return 1
    t_vals = set(T[col].dropna())
    return len(t_vals & ur_vals) / len(ur_vals)


def compute_ecoverage(T, UR):
    if T is None or T.empty:
        return 0.0, [0.0 for _ in UR]
    # UR is dict, so no UR_df — detect ID column from T
    id_col = detect_id_column(T)
    coverages = []

    for col in UR:
        if col == id_col:
            continue
        coverages.append(compute_attr_ecoverage(T, UR, col))

    overall = sum(coverages) / len(coverages) if coverages else 1
    return overall, coverages



# -------------------- UCOVERAGE  --------------------

# def compute_ucoverage(T, UR):
#     if T is None or T.empty:
#         return 0.0

#     attrs = list(UR.keys())
#     n = len(attrs)
#     if n == 0:
#         return 1.0

#     total = 0.0
#     count = 0

#     for comb in itertools.product(*[UR[a] for a in attrs]):
#         count += 1
#         best = 0.0
#         for _, row in T.iterrows():
#             match = 0
#             for a, v in zip(attrs, comb):
#                 if row[a] == v:
#                     match += 1
#             score = match / n
#             if score > best:
#                 best = score
#                 if best == 1.0:
#                     break
#         total += best

#     return total / count if count else 1.0

import itertools
from itertools import combinations


def compute_ucoverage(T, UR, skip_id_col=False):
    
    if T is None or T.empty:
        return 0.0

    attrs = list(UR.keys())
    if skip_id_col:
        id_col = detect_id_column(T)
        attrs = [a for a in attrs if a != id_col]

    n = len(attrs)
    if n == 0:
        return 1.0

    # If UR contains attrs not in T, original code would KeyError on row[a].
    # If you never have that case, fine. If you do, decide your desired behavior.
    missing = [a for a in attrs if a not in T.columns]
    if missing:
        # mimic "no matches possible" for missing attrs
        # (safer than crashing; change if you want strict behavior)
        # any subset containing missing attr can never match
        pass

    # Precompute, for each subset S of attrs, the set of observed tuples on S
    # using rows where all attrs in S are non-null.
    proj_sets = {}  # key: tuple of attrs, value: set of tuples of values

    # Build from largest subsets to smallest (useful for early exit later)
    for k in range(n, 0, -1):
        for sub_attrs in combinations(attrs, k):
            # if any attr missing in T, projection set is empty
            if any(a not in T.columns for a in sub_attrs):
                proj_sets[sub_attrs] = set()
                continue

            tmp = T[list(sub_attrs)].dropna(how="any")
            if tmp.empty:
                proj_sets[sub_attrs] = set()
            else:
                # store tuple rows
                proj_sets[sub_attrs] = set(map(tuple, tmp.to_numpy()))

    total = 0.0
    count = 0

    # Iterate over UR combinations (inevitable)
    for comb in itertools.product(*[UR[a] for a in attrs]):
        count += 1

        # Try best k/n from k=n down to 1
        best = 0.0
        # map attribute -> value for this comb
        val_map = dict(zip(attrs, comb))

        for k in range(n, 0, -1):
            found = False
            for sub_attrs in combinations(attrs, k):
                tup = tuple(val_map[a] for a in sub_attrs)
                if tup in proj_sets[sub_attrs]:
                    best = k / n
                    found = True
                    break
            if found:
                break

        total += best

    return total / count if count else 1.0



# -------------------- PENALTY  --------------------


def compute_attr_penalty(T, UR, col):
    if col not in T.columns:
        return 0
    t_vals = set(T[col].dropna())
    if not t_vals:
        return 0
    ur_vals = set(UR[col])
    return len(t_vals - ur_vals) / len(t_vals)


def compute_penalty(T, UR):
    id_col = detect_id_column(T)
    penalties = []

    for col in UR:
        if col == id_col:
            continue
        penalties.append(compute_attr_penalty(T, UR, col))

    overall = sum(penalties) / len(penalties) if penalties else 0
    return overall, penalties





# def main():
#     import pandas as pd

# # ---- Example table T ----
# T = pd.DataFrame([
#     {"illness": "flu",           "symptom": "dizziness", "treatment": "blabla"},
#     {"illness": "blabla",        "symptom": "dizziness", "treatment": "aspirin"},
#     {"illness": "flu",           "symptom": "blabla",    "treatment": "aspirin"},
#     {"illness": "flu",           "symptom": "chills",    "treatment": "aspirin"},
#     {"illness": "stomach_ache",  "symptom": "dizziness", "treatment": "aspirin"},
#     {"illness": "stomach_ache",  "symptom": "chills",    "treatment": "aspirin"},
# ])

# # ---- User Request ----
# UR = {
#     "illness": ["flu", "stomach_ache"],
#     "symptom": ["dizziness", "chills"],
#     "treatment": ["aspirin"],
# }

# T_perfect = pd.DataFrame([
#     {"illness": "flu", "symptom": "dizziness", "treatment": "aspirin"},
#     {"illness": "flu", "symptom": "chills",    "treatment": "aspirin"},
#     {"illness": "stomach_ache", "symptom": "dizziness", "treatment": "aspirin"},
#     {"illness": "stomach_ache", "symptom": "chills",    "treatment": "aspirin"},
# ])

# T_edge = pd.DataFrame([
#     {"illness": "flu",          "symptom": "x",         "treatment": "y"},
#     {"illness": "stomach_ache", "symptom": "x",         "treatment": "y"},
#     {"illness": "x",            "symptom": "dizziness", "treatment": "y"},
#     {"illness": "x",            "symptom": "chills",    "treatment": "y"},
#     {"illness": "x",            "symptom": "y",         "treatment": "aspirin"},
# ])

# print("UCoverage:", compute_ucoverage(T_edge, UR))


# print(compute_ucoverage(pd.DataFrame(), UR))  # should be 0.0
# print(compute_ucoverage(T_perfect, UR))  # should be 1.0

# # ---- Compute UCoverage ----
# ucov = compute_ucoverage(T, UR)

# print("UCoverage:", ucov)







# # -------------------- COVERAGE (DATAFRAME VERSION) --------------------


# def compute_attr_coverage_df(T, UR_df, col):
#     if col not in T.columns:
#         return 0
#     ur_vals = set(UR_df[col].dropna().unique())
#     if not ur_vals:
#         return 1
#     t_vals = set(T[col].dropna())
#     return len(t_vals & ur_vals) / len(ur_vals)


# def compute_overall_coverage_df(T, UR_df):
#     id_col = detect_id_column(UR_df)
#     coverages = []

#     for col in UR_df.columns:
#         if col == id_col:
#             continue
#         coverages.append(compute_attr_coverage_df(T, UR_df, col))

#     overall = sum(coverages) / len(coverages) if coverages else 1
#     return overall, coverages




# # -------------------- PENALTY (DATAFRAME VERSION) --------------------


# def compute_attr_penalty_df(T, UR_df, col):
#     if col not in T.columns:
#         return 0
#     t_vals = set(T[col].dropna())
#     if not t_vals:
#         return 0
#     ur_vals = set(UR_df[col].dropna().unique())
#     # print('length t_vals:', len(t_vals))
#     # print('length ur_vals:', len(ur_vals))
#     return len(t_vals - ur_vals) / len(t_vals)


# def compute_overall_penalty_df(T, UR_df):
#     id_col = detect_id_column(UR_df)
#     penalties = []

#     for col in UR_df.columns:
#         if col == id_col:
#             continue
#         penalties.append(compute_attr_penalty_df(T, UR_df, col))
        
#     # print('penalties:', penalties)
#     overall = sum(penalties) / len(penalties) if penalties else 0
#     return overall, penalties

