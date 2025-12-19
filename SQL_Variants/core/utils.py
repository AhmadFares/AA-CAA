from helpers.id_utils import detect_id_column


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


# -------------------- COVERAGE (DATAFRAME VERSION) --------------------


def compute_attr_coverage_df(T, UR_df, col):
    if col not in T.columns:
        return 0
    ur_vals = set(UR_df[col].dropna().unique())
    if not ur_vals:
        return 1
    t_vals = set(T[col].dropna())
    return len(t_vals & ur_vals) / len(ur_vals)


def compute_overall_coverage_df(T, UR_df):
    id_col = detect_id_column(UR_df)
    coverages = []

    for col in UR_df.columns:
        if col == id_col:
            continue
        coverages.append(compute_attr_coverage_df(T, UR_df, col))

    overall = sum(coverages) / len(coverages) if coverages else 1
    return overall, coverages


# -------------------- COVERAGE (DICT VERSION) --------------------


def compute_attr_coverage_dict(T, UR, col):
    if col not in T.columns:
        return 0
    ur_vals = set(UR[col])
    if not ur_vals:
        return 1
    t_vals = set(T[col].dropna())
    return len(t_vals & ur_vals) / len(ur_vals)


def compute_overall_coverage_dict(T, UR):
    # UR is dict, so no UR_df — detect ID column from T
    id_col = detect_id_column(T)
    coverages = []

    for col in UR:
        if col == id_col:
            continue
        coverages.append(compute_attr_coverage_dict(T, UR, col))

    overall = sum(coverages) / len(coverages) if coverages else 1
    return overall, coverages


# -------------------- PENALTY (DATAFRAME VERSION) --------------------


def compute_attr_penalty_df(T, UR_df, col):
    if col not in T.columns:
        return 0
    t_vals = set(T[col].dropna())
    if not t_vals:
        return 0
    ur_vals = set(UR_df[col].dropna().unique())
    return len(t_vals - ur_vals) / len(t_vals)


def compute_overall_penalty_df(T, UR_df):
    id_col = detect_id_column(UR_df)
    penalties = []

    for col in UR_df.columns:
        if col == id_col:
            continue
        penalties.append(compute_attr_penalty_df(T, UR_df, col))

    overall = sum(penalties) / len(penalties) if penalties else 0
    return overall, penalties


# -------------------- PENALTY (DICT VERSION) --------------------


def compute_attr_penalty_dict(T, UR, col):
    if col not in T.columns:
        return 0
    t_vals = set(T[col].dropna())
    if not t_vals:
        return 0
    ur_vals = set(UR[col])
    return len(t_vals - ur_vals) / len(t_vals)


def compute_overall_penalty_dict(T, UR):
    id_col = detect_id_column(T)
    penalties = []

    for col in UR:
        if col == id_col:
            continue
        penalties.append(compute_attr_penalty_dict(T, UR, col))

    overall = sum(penalties) / len(penalties) if penalties else 0
    return overall, penalties
