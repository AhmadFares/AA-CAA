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


def expected_delta_cov_for_source(
    src_idx, source_vectors, value_index, UR_df, already_covered
):
    """
    Universal expected coverage:

    - value_index: dict with STRING keys "col:val"  (from value_index.json)
    - source_vectors: np.ndarray [n_sources, n_values]
    - UR_df: DataFrame of the UR (only columns used in the UR)
    - already_covered: set of (col, val) pairs already covered

    Works when stats are:
      - only UR values (UR-dependent splits)
      - all dataset values (dataset-level splits)
    because we always restrict to UR values only.
    """
    id_col = detect_id_column(UR_df)
    vector = source_vectors[src_idx]

    # Build list of UR (col,val) pairs
    ur_pairs = []
    for col in UR_df.columns:
        if col == id_col:
            continue
        for val in UR_df[col].dropna().unique():
            ur_pairs.append((col, val))

    total = 0
    new_covered = 0

    for col, val in ur_pairs:
        key = f"{col}:{val}"

        # Skip if this UR value is not in stats
        if key not in value_index:
            continue

        idx = value_index[key]
        total += 1

        # Skip if already covered
        if (col, val) in already_covered:
            continue

        # If source has non-zero frequency → it helps cover this UR value
        if vector[idx] > 0:
            new_covered += 1

    if total == 0:
        return 0.0

    return new_covered / total
