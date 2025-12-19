import numpy as np

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

    # Return array, not dict
    return value_index, np.array(source_vectors, dtype=np.float32)


def compute_joint_stats_strict(sources_list, UR_df):
    import numpy as np

    ur_cols = [c for c in UR_df.columns if c != "Identifiant"]

    # 1) Global joint index over all UR values
    joint_index = {}
    idx = 0
    for i, col_i in enumerate(ur_cols):
        for j, col_j in enumerate(ur_cols):
            if j <= i:
                continue
            for val_i in UR_df[col_i].dropna().unique():
                for val_j in UR_df[col_j].dropna().unique():
                    joint_index[((col_i, val_i), (col_j, val_j))] = idx
                    idx += 1

    joint_vectors = []

    for df in sources_list:
        n_rows = len(df)
        # default = -1 → “pair not meaningful in this source”
        vec = np.full(len(joint_index), fill_value=-1.0, dtype=np.float32)

        # UR values actually present in this source
        present = {
            col: set(df[col].dropna().unique()).intersection(
                set(UR_df[col].dropna().unique())
            )
            for col in ur_cols if col in df.columns
        }

        if n_rows > 0:
            for col_i, vals_i in present.items():
                for col_j, vals_j in present.items():
                    if col_j <= col_i:
                        continue
                    for v_i in vals_i:
                        for v_j in vals_j:
                            key = ((col_i, v_i), (col_j, v_j))
                            if key not in joint_index:
                                continue
                            count = ((df[col_i] == v_i) & (df[col_j] == v_j)).sum()
                            # IMPORTANT: even if count==0, we overwrite -1 with 0
                            # meaning: both values exist in the source, but never together
                            vec[joint_index[key]] = count / n_rows

        joint_vectors.append(vec)

    return joint_index, np.array(joint_vectors, dtype=np.float32)

def print_stats(value_index, source_vectors, tiny=1e-9):
    import numpy as np
    reverse_index = {i: (col, val) for (col, val), i in value_index.items()}
    for s_idx, vec in enumerate(source_vectors):
        print(f"\nSource {s_idx}:")
        for i, p in enumerate(vec):
            col, v = reverse_index[i]
            if np.isnan(p):
                shown = "NaN"
            elif p == 0.0:
                shown = "0.0"
            elif 0.0 < p < tiny:
                shown = f"{p:.12e} (tiny)"
            else:
                shown = f"{p:.12e}"
            print(f"  {col} == {v!r} → {shown}")


def compute_joint_UR_value_frequencies_in_sources(sources_list, UR_df):
    """
    Compute joint probabilities P(col_i=v_i, col_j=v_j) for UR values that
    actually appear in each source.

    For each source:
      - Only considers UR values present in that source.
      - Keeps zero entries (meaning both values exist but never co-occur).

    Returns:
        joint_index: dict mapping ((col_i, val_i), (col_j, val_j)) -> global index
        joint_vectors: list[np.array], one per source, with frequencies
    """
    import numpy as np
    if len([c for c in UR_df.columns if c != "Identifiant"]) < 2:
        # Only one column in UR → no joint stats possible
        print("⚠️ Only one UR column — skipping joint computation.")
        value_index, source_stats= compute_UR_value_frequencies_in_sources(sources_list, UR_df)
        return value_index, source_stats


    joint_index = {}
    idx = 0
    ur_cols = [c for c in UR_df.columns if c != "Identifiant"]

    # build a global joint index over all UR columns and values
    for i, col_i in enumerate(ur_cols):
        for j, col_j in enumerate(ur_cols):
            if j <= i:
                continue
            for val_i in UR_df[col_i].dropna().unique():
                for val_j in UR_df[col_j].dropna().unique():
                    joint_index[((col_i, val_i), (col_j, val_j))] = idx
                    idx += 1

    joint_vectors = []

    for df in sources_list:
        n_rows = len(df)
        vector = np.zeros(len(joint_index), dtype=np.float32)

        # identify which UR values actually appear in this source
        ur_present = {
            col: set(df[col].unique()).intersection(set(UR_df[col].dropna().unique()))
            for col in ur_cols if col in df.columns
        }

        if n_rows > 0:
            for col_i, vals_i in ur_present.items():
                for col_j, vals_j in ur_present.items():
                    if col_j <= col_i:
                        continue
                    for val_i in vals_i:
                        for val_j in vals_j:
                            key = ((col_i, val_i), (col_j, val_j))
                            if key not in joint_index:
                                continue
                            count = ((df[col_i] == val_i) & (df[col_j] == val_j)).sum()
                            vector[joint_index[key]] = count / n_rows

        joint_vectors.append(vector)

    return joint_index, np.array(joint_vectors, dtype=np.float32)



if __name__ == "__main__":
    from helpers.test_cases import TestCases
    from helpers.Source_Constructors import SourceConstructor

    # Load a test case (adjust case id if needed)
    T_input, UR = TestCases().get_case(20)

    # Build sources (use your usual constructor)
    constructor = SourceConstructor(T_input, UR)
    sources_list = constructor.low_penalty_sources()  # or whichever you use

    # Compute stats
    value_index, source_stats = compute_joint_UR_value_frequencies_in_sources(sources_list, UR)

    # Print shapes
    print("Num sources:", len(sources_list))
    print_stats(value_index, source_stats)

