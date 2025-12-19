import numpy as np
import pandas as pd

def split_by_rows(df):
    """
    Splits the DataFrame into two sources:
    - M1: First 2000 rows
    - M2: Remaining rows
    """
    if "Identifiant" not in df.columns:
        raise ValueError("The table must contain an 'Identifiant' column.")

    M1 = df.iloc[:2000].reset_index(drop=True)
    M2 = df.iloc[2000:].reset_index(drop=True)

    return [M1, M2]

def split_by_columns(df):
    """
    Splits the DataFrame into two sources:
    - M1: First 5 columns + 'Identifiant'
    - M2: Remaining columns + 'Identifiant'
    """
    if "Identifiant" not in df.columns:
        raise ValueError("The table must contain an 'Identifiant' column.")

    first_5_cols = ["Identifiant"] + list(df.columns[1:6])  # Keep Identifiant + first 5 cols
    remaining_cols = ["Identifiant"] + list(df.columns[6:])  # Keep Identifiant + remaining cols

    M1 = df[first_5_cols].copy()
    M2 = df[remaining_cols].copy()

    return [M1, M2]

def split_by_hybrid(df):
    """
    Case 3:
    - M1 takes first half of columns completely + 'Identifiant'.
    - M1 also takes the first half of rows from the remaining columns.
    - M2 takes the second half of rows from the second half of columns.
    """
    if "Identifiant" not in df.columns:
        raise ValueError("The table must contain an 'Identifiant' column.")

    num_rows, num_cols = df.shape
    half_cols = num_cols // 2
    half_rows = num_rows // 2

    first_half_cols = ["Identifiant"] + list(df.columns[1:half_cols])  # Keep Identifiant + first half columns
    second_half_cols = df.iloc[:, half_cols:]  # Second half of columns

    M1 = pd.concat([df[first_half_cols], second_half_cols.iloc[:half_rows]], axis=1)
    M2 = second_half_cols.iloc[half_rows:]  # M2 gets only the second half of rows from second-half columns

    return [M1, M2]

def split_by_diagonal(df):
    """
    Case 4:
    - M1 takes top-left and bottom-right, including 'Identifiant'.
    - M2 takes bottom-left and top-right, including 'Identifiant'.
    """
    if "Identifiant" not in df.columns:
        raise ValueError("The table must contain an 'Identifiant' column.")

    num_rows, num_cols = df.shape
    half_rows = num_rows // 2
    half_cols = num_cols // 2

    # Extract the Identifiant column before splitting
    identifiants = df[["Identifiant"]].reset_index(drop=True)

    # Define diagonal splits
    M1_top_left = df.iloc[:half_rows, 1:half_cols]  # Exclude Identifiant (1:half_cols)
    M1_bottom_right = df.iloc[half_rows:, half_cols:]

    M2_top_right = df.iloc[:half_rows, half_cols:]
    M2_bottom_left = df.iloc[half_rows:, 1:half_cols]  # Exclude Identifiant (1:half_cols)

    # Reset index to prevent NaN issues
    M1 = pd.concat([identifiants, pd.concat([M1_top_left, M1_bottom_right], axis=1)], axis=1).reset_index(drop=True)
    M2 = pd.concat([identifiants, pd.concat([M2_top_right, M2_bottom_left], axis=1)], axis=1).reset_index(drop=True)
    print("m1" ,M1)
    print("m2", M2)
    return [M1, M2]


def split_by_keywords(df):
    """
    Splits the DataFrame based on Keyword columns:
    - M1 contains 'Identifiant' + 'Keyword1'
    - M2 contains 'Identifiant' + 'Keyword2'
    """
    if "Identifiant" not in df.columns or "Keyword1" not in df.columns or "Keyword2" not in df.columns:
        raise ValueError("The table must contain 'Identifiant', 'Keyword1', and 'Keyword2' columns.")

    M1 = df[["Identifiant", "Keyword1"]].copy()
    M2 = df[["Identifiant", "Keyword2"]].copy()

    return [M1, M2]


def split_by_overlapping_rows(df, overlap_size=5):
    """
    Splits the DataFrame into two overlapping row-based sources:
    - M1 contains all rows except the last `overlap_size` rows.
    - M2 contains all rows except the first `overlap_size` rows.
    - The middle part is overlapping between both M1 and M2.

    Args:
        df (DataFrame): The input dataset.
        overlap_size (int): Number of overlapping rows (default: 5).

    Returns:
        List of DataFrames: [M1, M2] with overlapping rows.
    """
    if "Identifiant" not in df.columns:
        raise ValueError("The table must contain an 'Identifiant' column.")

    num_rows = len(df)
    if overlap_size >= num_rows // 2:
        raise ValueError("Overlap size is too large relative to dataset size.")

    # M1: All rows except the last `overlap_size` rows
    M1 = df.iloc[:-overlap_size].reset_index(drop=True)

    # M2: All rows except the first `overlap_size` rows
    M2 = df.iloc[overlap_size:].reset_index(drop=True)

    return [M1, M2]

def split_uniform_by_rows(df, n_sources):
    """
    Splits a DataFrame into `n_sources` equally sized row-based sources with the same schema.
    Each source has the same columns.

    Args:
        df (pd.DataFrame): The full input table.
        n_sources (int): Number of sources to split into.

    Returns:
        List[pd.DataFrame]: A list of DataFrames (sources).
    """
    if "Identifiant" not in df.columns:
        raise ValueError("The table must contain an 'Identifiant' column.")

    # df_shuffled = df.sample(frac=1).reset_index(drop=True)
    # return np.array_split(df_shuffled, n_sources)
    # No shuffling
    return np.array_split(df.reset_index(drop=True), n_sources)
