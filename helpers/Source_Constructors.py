import pandas as pd
import numpy as np
import random
from itertools import product
from helpers.id_utils import detect_id_column


def dataframe_to_ur_dict(df):
    """Convert a DataFrame to a user request dictionary."""
    return {col: set(df[col].dropna().unique()) for col in df.columns}


# -----------------------------------------------------
# NEW: helper to force ID column to safe type
# -----------------------------------------------------
def _force_id_string(df):
    """Ensure the detected ID column is stored as string[pyarrow]."""
    id_col = detect_id_column(df)
    if id_col is not None:
        df[id_col] = df[id_col].astype("string[pyarrow]")
    return df


class SourceConstructor:
    def __init__(self, T: pd.DataFrame, UR: dict, seed: int = 42):
        self.T = T.copy()
        self.UR = UR
        self.seed = seed
        self.rng = random.Random(seed)
        self.np_rng = np.random.RandomState(seed)

    # -----------------------------------------------------
    # RANDOM SPLIT (patched)
    # -----------------------------------------------------
    def random_split(self, df, n_sources=10):
        df_shuffled = df.sample(frac=1, random_state=self.seed).reset_index(drop=True)
        splits = np.array_split(df_shuffled, n_sources)

        # Force ID-type fix on ALL splits
        return [_force_id_string(s.copy()) for s in splits]

    # -----------------------------------------------------
    # LOW PENALTY
    # -----------------------------------------------------
    def low_penalty_sources(self, n_sources):
        columns = list(self.UR.keys())
        value_lists = [
            [val for val in self.UR[col] if pd.notna(val)] for col in columns
        ]
        cartesian_rows = list(product(*value_lists))

        T_augmented = self.T.copy()

        for row_values in cartesian_rows:
            new_row = {col: val for col, val in zip(columns, row_values)}
            for col in T_augmented.columns:
                if col not in new_row:
                    if pd.api.types.is_numeric_dtype(T_augmented[col]):
                        # keep numeric columns numeric
                        new_row[col] = self.rng.randint(1000, 9999)
                    else:
                        # fill string columns with string values
                        new_row[col] = f"default_{self.rng.randint(1000,9999)}"

            T_augmented = pd.concat(
                [T_augmented, pd.DataFrame([new_row])], ignore_index=True
            )

        # Final fix on all outputs
        return self.random_split(T_augmented, n_sources=n_sources)

    # -----------------------------------------------------
    # LOW COVERAGE
    # -----------------------------------------------------
    def low_coverage_sources(self, n_sources, remove_fraction=0.3):
        T_mutated = self.T.copy()
        ur_values_to_remove = {}

        for col, vals in self.UR.items():
            vals_list = list(vals)
            n_remove = max(1, int(len(vals_list) * remove_fraction))
            if len(vals_list) >= n_remove:
                vals_to_remove = self.rng.sample(vals_list, n_remove)
            else:
                vals_to_remove = vals_list
            ur_values_to_remove[col] = vals_to_remove

        for col, vals in ur_values_to_remove.items():
            for val in vals:
                if pd.api.types.is_numeric_dtype(T_mutated[col]):
                    # keep numeric column numeric
                    replacement = self.rng.randint(1000, 9999)
                else:
                    replacement = f"noise_{self.rng.randint(1000,9999)}"

                T_mutated.loc[T_mutated[col] == val, col] = replacement

        return self.random_split(T_mutated, n_sources=n_sources)

    # -----------------------------------------------------
    # HIGH PENALTY
    # -----------------------------------------------------
    def high_penalty_sources(self, n_sources):
        T_mutated = self.T.copy()

        if isinstance(self.UR, pd.DataFrame):
            ur_columns = list(self.UR.columns)
            ur_dict = {
                col: set(self.UR[col].dropna().unique()) for col in self.UR.columns
            }
        else:
            ur_columns = list(self.UR.keys())
            ur_dict = self.UR

        ur_values_flat = set(val for vals in ur_dict.values() for val in vals)

        perfect_rows = []
        for idx, row in T_mutated.iterrows():
            if all(row[col] in ur_dict[col] for col in ur_columns):
                perfect_rows.append((idx, row.copy()))

        donor_rows = []
        for idx, row in T_mutated.iterrows():
            if all(row[col] not in ur_values_flat for col in ur_columns):
                donor_rows.append((idx, row.copy()))

        used_donors = set()
        for perfect_idx, perfect_row in perfect_rows:
            donor = None
            for donor_idx, donor_row in donor_rows:
                if donor_idx not in used_donors:
                    donor = donor_row
                    used_donors.add(donor_idx)
                    break
            if donor is None:
                break

            row1 = perfect_row.copy()
            row2 = perfect_row.copy()

            if len(ur_columns) > 1:
                row1[ur_columns[1]] = donor[ur_columns[1]]
                row2[ur_columns[0]] = donor[ur_columns[0]]
            else:
                row1[ur_columns[0]] = donor[ur_columns[0]]

            T_mutated = T_mutated.drop(index=perfect_idx)
            T_mutated = pd.concat(
                [T_mutated, pd.DataFrame([row1, row2])], ignore_index=True
            )

        return self.random_split(T_mutated, n_sources=n_sources)

    # -----------------------------------------------------
    # SKEWED SPLIT
    # -----------------------------------------------------
    def skewed_split(self, df, n_sources=10, big_ratio=0.7):
        df_shuffled = df.sample(frac=1, random_state=self.seed).reset_index(drop=True)

        n_big = max(1, int(n_sources * 0.3))
        n_small = n_sources - n_big

        total_rows = len(df_shuffled)
        big_rows = int(total_rows * big_ratio)
        small_rows = total_rows - big_rows

        big_sizes = [big_rows // n_big] * n_big
        small_sizes = [max(1, small_rows // n_small)] * n_small

        all_sizes = big_sizes + small_sizes

        sources = []
        start = 0
        for size in all_sizes:
            end = start + size
            sources.append(df_shuffled.iloc[start:end].copy())
            start = end

        return [_force_id_string(s) for s in sources]
