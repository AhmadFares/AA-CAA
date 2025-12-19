# SQL_Variants/scripts/generate_splits.py

import os
import json
import pandas as pd

from SQL_Variants.config.test_config import GENERAL_CONFIG
from SQL_Variants.core.data_loading import load_ur_and_base_table
from helpers.Source_Constructors import SourceConstructor, dataframe_to_ur_dict
from SQL_Variants.core.stats import compute_UR_value_frequencies_in_sources
from helpers.id_utils import detect_id_column


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
OUTPUT_ROOT = os.path.join(PROJECT_ROOT, "data", "generated_splits")


# ---------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------


def exists(folder):
    """Folder exists if it contains CSV files."""
    return os.path.isdir(folder) and any(f.endswith(".csv") for f in os.listdir(folder))


def parquet_missing(folder):
    """Return True if parquet files are missing for the CSVs."""
    csv_files = sorted([f for f in os.listdir(folder) if f.endswith(".csv")])
    parquet_files = sorted([f for f in os.listdir(folder) if f.endswith(".parquet")])
    return len(parquet_files) == 0 or len(parquet_files) != len(csv_files)


def stats_missing(folder):
    """True if stats.parquet or value_index.json not found."""
    stats_path = os.path.join(folder, "stats.parquet")
    value_index_path = os.path.join(folder, "value_index.json")
    return not (os.path.exists(stats_path) and os.path.exists(value_index_path))


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def save_sources(folder, sources):
    ensure_dir(folder)

    for i, src in enumerate(sources):

        # ---- Detect the true ID column ----
        id_col = detect_id_column(src)

        # ---- Cast ONLY the actual ID column ----
        if id_col and id_col in src.columns:
            src[id_col] = src[id_col].astype("string[pyarrow]")

        csv_path = os.path.join(folder, f"src_{i+1}.csv")
        parquet_path = os.path.join(folder, f"src_{i+1}.parquet")

        src.to_csv(csv_path, index=False)
        src.to_parquet(parquet_path, index=False)

        print("Saving:", parquet_path)
        print(src.dtypes)


def generate_missing_parquet(folder):
    if not parquet_missing(folder):
        return

    print(f"    → Parquet missing in {folder}, generating Parquet...")
    for f in sorted(os.listdir(folder)):
        if f.endswith(".csv"):
            csv_path = os.path.join(folder, f)
            parquet_path = os.path.join(folder, f.replace(".csv", ".parquet"))
            df = pd.read_csv(csv_path)
            df.to_parquet(parquet_path, index=False)


def generate_stats(folder, UR_df):
    """Compute source value frequency statistics."""
    print(f"    → Stats missing in {folder}, generating stats...")

    csv_files = sorted([f for f in os.listdir(folder) if f.endswith(".csv")])
    sources_list = [pd.read_csv(os.path.join(folder, f)) for f in csv_files]

    # Compute vectors
    value_index, source_vectors = compute_UR_value_frequencies_in_sources(
        sources_list, UR_df
    )

    stats_path = os.path.join(folder, "stats.parquet")
    mapping_path = os.path.join(folder, "value_index.json")

    # Save vectors as parquet
    pd.DataFrame(source_vectors).to_parquet(stats_path, index=False)

    # Save mapping
    value_index_json = {f"{col}:{val}": idx for (col, val), idx in value_index.items()}
    with open(mapping_path, "w") as f:
        json.dump(value_index_json, f)


# ---------------------------------------------------------
# DATASET-LEVEL SPLITS
# ---------------------------------------------------------

PERCENTAGES = [0.01, 0.05, 0.10, 0.20]


def generate_dataset_level_splits(dataset_name, T):
    print(f"\n=== Dataset-level splits for {dataset_name} ===")

    dataset_root = os.path.join(OUTPUT_ROOT, dataset_name)

    # Build dataset-wide UR_df_all (correct shape)
    UR_all = {col: T[col].dropna().unique().tolist() for col in T.columns}

    max_len = max(len(v) for v in UR_all.values())
    rows = []
    for i in range(max_len):
        row = {}
        for col, vals in UR_all.items():
            row[col] = vals[i] if i < len(vals) else None
        rows.append(row)
    UR_df_all = pd.DataFrame(rows)

    for p in PERCENTAGES:
        n_sources = max(1, round(1 / p))
        SC = SourceConstructor(T, UR={}, seed=42)

        # RANDOM
        random_folder = os.path.join(dataset_root, f"random_{n_sources}")
        if exists(random_folder):
            generate_missing_parquet(random_folder)
            print(f"  → random_{n_sources} (exists)")
        else:
            save_sources(random_folder, SC.random_split(T, n_sources))
            print(f"  → random_{n_sources} (generated)")

        if stats_missing(random_folder):
            generate_stats(random_folder, UR_df_all)
        else:
            print(f"    → Stats OK in {random_folder}")

        # SKEWED
        skewed_folder = os.path.join(dataset_root, f"skewed_{n_sources}")
        if exists(skewed_folder):
            generate_missing_parquet(skewed_folder)
            print(f"  → skewed_{n_sources} (exists)")
        else:
            save_sources(skewed_folder, SC.skewed_split(T, n_sources))
            print(f"  → skewed_{n_sources} (generated)")

        if stats_missing(skewed_folder):
            generate_stats(skewed_folder, UR_df_all)
        else:
            print(f"    → Stats OK in {skewed_folder}")


# ---------------------------------------------------------
# UR-DEPENDENT SPLITS
# ---------------------------------------------------------

UR_N_SOURCES = [5, 20]


def generate_ur_dependent_splits(ur_id, T, UR_df):
    print(f"\n=== UR-dependent splits for UR {ur_id} ===")

    ur_root = os.path.join(OUTPUT_ROOT, f"UR{ur_id}")
    ur_dict = dataframe_to_ur_dict(UR_df)
    SC = SourceConstructor(T, ur_dict, seed=42)

    for n_sources in UR_N_SOURCES:

        # LOW PENALTY
        lp = os.path.join(ur_root, f"low_penalty_{n_sources}")
        if exists(lp):
            generate_missing_parquet(lp)
            print(f"  → low_penalty_{n_sources} (exists)")
        else:
            save_sources(lp, SC.low_penalty_sources(n_sources=n_sources))
            print(f"  → low_penalty_{n_sources} (generated)")

        if stats_missing(lp):
            generate_stats(lp, UR_df)
        else:
            print(f"    → Stats OK in {lp}")

        # HIGH PENALTY
        hp = os.path.join(ur_root, f"high_penalty_{n_sources}")
        if exists(hp):
            generate_missing_parquet(hp)
            print(f"  → high_penalty_{n_sources} (exists)")
        else:
            save_sources(hp, SC.high_penalty_sources(n_sources=n_sources))
            print(f"  → high_penalty_{n_sources} (generated)")

        if stats_missing(hp):
            generate_stats(hp, UR_df)
        else:
            print(f"    → Stats OK in {hp}")

        # LOW COVERAGE
        lc = os.path.join(ur_root, f"low_coverage_{n_sources}")
        if exists(lc):
            generate_missing_parquet(lc)
            print(f"  → low_coverage_{n_sources} (exists)")
        else:
            save_sources(lc, SC.low_coverage_sources(n_sources=n_sources))
            print(f"  → low_coverage_{n_sources} (generated)")

        if stats_missing(lc):
            generate_stats(lc, UR_df)
        else:
            print(f"    → Stats OK in {lc}")


# ---------------------------------------------------------
# MAIN DRIVER
# ---------------------------------------------------------


def dataset_from_ur_id(ur_id):
    if ur_id in {20, 21, 22, 23, 29}:
        return "MATHE"
    return "UNKNOWN"


# SQL_Variants/scripts/generate_splits.py





SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
OUTPUT_ROOT = os.path.join(PROJECT_ROOT, "data", "generated_splits")


# ---------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------


def exists(folder):
    """Folder exists if it contains CSV files."""
    return os.path.isdir(folder) and any(f.endswith(".csv") for f in os.listdir(folder))


def parquet_missing(folder):
    """Return True if parquet files are missing for the CSVs."""
    csv_files = sorted([f for f in os.listdir(folder) if f.endswith(".csv")])
    parquet_files = sorted([f for f in os.listdir(folder) if f.endswith(".parquet")])
    return len(parquet_files) == 0 or len(parquet_files) != len(csv_files)


def stats_missing(folder):
    """True if stats.parquet or value_index.json not found."""
    stats_path = os.path.join(folder, "stats.parquet")
    value_index_path = os.path.join(folder, "value_index.json")
    return not (os.path.exists(stats_path) and os.path.exists(value_index_path))


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def save_sources(folder, sources):
    ensure_dir(folder)
    for i, src in enumerate(sources):
        csv_path = os.path.join(folder, f"src_{i+1}.csv")
        parquet_path = os.path.join(folder, f"src_{i+1}.parquet")

        src.to_csv(csv_path, index=False)
        src.to_parquet(parquet_path, index=False)


def generate_missing_parquet(folder):
    if not parquet_missing(folder):
        return

    print(f"    → Parquet missing in {folder}, generating Parquet...")
    for f in sorted(os.listdir(folder)):
        if f.endswith(".csv"):
            csv_path = os.path.join(folder, f)
            parquet_path = os.path.join(folder, f.replace(".csv", ".parquet"))
            df = pd.read_csv(csv_path)
            df.to_parquet(parquet_path, index=False)


def generate_stats(folder, UR_df):
    """Compute source value frequency statistics."""
    print(f"    → Stats missing in {folder}, generating stats...")

    csv_files = sorted([f for f in os.listdir(folder) if f.endswith(".csv")])
    sources_list = [pd.read_csv(os.path.join(folder, f)) for f in csv_files]

    # Compute vectors
    value_index, source_vectors = compute_UR_value_frequencies_in_sources(
        sources_list, UR_df
    )

    stats_path = os.path.join(folder, "stats.parquet")
    mapping_path = os.path.join(folder, "value_index.json")

    # Save vectors as parquet
    pd.DataFrame(source_vectors).to_parquet(stats_path, index=False)

    # Save mapping
    value_index_json = {f"{col}:{val}": idx for (col, val), idx in value_index.items()}
    with open(mapping_path, "w") as f:
        json.dump(value_index_json, f)


# ---------------------------------------------------------
# DATASET-LEVEL SPLITS
# ---------------------------------------------------------

PERCENTAGES = [0.01, 0.05, 0.10, 0.20]


def generate_dataset_level_splits(dataset_name, T):
    print(f"\n=== Dataset-level splits for {dataset_name} ===")

    dataset_root = os.path.join(OUTPUT_ROOT, dataset_name)

    # Build dataset-wide UR_df_all (correct shape)
    UR_all = {col: T[col].dropna().unique().tolist() for col in T.columns}

    max_len = max(len(v) for v in UR_all.values())
    rows = []
    for i in range(max_len):
        row = {}
        for col, vals in UR_all.items():
            row[col] = vals[i] if i < len(vals) else None
        rows.append(row)
    UR_df_all = pd.DataFrame(rows)

    for p in PERCENTAGES:
        n_sources = max(1, round(1 / p))
        SC = SourceConstructor(T, UR={}, seed=42)

        # RANDOM
        random_folder = os.path.join(dataset_root, f"random_{n_sources}")
        if exists(random_folder):
            generate_missing_parquet(random_folder)
            print(f"  → random_{n_sources} (exists)")
        else:
            save_sources(random_folder, SC.random_split(T, n_sources))
            print(f"  → random_{n_sources} (generated)")

        if stats_missing(random_folder):
            generate_stats(random_folder, UR_df_all)
        else:
            print(f"    → Stats OK in {random_folder}")

        # SKEWED
        skewed_folder = os.path.join(dataset_root, f"skewed_{n_sources}")
        if exists(skewed_folder):
            generate_missing_parquet(skewed_folder)
            print(f"  → skewed_{n_sources} (exists)")
        else:
            save_sources(skewed_folder, SC.skewed_split(T, n_sources))
            print(f"  → skewed_{n_sources} (generated)")

        if stats_missing(skewed_folder):
            generate_stats(skewed_folder, UR_df_all)
        else:
            print(f"    → Stats OK in {skewed_folder}")


# ---------------------------------------------------------
# UR-DEPENDENT SPLITS
# ---------------------------------------------------------

UR_N_SOURCES = [5, 20]


def generate_ur_dependent_splits(ur_id, T, UR_df):
    print(f"\n=== UR-dependent splits for UR {ur_id} ===")

    ur_root = os.path.join(OUTPUT_ROOT, f"UR{ur_id}")
    ur_dict = dataframe_to_ur_dict(UR_df)
    SC = SourceConstructor(T, ur_dict, seed=42)

    for n_sources in UR_N_SOURCES:

        # LOW PENALTY
        lp = os.path.join(ur_root, f"low_penalty_{n_sources}")
        if exists(lp):
            generate_missing_parquet(lp)
            print(f"  → low_penalty_{n_sources} (exists)")
        else:
            save_sources(lp, SC.low_penalty_sources(n_sources=n_sources))
            print(f"  → low_penalty_{n_sources} (generated)")

        if stats_missing(lp):
            generate_stats(lp, UR_df)
        else:
            print(f"    → Stats OK in {lp}")

        # HIGH PENALTY
        hp = os.path.join(ur_root, f"high_penalty_{n_sources}")
        if exists(hp):
            generate_missing_parquet(hp)
            print(f"  → high_penalty_{n_sources} (exists)")
        else:
            save_sources(hp, SC.high_penalty_sources(n_sources=n_sources))
            print(f"  → high_penalty_{n_sources} (generated)")

        if stats_missing(hp):
            generate_stats(hp, UR_df)
        else:
            print(f"    → Stats OK in {hp}")

        # LOW COVERAGE
        lc = os.path.join(ur_root, f"low_coverage_{n_sources}")
        if exists(lc):
            generate_missing_parquet(lc)
            print(f"  → low_coverage_{n_sources} (exists)")
        else:
            save_sources(lc, SC.low_coverage_sources(n_sources=n_sources))
            print(f"  → low_coverage_{n_sources} (generated)")

        if stats_missing(lc):
            generate_stats(lc, UR_df)
        else:
            print(f"    → Stats OK in {lc}")


# ---------------------------------------------------------
# MAIN DRIVER
# ---------------------------------------------------------


def dataset_from_ur_id(ur_id):
    if ur_id in {20, 21, 22, 23, 29, 30}:
        return "MATHE"
    return "UNKNOWN"


def generate_all_splits():
    dataset_done = set()

    # -----------------------------------------
    # 1) Dataset-level splits → MUST use FULL MATHE dataset
    # -----------------------------------------
    # Load full MATHE table once (not the trimmed UR-based one!)
    T_full = pd.read_csv("data/MATHE/output_table.csv", sep=";", low_memory=False)
    # <-- adjust path to your file

    for ur_id in GENERAL_CONFIG["URs"]:
        _, UR_df = load_ur_and_base_table(ur_id)  # only load UR
        dataset = dataset_from_ur_id(ur_id)

        if dataset not in dataset_done:
            generate_dataset_level_splits(dataset, T_full)
            dataset_done.add(dataset)

    # -----------------------------------------
    # 2) UR-dependent splits → also must use FULL dataset
    # -----------------------------------------
    for ur_id in GENERAL_CONFIG["URs"]:
        _, UR_df = load_ur_and_base_table(ur_id)
        generate_ur_dependent_splits(ur_id, T_full, UR_df)

    print("\n=== All splits checked/generated ===")


if __name__ == "__main__":
    generate_all_splits()


if __name__ == "__main__":
    generate_all_splits()
