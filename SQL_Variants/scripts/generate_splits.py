# SQL_Variants/scripts/generate_splits.py

import os,sys
import json
import pandas as pd
import numpy as np
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))



from SQL_Variants.config.test_config import GENERAL_CONFIG
from SQL_Variants.core.data_loading import load_ur
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
        id_col = detect_id_column(src)
        if id_col and id_col in src.columns:
            src[id_col] = src[id_col].astype("string[pyarrow]")

        csv_path = os.path.join(folder, f"src_{i+1}.csv")
        parquet_path = os.path.join(folder, f"src_{i+1}.parquet")

        src.to_csv(csv_path, index=False)
        src.to_parquet(parquet_path, index=False)

        print("Saving:", parquet_path)
        print(src.dtypes)


def generate_missing_parquet(folder):
    """Regenerate parquet from existing CSVs."""
    if not parquet_missing(folder):
        return

    print(f"    → Parquet missing in {folder}, generating Parquet...")
    for f in sorted(os.listdir(folder)):
        if f.endswith(".csv"):
            csv_path = os.path.join(folder, f)
            parquet_path = os.path.join(folder, f.replace(".csv", ".parquet"))
            df = pd.read_csv(csv_path, low_memory=False)
            df.to_parquet(parquet_path, index=False)


def generate_stats(folder, UR_df):
    """Compute source value frequency statistics."""
    print(f"    → Stats missing in {folder}, generating stats...")
    

    csv_files = sorted([f for f in os.listdir(folder) if f.endswith(".csv")])
    sources_list = [pd.read_csv(os.path.join(folder, f), low_memory=False) for f in csv_files]

    value_index, source_vectors = compute_UR_value_frequencies_in_sources(sources_list, UR_df)

    stats_path = os.path.join(folder, "stats.parquet")

    mapping_path = os.path.join(folder, "value_index.json")

    pd.DataFrame(np.asarray(source_vectors, dtype="float32")).to_parquet(stats_path, index=False)
    value_index_json = {f"{col}:{val}": idx for (col, val), idx in value_index.items()}
    with open(mapping_path, "w") as f:  

        json.dump(value_index_json, f)


# ---------------------------------------------------------
# DATASET-LEVEL SPLITS
# ---------------------------------------------------------

PERCENTAGES = [0.01, 0.05, 0.10, 0.20]


def _build_union_UR_df_for_dataset(dataset_name: str) -> pd.DataFrame:
    """
    Dataset-level UR_df_all = union of UR values for this dataset,
    based on dataset_from_ur_id + GENERAL_CONFIG["URs"].
    """
    union = {}  # col -> list of values (unique)
    for ur_id in GENERAL_CONFIG["URs"]:
        if dataset_from_ur_id(ur_id) != dataset_name:
            continue
        UR_df = load_ur(ur_id)

        for col in UR_df.columns:
            vals = UR_df[col].dropna().tolist()
            if not vals:
                continue
            if col not in union:
                union[col] = []
            union[col].extend(vals)

    # unique per col preserving order
    for col in list(union.keys()):
        seen = set()
        out = []
        for v in union[col]:
            if v not in seen:
                seen.add(v)
                out.append(v)
        union[col] = out

    if not union:
        return pd.DataFrame()

    max_len = max(len(v) for v in union.values())
    rows = []
    for i in range(max_len):
        row = {}
        for col, vals in union.items():
            row[col] = vals[i] if i < len(vals) else None
        rows.append(row)

    return pd.DataFrame(rows)


def generate_dataset_level_splits(dataset_name, T):
    print(f"\n=== Dataset-level splits for {dataset_name} ===")

    dataset_root = os.path.join(OUTPUT_ROOT, dataset_name)

    # IMPORTANT CHANGE: dataset-level stats are based on union of URs for that dataset
    UR_df_all = _build_union_UR_df_for_dataset(dataset_name)
    if UR_df_all.empty:
        print(f"  [WARN] No URs found for dataset={dataset_name}. Dataset-level stats will be skipped.")
    else:
        print(f"  [INFO] Dataset-level stats based on UR union: cols={list(UR_df_all.columns)}")

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

        if not UR_df_all.empty:
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

        if not UR_df_all.empty:
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

def dataset_from_ur_id(ur_id: int) -> str:
    if 1 <= ur_id <= 20:
        return "MOVIELENS"
    if 21 <= ur_id <= 48:
        return "TUS"
    return "UNKNOWN"



def load_full_table(dataset):
    if dataset == "MATHE":
        return pd.read_csv("data/MATHE/output_table.csv", sep=";", low_memory=False)
    if dataset == "MOVIELENS":
        return pd.read_csv("/home/slide/faresa/TVD/data/Movie_Lens/movielens-1m-full.csv", sep=",", low_memory=False)
    raise ValueError(f"Unknown dataset: {dataset}")


def generate_all_splits():
    dataset_done = set()

    # 1) Dataset-level splits → once per dataset
    for ur_id in GENERAL_CONFIG["URs"]:
        dataset = dataset_from_ur_id(ur_id)
        if dataset == "UNKNOWN":
            continue

        if dataset not in dataset_done:
            T_full = load_full_table(dataset)
            generate_dataset_level_splits(dataset, T_full)
            dataset_done.add(dataset)

    # 2) UR-dependent splits → per UR
    for ur_id in GENERAL_CONFIG["URs"]:
        dataset = dataset_from_ur_id(ur_id)
        if dataset == "UNKNOWN":
            continue

        T_full = load_full_table(dataset)
        UR_df = load_ur(ur_id)
        generate_ur_dependent_splits(ur_id, T_full, UR_df)

    print("\n=== All splits checked/generated ===")


if __name__ == "__main__":
    generate_all_splits()
