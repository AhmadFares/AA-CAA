import os, json
import pandas as pd
import numpy as np

from SQL_Variants.core.data_loading import load_ur
from helpers.Source_Constructors import SourceConstructor, dataframe_to_ur_dict
from SQL_Variants.core.stats import compute_UR_value_frequencies_in_sources

def ensure_dir(p): os.makedirs(p, exist_ok=True)
def read_csv_safe(path):
    # try utf-8 first, then common fallbacks
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        try:
            return pd.read_csv(path, low_memory=False, encoding=enc)
        except UnicodeDecodeError:
            continue
    # last resort: replace bad bytes
    return pd.read_csv(path, low_memory=False, encoding="utf-8", encoding_errors="replace")


def parquet_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make df safe for pyarrow parquet:
    - any object column -> cast to string
    This avoids ArrowTypeError from mixed int/str in object columns (like U_20).
    """
    df2 = df.copy()
    obj_cols = df2.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df2[c] = df2[c].astype(str)
    return df2

def save_one_table(df, out_dir, idx):
    csv_path = os.path.join(out_dir, f"src_{idx}.csv")
    parquet_path = os.path.join(out_dir, f"src_{idx}.parquet")

    # Always save CSV (no strict typing issues)
    df.to_csv(csv_path, index=False)

    # Make parquet-safe and save
    df2 = parquet_safe(df)
    df2.to_parquet(parquet_path, index=False)

def generate_stats_for_folder(folder, UR_df):
    csvs = sorted([f for f in os.listdir(folder) if f.endswith(".csv")])
    sources = [read_csv_safe(os.path.join(folder,f)) for f in csvs]
    value_index, source_vectors = compute_UR_value_frequencies_in_sources(sources, UR_df)
    pd.DataFrame(np.asarray(source_vectors, dtype="float32")).to_parquet(os.path.join(folder,"stats.parquet"), index=False)
    with open(os.path.join(folder,"value_index.json"), "w") as f:
        json.dump({f"{c}:{v}": i for (c,v), i in value_index.items()}, f)

def generate_tus_splits_for_one_ur(
    ur_id: int,
    candidates_dir: str,          # folder with src_*.csv
    out_root: str,                # e.g. data/generated_splits/UR40
    UR_df: pd.DataFrame,
    do_stats: bool = True
):
    ur_dict = dataframe_to_ur_dict(UR_df)

    # load candidate tables (real sources)
    cand_files = sorted([f for f in os.listdir(candidates_dir) if f.endswith(".csv")])
    cand_tables = [read_csv_safe(os.path.join(candidates_dir,f)) for f in cand_files]

    # output folders
    cand_out = os.path.join(out_root, "candidates")
    hp_out   = os.path.join(out_root, "high_penalty")
    lp_out   = os.path.join(out_root, "low_penalty")
    lc_out   = os.path.join(out_root, "low_coverage")
    for d in [cand_out, hp_out, lp_out, lc_out]:
        ensure_dir(d)

    # process each table independently
    for i, S in enumerate(cand_tables, start=1):
        # 1) as-is
        save_one_table(S, cand_out, i)

        # Build a constructor per table (T = this table)
        SC = SourceConstructor(S, ur_dict, seed=42)

        # 2) high penalty version of THIS table
        hp = SC.high_penalty_sources(n_sources=1)[0]
        save_one_table(hp, hp_out, i)

        # 3) low penalty version
        lp = SC.low_penalty_sources(n_sources=1)[0]
        save_one_table(lp, lp_out, i)

        # 4) low coverage version
        lc = SC.low_coverage_sources(n_sources=1)[0]
        save_one_table(lc, lc_out, i)

        print(f"[UR{ur_id}] processed src_{i} ({len(S)} rows)")

    # stats (optional)
    if do_stats:
        for folder in [cand_out, hp_out, lp_out, lc_out]:
            generate_stats_for_folder(folder, UR_df)
            print(f"[UR{ur_id}] stats OK: {folder}")



if __name__ == "__main__":
    UR_ID = [21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40]
    for ur in UR_ID:
        candidates_dir = f"/home/slide/faresa/TVD/data/tus_20_selected/candidates/UR{ur}"
        out_root = f"/home/slide/faresa/TVD/data/generated_splits/UR{ur}"
        UR_df = load_ur(ur) 

        generate_tus_splits_for_one_ur(ur, candidates_dir, out_root, UR_df, do_stats=True)