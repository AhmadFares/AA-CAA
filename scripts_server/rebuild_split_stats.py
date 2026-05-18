"""
Rebuild stats.parquet + value_index.json for any split directory
using ALL unique column values from the source files.

Usage:
  python3 scripts_server/rebuild_split_stats.py <split_dir> [--exclude col1,col2,...]

Default excluded cols: UserID, MovieID, Timestamp (too high cardinality, not query-useful).
"""
import json, os, sys, argparse
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from SQL_Variants.core.stats import compute_UR_value_frequencies_in_sources

DEFAULT_EXCLUDE = {"UserID", "MovieID", "Timestamp"}

ap = argparse.ArgumentParser()
ap.add_argument("split_dir")
ap.add_argument("--exclude", default="", help="comma-separated cols to skip")
args = ap.parse_args()

SPLIT_DIR = Path(args.split_dir)
exclude = DEFAULT_EXCLUDE | set(s.strip() for s in args.exclude.split(",") if s.strip())

sorted_files = sorted(SPLIT_DIR.glob("src_*.parquet"))
print(f"Loading {len(sorted_files)} source files from {SPLIT_DIR}...")
sources = [pd.read_parquet(f) for f in sorted_files]

all_vals = {}
for df in sources:
    for col in df.columns:
        if col in exclude:
            continue
        all_vals.setdefault(col, set()).update(str(v) for v in df[col].dropna().unique())

print("Columns and unique value counts:")
for col, vals in all_vals.items():
    print(f"  {col}: {len(vals)} unique values")
print(f"Excluded: {sorted(exclude & set(sources[0].columns))}")

# Flat UR_df: one row per value
rows = []
for col, vals in all_vals.items():
    for v in vals:
        rows.append({c: (v if c == col else None) for c in all_vals})
ur_df = pd.DataFrame(rows)

print(f"\nBuilding stats ({len(ur_df)} value rows × {len(all_vals)} cols)...")
value_index, source_vectors = compute_UR_value_frequencies_in_sources(sources, ur_df)

pd.DataFrame(source_vectors.astype("float32")).to_parquet(SPLIT_DIR / "stats.parquet", index=False)
vi_json = {f"{col}:{val}": idx for (col, val), idx in value_index.items()}
with open(SPLIT_DIR / "value_index.json", "w") as f:
    json.dump(vi_json, f)

print(f"Done. stats.parquet: {source_vectors.shape}, value_index: {len(vi_json)} entries")
