"""
Generate MIMIC-IV admissions split: admission_location × decade, enriched
with primary ICD diagnosis chapter (first 3 chars of icd_code).

The icd_chapter column adds a high-cardinality (~2000 unique values),
naturally concentrated attribute — analogous to Title in MovieLens and
TOPIC in CORDIS. Without it, MIMIC's demographically uniform categorical
columns can't produce diverse URs.

Run from ~/TVD:
  .venv/bin/python3 scripts_server/generate_mimic_split.py
"""
import os, sys, json
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from SQL_Variants.core.stats import compute_UR_value_frequencies_in_sources

DATA_PATH      = "data/mimic_iv/admissions.csv"
DIAGNOSES_PATH = "data/mimic_iv/diagnoses_icd.csv"
OUTPUT_DIR     = "data/generated_splits/MIMIC/admissions"
MIN_ROWS       = 100

os.makedirs(OUTPUT_DIR, exist_ok=True)

KEEP_COLS = [
    "subject_id", "hadm_id",
    "admission_type", "admission_location", "discharge_location",
    "insurance", "language", "marital_status", "race",
    "hospital_expire_flag", "decade", "icd_chapter",
]

print("Loading MIMIC-IV admissions...")
df = pd.read_csv(DATA_PATH, low_memory=False)
print(f"  {len(df):,} admissions, {df['subject_id'].nunique():,} patients")

# Derive decade from admittime
df["admittime"] = pd.to_datetime(df["admittime"])
df["decade"] = (df["admittime"].dt.year // 10 * 10).astype(int)

# Enrich with primary ICD diagnosis chapter
print("Loading primary diagnoses...")
diag = pd.read_csv(DIAGNOSES_PATH)
primary = diag[diag["seq_num"] == 1][["hadm_id", "icd_code"]].copy()
primary["icd_chapter"] = primary["icd_code"].astype(str).str[:3]
primary = primary.drop(columns=["icd_code"]).drop_duplicates("hadm_id")
df = df.merge(primary, on="hadm_id", how="left")
print(f"  Merged: {df['icd_chapter'].notna().sum():,} admissions have a primary ICD chapter")

# Drop rows with missing split keys
before = len(df)
df = df.dropna(subset=["admission_location", "decade"])
print(f"  Dropped {before - len(df)} rows with missing admission_location or decade")

df["_src_key"] = df["admission_location"] + "__" + df["decade"].astype(str)

# Group and filter small buckets
groups = df.groupby("_src_key")
keys = sorted(groups.groups.keys())
sizes = {k: len(groups.get_group(k)) for k in keys}
keys = [k for k in keys if sizes[k] >= MIN_ROWS]

print(f"\n{len(keys)} source groups (>= {MIN_ROWS} rows each):")
for k in sorted(keys, key=lambda x: sizes[x], reverse=True)[:10]:
    print(f"  {k:50s}  {sizes[k]:>7,} rows")
print(f"  ... ({len(keys)} total)")

# Write src files
print(f"\nWriting sources to {OUTPUT_DIR}/")
sources_list = []
for i, key in enumerate(keys, start=1):
    grp = groups.get_group(key)[KEEP_COLS].reset_index(drop=True)
    grp.to_csv(os.path.join(OUTPUT_DIR, f"src_{i}.csv"), index=False)
    grp.to_parquet(os.path.join(OUTPUT_DIR, f"src_{i}.parquet"), index=False)
    sources_list.append(grp)

# Build data-driven UR_df (all values, all categorical columns)
# Preserve native types so int columns (decade, flags) compare correctly against source data
EXCLUDE = {"subject_id", "hadm_id"}
all_vals = {}
for src in sources_list:
    for col in src.columns:
        if col in EXCLUDE:
            continue
        all_vals.setdefault(col, set()).update(v for v in src[col].dropna().unique())

print(f"\nIndexed columns and unique value counts:")
for col, vals in all_vals.items():
    print(f"  {col}: {len(vals)} unique values")

rows = []
for col, vals in all_vals.items():
    for v in vals:
        rows.append({c: (v if c == col else None) for c in all_vals})
ur_df = pd.DataFrame(rows)

print(f"\nComputing stats ({len(ur_df)} value rows × {len(all_vals)} cols)...")
# IMPORTANT: load sources in alphabetically-sorted filename order, which matches
# how run_experiments.py reads them (sorted(glob("src_*.parquet"))).
# src_1, src_10, src_11, ... NOT src_1, src_2, ... (numeric order).
import glob as _glob
_sorted_parquets = sorted(_glob.glob(os.path.join(OUTPUT_DIR, "src_*.parquet")))
sources_for_stats = [pd.read_parquet(f) for f in _sorted_parquets]
value_index, source_vectors = compute_UR_value_frequencies_in_sources(sources_for_stats, ur_df)

pd.DataFrame(source_vectors.astype("float32")).to_parquet(
    os.path.join(OUTPUT_DIR, "stats.parquet"), index=False)
vi_json = {f"{col}:{val}": idx for (col, val), idx in value_index.items()}
with open(os.path.join(OUTPUT_DIR, "value_index.json"), "w") as f:
    json.dump(vi_json, f)

# Manifest
manifest = pd.DataFrame({
    "src_id":   [f"src_{i}" for i in range(1, len(keys)+1)],
    "key":      keys,
    "location": [k.rsplit("__", 1)[0] for k in keys],
    "decade":   [k.rsplit("__", 1)[1] for k in keys],
    "n_rows":   [sizes[k] for k in keys],
})
manifest.to_csv(os.path.join(OUTPUT_DIR, "sources_manifest.csv"), index=False)

print(f"\nDone. {len(keys)} sources, stats: {source_vectors.shape}, index: {len(vi_json)} entries")
