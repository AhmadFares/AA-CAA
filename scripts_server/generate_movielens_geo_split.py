"""
Generate MovieLens geo split: US region × movie release decade.

Each source = all ratings by users in a geographic region
              for movies released in a given decade.

Sources are natural data silos: regional content providers
holding subscriber viewing histories for different catalog eras.

Run from ~/TVD:
  .venv/bin/python3 scripts_server/generate_movielens_geo_split.py
"""

import os
import sys
import json
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from SQL_Variants.core.stats import compute_UR_value_frequencies_in_sources

# ── Paths ─────────────────────────────────────────────────────────────────────

DATA_PATH  = "data/raw/Movie_Lens/movielens-1m-full.csv"
OUTPUT_DIR = "data/generated_splits/MovieLens/geo"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Zip → US Region ───────────────────────────────────────────────────────────

ZIP1_TO_REGION = {
    "0": "Northeast",   # CT, MA, ME, NH, NJ, NY, RI, VT
    "1": "Northeast",   # DE, NY, PA
    "2": "Southeast",   # DC, MD, NC, SC, VA, WV
    "3": "Southeast",   # AL, FL, GA, MS, TN
    "4": "Midwest",     # IN, KY, MI, OH
    "5": "Midwest",     # IA, MN, MT, ND, SD, WI
    "6": "Midwest",     # IL, KS, MO, NE
    "7": "South",       # AR, LA, OK, TX
    "8": "Mountain",    # AZ, CO, ID, NM, NV, UT, WY
    "9": "West",        # AK, CA, HI, OR, WA
}

# ── Load ──────────────────────────────────────────────────────────────────────

print("Loading MovieLens...")
df = pd.read_csv(DATA_PATH, low_memory=False)
print(f"  {len(df):,} rows, {df['UserID'].nunique():,} users, {df['MovieID'].nunique():,} movies")

# ── Derive split keys (temporary columns, NOT written to src files) ───────────

df["_zip1"]   = df["Zip-code"].astype(str).str[0]
df["_region"] = df["_zip1"].map(ZIP1_TO_REGION)
df["_year"]   = df["Title"].str.extract(r"\((\d{4})\)")[0].astype(float)
df["_decade"] = (df["_year"] // 10 * 10).astype("Int64")
df["_src_key"] = df["_region"] + "__" + df["_decade"].astype(str)

# Drop rows with no region or no year
before = len(df)
df = df.dropna(subset=["_region", "_year"])
print(f"  Dropped {before - len(df)} rows with missing region or year")

# ── Build source groups ────────────────────────────────────────────────────────

OUTPUT_COLS = ["UserID", "MovieID", "Rating", "Timestamp",
               "Gender", "Age", "Occupation", "Zip-code", "Title", "Genres"]

groups = df.groupby("_src_key")
keys   = sorted(groups.groups.keys())

print(f"\n{len(keys)} source groups found:")
sizes = {k: len(groups.get_group(k)) for k in keys}
for k in sorted(sizes, key=sizes.get, reverse=True):
    print(f"  {k:30s}  {sizes[k]:>7,} rows")

# ── Write src_N.csv + src_N.parquet ───────────────────────────────────────────

print(f"\nWriting sources to {OUTPUT_DIR}/")
sources_list = []

for i, key in enumerate(keys, start=1):
    grp = groups.get_group(key)[OUTPUT_COLS].reset_index(drop=True)

    csv_path     = os.path.join(OUTPUT_DIR, f"src_{i}.csv")
    parquet_path = os.path.join(OUTPUT_DIR, f"src_{i}.parquet")

    grp.to_csv(csv_path,     index=False)
    grp.to_parquet(parquet_path, index=False)

    sources_list.append(grp)
    print(f"  src_{i:>2}.csv  ←  {key:30s}  ({len(grp):>7,} rows)")

# ── Build union UR_df from all MovieLens URs ──────────────────────────────────
# Manually union the UR attribute columns so stats cover everything the
# experiment runner will ever ask about.

print("\nBuilding union UR_df from all MovieLens UR values...")

# Load all URs from test_cases (IDs 1-20 are MovieLens)
try:
    from helpers.test_cases import TestCases
    tc = TestCases()
    union = {}
    for ur_id in range(1, 21):
        if ur_id not in tc.cases:
            continue
        ur_df = tc.cases[ur_id]
        for col in ur_df.columns:
            vals = ur_df[col].dropna().tolist()
            if not vals:
                continue
            union.setdefault(col, [])
            for v in vals:
                if v not in union[col]:
                    union[col].append(v)

    max_len = max(len(v) for v in union.values())
    rows = []
    for i in range(max_len):
        row = {col: vals[i] if i < len(vals) else None
               for col, vals in union.items()}
        rows.append(row)
    ur_df_all = pd.DataFrame(rows)
    print(f"  Union UR_df: {len(ur_df_all)} rows × {list(ur_df_all.columns)}")

except Exception as e:
    print(f"  [WARN] Could not load URs from TestCases ({e}). Using full table as UR_df.")
    ur_df_all = df[OUTPUT_COLS].copy()

# ── Compute & save stats ──────────────────────────────────────────────────────

print("\nComputing stats (value frequencies per source)...")
value_index, source_vectors = compute_UR_value_frequencies_in_sources(sources_list, ur_df_all)

stats_path = os.path.join(OUTPUT_DIR, "stats.parquet")
index_path = os.path.join(OUTPUT_DIR, "value_index.json")

pd.DataFrame(np.asarray(source_vectors, dtype="float32")).to_parquet(stats_path, index=False)

value_index_json = {f"{col}:{val}": idx for (col, val), idx in value_index.items()}
with open(index_path, "w") as f:
    json.dump(value_index_json, f)

print(f"  stats.parquet  → {source_vectors.shape[0]} sources × {source_vectors.shape[1]} attributes")
print(f"  value_index.json → {len(value_index_json)} entries")

# ── Write source key manifest (for reference) ─────────────────────────────────

manifest_path = os.path.join(OUTPUT_DIR, "sources_manifest.csv")
manifest = pd.DataFrame({
    "src_id":   [f"src_{i}" for i in range(1, len(keys)+1)],
    "key":      keys,
    "region":   [k.split("__")[0] for k in keys],
    "decade":   [k.split("__")[1] for k in keys],
    "n_rows":   [sizes[k] for k in keys],
})
manifest.to_csv(manifest_path, index=False)
print(f"\nManifest saved: {manifest_path}")

print(f"\n✓ Done — {len(keys)} sources in {OUTPUT_DIR}")
