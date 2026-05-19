"""
Generate MovieLens scalability splits for source-count experiments.

Three splits of the same MovieLens dataset at different granularities:
  geo_10   — zip 1-digit prefix              → ~10  sources
  geo_100  — zip 2-digit prefix              → ~100 sources
  geo_1000 — zip 3-digit prefix × Gender     → ~1000 sources

The existing geo split (60 sources) is used as-is.
Stats are computed using the union of all values from URs 201-240.

Run from repo root:
  .venv/bin/python3 scripts_server/generate_movielens_scale_splits.py
"""

import os
import sys
import json
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from SQL_Variants.core.stats import compute_UR_value_frequencies_in_sources

DATA_PATH  = "data/raw/Movie_Lens/movielens-1m-full.csv"
OUTPUT_ROOT = "data/generated_splits/MovieLens"

OUTPUT_COLS = ["UserID", "MovieID", "Rating", "Timestamp",
               "Gender", "Age", "Occupation", "Zip-code", "Title", "Genres"]

# ── Load data ─────────────────────────────────────────────────────────────────

print("Loading MovieLens...")
df = pd.read_csv(DATA_PATH, low_memory=False)
print(f"  {len(df):,} rows")

df["_zip"] = df["Zip-code"].astype(str).str.strip()

# ── Build union UR_df from URs 201-240 for stats ──────────────────────────────

print("\nBuilding union UR_df from URs 201-240...")
try:
    from helpers.test_cases import TestCases
    tc = TestCases()
    union = {}
    for ur_id in range(201, 241):
        if ur_id not in tc.cases:
            continue
        ur_df = tc.cases[ur_id]
        for col in ur_df.columns:
            vals = ur_df[col].dropna().tolist()
            union.setdefault(col, [])
            for v in vals:
                if v not in union[col]:
                    union[col].append(v)
    max_len = max(len(v) for v in union.values())
    ur_df_all = pd.DataFrame({
        col: vals + [None] * (max_len - len(vals))
        for col, vals in union.items()
    })
    print(f"  Union UR: {len(ur_df_all)} rows × {list(ur_df_all.columns)}")
except Exception as e:
    print(f"  [WARN] Could not load URs ({e}) — using full table for stats")
    ur_df_all = df[OUTPUT_COLS].copy()

# ── Split definitions ─────────────────────────────────────────────────────────

SPLITS = [
    {
        "name":    "geo_10",
        "key_fn":  lambda d: d["_zip"].str[0],
        "desc":    "zip 1-digit prefix",
    },
    {
        "name":    "geo_100",
        "key_fn":  lambda d: d["_zip"].str[:2],
        "desc":    "zip 2-digit prefix",
    },
    {
        "name":    "geo_1000",
        "key_fn":  lambda d: d["_zip"].str[:3] + "_" + d["Gender"].astype(str),
        "desc":    "zip 3-digit prefix × Gender",
    },
]

# ── Generate each split ───────────────────────────────────────────────────────

for split in SPLITS:
    name  = split["name"]
    outdir = os.path.join(OUTPUT_ROOT, name)
    os.makedirs(outdir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Split: {name}  ({split['desc']})")

    df["_key"] = split["key_fn"](df)

    # drop rows with missing key
    df_clean = df.dropna(subset=["_key"])
    groups   = df_clean.groupby("_key")
    keys     = sorted(groups.groups.keys())

    # filter out tiny sources (< 20 rows) to keep splits clean
    keys = [k for k in keys if len(groups.get_group(k)) >= 20]

    print(f"  {len(keys)} sources (after dropping sources with <20 rows)")

    sizes        = {k: len(groups.get_group(k)) for k in keys}
    size_vals    = list(sizes.values())
    print(f"  Size range: {min(size_vals):,} – {max(size_vals):,} rows")
    print(f"  Median:     {int(np.median(size_vals)):,} rows")

    sources_list = []
    for i, key in enumerate(keys, start=1):
        grp = groups.get_group(key)[OUTPUT_COLS].reset_index(drop=True)
        grp.to_csv(    os.path.join(outdir, f"src_{i}.csv"),     index=False)
        grp.to_parquet(os.path.join(outdir, f"src_{i}.parquet"), index=False)
        sources_list.append(grp)

    print(f"  Written {len(keys)} source files to {outdir}/")

    # ── Stats ─────────────────────────────────────────────────────────────────
    print("  Computing stats...")
    value_index, source_vectors = compute_UR_value_frequencies_in_sources(
        sources_list, ur_df_all
    )

    pd.DataFrame(np.asarray(source_vectors, dtype="float32")).to_parquet(
        os.path.join(outdir, "stats.parquet"), index=False
    )
    vi_json = {f"{col}:{val}": idx for (col, val), idx in value_index.items()}
    with open(os.path.join(outdir, "value_index.json"), "w") as f:
        json.dump(vi_json, f)

    print(f"  Stats: {source_vectors.shape[0]} sources × {source_vectors.shape[1]} values")

    # ── Manifest ──────────────────────────────────────────────────────────────
    manifest = pd.DataFrame({
        "src_id": [f"src_{i}" for i in range(1, len(keys) + 1)],
        "key":    keys,
        "n_rows": [sizes[k] for k in keys],
    })
    manifest.to_csv(os.path.join(outdir, "sources_manifest.csv"), index=False)

print(f"\n✓ Done — generated {len(SPLITS)} scale splits under {OUTPUT_ROOT}/")
