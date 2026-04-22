"""
Build CORDIS split folders:
  data/generated_splits/CORDIS/candidates/  ← master CSV + parquet
  data/generated_splits/UR{id}/candidates/ ← symlinks + UR-specific stats
"""

import os, sys, json
import numpy as np
import pandas as pd

sys.path.insert(0, ".")
from helpers.test_cases import TestCases
from SQL_Variants.scripts.generate_splits import generate_stats, stats_missing

RAW_DIR    = "data/raw/cordis"
SPLIT_ROOT = "data/generated_splits"
MASTER_DIR = os.path.join(SPLIT_ROOT, "CORDIS", "candidates")
CORDIS_UR_RANGE = range(51, 71)

# ── 1. Create master CORDIS/candidates/ ───────────────────────────────────────
os.makedirs(MASTER_DIR, exist_ok=True)
src_files = sorted([f for f in os.listdir(RAW_DIR) if f.startswith("src_") and f.endswith(".csv")])
print(f"Found {len(src_files)} source files in {RAW_DIR}")

for fname in src_files:
    src_path  = os.path.abspath(os.path.join(RAW_DIR, fname))
    dest_csv  = os.path.join(MASTER_DIR, fname)
    dest_pq   = dest_csv.replace(".csv", ".parquet")

    # Copy CSV if not present
    if not os.path.exists(dest_csv):
        import shutil
        shutil.copy2(src_path, dest_csv)
        print(f"  Copied {fname}")
    else:
        print(f"  {fname} already exists")

    # Build parquet
    if not os.path.exists(dest_pq):
        df = pd.read_csv(dest_csv)
        df.to_parquet(dest_pq, index=False)
        print(f"  → {fname.replace('.csv', '.parquet')} written")

print(f"Master split at {MASTER_DIR}")

# ── 2. Load URs ───────────────────────────────────────────────────────────────
tc = TestCases()

# ── 3. Create per-UR split folders with symlinks + stats ─────────────────────
master_abs = os.path.abspath(MASTER_DIR)

for ur_id in CORDIS_UR_RANGE:
    if ur_id not in tc.cases:
        print(f"[WARN] UR{ur_id} not found in TestCases — skipping")
        continue

    ur_dir = os.path.join(SPLIT_ROOT, f"UR{ur_id}", "candidates")
    os.makedirs(ur_dir, exist_ok=True)
    ur_abs = os.path.abspath(ur_dir)

    # Symlink each CSV + parquet from master
    for fname in src_files:
        for ext_fname in [fname, fname.replace(".csv", ".parquet")]:
            link_path = os.path.join(ur_abs, ext_fname)
            target    = os.path.join(master_abs, ext_fname)
            if not os.path.exists(link_path):
                os.symlink(target, link_path)

    # Compute UR-specific stats
    UR_df = tc.cases[ur_id]
    if stats_missing(ur_abs):
        print(f"  Generating stats for UR{ur_id}...")
        generate_stats(ur_abs, UR_df)
    else:
        print(f"  UR{ur_id}: stats OK")

print("\nDone. CORDIS split folders ready.")
