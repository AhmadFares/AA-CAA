"""
Build CORDIS split folders on the server.
Standalone — avoids full project import chain (no TestCases, no data_loading).
Schema: TOPIC, FUNDING_SCHEME, ACTIVITY_TYPE, STATUS, SME, MASTER_CALL, LEGAL_BASIS
"""

import os, json, shutil
import numpy as np
import pandas as pd

RAW_DIR    = "data/raw/cordis"
SPLIT_ROOT = "data/generated_splits"
MASTER_DIR = os.path.join(SPLIT_ROOT, "CORDIS", "candidates")

# ── All CORDIS URs defined inline — no project import needed ──────────────────
CORDIS_URS = {
    51: {"TOPIC": ["CNS"]},
    52: {"TOPIC": ["GEANT", "EURATOM", "SECURITY"]},
    53: {"TOPIC": ["Engines", "Airframes", "Systems"],
         "MASTER_CALL": ["H2020-CS2-CFP09-2018-02"]},
    54: {"TOPIC": ["5G"], "MASTER_CALL": ["H2020-FETFLAG-2014"],
         "FUNDING_SCHEME": ["COFUND"]},
    55: {"TOPIC": ["FETFLAGSHIP", "5G and beyond", "EDGE COMPUTING"],
         "MASTER_CALL": ["H2020-FETHPC-2018-2020"]},
    56: {"TOPIC": ["Smart Mobility", "Science4Refugees", "Design Technology"],
         "MASTER_CALL": ["H2020-EEN-GA-2017-2018"], "FUNDING_SCHEME": ["COFUND-PCP"]},
    57: {"MASTER_CALL": ["EURATOM-Adhoc-2014-20", "H2020-BIR-2014", "H2020-CBBA-2016"]},
    58: {"MASTER_CALL": ["H2020-ECSEL-2018-3-CSA-Industry4E-one-stage",
                         "H2020-ECSEL-2019-3-CSA-Health-E-one-stage",
                         "H2020-ECSEL-2018-4-CSA-MobilityE-one-stage"],
         "LEGAL_BASIS": ["H2020-EU.2."]},
    59: {"LEGAL_BASIS": ["H2020-EU.3.3.;H2020-EU.3.3.3.1.;H2020-EU.3.3.3.3.",
                         "H2020-EU.4.e.", "H2020-EU.5.g."]},
    60: {"TOPIC": ["CNS", "5G and beyond", "U-space", "Health"],
         "LEGAL_BASIS": ["H2020-EU.2.", "H2020-EU.2.1."]},
    61: {"FUNDING_SCHEME": ["COFUND", "CS2-CSA", "COFUND-PCP", "IA-LS"]},
    62: {"TOPIC": ["FETFLAGSHIP", "Smart Mobility"],
         "FUNDING_SCHEME": ["IMI2-CSA", "SESAR-CSA"],
         "MASTER_CALL": ["H2020-EEN-GA3-2018"]},
    63: {"TOPIC": ["Engines", "Datalink", "CWP - HMI", "H2 Valley"],
         "FUNDING_SCHEME": ["Shift2Rail-IA", "Shift2Rail-CSA"]},
    64: {"MASTER_CALL": ["H2020-ALTFI-2017", "H2020-BBI-JTI-2015-01", "H2020-EUK-2016"],
         "LEGAL_BASIS": ["H2020-EU.4.f.", "H2020-EU.5.e."]},
    65: {"TOPIC": ["CNS", "COST", "Food", "MSCA", "INFRA", "ERC-14", "EURATOM"]},
    66: {"TOPIC": ["5G Networks", "SECURITY", "IPorta 2", "SPREADING"],
         "MASTER_CALL": ["H2020-EIC-Mutuallearning", "H2020-EEN-GA-2017-2018-2"]},
    67: {"TOPIC": ["SPREADING", "IPorta 2", "EUCYS 2020"],
         "FUNDING_SCHEME": ["PPI", "ERC-LVG"],
         "MASTER_CALL": ["H2020-FPA-SGA-SC1-CEPI-2019"], "STATUS": ["TERMINATED"]},
    68: {"TOPIC": ["Privacy", "Teaming", "Space Weather", "ERA Chairs"],
         "MASTER_CALL": ["ERC-2014-SUPPORT-1"], "LEGAL_BASIS": ["H2020-EU.5.h."]},
    69: {"MASTER_CALL": ["H2020-IBA-19-CHAIR-2017", "H2020-IBA-ARF-Austria-2018"],
         "LEGAL_BASIS": ["H2020-EU.4.e.", "H2020-EU.5.g."],
         "FUNDING_SCHEME": ["COFUND", "COFUND-EJP"]},
    70: {"TOPIC": ["CNS", "GEANT", "Engines", "Airframes", "5G",
                   "Smart Mobility", "U-space", "SPREADING"],
         "MASTER_CALL": ["EURATOM-Adhoc-2014-20", "H2020-BIR-2014",
                         "H2020-FETHPC-2018-2020"],
         "LEGAL_BASIS": ["H2020-EU.2.", "H2020-EU.4.e."]},
}

# ── Stats helpers ──────────────────────────────────────────────────────────────
def compute_stats(sources, ur_dict):
    value_index = {}
    idx = 0
    for col, vals in ur_dict.items():
        for val in vals:
            value_index[(col, val)] = idx
            idx += 1

    source_vectors = []
    for df in sources:
        vec = np.zeros(len(value_index), dtype=np.float32)
        n = len(df)
        if n > 0:
            for (col, val), i in value_index.items():
                if col in df.columns:
                    vec[i] = (df[col] == val).sum() / n
        source_vectors.append(vec)

    return value_index, np.array(source_vectors, dtype=np.float32)

def save_stats(folder, value_index, source_vectors):
    vi_json = {f"{col}:{val}": i for (col, val), i in value_index.items()}
    with open(os.path.join(folder, "value_index.json"), "w") as f:
        json.dump(vi_json, f)
    pd.DataFrame(source_vectors).to_parquet(
        os.path.join(folder, "stats.parquet"), index=False)

def stats_ok(folder):
    return (os.path.exists(os.path.join(folder, "stats.parquet")) and
            os.path.exists(os.path.join(folder, "value_index.json")))

# ── 1. Build master CORDIS/candidates/ ────────────────────────────────────────
os.makedirs(MASTER_DIR, exist_ok=True)
src_files = sorted([f for f in os.listdir(RAW_DIR)
                    if f.startswith("src_") and f.endswith(".csv")])
print(f"Found {len(src_files)} source files")

for fname in src_files:
    dest_csv = os.path.join(MASTER_DIR, fname)
    dest_pq  = dest_csv.replace(".csv", ".parquet")
    if not os.path.exists(dest_csv):
        shutil.copy2(os.path.join(RAW_DIR, fname), dest_csv)
    if not os.path.exists(dest_pq):
        pd.read_csv(dest_csv).to_parquet(dest_pq, index=False)

print(f"Master split ready at {MASTER_DIR}")

# Pre-load all 26 sources once
sources = [pd.read_csv(os.path.join(MASTER_DIR, f)) for f in src_files]
master_abs = os.path.abspath(MASTER_DIR)

# ── 2. Build per-UR split folders ─────────────────────────────────────────────
for ur_id, ur_dict in CORDIS_URS.items():
    ur_dir = os.path.abspath(os.path.join(SPLIT_ROOT, f"UR{ur_id}", "candidates"))
    os.makedirs(ur_dir, exist_ok=True)

    for fname in src_files:
        for ext in [fname, fname.replace(".csv", ".parquet")]:
            link   = os.path.join(ur_dir, ext)
            target = os.path.join(master_abs, ext)
            if not os.path.exists(link):
                os.symlink(target, link)

    if not stats_ok(ur_dir):
        vi, sv = compute_stats(sources, ur_dict)
        save_stats(ur_dir, vi, sv)
        print(f"  UR{ur_id}: stats generated")
    else:
        print(f"  UR{ur_id}: stats OK")

print("\nDone. All CORDIS splits ready.")
