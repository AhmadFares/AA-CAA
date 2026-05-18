"""
auto_ur_generator.py
====================
Automatically generate Deep + Shallow URs from any split's stats.

Deep  (2-3 attrs, 3-5 values each):
    Values span DIFFERENT, mostly-disjoint source clusters.
    Stats Guided must visit multiple source groups to reach full coverage.

Shallow (4-5 attrs, 1-2 values each):
    High specificity → only 1-6 sources contain all constraints.
    Stats Guided immediately identifies and selects those sources.

Input:  split directory with stats.parquet + value_index.json
Output: dict  {name: {attr: [values]}}
"""

import json
import random
import numpy as np
import pandas as pd
from pathlib import Path


# ── helpers ──────────────────────────────────────────────────────────────────

def _normalize_val(val_str):
    """
    Convert a value string from value_index keys to a Python-native type.
    "35.0" → 35  (int)    "0.5" → 0.5  (float)   "98103" → "98103"  (str)

    Rules:
    - Strings with leading zeros kept as-is ("06880" → "06880").
    - Strings WITHOUT a decimal point kept as strings — avoids converting
      zip codes like "11355" to the integer 11355.
    - Only strings WITH a "." are candidates for numeric conversion.
    """
    s = str(val_str)
    if len(s) > 1 and s[0] == "0" and s[1].isdigit():
        return s
    if "." in s:
        try:
            f = float(s)
            if f == int(f):
                return int(f)
            return f
        except (ValueError, TypeError):
            pass
    return s


def _load_split_index(split_path):
    """
    Returns:
        stats    : pd.DataFrame  shape (n_sources, n_features)
        attr_info: dict  {attr -> list of {val, col_idx, n_src, frac, src_set}}
                   sorted by ascending n_src (most scarce first)
    """
    split_path = Path(split_path)
    stats = pd.read_parquet(split_path / "stats.parquet")

    with open(split_path / "value_index.json") as f:
        vi = json.load(f)

    n_src = len(stats)

    attr_info = {}
    for key, col_idx in vi.items():
        if ":" not in key:
            continue
        attr, val = key.split(":", 1)
        col = stats[str(col_idx)]
        src_set = frozenset(int(i) for i in stats.index[col > 0])
        entry = dict(val=val, col=col_idx, n_src=len(src_set),
                     frac=len(src_set) / n_src, src_set=src_set)
        attr_info.setdefault(attr, []).append(entry)

    for attr in attr_info:
        attr_info[attr].sort(key=lambda x: x["n_src"])

    return stats, attr_info, n_src


# ── deep UR builder ───────────────────────────────────────────────────────────

def _build_deep(attr_info, n_src, n_rows=4, n_attrs=2, rng=None, attempt=0, frac_upper=0.40):
    """
    Pick `n_attrs` attributes and `n_rows` value-rows such that:
      - Each row targets a mostly-disjoint source cluster (primary attribute).
      - For each secondary attribute, the chosen value co-occurs with that
        row's primary value in at least one source (row_live intersection).

    Strategy
    --------
    1. Pick a PRIMARY attribute with values in 2%–frac_upper of sources.
    2. Greedily pick n_rows primary values with mostly disjoint source sets.
    3. For each SECONDARY attribute, pick one value PER ROW that intersects
       with that row's current live source set.
    """
    if rng is None:
        rng = random.Random(0)

    candidates = {
        attr: [v for v in vals if 0.02 < v["frac"] < frac_upper]
        for attr, vals in attr_info.items()
    }
    usable_attrs = [a for a, c in candidates.items() if len(c) >= n_rows]
    if len(usable_attrs) < n_attrs:
        return None

    rng.shuffle(usable_attrs)
    primary_attr = usable_attrs[attempt % len(usable_attrs)]
    secondary_attrs = [a for a in usable_attrs if a != primary_attr][:n_attrs - 1]

    pool = candidates[primary_attr].copy()
    rng.shuffle(pool)

    selected = []
    covered  = set()
    for entry in pool:
        if not selected:
            selected.append(entry)
            covered = set(entry["src_set"])
        else:
            overlap_ratio = len(entry["src_set"] & covered) / max(len(entry["src_set"]), 1)
            if overlap_ratio < 0.4:
                selected.append(entry)
                covered |= entry["src_set"]
        if len(selected) >= n_rows:
            break

    if len(selected) < 2:
        return None

    n_actual = len(selected)
    ur = {primary_attr: [_normalize_val(e["val"]) for e in selected]}

    row_live = [set(e["src_set"]) for e in selected]

    for sec_attr in secondary_attrs:
        row_vals = []
        new_live = []
        for i, prim_entry in enumerate(selected):
            live = row_live[i]
            good = [v for v in candidates[sec_attr]
                    if len(v["src_set"] & live) >= 1]
            if not good:
                good = [v for v in candidates[sec_attr]
                        if len(v["src_set"] & prim_entry["src_set"]) >= 1]
            if not good:
                break
            pick = rng.choice(good)
            row_vals.append(_normalize_val(pick["val"]))
            new_live.append(live & pick["src_set"])
        if len(row_vals) == n_actual:
            ur[sec_attr] = row_vals
            row_live = new_live

    if len(ur) < 2:
        return None
    return ur


# ── shallow UR builder ────────────────────────────────────────────────────────

def _build_shallow(attr_info, n_src, split_path, n_attrs=4, n_rows=1, rng=None, attempt=0, df_cache=None):
    """
    Shallow UR: uses ONLY indexed attrs (frac 2–40%), so SG gets accurate gain
    estimates (p>0 for all attrs).  Conjunction of chosen attrs targets ≤15% of
    sources.  Record-level satisfiability is verified by finding a real record in
    one of the conjunction sources.

    Strategy
    --------
    1. Build a pool of all (attr, val, src_set) indexed at 2–40% frac.
    2. Pick an anchor entry (cycle by attempt for diversity).
    3. Greedily add entries that shrink the intersection without emptying it.
    4. Accept when conjunction ≤ max_conj and len ≥ 2.
    5. Verify: find an actual record in the conjunction with all attrs matching.
    """
    if rng is None:
        rng = random.Random(0)

    split_path  = Path(split_path)
    max_conj    = max(3, int(0.20 * n_src))   # ≤20% of sources

    # stats.parquet rows are in lexicographic order of source filenames.
    # stats row idx  →  sorted_files[idx]
    sorted_files = sorted(split_path.glob("src_*.parquet"))
    if not sorted_files:
        return None

    # Fast lookup: attr → {val_str: src_set} for indexed vals at 2–40%
    indexed = {}
    for attr, vals in attr_info.items():
        indexed[attr] = {v["val"]: v["src_set"] for v in vals if 0.02 <= v["frac"] <= 0.40}

    MAX_INNER = len(sorted_files) * 4

    for i in range(MAX_INNER):
        src_file = sorted_files[(attempt * 7 + i) % len(sorted_files)]
        # Use shared cache to avoid re-reading the same file
        if df_cache is None:
            try:
                df = pd.read_parquet(src_file)
            except Exception:
                continue
        else:
            key = str(src_file)
            if key not in df_cache:
                try:
                    df_cache[key] = pd.read_parquet(src_file)
                except Exception:
                    df_cache[key] = pd.DataFrame()
            df = df_cache[key]
        if df.empty:
            continue

        record = df.iloc[rng.randint(0, len(df) - 1)]

        # Find all indexed attrs whose record value is in the index at 2–40% frac
        cands = []   # (attr, val_str, src_set)
        for attr, val_map in indexed.items():
            if attr not in record.index or not pd.notna(record[attr]):
                continue
            raw  = str(record[attr]).strip()
            norm = _normalize_val(raw)
            chk  = {str(norm), raw}
            try:
                f = float(norm)
                chk.add(f"{f:.1f}")
                if f == int(f):
                    chk.add(str(int(f)))
            except (ValueError, TypeError):
                pass
            for v_str, src_set in val_map.items():
                if v_str in chk:
                    cands.append((attr, v_str, src_set))
                    break

        if len(cands) < 2:
            continue

        # Sort by ascending src_set size; rotate start by attempt+i for variety
        cands.sort(key=lambda x: len(x[2]))
        start = (attempt + i) % len(cands)
        cands = cands[start:] + cands[:start]

        # Greedy conjunction
        chosen = [(cands[0][0], cands[0][1])]
        conj   = set(cands[0][2])

        for attr, val_str, src_set in cands[1:]:
            if attr in {a for a, _ in chosen}:
                continue
            new_conj = conj & set(src_set)
            if len(new_conj) == 0:
                continue
            chosen.append((attr, val_str))
            conj = new_conj
            if len(chosen) >= n_attrs:
                break

        if len(chosen) < 2 or len(conj) == 0 or len(conj) > max_conj:
            continue

        # All values came from the same real record → satisfiability guaranteed
        ur = {attr: [_normalize_val(val_str)] for attr, val_str in chosen[:n_attrs]}
        return ur

    return None


# ── main public API ───────────────────────────────────────────────────────────

def generate_auto_urs(split_path, n_deep=10, n_shallow=10, seed=42):
    """
    Generate `n_deep` Deep URs and `n_shallow` Shallow URs for `split_path`.

    Deep  configs: (n_rows, n_attrs) — 3–5 rows, 2–3 attrs
    Shallow configs: (n_attrs, n_rows) — 4–5 attrs, 1–2 rows
    """
    stats, attr_info, n_src = _load_split_index(split_path)
    rng = random.Random(seed)
    urs = {}

    deep_configs = [
        (4, 2), (4, 2), (5, 2), (5, 2),
        (3, 3), (3, 3), (4, 3), (4, 3),
        (5, 3), (5, 3),
    ]
    deep_count = 0
    attempt = 0
    while deep_count < n_deep and attempt < n_deep * 10:
        n_rows, n_attrs = deep_configs[deep_count % len(deep_configs)]
        ur = _build_deep(attr_info, n_src, n_rows=n_rows, n_attrs=n_attrs,
                         rng=rng, attempt=attempt, frac_upper=0.40)
        if ur is not None:
            urs[f"auto_deep_{deep_count + 1}"] = ur
            deep_count += 1
        attempt += 1

    # Fallback: if 0.40 didn't yield enough, retry remaining with frac_upper=0.50
    attempt = 0
    while deep_count < n_deep and attempt < n_deep * 10:
        n_rows, n_attrs = deep_configs[deep_count % len(deep_configs)]
        ur = _build_deep(attr_info, n_src, n_rows=n_rows, n_attrs=n_attrs,
                         rng=rng, attempt=attempt, frac_upper=0.50)
        if ur is not None:
            urs[f"auto_deep_{deep_count + 1}"] = ur
            deep_count += 1
        attempt += 1

    # n_attrs cycles 4,5,6,... — always 1 row (new design)
    shallow_n_attrs = [4, 5, 6, 4, 5, 6, 4, 5, 6, 4]
    shallow_count = 0
    attempt = 0
    seen_shallow = set()
    df_cache: dict = {}        # shared parquet cache — each file read at most once
    while shallow_count < n_shallow and attempt < n_shallow * 50:
        n_attrs = shallow_n_attrs[shallow_count % len(shallow_n_attrs)]
        ur = _build_shallow(attr_info, n_src, split_path,
                            n_attrs=n_attrs, n_rows=1,
                            rng=rng, attempt=attempt, df_cache=df_cache)
        if ur is not None:
            key = frozenset((a, tuple(v)) for a, v in ur.items())
            if key not in seen_shallow:
                seen_shallow.add(key)
                urs[f"auto_shallow_{shallow_count + 1}"] = ur
                shallow_count += 1
        attempt += 1

    return urs


# ── quick diagnostic ──────────────────────────────────────────────────────────

def describe_ur(ur, attr_info, n_src):
    """Print a brief description of a generated UR."""
    print(f"  Attrs: {list(ur.keys())}  |  Rows: {max(len(v) for v in ur.values())}")
    for attr, vals in ur.items():
        val_to_src = {e["val"]: e["src_set"] for e in attr_info.get(attr, [])}
        for v in set(vals):
            srcs = (val_to_src.get(str(v))
                    or val_to_src.get(f"{float(v):.1f}" if isinstance(v, (int, float)) else str(v))
                    or frozenset())
            print(f"    {attr}={v!r}  →  {len(srcs)}/{n_src} sources")


if __name__ == "__main__":
    import sys
    split = sys.argv[1] if len(sys.argv) > 1 else "data/generated_splits/MovieLens/geo"
    urs = generate_auto_urs(split)
    _, attr_info, n_src = _load_split_index(split)
    print(f"\nGenerated {len(urs)} URs for {split}\n")
    for name, ur in urs.items():
        print(f"[{name}]")
        describe_ur(ur, attr_info, n_src)
        print()
