# SQL_Variants/core/sql_builders.py

from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional
from typing import Dict, List, Any
import pandas as pd


def _sql_ident(col: str) -> str:
    # DuckDB/SQL identifier quoting
    return '"' + col.replace('"', '""') + '"'

def _is_numeric(v) -> bool:
    if isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        return True
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return False
        try:
            float(s)
            return True
        except ValueError:
            return False
    return False

def construct_AM_sql(UR):
    parts = []
    for col, vals in UR.items():
        if not vals:
            continue
        in_list = ", ".join(_sql_literal(v) for v in vals)
        col_q = _sql_ident(col)
        parts.append(f"(CAST({col_q} AS VARCHAR) IN ({in_list}))")
    return " OR ".join(parts) if parts else "FALSE"


from typing import Dict, List, Any
import pandas as pd


def reformualte_sql(
    T: pd.DataFrame,
    UR: Dict[str, List[Any]],
    mode: str,
    method: str
) -> str:
    """
    Reformulated OR-over-columns SQL.

    tvd-exi:
      remove (col,value) if it appears in at least one CLEAN row.

    tvd-uni (exact):
      remove (col,value) if it appears in all required Cartesian
      combinations, i.e., exactly Π_{d≠col} |UR[d]| times in CLEAN rows.
    """

    # --- trivial cases ---
    if T is None or T.empty:
        if method == "AM":
            return construct_AM_sql(UR)
        else:
            return construct_TM_sql(UR)

    cols = list(UR.keys())

    # --- 1. keep only CLEAN rows ---
    clean_mask = pd.Series(True, index=T.index)
    for c in cols:
        if c not in T.columns:
            clean_mask &= False
        else:
            clean_mask &= T[c].isin(UR[c])

    clean = T.loc[clean_mask, cols]

    if clean.empty:
        if method == "AM":
            return construct_AM_sql(UR)
        else:
            return construct_TM_sql(UR)

    # --- 2. deduplicate clean rows ---
    covered = set(map(tuple, clean.itertuples(index=False, name=None)))

    # --- 3. count occurrences per (col=value) ---
    counts = {c: {v: 0 for v in UR[c]} for c in cols}

    for tup in covered:
        for i, c in enumerate(cols):
            counts[c][tup[i]] += 1

    # =========================
    # tvd-exi: value seen once is enough
    # =========================
    if mode == "tvd-exi":
        reduced_UR = {}
        for c in cols:
            remaining = [v for v in UR[c] if counts[c][v] == 0]
            if remaining:
                reduced_UR[c] = remaining

        if reduced_UR:
            if method == "AM":
                return construct_AM_sql(reduced_UR)
            else:
                return construct_TM_sql(reduced_UR)
        else:
            return "FALSE"
  

    # =========================
    # tvd-uni (exact): value must appear x times
    # =========================
    # x(c) = product of sizes of other columns
    required = {}
    for c in cols:
        x = 1
        for d in cols:
            if d != c:
                x *= len(UR[d])
        required[c] = x
    reduced_UR = {}
    for c in cols:
        remaining = [
            v for v in UR[c]
            if counts[c][v] < required[c]
        ]
        if remaining:
            reduced_UR[c] = remaining


    if reduced_UR:            
        if method == "AM":
            return construct_AM_sql(reduced_UR)
        else:
            return construct_TM_sql(reduced_UR)
    else:
        return "FALSE"







def construct_TM_sql(UR):
    parts = []
    for col, vals in UR.items():
        if not vals:
            continue
        in_list = ", ".join(_sql_literal(v) for v in vals)
        col_q = _sql_ident(col)
        parts.append(f"(CAST({col_q} AS VARCHAR) IN ({in_list}))")
    return " AND ".join(parts) if parts else "FALSE"





# ------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------
def _sql_literal(x: Any) -> str:
    if x is None:
        return "NULL"
    if isinstance(x, bool):
        return "TRUE" if x else "FALSE"

    # IMPORTANT: quote numbers too, to keep IN-lists homogeneous (VARCHAR)
    if isinstance(x, (int, float)):
        return f"'{x}'"

    s = str(x).replace("'", "''")
    return f"'{s}'"


