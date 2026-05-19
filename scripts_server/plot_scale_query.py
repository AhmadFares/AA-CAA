"""
Scalability plot — query complexity (number of requested values).

Uses existing TVD-AA results on MovieLens geo (60 sources), θ=0.8.
Groups URs 201-240 by total number of requested values and plots
runtime and coverage vs. query complexity.

X-axis: total number of requested values in the query
Y-axis (top): total time (seconds)
Y-axis (bottom): AA-Coverage after stopping

Run from repo root:
  .venv/bin/python3 scripts_server/plot_scale_query.py
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.lines import Line2D
from pathlib import Path
import sys, os

sys.path.insert(0, os.path.abspath("."))

sns.set_theme(style="whitegrid", font_scale=1.3)

OUTDIR = Path("results/paper_plots")
OUTDIR.mkdir(exist_ok=True)

THETA = 0.8
MODE  = "tvd-aa"
SPLIT = "geo"

SUMMARY_FILES = [
    "results/aa_auto_geo_v2/summary.csv",
    "results/aa_auto_geo_v3/summary.csv",
]

VARIANTS = ["Stats Guided", "Random", "All Source"]
COLORS   = {"Stats Guided": "#4C72B0", "Random": "#DD8452", "All Source": "#2ca02c"}
LABELS   = {"Stats Guided": "TVD", "Random": "Random", "All Source": "All Source"}

# ── Load UR complexity from TestCases ─────────────────────────────────────────

from helpers.test_cases import TestCases
tc = TestCases()

ur_complexity = {}
for ur_id in range(201, 241):
    if ur_id in tc.cases:
        ur_df = tc.cases[ur_id]
        n_vals = sum(ur_df[c].dropna().nunique() for c in ur_df.columns)
        ur_complexity[ur_id] = n_vals

print(f"Loaded complexity for {len(ur_complexity)} URs: "
      f"{min(ur_complexity.values())}–{max(ur_complexity.values())} values")

# ── Load summary results ───────────────────────────────────────────────────────

frames = []
for f in SUMMARY_FILES:
    try:
        frames.append(pd.read_csv(f))
    except FileNotFoundError:
        print(f"[WARN] Missing: {f}")

if not frames:
    print("No summary files found. Run run_all_experiments.sh first.")
    exit(0)

df = pd.concat(frames, ignore_index=True).drop_duplicates(
    subset=["mode", "UR_id", "split", "variant", "theta"])
df = df[(df["mode"] == MODE) & (df["split"] == SPLIT) &
        (df["theta"] == THETA) & df["UR_id"].between(201, 240)]

df["n_vals"]  = df["UR_id"].map(ur_complexity)
df["time"]    = df["shipping_time_total"] + df["processing_time_total"]
df["coverage"] = df["ucoverage_final"]

print(f"Loaded {len(df)} rows across {df['UR_id'].nunique()} URs")

# ── Plot: scatter with trend line ─────────────────────────────────────────────

fig, (ax_time, ax_cov) = plt.subplots(2, 1, figsize=(7, 8), sharex=True)

x_all = sorted(df["n_vals"].unique())

for variant in VARIANTS:
    sub   = df[df["variant"] == variant].copy()
    color = COLORS[variant]
    label = LABELS[variant]

    if sub.empty:
        continue

    # scatter points
    ax_time.scatter(sub["n_vals"], sub["time"], color=color, alpha=0.4, s=20)
    ax_cov.scatter( sub["n_vals"], sub["coverage"], color=color, alpha=0.4, s=20)

    # mean per n_vals
    grp = sub.groupby("n_vals").agg(
        time_mean=("time",     "mean"),
        cov_mean= ("coverage", "mean"),
    ).reset_index()

    ax_time.plot(grp["n_vals"], grp["time_mean"], marker="o", color=color,
                 lw=2, label=label)
    ax_cov.plot( grp["n_vals"], grp["cov_mean"],  marker="o", color=color,
                 lw=2, label=label)

ax_time.set_ylabel("Total time (seconds)", fontsize=13)
ax_time.set_yscale("log")
ax_time.yaxis.set_major_formatter(
    plt.FuncFormatter(lambda y, _: f"{y:.0f}s" if y >= 1 else f"{y:.2f}s"))
ax_time.annotate("↓ better", xy=(0.98, 0.97), xycoords="axes fraction",
                 ha="right", va="top", fontsize=11, color="dimgray", style="italic")

ax_cov.set_ylabel("AA-Coverage (avg. over 40 queries)", fontsize=13)
ax_cov.set_ylim(-0.03, 1.03)
ax_cov.axhline(THETA, color="gray", lw=1, ls=":", alpha=0.7, label=f"θ={THETA}")
ax_cov.annotate("↑ better", xy=(0.98, 0.03), xycoords="axes fraction",
                ha="right", fontsize=11, color="dimgray", style="italic")

ax_cov.set_xlabel("Number of requested values in query", fontsize=13)
ax_cov.set_xticks(x_all)

handles = [Line2D([0],[0], color=COLORS[v], lw=2, marker="o", label=LABELS[v])
           for v in VARIANTS if v in df["variant"].values]
handles.append(Line2D([0],[0], color="gray", lw=1, ls=":", label=f"θ={THETA}"))
fig.legend(handles=handles, loc="lower center", ncol=4, fontsize=11,
           bbox_to_anchor=(0.5, -0.02), frameon=True)

plt.suptitle("Scalability: Query Complexity (MovieLens, TVD-AA, 60 sources)",
             fontsize=13, fontweight="bold")
plt.tight_layout()

for ext in ("pdf", "png"):
    out = OUTDIR / f"scale_query.{ext}"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Saved {out}")
plt.close()
