"""
Scalability plot — number of sources.

Shows how TVD, Random, and AllSources scale as the number of sources grows
(10 → 60 → 100 → 1000) on MovieLens geo, TVD-AA mode, θ=0.8.

X-axis: number of sources
Y-axis (top): total time (seconds)
Y-axis (bottom): AA-Coverage after stopping

Run from repo root:
  .venv/bin/python3 scripts_server/plot_scale_sources.py
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.lines import Line2D
from pathlib import Path

sns.set_theme(style="whitegrid", font_scale=1.3)

OUTDIR = Path("results/paper_plots")
OUTDIR.mkdir(exist_ok=True)

THETA   = 0.8
MODE    = "tvd-aa"
SPLIT   = "geo"

VARIANTS = ["Stats Guided", "Random", "All Source"]
COLORS   = {"Stats Guided": "#4C72B0", "Random": "#DD8452", "All Source": "#2ca02c"}
LABELS   = {"Stats Guided": "TVD", "Random": "Random", "All Source": "All Source"}

# (n_sources, summary_file, split_name)
CONFIGS = [
    (10,   "results/scale_geo10/summary.csv",   "geo_10"),
    (60,   "results/aa_auto_geo_v2/summary.csv", "geo"),    # reuse existing results
    (100,  "results/scale_geo100/summary.csv",  "geo_100"),
    (1082, "results/scale_geo1000/summary.csv", "geo_1000"),
]

# ── Load data ─────────────────────────────────────────────────────────────────

records = []
for n_src, fpath, split_name in CONFIGS:
    try:
        df = pd.read_csv(fpath)
    except FileNotFoundError:
        print(f"[WARN] Missing: {fpath}")
        continue
    df = df[(df["mode"] == MODE) & (df["split"] == split_name) &
            (df["theta"] == THETA) & df["UR_id"].between(201, 240)]
    if df.empty:
        # fallback: try any split in the file
        df2 = pd.read_csv(fpath)
        df2 = df2[(df2["mode"] == MODE) & (df2["theta"] == THETA) &
                  df2["UR_id"].between(201, 240)]
        df = df2
    for _, row in df.iterrows():
        records.append({
            "n_sources": n_src,
            "variant":   row["variant"],
            "time":      row["shipping_time_total"] + row["processing_time_total"],
            "coverage":  row.get("ucoverage_final", np.nan),
        })

data = pd.DataFrame(records)

if data.empty:
    print("No data found. Run run_scale_experiments.sh first.")
    exit(0)

# ── Aggregate: mean ± std per (n_sources, variant) ───────────────────────────

agg = data.groupby(["n_sources", "variant"]).agg(
    time_mean=("time",     "mean"),
    time_std= ("time",     "std"),
    cov_mean= ("coverage", "mean"),
    cov_std=  ("coverage", "std"),
).reset_index()

x_vals = sorted(data["n_sources"].unique())

# ── Plot ──────────────────────────────────────────────────────────────────────

fig, (ax_time, ax_cov) = plt.subplots(2, 1, figsize=(7, 8), sharex=True)

for variant in VARIANTS:
    sub   = agg[agg["variant"] == variant].sort_values("n_sources")
    color = COLORS[variant]
    label = LABELS[variant]

    if sub.empty:
        continue

    ax_time.plot(sub["n_sources"], sub["time_mean"], marker="o", color=color,
                 lw=2, label=label)
    ax_time.fill_between(sub["n_sources"],
                         sub["time_mean"] - sub["time_std"],
                         sub["time_mean"] + sub["time_std"],
                         color=color, alpha=0.15)

    ax_cov.plot(sub["n_sources"], sub["cov_mean"], marker="o", color=color,
                lw=2, label=label)
    ax_cov.fill_between(sub["n_sources"],
                        sub["cov_mean"] - sub["cov_std"],
                        sub["cov_mean"] + sub["cov_std"],
                        color=color, alpha=0.15)

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

ax_cov.set_xlabel("Number of sources", fontsize=13)
ax_cov.set_xscale("log")
ax_cov.set_xticks(x_vals)
ax_cov.set_xticklabels([str(x) for x in x_vals])

handles = [Line2D([0],[0], color=COLORS[v], lw=2, marker="o", label=LABELS[v])
           for v in VARIANTS if v in agg["variant"].values]
handles.append(Line2D([0],[0], color="gray", lw=1, ls=":", label=f"θ={THETA}"))
fig.legend(handles=handles, loc="lower center", ncol=4, fontsize=11,
           bbox_to_anchor=(0.5, -0.02), frameon=True)

plt.suptitle("Scalability: Number of Sources (MovieLens, TVD-AA)", fontsize=13, fontweight="bold")
plt.tight_layout()

for ext in ("pdf", "png"):
    out = OUTDIR / f"scale_sources.{ext}"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Saved {out}")
plt.close()
