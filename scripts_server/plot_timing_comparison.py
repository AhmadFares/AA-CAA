"""
Timing comparison across variants (Random, Stats Guided, LLM Guided, LLM Adaptive, All Source).

Stacked bar: shipping time + processing time, per variant.
One subplot per dataset × mode (3 datasets × 2 modes = 6 panels).

Run from ~/TVD:
  .venv/bin/python3 scripts_server/plot_timing_comparison.py
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D
import matplotlib.patches as mpatches
import seaborn as sns
from pathlib import Path

sns.set_theme(style="whitegrid", font_scale=1.1)

OUTDIR = Path("results/paper_plots/llm_results")
OUTDIR.mkdir(exist_ok=True)

# ── load all auto-UR summary files ────────────────────────────────────────────
CONFIGS = [
    # (summary_files,                       dataset,    split,        mode,    ur_lo, ur_hi)
    (["results/aa_auto_geo_v2/summary.csv",
      "results/aa_auto_geo_v3/summary.csv"],   "MovieLens", "geo",        "tvd-aa", 201, 240),
    (["results/caa_auto_geo_v3/summary.csv"],  "MovieLens", "geo",        "tvd-caa",201, 240),
    (["results/aa_auto_cordis_v2/summary.csv",
      "results/aa_auto_cordis_v3/summary.csv"],"CORDIS",   "candidates", "tvd-aa", 301, 340),
    (["results/caa_auto_cordis_v3/summary.csv"],"CORDIS",  "candidates", "tvd-caa",301, 340),
    (["results/aa_auto_mimic_v3/summary.csv"], "MIMIC-IV", "admissions", "tvd-aa", 401, 440),
    (["results/caa_auto_mimic_v3/summary.csv"],"MIMIC-IV", "admissions", "tvd-caa",401, 440),
]

VARIANTS = ["Stats Guided", "LLM Guided", "LLM Adaptive", "Random", "All Source"]
LABELS   = ["TVD", "LLM\nGuided", "LLM\nAdaptive", "Random", "All\nSource"]

COLOR_SHIP = "#5b9bd5"   # blue — shipping (I/O)
COLOR_PROC = "#ed7d31"   # orange — processing (compute / LLM)

PANEL_TITLES = [
    "MovieLens\n(AA)",  "MovieLens\n(CAA)",
    "CORDIS\n(AA)",     "CORDIS\n(CAA)",
    "MIMIC-IV\n(AA)",   "MIMIC-IV\n(CAA)",
]

fig, axes = plt.subplots(2, 3, figsize=(14, 8), sharey=False)

for ax, (files, dataset, split, mode, lo, hi), title in zip(
        axes.flatten(), CONFIGS, PANEL_TITLES):

    frames = []
    for f in files:
        try: frames.append(pd.read_csv(f))
        except FileNotFoundError: pass
    if not frames:
        ax.set_title(title); ax.set_visible(False); continue

    df = pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["mode","UR_id","dataset","split","variant","theta"])
    df = df[(df["mode"] == mode) & (df["split"] == split) &
            df["UR_id"].between(lo, hi)]

    grp    = df.groupby("variant")[["shipping_time_total","processing_time_total"]]
    means  = grp.mean()

    x      = np.arange(len(VARIANTS))
    ship   = [means.loc[v, "shipping_time_total"]   if v in means.index else 0 for v in VARIANTS]
    proc   = [means.loc[v, "processing_time_total"] if v in means.index else 0 for v in VARIANTS]
    totals = [s + p for s, p in zip(ship, proc)]

    # geometric std: symmetric in log space, always positive
    def gstd_factor(variant):
        vals = df[df["variant"] == variant]["shipping_time_total"] + \
               df[df["variant"] == variant]["processing_time_total"]
        vals = vals[vals > 0]
        if len(vals) < 2:
            return 1.0
        return float(np.exp(np.log(vals).std(ddof=1)))

    gstd = [gstd_factor(v) for v in VARIANTS]
    err_lo = [t - t / g if g > 1 else 0 for t, g in zip(totals, gstd)]
    err_hi = [t * g - t if g > 1 else 0 for t, g in zip(totals, gstd)]

    bars_s = ax.bar(x, ship, color=COLOR_SHIP, label="Shipping (I/O)")
    bars_p = ax.bar(x, proc, bottom=ship, color=COLOR_PROC, label="Processing (compute)")

    # error bars on total (asymmetric, geometric std)
    ax.errorbar(x, totals, yerr=[err_lo, err_hi], fmt="none", color="black",
                capsize=4, capthick=1.2, elinewidth=1.2, zorder=5)


    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(LABELS, fontsize=9, fontweight="bold")
    ax.set_ylabel("Time (seconds, log scale)", fontsize=13)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda y, _: f"{y:.0f}s" if y >= 1 else f"{y:.2f}s"))

legend_handles = [
    mpatches.Patch(color=COLOR_SHIP, label="Shipping (I/O)"),
    mpatches.Patch(color=COLOR_PROC, label="Processing (compute / LLM)"),
]
fig.legend(handles=legend_handles, loc="lower center", ncol=2, fontsize=13,
           bbox_to_anchor=(0.5, -0.03), frameon=True)

plt.tight_layout()
out = OUTDIR / "timing_comparison.pdf"
plt.savefig(out, dpi=150, bbox_inches="tight")
plt.savefig(str(out).replace(".pdf", ".png"), dpi=150, bbox_inches="tight")
print(f"Saved {out}")
plt.close()
