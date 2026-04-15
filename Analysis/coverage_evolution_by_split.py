"""
Two separate 12-panel figures (one per split type):
  Image 1: random split  (random_20 for ML, candidates for TUS)
  Image 2: low_penalty   (low_penalty_20 for ML, low_penalty for TUS)

Layout each: 6 rows (URs 18-23) × 2 cols (AM, TM)
Each subplot: Random mean ± std band + Stats Guided line
"""

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D

STEPS_CSV = "data/experiment_results/multi_ur_seeds/steps.csv"
OUT_DIR   = "Analysis"

df   = pd.read_csv(STEPS_CSV)
real = df[df["source_selected"] != "__PRUNE__"].copy()

UR_IDS  = [19, 20, 21, 22, 23]
METHODS = ["AM", "TM"]
THETA   = 0.6

# Split to use per figure per UR
SPLIT_MAP = {
    "random":      {ur: ("random_20"      if ur <= 20 else "candidates")   for ur in UR_IDS},
    "low_penalty": {ur: ("low_penalty_20" if ur <= 20 else "low_penalty")  for ur in UR_IDS},
}

FIGURE_TITLES = {
    "random":      "Coverage Evolution — TVD-AA, θ=0.6 | Split: random_20 (ML) / candidates (TUS)",
    "low_penalty": "Coverage Evolution — TVD-AA, θ=0.6 | Split: low_penalty_20 (ML) / low_penalty (TUS)",
}

C_RANDOM = "#d6604d"
C_SG     = "#2166ac"


def make_figure(split_key, theta):
    split_map = SPLIT_MAP[split_key]

    fig, axes = plt.subplots(
        len(UR_IDS), len(METHODS),
        figsize=(12, 4.0 * len(UR_IDS)),
        constrained_layout=True,
    )
    fig.suptitle(
        FIGURE_TITLES[split_key] + f" | θ = {theta}\n"
        "Blue solid = Stats Guided | Red dashed+band = Random (mean ± std, 15 seeds)",
        fontsize=11, fontweight="bold",
    )

    for row, ur_id in enumerate(UR_IDS):
        split     = split_map[ur_id]
        dataset   = "ML" if ur_id <= 20 else "TUS"

        for col, method in enumerate(METHODS):
            ax  = axes[row][col]
            sub = real[
                (real["UR_id"] == ur_id) &
                (real["method"] == method) &
                (real["split"]  == split) &
                (real["theta"]  == theta)
            ]

            # ── Random ───────────────────────────────────────────────────────
            rand = sub[sub["variant"] == "Random"]
            if not rand.empty:
                agg = (
                    rand.groupby("step")["ucoverage_current"]
                    .agg(["mean", "std", "min", "max"])
                    .reset_index()
                    .sort_values("step")
                )
                agg["std"] = agg["std"].fillna(0)
                # Prepend (0, 0) origin
                origin = pd.DataFrame({"step": [0], "mean": [0.0], "std": [0.0], "min": [0.0], "max": [0.0]})
                agg = pd.concat([origin, agg], ignore_index=True)
                # Only shade where mean > 0 to avoid false shading on flat-zero lines
                # Use actual seed min/max as band bounds — avoids clipping artefacts
                shade = agg[agg["mean"] > 0]
                if not shade.empty:
                    ax.fill_between(
                        shade["step"],
                        shade["min"],
                        shade["max"],
                        color=C_RANDOM, alpha=0.15,
                    )
                ax.plot(
                    agg["step"], agg["mean"],
                    color=C_RANDOM, linewidth=2.2, linestyle="--",
                    marker="o", markersize=5,
                    label="Random (mean)",
                )
                # If only 1 step, add annotation so it's visible
                if len(agg) == 1:
                    ax.annotate(
                        f"All seeds done\nin 1 step\n(cov={agg['mean'].iloc[0]:.2f})",
                        xy=(agg["step"].iloc[0], agg["mean"].iloc[0]),
                        xytext=(agg["step"].iloc[0] + 0.3, agg["mean"].iloc[0] - 0.2),
                        arrowprops=dict(arrowstyle="->", color=C_RANDOM, lw=0.8),
                        color=C_RANDOM, fontsize=7,
                    )

            # ── Stats Guided ─────────────────────────────────────────────────
            sg = sub[sub["variant"] == "Stats Guided"].sort_values("step")
            if sg.empty:
                # pruned at 0 — just mark with X at origin, no text
                ax.scatter([0], [0], color=C_SG, s=100, marker="X", zorder=6,
                           label="Stats Guided (pruned at step 0)")
            else:
                sg_steps = [0] + list(sg["step"])
                sg_cov   = [0.0] + list(sg["ucoverage_current"])
                ax.plot(
                    sg_steps, sg_cov,
                    color=C_SG, linewidth=2.2, linestyle="-",
                    marker="o", markersize=3,
                    label="Stats Guided",
                )

            # Theta
            ax.axhline(theta, color="grey", linestyle=":", linewidth=1, alpha=0.6)

            n_src = int(sub["step"].max()) if not sub.empty else "?"
            ax.set_title(f"UR{ur_id} ({dataset}) — {method}  [split: {split}]",
                         fontsize=8.5, fontweight="bold")
            ax.set_xlabel("Step (source added)", fontsize=8)
            ax.set_ylabel("u-coverage", fontsize=8)
            ax.set_ylim(-0.05, 1.12)
            ax.tick_params(labelsize=7)
            ax.grid(axis="y", alpha=0.25)
            # Force integer x-axis ticks
            max_step = int(sub["step"].max()) if not sub.empty else 1
            ax.set_xlim(0, max_step + 1)
            ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True))

    # Shared legend
    legend_elements = [
        Line2D([0], [0], color=C_RANDOM, lw=2, linestyle="--", label="Random — mean"),
        mpatches.Patch(color=C_RANDOM, alpha=0.3,               label="Random — min/max range"),
        Line2D([0], [0], color=C_SG,     lw=2, linestyle="-",  label="Stats Guided"),
        Line2D([0], [0], color="grey",   lw=1, linestyle=":",  label=f"θ = {theta}"),
    ]
    fig.legend(
        handles=legend_elements,
        loc="lower center", ncol=4, fontsize=9,
        bbox_to_anchor=(0.5, -0.015), framealpha=0.9,
    )

    theta_str = str(theta).replace(".", "_")
    out_png = os.path.join(OUT_DIR, f"coverage_evolution_{split_key}_theta{theta_str}.png")
    plt.savefig(out_png, bbox_inches="tight", dpi=150)
    print(f"Saved: {out_png}")
    plt.close()


for split_key in ["random", "low_penalty"]:
    for theta in [0.6, 0.8, 1.0]:
        make_figure(split_key, theta)
