"""
AM-only evolution plots for nopen experiments.
Layout per figure: 5 rows (URs 19-23) × 2 cols (ucoverage | penalty)
Left col  : u-coverage evolution
Right col : penalty evolution
Random = mean ± min/max band (15 seeds), Stats Guided = single line
6 figures: 2 splits × 3 thetas
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D

STEPS_CSV = "data/experiment_results/nopen/steps.csv"
OUT_DIR   = "Analysis"

df    = pd.read_csv(STEPS_CSV)
real  = df[df["source_selected"] != "__PRUNE__"].copy()
prune = df[df["source_selected"] == "__PRUNE__"].copy()

UR_IDS = [19, 20, 21, 22, 23]

SPLIT_MAP = {
    "random":      {ur: ("random_20"      if ur <= 20 else "candidates")  for ur in UR_IDS},
    "low_penalty": {ur: ("low_penalty_20" if ur <= 20 else "low_penalty") for ur in UR_IDS},
}

C_RANDOM = "#d6604d"
C_SG     = "#2166ac"


def plot_metric(ax, sub, sub_prune, metric, theta_line, color_r, color_sg, ylabel, show_prune=True):
    # ── Random ───────────────────────────────────────────────────────────────
    rand = sub[sub["variant"] == "Random"]
    if not rand.empty:
        agg = (
            rand.groupby("step")[metric]
            .agg(["mean", "std", "min", "max"])
            .reset_index()
            .sort_values("step")
        )
        agg["std"] = agg["std"].fillna(0)
        origin = pd.DataFrame({"step": [0], "mean": [0.0], "std": [0.0], "min": [0.0], "max": [0.0]})
        agg = pd.concat([origin, agg], ignore_index=True)
        shade = agg[agg["mean"] > 0]
        if not shade.empty:
            ax.fill_between(shade["step"], shade["min"], shade["max"],
                            color=color_r, alpha=0.15)
        ax.plot(agg["step"], agg["mean"],
                color=color_r, linewidth=2.2, linestyle="--",
                marker="o", markersize=5, label="Random (mean)")

        # Prune point for Random — single star at mean position across all seeds
        rand_prune = sub_prune[sub_prune["variant"] == "Random"]
        if show_prune and not rand_prune.empty:
            star_x = rand_prune["step"].mean()
            star_y = rand_prune[metric].mean()
            ax.scatter([star_x], [star_y],
                       color=color_r, s=180, marker="*", zorder=7,
                       edgecolors="black", linewidths=0.5)

    # ── Stats Guided ─────────────────────────────────────────────────────────
    sg = sub[sub["variant"] == "Stats Guided"].sort_values("step")
    sg_prune = sub_prune[sub_prune["variant"] == "Stats Guided"]
    if sg.empty:
        ax.scatter([0], [0], color=color_sg, s=100, marker="X", zorder=6,
                   label="Stats Guided (pruned at step 0)")
    else:
        sg_steps = [0] + list(sg["step"])
        sg_vals  = [0.0] + list(sg[metric])
        ax.plot(sg_steps, sg_vals,
                color=color_sg, linewidth=2.2, linestyle="-",
                marker="o", markersize=3, label="Stats Guided")

        # Prune point for Stats Guided
        if show_prune and not sg_prune.empty:
            ax.scatter(sg_prune["step"], sg_prune[metric],
                       color=color_sg, s=180, marker="*", zorder=7,
                       edgecolors="black", linewidths=0.5)

    if theta_line is not None:
        ax.axhline(theta_line, color="grey", linestyle=":", linewidth=1, alpha=0.6)

    ax.set_ylim(-0.05, 1.12)
    ax.set_ylabel(ylabel, fontsize=8)
    ax.set_xlabel("Step (source added)", fontsize=8)
    ax.tick_params(labelsize=7)
    ax.grid(axis="y", alpha=0.25)
    all_steps = list(sub["step"]) + list(sub_prune["step"])
    max_step = int(max(all_steps)) if all_steps else 1
    ax.set_xlim(0, max_step + 1)
    ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True))


def make_figure(split_key, theta):
    split_map = SPLIT_MAP[split_key]

    fig, axes = plt.subplots(
        len(UR_IDS), 2,
        figsize=(12, 4.0 * len(UR_IDS)),
        constrained_layout=True,
    )
    fig.suptitle(
        f"AM Evolution — TVD-AA, θ={theta} | Split: {split_key}\n"
        "Blue = Stats Guided | Red dashed+band = Random (mean, min/max, 15 seeds)",
        fontsize=11, fontweight="bold",
    )

    for row, ur_id in enumerate(UR_IDS):
        split   = split_map[ur_id]
        dataset = "ML" if ur_id <= 20 else "TUS"

        sub = real[
            (real["UR_id"]  == ur_id) &
            (real["method"] == "AM") &
            (real["split"]  == split) &
            (real["theta"]  == theta)
        ]
        sub_p = prune[
            (prune["UR_id"]  == ur_id) &
            (prune["method"] == "AM") &
            (prune["split"]  == split) &
            (prune["theta"]  == theta)
        ]

        ax_cov = axes[row][0]
        ax_pen = axes[row][1]

        title = f"UR{ur_id} ({dataset})  [split: {split}]"
        ax_cov.set_title(title, fontsize=8.5, fontweight="bold")
        ax_pen.set_title(title, fontsize=8.5, fontweight="bold")

        plot_metric(ax_cov, sub, sub_p, "ucoverage_current", theta,
                    C_RANDOM, C_SG, "u-coverage", show_prune=False)
        plot_metric(ax_pen, sub, sub_p, "penalty_current",   None,
                    C_RANDOM, C_SG, "penalty", show_prune=True)

    # Column headers
    axes[0][0].set_title("u-coverage\n" + axes[0][0].get_title(),
                          fontsize=8.5, fontweight="bold")
    axes[0][1].set_title("penalty\n" + axes[0][1].get_title(),
                          fontsize=8.5, fontweight="bold")

    legend_elements = [
        Line2D([0], [0], color=C_RANDOM, lw=2, linestyle="--", label="Random — mean"),
        mpatches.Patch(color=C_RANDOM, alpha=0.3,              label="Random — min/max range"),
        Line2D([0], [0], color=C_SG,    lw=2, linestyle="-",  label="Stats Guided"),
        Line2D([0], [0], color="grey",  lw=1, linestyle=":",  label=f"θ = {theta}"),
        Line2D([0], [0], color="black", lw=0, marker="*", markersize=8, label="After pruning"),
    ]
    fig.legend(handles=legend_elements, loc="lower center", ncol=4,
               fontsize=9, bbox_to_anchor=(0.5, -0.015), framealpha=0.9)

    theta_str = str(theta).replace(".", "_")
    out = os.path.join(OUT_DIR, f"nopen_{split_key}_theta{theta_str}.png")
    plt.savefig(out, bbox_inches="tight", dpi=150)
    print(f"Saved: {out}")
    plt.close()


for split_key in ["random", "low_penalty"]:
    for theta in [0.6, 0.8, 1.0]:
        make_figure(split_key, theta)
